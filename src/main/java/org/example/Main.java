package org.example;

import java.sql.*;

public class Main {
    public static void main(String[] args) {
        // Put your connection details here
        String url = "";
        String username = "";
        String password = "";

        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {

            statement.execute("DROP TABLE IF EXISTS testch");
            statement.execute("CREATE TABLE testch (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id");

            statement.executeUpdate("""
                INSERT INTO testch (id, data) VALUES
                (1, '{"FailedToReadValueForColumnData":{"bar":[{"field1":"value1","field2":"value2","field3":"value3","field4":"value4"},{"field1":"value5","field2":"value6","field3":"value7","field4":"value8"}]}}'),
                (2, '{"Works":"bye"}'),
                (3, '{"FailedReadNextRow":{"hi":[{"hi":"byyyyyyyyye"}]}}')
            """);

            int[] ids = {1, 2, 3};
            for (int id : ids) {
                System.out.println("\n==============================");
                System.out.println(" Executing query for id=" + id);
                System.out.println("==============================");

                try (ResultSet rs = statement.executeQuery("SELECT * FROM default.testch WHERE id=" + id)) {
                    while (rs.next()) {
                        System.out.println("Row for id=" + id + " -> " +
                                rs.getLong("id") + " : " + rs.getObject("data"));
                    }
                } catch (SQLException e) {
                    System.out.println("❌ Error while querying id=" + id + ":");
                    printFullStackTrace(e);
                }

                System.out.println("---- End of query id=" + id + " ----\n");
            }

        } catch (SQLException e) {
            System.out.println("Top-level connection error: " + e.getMessage());
        }
    }

    public static void printFullStackTrace(Throwable e) {
        while (e != null) {
            System.out.println("Exception: " + e.getClass().getName() + " - " + e.getMessage());
            for (StackTraceElement ste : e.getStackTrace()) {
                System.out.println("    at " + ste);
            }
            e = e.getCause();
            if (e != null) {
                System.out.println("Caused by:");
            }
        }
    }
}
