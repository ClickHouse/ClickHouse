import com.mysql.cj.MysqlType;

import java.sql.*;

public class PreparedStatementsTest {
    public static void main(String[] args) {
        int i = 0;
        String host = "127.0.0.1";
        String port = "9004";
        String user = "default";
        String password = "";
        String database = "default";
        while (i < args.length) {
            switch (args[i]) {
                case "--host":
                    host = args[++i];
                    break;
                case "--port":
                    port = args[++i];
                    break;
                case "--user":
                    user = args[++i];
                    break;
                case "--password":
                    password = args[++i];
                    break;
                case "--database":
                    database = args[++i];
                    break;
                default:
                    i++;
                    break;
            }
        }

        // useServerPrepStmts uses COM_STMT_PREPARE and COM_STMT_EXECUTE
        // instead of COM_QUERY which allows us to test the binary protocol
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&useServerPrepStmts=true",
                host, port, database);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
            testSimpleDataTypes(conn);
            testStringTypes(conn);
            testLowCardinalityAndNullableTypes(conn);
            testDecimalTypes(conn);
            testMiscTypes(conn);
            testDateTypes(conn);
            testUnusualDateTime64Scales(conn);
            testDateTimeTimezones(conn);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void testSimpleDataTypes(Connection conn) throws SQLException {
        System.out.println("### testSimpleDataTypes");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_simple_data_types").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "i8"), rs.getInt("i8"));
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "i16"), rs.getInt("i16"));
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "i32"), rs.getInt("i32"));
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "i64"), rs.getLong("i64"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "i128"), rs.getString("i128"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "i256"), rs.getString("i256"));
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "ui8"), rs.getInt("ui8"));
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "ui16"), rs.getInt("ui16"));
            System.out.printf("%s, value: %d\n", getMysqlType(rs, "ui32"), rs.getLong("ui32"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "ui64"), rs.getString("ui64"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "ui128"), rs.getString("ui128"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "ui256"), rs.getString("ui256"));
            System.out.printf("%s, value: %f\n", getMysqlType(rs, "f32"), rs.getFloat("f32"));
            System.out.printf("%s, value: %f\n", getMysqlType(rs, "f64"), rs.getFloat("f64"));
            System.out.printf("%s, value: %b\n", getMysqlType(rs, "b"), rs.getBoolean("b"));
        }
        System.out.println();
    }

    private static void testStringTypes(Connection conn) throws SQLException {
        System.out.println("### testStringTypes");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_string_types").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "s"), rs.getString("s"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "sn"), rs.getString("sn"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "lc"), rs.getString("lc"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "nlc"), rs.getString("nlc"));
        }
        System.out.println();
    }

    private static void testLowCardinalityAndNullableTypes(Connection conn) throws SQLException {
        System.out.println("### testLowCardinalityAndNullableTypes");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_low_cardinality_and_nullable_types").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "ilc"), rs.getInt("ilc"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dlc"), rs.getDate("dlc"));
            // NULL int is represented as zero
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "ni"), rs.getInt("ni"));
        }
        System.out.println();
    }

    private static void testDecimalTypes(Connection conn) throws SQLException {
        System.out.println("### testDecimalTypes");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_decimal_types").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d32"), rs.getBigDecimal("d32").toPlainString());
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d64"), rs.getBigDecimal("d64").toPlainString());
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d128_native"),
                    rs.getBigDecimal("d128_native").toPlainString());
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d128_text"), rs.getString("d128_text"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d256"), rs.getString("d256"));
        }
        System.out.println();
    }

    private static void testDateTypes(Connection conn) throws SQLException {
        System.out.println("### testDateTypes");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_date_types").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d"), rs.getDate("d"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "d32"), rs.getDate("d32"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt"), rs.getTimestamp("dt"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_3"), rs.getTimestamp("dt64_3"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_6"), rs.getTimestamp("dt64_6"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_9"), rs.getTimestamp("dt64_9"));
        }
        System.out.println();
    }

    private static void testUnusualDateTime64Scales(Connection conn) throws SQLException {
        System.out.println("### testUnusualDateTime64Scales");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_unusual_datetime64_scales").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_0"), rs.getTimestamp("dt64_0"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_1"), rs.getTimestamp("dt64_1"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_2"), rs.getTimestamp("dt64_2"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_4"), rs.getTimestamp("dt64_4"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_5"), rs.getTimestamp("dt64_5"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_7"), rs.getTimestamp("dt64_7"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_8"), rs.getTimestamp("dt64_8"));
        }
        System.out.println();
    }

    private static void testDateTimeTimezones(Connection conn) throws SQLException {
        System.out.println("### testDateTimeTimezones");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_datetime_timezones").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt"), rs.getTimestamp("dt"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "dt64_3"), rs.getTimestamp("dt64_3"));
        }
        System.out.println();
    }

    private static void testMiscTypes(Connection conn) throws SQLException {
        System.out.println("### testMiscTypes");
        ResultSet rs = conn.prepareStatement("SELECT * FROM ps_misc_types").executeQuery();
        int rowNum = 1;
        while (rs.next()) {
            System.out.printf("Row #%d\n", rowNum++);
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "a"), rs.getString("a"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "u"), rs.getString("u"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "t"), rs.getString("t"));
            System.out.printf("%s, value: %s\n", getMysqlType(rs, "m"), rs.getString("m"));
        }
        System.out.println();
    }

    private static String getMysqlType(ResultSet rs, String columnLabel) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        return String.format("%s type is %s", columnLabel,
                MysqlType.getByJdbcType(meta.getColumnType(rs.findColumn(columnLabel))));
    }

}
