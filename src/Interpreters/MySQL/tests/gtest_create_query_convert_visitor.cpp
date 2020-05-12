#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/MySQL/CreateQueryConvertVisitor.h>

using namespace DB;
using namespace MySQLParser;
using namespace MySQLVisitor;

static ContextShared * contextShared()
{
    static SharedContextHolder shared = Context::createShared();
    return shared.get();
}

static String convert(const String & input)
{
    ParserCreateQuery p_create_query;
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{
        .out = out, .context = Context::createGlobal(contextShared()), .max_ranges = 1000, .min_rows_pre_range = 1000000};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);
    return out.str();
}

TEST(CreateQueryConvert, TestConvertNumberColumnsType)
{
    EXPECT_EQ(
        convert("CREATE TABLE test(a tinyint, b SMALLINT, c MEDIUMINT, d INT, e INTEGER, f BIGINT, g DECIMAL, h DEC, i NUMERIC, j FIXED, k "
                "FLOAT, l DOUBLE, m DOUBLE PRECISION, n REAL)"),
        "CREATE TABLE test(`a` Nullable(Int8), `b` Nullable(Int16), `c` Nullable(Int32), `d` Nullable(Int32), `e` Nullable(Int32), `f` "
        "Nullable(Int64), `g` Nullable(Decimal(10, 0)), `h` Nullable(Decimal(10, 0)), `i` Nullable(Decimal(10, 0)), `j` "
        "Nullable(Decimal(10, 0)), `k` Nullable(Float32), `l` Nullable(Float64), `m` Nullable(Float64), `n` Nullable(Float64)) ENGINE = "
        "MergeTree() PARTITION BY tuple() ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a tinyint(1), b SMALLINT(1), c MEDIUMINT(1), d INT(1), e INTEGER(1), f BIGINT(1), g DECIMAL(1), h "
                "DEC(2, 1), i NUMERIC(4, 3), j FIXED(6, 5), k FLOAT(1), l DOUBLE(1, 2), m DOUBLE PRECISION(3, 4), n REAL(5, 6))"),
        "CREATE TABLE test(`a` Nullable(Int8), `b` Nullable(Int16), `c` Nullable(Int32), `d` Nullable(Int32), `e` Nullable(Int32), `f` "
        "Nullable(Int64), `g` Nullable(Decimal(1, 0)), `h` Nullable(Decimal(2, 1)), `i` Nullable(Decimal(4, 3)), `j` Nullable(Decimal(6, "
        "5)), `k` Nullable(Float32), `l` Nullable(Float64), `m` Nullable(Float64), `n` Nullable(Float64)) ENGINE = MergeTree() PARTITION "
        "BY tuple() ORDER BY tuple()");

    /// UNSIGNED
    EXPECT_EQ(
        convert("CREATE TABLE test(a tinyint UNSIGNED, b SMALLINT(1) UNSIGNED, c MEDIUMINT(1) UNSIGNED, d INT(1) UNSIGNED, e INTEGER(1), f "
        "BIGINT(1) UNSIGNED, g DECIMAL(1) UNSIGNED, h DEC(2, 1) UNSIGNED, i NUMERIC(4, 3) UNSIGNED, j FIXED(6, 5) UNSIGNED, k FLOAT(1) "
        "UNSIGNED, l DOUBLE(1, 2) UNSIGNED, m DOUBLE PRECISION(3, 4) UNSIGNED, n REAL(5, 6) UNSIGNED)"),
        "CREATE TABLE test(`a` Nullable(UInt8), `b` Nullable(UInt16), `c` Nullable(UInt32), `d` Nullable(UInt32), `e` Nullable(Int32), `f` "
        "Nullable(UInt64), `g` Nullable(Decimal(1, 0)), `h` Nullable(Decimal(2, 1)), `i` Nullable(Decimal(4, 3)), `j` Nullable(Decimal(6, "
        "5)), `k` Nullable(Float32), `l` Nullable(Float64), `m` Nullable(Float64), `n` Nullable(Float64)) ENGINE = MergeTree() PARTITION "
        "BY tuple() ORDER BY tuple()");

    /// NOT NULL
    EXPECT_EQ(
        convert("CREATE TABLE test(a tinyint NOT NULL, b SMALLINT(1) NOT NULL, c MEDIUMINT(1) NOT NULL, d INT(1) NOT NULL, e INTEGER(1), f "
        "BIGINT(1) NOT NULL, g DECIMAL(1) NOT NULL, h DEC(2, 1) NOT NULL, i NUMERIC(4, 3) NOT NULL, j FIXED(6, 5) NOT NULL, k FLOAT(1) NOT "
        "NULL, l DOUBLE(1, 2) NOT NULL, m DOUBLE PRECISION(3, 4) NOT NULL, n REAL(5, 6) NOT NULL)"),
        "CREATE TABLE test(`a` Int8, `b` Int16, `c` Int32, `d` Int32, `e` Nullable(Int32), `f` Int64, `g` Decimal(1, 0), `h` Decimal(2, "
        "1), `i` Decimal(4, 3), `j` Decimal(6, 5), `k` Float32, `l` Float64, `m` Float64, `n` Float64) ENGINE = MergeTree() PARTITION BY "
        "tuple() ORDER BY tuple()");

    /// ZEROFILL
    EXPECT_EQ(
        convert("CREATE TABLE test(a tinyint ZEROFILL, b SMALLINT(1) ZEROFILL, c MEDIUMINT(1) ZEROFILL, d INT(1) ZEROFILL, e INTEGER(1), f "
        "BIGINT(1) ZEROFILL, g DECIMAL(1) ZEROFILL, h DEC(2, 1) ZEROFILL, i NUMERIC(4, 3) ZEROFILL, j FIXED(6, 5) ZEROFILL, k FLOAT(1) "
        "ZEROFILL, l DOUBLE(1, 2) ZEROFILL, m DOUBLE PRECISION(3, 4) ZEROFILL, n REAL(5, 6) ZEROFILL)"),
        "CREATE TABLE test(`a` Nullable(UInt8), `b` Nullable(UInt16), `c` Nullable(UInt32), `d` Nullable(UInt32), `e` Nullable(Int32), `f` "
        "Nullable(UInt64), `g` Nullable(Decimal(1, 0)), `h` Nullable(Decimal(2, 1)), `i` Nullable(Decimal(4, 3)), `j` Nullable(Decimal(6, "
        "5)), `k` Nullable(Float32), `l` Nullable(Float64), `m` Nullable(Float64), `n` Nullable(Float64)) ENGINE = MergeTree() PARTITION "
        "BY tuple() ORDER BY tuple()");
}

TEST(CreateQueryConvert, TestConvertDateTimesColumnsType)
{
    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE, b DATETIME, c TIMESTAMP, d TIME, e year)"),
        "CREATE TABLE test(`a` Nullable(Date), `b` Nullable(DateTime), `c` Nullable(DateTime), `d` Nullable(DateTime64(3)), `e` "
        "Nullable(Int16)) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE, b DATETIME(1), c TIMESTAMP(1), d TIME(1), e year(4))"),
        "CREATE TABLE test(`a` Nullable(Date), `b` Nullable(DateTime), `c` Nullable(DateTime), `d` Nullable(DateTime64(3)), `e` "
        "Nullable(Int16)) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY tuple()");

    EXPECT_EQ(
        convert(
            "CREATE TABLE test(a DATE NOT NULL, b DATETIME(1) NOT NULL, c TIMESTAMP(1) NOT NULL, d TIME(1) NOT NULL, e year(4) NOT NULL)"),
        "CREATE TABLE test(`a` Date, `b` DateTime, `c` DateTime, `d` DateTime64(3), `e` Int16) ENGINE = MergeTree() PARTITION BY tuple() "
        "ORDER BY tuple()");
}

TEST(CreateQueryConvert, TestConvertParitionOptions)
{
    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE NOT NULL) PARTITION BY HASH a"),
        "CREATE TABLE test(`a` Date) ENGINE = MergeTree() PARTITION BY tuple(a) ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE NOT NULL) PARTITION BY LINEAR HASH a"),
        "CREATE TABLE test(`a` Date) ENGINE = MergeTree() PARTITION BY tuple(a) ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE NOT NULL) PARTITION BY RANGE(a)"),
        "CREATE TABLE test(`a` Date) ENGINE = MergeTree() PARTITION BY tuple(a) ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE NOT NULL, b INT) PARTITION BY RANGE COLUMNS(a, b)"),
        "CREATE TABLE test(`a` Date, `b` Nullable(Int32)) ENGINE = MergeTree() PARTITION BY (a, b) ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE NOT NULL) PARTITION BY LIST(a)"),
        "CREATE TABLE test(`a` Date) ENGINE = MergeTree() PARTITION BY tuple(a) ORDER BY tuple()");

    EXPECT_EQ(
        convert("CREATE TABLE test(a DATE NOT NULL, b INT) PARTITION BY LIST COLUMNS(a, b)"),
        "CREATE TABLE test(`a` Date, `b` Nullable(Int32)) ENGINE = MergeTree() PARTITION BY (a, b) ORDER BY tuple()");
}

TEST(CreateQueryConvert, TestConvertPrimaryToPartitionBy)
{
    EXPECT_EQ(convert("CREATE TABLE test(a DATE NOT NULL PRIMARY KEY)"),
        "CREATE TABLE test(`a` Date) ENGINE = MergeTree() PARTITION BY tuple(toYYYYMM(a)) ORDER BY tuple(a)");

    EXPECT_EQ(convert("CREATE TABLE test(a DATETIME NOT NULL PRIMARY KEY)"),
              "CREATE TABLE test(`a` DateTime) ENGINE = MergeTree() PARTITION BY tuple(toYYYYMM(a)) ORDER BY tuple(a)");

    EXPECT_EQ(convert("CREATE TABLE test(a TINYINT NOT NULL PRIMARY KEY)"),
              "CREATE TABLE test(`a` Int8) ENGINE = MergeTree() PARTITION BY tuple(a) ORDER BY tuple(a)");

    EXPECT_EQ(convert("CREATE TABLE test(a SMALLINT NOT NULL PRIMARY KEY)"),
              "CREATE TABLE test(`a` Int16) ENGINE = MergeTree() PARTITION BY tuple(a) ORDER BY tuple(a)");

    EXPECT_EQ(convert("CREATE TABLE test(a INT NOT NULL PRIMARY KEY)"),
              "CREATE TABLE test(`a` Int32) ENGINE = MergeTree() PARTITION BY tuple(a / 4294968) ORDER BY tuple(a)");

    EXPECT_EQ(convert("CREATE TABLE test(a BIGINT NOT NULL PRIMARY KEY)"),
              "CREATE TABLE test(`a` Int64) ENGINE = MergeTree() PARTITION BY tuple(a / 18446744073709552) ORDER BY tuple(a)");

    EXPECT_EQ(
        convert("CREATE TABLE test(a BIGINT PRIMARY KEY)"),
        "CREATE TABLE test(`a` Nullable(Int64)) ENGINE = MergeTree() PARTITION BY tuple(assumeNotNull(a) / 18446744073709552) ORDER BY "
        "tuple(a)");
}

