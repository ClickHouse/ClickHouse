#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/MySQL/CreateQueryVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace MySQLParser;
using namespace MySQLVisitor;

static DataTypePtr getType(const String & data_type)
{
    return DataTypeFactory::instance().get(data_type);
}

static ContextShared * contextShared()
{
    static SharedContextHolder shared = Context::createShared();
    return shared.get();
}

static MySQLTableStruct visitQuery(const String & query)
{
    ParserCreateQuery p_create_query;
    ASTPtr ast = parseQuery(p_create_query, query.data(), query.data() + query.size(), "", 0, 0);

    CreateQueryVisitor::Data data(Context::createGlobal(contextShared()));
    data.max_ranges = 1000;
    data.min_rows_pre_range = 1000000;
    CreateQueryVisitor visitor(data);
    visitor.visit(ast);
    return std::move(data);
}

TEST(CreateQueryVisitor, TestWithNumberColumnsType)
{
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a tinyint, b SMALLINT, c MEDIUMINT, d INT, e INTEGER, f BIGINT, g DECIMAL, h DEC, i NUMERIC, j "
                   "FIXED, k FLOAT, l DOUBLE, m DOUBLE PRECISION, n REAL)"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Nullable(Int8)")}, {"b", getType("Nullable(Int16)")}
                , {"c", getType("Nullable(Int32)")}, {"d", getType("Nullable(Int32)")}, {"e", getType("Nullable(Int32)")}
                , {"f", getType("Nullable(Int64)")}, {"g", getType("Nullable(Decimal(10, 0))")}, {"h", getType("Nullable(Decimal(10, 0))")}
                , {"i", getType("Nullable(Decimal(10, 0))")}, {"j", getType("Nullable(Decimal(10, 0))")}
                , {"k", getType("Nullable(Float32)")}, {"l", getType("Nullable(Float64)")}, {"m", getType("Nullable(Float64)")}
                , {"n", getType("Nullable(Float64)")}}
        )
    );

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a tinyint(1), b SMALLINT(1), c MEDIUMINT(1), d INT(1), e INTEGER(1), f BIGINT(1), g DECIMAL(1), h "
                   "DEC(2, 1), i NUMERIC(4, 3), j FIXED(6, 5), k FLOAT(1), l DOUBLE(1, 2), m DOUBLE PRECISION(3, 4), n REAL(5, 6))"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Nullable(Int8)")}, {"b", getType("Nullable(Int16)")}
            , {"c", getType("Nullable(Int32)")}, {"d", getType("Nullable(Int32)")}, {"e", getType("Nullable(Int32)")}
            , {"f", getType("Nullable(Int64)")}, {"g", getType("Nullable(Decimal(1, 0))")}, {"h", getType("Nullable(Decimal(2, 1))")}
            , {"i", getType("Nullable(Decimal(4, 3))")}, {"j", getType("Nullable(Decimal(6, 5))")}
            , {"k", getType("Nullable(Float32)")}, {"l", getType("Nullable(Float64)")}, {"m", getType("Nullable(Float64)")}
            , {"n", getType("Nullable(Float64)")}}
        )
    );

    /// UNSIGNED
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a tinyint UNSIGNED, b SMALLINT(1) UNSIGNED, c MEDIUMINT(1) UNSIGNED, d INT(1) UNSIGNED, e INTEGER(1), f "
                   "BIGINT(1) UNSIGNED, g DECIMAL(1) UNSIGNED, h DEC(2, 1) UNSIGNED, i NUMERIC(4, 3) UNSIGNED, j FIXED(6, 5) UNSIGNED, k FLOAT(1) "
                   "UNSIGNED, l DOUBLE(1, 2) UNSIGNED, m DOUBLE PRECISION(3, 4) UNSIGNED, n REAL(5, 6) UNSIGNED)"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Nullable(UInt8)")}, {"b", getType("Nullable(UInt16)")}
            , {"c", getType("Nullable(UInt32)")}, {"d", getType("Nullable(UInt32)")}, {"e", getType("Nullable(Int32)")}
            , {"f", getType("Nullable(UInt64)")}, {"g", getType("Nullable(Decimal(1, 0))")}, {"h", getType("Nullable(Decimal(2, 1))")}
            , {"i", getType("Nullable(Decimal(4, 3))")}, {"j", getType("Nullable(Decimal(6, 5))")}
            , {"k", getType("Nullable(Float32)")}, {"l", getType("Nullable(Float64)")}, {"m", getType("Nullable(Float64)")}
            , {"n", getType("Nullable(Float64)")}}
        )
    );

    /// NOT NULL
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a tinyint NOT NULL, b SMALLINT(1) NOT NULL, c MEDIUMINT(1) NOT NULL, d INT(1) NOT NULL, e INTEGER(1), f "
                "BIGINT(1) NOT NULL, g DECIMAL(1) NOT NULL, h DEC(2, 1) NOT NULL, i NUMERIC(4, 3) NOT NULL, j FIXED(6, 5) NOT NULL, k FLOAT(1) NOT "
                "NULL, l DOUBLE(1, 2) NOT NULL, m DOUBLE PRECISION(3, 4) NOT NULL, n REAL(5, 6) NOT NULL)"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Int8")}, {"b", getType("Int16")}
            , {"c", getType("Int32")}, {"d", getType("Int32")}, {"e", getType("Nullable(Int32)")}
            , {"f", getType("Int64")}, {"g", getType("Decimal(1, 0)")}, {"h", getType("Decimal(2, 1)")}
            , {"i", getType("Decimal(4, 3)")}, {"j", getType("Decimal(6, 5)")}
            , {"k", getType("Float32")}, {"l", getType("Float64")}, {"m", getType("Float64")}, {"n", getType("Float64")}}
        )
    );

    /// ZEROFILL
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a tinyint ZEROFILL, b SMALLINT(1) ZEROFILL, c MEDIUMINT(1) ZEROFILL, d INT(1) ZEROFILL, e INTEGER(1), f "
                   "BIGINT(1) ZEROFILL, g DECIMAL(1) ZEROFILL, h DEC(2, 1) ZEROFILL, i NUMERIC(4, 3) ZEROFILL, j FIXED(6, 5) ZEROFILL, k FLOAT(1) "
                   "ZEROFILL, l DOUBLE(1, 2) ZEROFILL, m DOUBLE PRECISION(3, 4) ZEROFILL, n REAL(5, 6) ZEROFILL)"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Nullable(UInt8)")}, {"b", getType("Nullable(UInt16)")}
            , {"c", getType("Nullable(UInt32)")}, {"d", getType("Nullable(UInt32)")}, {"e", getType("Nullable(Int32)")}
            , {"f", getType("Nullable(UInt64)")}, {"g", getType("Nullable(Decimal(1, 0))")}, {"h", getType("Nullable(Decimal(2, 1))")}
            , {"i", getType("Nullable(Decimal(4, 3))")}, {"j", getType("Nullable(Decimal(6, 5))")}
            , {"k", getType("Nullable(Float32)")}, {"l", getType("Nullable(Float64)")}, {"m", getType("Nullable(Float64)")}
            , {"n", getType("Nullable(Float64)")}}
        )
    );
}

TEST(CreateQueryVisitor, TestWithDateTimesColumnsType)
{
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE, b DATETIME, c TIMESTAMP, d TIME, e year)"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Nullable(Date)")}, {"b", getType("Nullable(DateTime)")}
            , {"c", getType("Nullable(DateTime)")}, {"d", getType("Nullable(DateTime64(3))")}, {"e", getType("Nullable(Int16)")} }
        )
    );

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE, b DATETIME(1), c TIMESTAMP(1), d TIME(1), e year(4))"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Nullable(Date)")}, {"b", getType("Nullable(DateTime)")}
            , {"c", getType("Nullable(DateTime)")}, {"d", getType("Nullable(DateTime64(3))")}, {"e", getType("Nullable(Int16)")} }
        )
    );

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL, b DATETIME(1) NOT NULL, c TIMESTAMP(1) NOT NULL, d TIME(1) NOT NULL, e year(4) NOT NULL)"),
        MySQLTableStruct(ASTs{}, ASTs{}, {{"a", getType("Date")}, {"b", getType("DateTime")} , {"c", getType("DateTime")}, {"d", getType("DateTime64")}, {"e", getType("Int16")} }
        )
    );
}

TEST(CreateQueryVisitor, TestWithParitionOptions)
{
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL) PARTITION BY HASH a"),
        MySQLTableStruct(ASTs{}, ASTs{std::make_shared<ASTIdentifier>("a")}, {{"a", getType("Date")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL) PARTITION BY LINEAR HASH a"),
        MySQLTableStruct(ASTs{}, ASTs{std::make_shared<ASTIdentifier>("a")}, {{"a", getType("Date")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL) PARTITION BY RANGE(a)"),
        MySQLTableStruct(ASTs{}, ASTs{std::make_shared<ASTIdentifier>("a")}, {{"a", getType("Date")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL, b INT) PARTITION BY RANGE COLUMNS(a, b)"),
        MySQLTableStruct(ASTs{}, ASTs{std::make_shared<ASTIdentifier>("a"), std::make_shared<ASTIdentifier>("b")}, {{"a", getType("Date")}, {"b", getType("Nullable(Int32)")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL) PARTITION BY LIST(a)"),
        MySQLTableStruct(ASTs{}, ASTs{std::make_shared<ASTIdentifier>("a")}, {{"a", getType("Date")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL, b INT) PARTITION BY LIST COLUMNS(a, b)"),
        MySQLTableStruct(ASTs{}, ASTs{std::make_shared<ASTIdentifier>("a"), std::make_shared<ASTIdentifier>("b")},
            {{"a", getType("Date")}, {"b", getType("Nullable(Int32)")}}));
}

TEST(CreateQueryVisitor, TestWithPrimaryToPartitionBy)
{
    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATE NOT NULL PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("Date")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a DATETIME NOT NULL PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("DateTime")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a TINYINT NOT NULL PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("Int8")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a SMALLINT NOT NULL PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("Int16")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a INT NOT NULL PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("Int32")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a BIGINT NOT NULL PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("Int64")}}));

    EXPECT_EQ(
        visitQuery("CREATE TABLE test(a BIGINT PRIMARY KEY)"),
        MySQLTableStruct(ASTs{std::make_shared<ASTIdentifier>("a")}, ASTs{}, {{"a", getType("Nullable(Int64)")}}));
}

