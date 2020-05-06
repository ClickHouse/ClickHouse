#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Interpreters/MySQL/CreateQueryConvertVisitor.h>

using namespace DB;
using namespace MySQLParser;
using namespace MySQLVisitor;

TEST(CreateQueryConvert, SimpleNumbersType)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a tinyint, b SMALLINT, c MEDIUMINT, d INT, e INTEGER, f BIGINT, g DECIMAL, h DEC, i NUMERIC, j "
                   "FIXED, k FLOAT, l DOUBLE, m DOUBLE PRECISION, n REAL)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Nullable(Int8),b Nullable(Int16),c Nullable(Int32),d Nullable(Int32),e Nullable(Int32),f Nullable(Int64),g "
        "Nullable(Decimal(10, 0)),h Nullable(Decimal(10, 0)),i Nullable(Decimal(10, 0)),j Nullable(Decimal(10, 0)),k Nullable(Float32),l "
        "Nullable(Float64),m Nullable(Float64),n Nullable(Float64)) ENGINE = MergeTree()");
}

TEST(CreateQueryConvert, NumbersTypeWithLength)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a tinyint(1), b SMALLINT(1), c MEDIUMINT(1), d INT(1), e INTEGER(1), f BIGINT(1), g DECIMAL(1), h DEC(1, 2), i NUMERIC(3, 4), j "
                   "FIXED(5, 6), k FLOAT(1), l DOUBLE(1, 2), m DOUBLE PRECISION(3, 4), n REAL(5, 6))";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Nullable(Int8),b Nullable(Int16),c Nullable(Int32),d Nullable(Int32),e Nullable(Int32),f Nullable(Int64),g "
        "Nullable(Decimal(1)),h Nullable(Decimal(1, 2)),i Nullable(Decimal(3, 4)),j Nullable(Decimal(5, 6)),k Nullable(Float32),l "
        "Nullable(Float64),m Nullable(Float64),n Nullable(Float64)) ENGINE = MergeTree()");
}

TEST(CreateQueryConvert, NumbersTypeWithUnsigned)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a tinyint UNSIGNED, b SMALLINT(1) UNSIGNED, c MEDIUMINT(1) UNSIGNED, d INT(1) UNSIGNED, e "
                   "INTEGER(1), f BIGINT(1) UNSIGNED, g DECIMAL(1) UNSIGNED, h DEC(1, 2) UNSIGNED, i NUMERIC(3, 4) UNSIGNED, j FIXED(5, 6) "
                   "UNSIGNED, k FLOAT(1) UNSIGNED, l DOUBLE(1, 2) UNSIGNED, m DOUBLE PRECISION(3, 4) UNSIGNED, n REAL(5, 6) UNSIGNED)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Nullable(UInt8),b Nullable(UInt16),c Nullable(UInt32),d Nullable(UInt32),e Nullable(Int32),f "
        "Nullable(UInt64),g Nullable(Decimal(1)),h Nullable(Decimal(1, 2)),i Nullable(Decimal(3, 4)),j Nullable(Decimal(5, 6)),k "
        "Nullable(Float32),l Nullable(Float64),m Nullable(Float64),n Nullable(Float64)) ENGINE = MergeTree()");
}

TEST(CreateQueryConvert, NumbersTypeWithNotNull)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a tinyint NOT NULL, b SMALLINT(1) NOT NULL, c MEDIUMINT(1) NOT NULL, d INT(1) NOT NULL, e "
                   "INTEGER(1), f BIGINT(1) NOT NULL, g DECIMAL(1) NOT NULL, h DEC(1, 2) NOT NULL, i NUMERIC(3, 4) NOT NULL, j FIXED(5, 6) "
                   "NOT NULL, k FLOAT(1) NOT NULL, l DOUBLE(1, 2) NOT NULL, m DOUBLE PRECISION(3, 4) NOT NULL, n REAL(5, 6) NOT NULL)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Int8,b Int16,c Int32,d Int32,e Nullable(Int32),f Int64,g Decimal(1),h Decimal(1, 2),i Decimal(3, 4),j "
        "Decimal(5, 6),k Float32,l Float64,m Float64,n Float64) ENGINE = MergeTree()");
}

TEST(CreateQueryConvert, NumbersTypeWithZeroFill)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a tinyint ZEROFILL, b SMALLINT(1) ZEROFILL, c MEDIUMINT(1) ZEROFILL, d INT(1) ZEROFILL, e "
                   "INTEGER(1), f BIGINT(1) ZEROFILL, g DECIMAL(1) ZEROFILL, h DEC(1, 2) ZEROFILL, i NUMERIC(3, 4) ZEROFILL, j FIXED(5, 6) "
                   "ZEROFILL, k FLOAT(1) ZEROFILL, l DOUBLE(1, 2) ZEROFILL, m DOUBLE PRECISION(3, 4) ZEROFILL, n REAL(5, 6) ZEROFILL)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Nullable(UInt8),b Nullable(UInt16),c Nullable(UInt32),d Nullable(UInt32),e Nullable(Int32),f "
        "Nullable(UInt64),g Nullable(Decimal(1)),h Nullable(Decimal(1, 2)),i Nullable(Decimal(3, 4)),j Nullable(Decimal(5, 6)),k "
        "Nullable(Float32),l Nullable(Float64),m Nullable(Float64),n Nullable(Float64)) ENGINE = MergeTree()");
}

TEST(CreateQueryConvert, SimpleDateTimesType)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a DATE, b DATETIME, c TIMESTAMP, d TIME, e year)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Nullable(Date),b Nullable(DateTime),c Nullable(DateTime),d Nullable(DateTime64),e Nullable(Int16)) ENGINE = "
        "MergeTree()");
}

TEST(CreateQueryConvert, DateTimeTypesWithLength)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a DATE, b DATETIME(1), c TIMESTAMP(1), d TIME(1), e year(4))";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(
        out.str(),
        "CREATE TABLE test(a Nullable(Date),b Nullable(DateTime),c Nullable(DateTime),d Nullable(DateTime64),e Nullable(Int16)) ENGINE = "
        "MergeTree()");
}

TEST(CreateQueryConvert, DateTimeTypesWithNotNull)
{
    ParserCreateQuery p_create_query;
    String input = "CREATE TABLE test(a DATE NOT NULL, b DATETIME(1) NOT NULL, c TIMESTAMP(1) NOT NULL, d TIME(1) NOT NULL, e year(4) NOT NULL)";
    ASTPtr ast = parseQuery(p_create_query, input.data(), input.data() + input.size(), "", 0, 0);

    WriteBufferFromOwnString out;
    CreateQueryConvertVisitor::Data data{.out = out};
    CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);

    EXPECT_EQ(out.str(), "CREATE TABLE test(a Date,b DateTime,c DateTime,d DateTime64,e Int16) ENGINE = MergeTree()");
}
