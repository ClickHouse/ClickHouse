#include <gtest/gtest.h>

#include <sstream>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/QualifiedTableName.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

/// Minimal dictionary structure: one complex String key, no attributes.
static DictionaryStructure makeSingleStringKeyStructure()
{
    const char * xml = R"(<?xml version="1.0"?>
<dictionaries>
  <dictionary>
    <structure>
      <key>
        <attribute>
          <name>k</name>
          <type>String</type>
        </attribute>
      </key>
    </structure>
  </dictionary>
</dictionaries>)";
    std::istringstream stream(xml);            // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto config = Poco::AutoPtr<Poco::Util::XMLConfiguration>(new Poco::Util::XMLConfiguration(stream));
    return DictionaryStructure(*config, "dictionary");
}

/// Returns the WHERE clause of a composeLoadKeysQuery call for the given values.
static std::string buildWhereClause(
    IdentifierQuotingStyle style, const std::vector<std::string> & values, const std::string & table = "t")
{
    auto dict_struct = makeSingleStringKeyStructure();
    ExternalQueryBuilder builder(dict_struct, "", "", table, "", "", style);

    auto col = ColumnString::create();
    for (const auto & v : values)
        col->insert(v);

    Columns key_columns;
    key_columns.push_back(std::move(col));

    VectorWithMemoryTracking<size_t> rows;
    for (size_t i = 0; i < values.size(); ++i)
        rows.push_back(i);

    return builder.composeLoadKeysQuery(key_columns, rows, ExternalQueryBuilder::AND_OR_CHAIN);
}

/// DoubleQuotes (PostgreSQL / Cassandra): '' for single quotes, \ left intact.
TEST(ExternalQueryBuilderEscaping, DoubleQuotesEscapesSingleQuoteWithDoubling)
{
    std::string sql = buildWhereClause(IdentifierQuotingStyle::DoubleQuotes, {"it's"});
    EXPECT_NE(sql.find("'it''s'"), std::string::npos) << "SQL: " << sql;
    EXPECT_EQ(sql.find("it\\'s"), std::string::npos) << "Must not use backslash escaping. SQL: " << sql;
}

TEST(ExternalQueryBuilderEscaping, DoubleQuotesLeavesBackslashUnchanged)
{
    // PostgreSQL/Cassandra treat '\' as a literal character; the value must round-trip correctly.
    std::string sql = buildWhereClause(IdentifierQuotingStyle::DoubleQuotes, {"foo\\bar"});
    EXPECT_NE(sql.find("'foo\\bar'"), std::string::npos) << "SQL: " << sql;
    // Must NOT double the backslash (that would store two backslashes in the DB).
    EXPECT_EQ(sql.find("'foo\\\\bar'"), std::string::npos) << "Must not double backslash. SQL: " << sql;
}

/// DoubleQuotesPostgreSQL (PostgreSQL): identifiers escape '"' by doubling it.
TEST(ExternalQueryBuilderEscaping, DoubleQuotesPostgreSQLDoublesQuoteInIdentifier)
{
    std::string sql = buildWhereClause(IdentifierQuotingStyle::DoubleQuotesPostgreSQL, {"key"}, R"(t"x)");
    EXPECT_NE(sql.find(R"("t""x")"), std::string::npos) << "SQL: " << sql;
    // A '\"' would end the identifier for PostgreSQL and turn the rest of the query into statements.
    EXPECT_EQ(sql.find(R"("t\"x")"), std::string::npos) << "Must not use backslash escaping. SQL: " << sql;
}

TEST(ExternalQueryBuilderEscaping, DoubleQuotesPostgreSQLKeepsQuoteDoublingForLiterals)
{
    // The dictionary source picks one style for identifiers and literals both, so the PostgreSQL
    // identifier dialect must keep the '' literal escaping that DoubleQuotes enables.
    std::string sql = buildWhereClause(IdentifierQuotingStyle::DoubleQuotesPostgreSQL, {"it's"});
    EXPECT_NE(sql.find("'it''s'"), std::string::npos) << "SQL: " << sql;
    EXPECT_EQ(sql.find("it\\'s"), std::string::npos) << "Must not use backslash escaping. SQL: " << sql;
}

/// Backticks (ClickHouse / MySQL): \' for single quotes, \\ for backslashes.
TEST(ExternalQueryBuilderEscaping, BackticksEscapesSingleQuoteWithBackslash)
{
    std::string sql = buildWhereClause(IdentifierQuotingStyle::Backticks, {"it's"});
    EXPECT_NE(sql.find("\\'"), std::string::npos) << "SQL: " << sql;
    EXPECT_EQ(sql.find("''"), std::string::npos) << "Must not use quote-doubling. SQL: " << sql;
}

TEST(ExternalQueryBuilderEscaping, BackticksDoublesBackslash)
{
    // ClickHouse/MySQL treat '\' as an escape character; backslashes must be doubled.
    std::string sql = buildWhereClause(IdentifierQuotingStyle::Backticks, {"foo\\bar"});
    EXPECT_NE(sql.find("foo\\\\bar"), std::string::npos) << "SQL: " << sql;
}

/// A source configured with `query` instead of `table` leaves the table name empty, and the schema
/// must then be left empty too: there is nothing to split into `schema.table`.
/// Regression for the MSSQL/ODBC dictionary that died with "Invalid qualified name:" (issue 75101).
TEST(ExternalQueryBuilderQueryMode, EmptyTableWithQueryIsAccepted)
{
    auto dict_struct = makeSingleStringKeyStructure();
    /// No throw: `query` alone is a valid configuration.
    ExternalQueryBuilder builder(dict_struct, "MYDB", "", "", "select 1 id, 1 name", "", IdentifierQuotingStyle::DoubleQuotes);
    EXPECT_EQ(builder.composeLoadAllQuery(), "select 1 id, 1 name");
}

TEST(ExternalQueryBuilderQueryMode, NeitherTableNorQueryIsRejected)
{
    auto dict_struct = makeSingleStringKeyStructure();
    ASSERT_THROW(
        ExternalQueryBuilder(dict_struct, "MYDB", "", "", "", "", IdentifierQuotingStyle::DoubleQuotes), DB::Exception);
}

/// The guarded-away input of `makeExternalQueryBuilder`: parsing an empty name is a syntax error, so
/// the schema branch must skip it rather than let an empty `table` reach it.
TEST(QualifiedTableNameParsing, EmptyNameCannotBeParsed)
{
    EXPECT_FALSE(QualifiedTableName::tryParseFromString("").has_value());

    try
    {
        QualifiedTableName::parseFromString("");
        FAIL() << "parseFromString of an empty name must throw";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SYNTAX_ERROR);
        EXPECT_NE(std::string(e.message()).find("Invalid qualified name"), std::string::npos) << e.message();
    }
}

/// An unqualified table name still splits cleanly, so the guard does not change the schema path.
TEST(QualifiedTableNameParsing, SchemaAndTableStillSplit)
{
    auto name = QualifiedTableName::parseFromString("dbo.t");
    EXPECT_EQ(name.database, "dbo");
    EXPECT_EQ(name.table, "t");

    name = QualifiedTableName::parseFromString("t");
    EXPECT_TRUE(name.database.empty());
    EXPECT_EQ(name.table, "t");
}

/// The schema/table split an external dictionary source performs on its `table` setting. An empty
/// `table` means the source is configured with `query` instead, and parsing an empty name would
/// throw SYNTAX_ERROR, so it must be left alone rather than parsed.
/// Regression for the MSSQL/ODBC dictionary that died with "Invalid qualified name:" (issue 75101).
TEST(MakeQualifiedTableName, EmptyTableIsNotParsed)
{
    auto name = makeQualifiedTableName("", "");
    EXPECT_TRUE(name.database.empty());
    EXPECT_TRUE(name.table.empty());
}

TEST(MakeQualifiedTableName, ExplicitSchemaIsKept)
{
    /// A schema given alongside a table is not overwritten by anything in the table name.
    auto name = makeQualifiedTableName("myschema", "t");
    EXPECT_EQ(name.database, "myschema");
    EXPECT_EQ(name.table, "t");

    /// ... and it survives an empty table too.
    name = makeQualifiedTableName("myschema", "");
    EXPECT_EQ(name.database, "myschema");
    EXPECT_TRUE(name.table.empty());
}

TEST(MakeQualifiedTableName, SchemaEmbeddedInTableOverrides)
{
    auto name = makeQualifiedTableName("", "dbo.t");
    EXPECT_EQ(name.database, "dbo");
    EXPECT_EQ(name.table, "t");

    name = makeQualifiedTableName("", "t");
    EXPECT_TRUE(name.database.empty());
    EXPECT_EQ(name.table, "t");
}
