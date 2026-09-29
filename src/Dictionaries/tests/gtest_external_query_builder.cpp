#include <gtest/gtest.h>

#include <sstream>

#include <Columns/ColumnString.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

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
