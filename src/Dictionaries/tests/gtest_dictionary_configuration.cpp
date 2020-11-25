#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Dictionaries/registerDictionaries.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <common/types.h>

#include <gtest/gtest.h>

using namespace DB;

static bool registered = false;
/// For debug
#pragma GCC diagnostic ignored "-Wunused-function"
static std::string configurationToString(const DictionaryConfigurationPtr & config)
{
    const Poco::Util::XMLConfiguration * xml_config = dynamic_cast<const Poco::Util::XMLConfiguration *>(config.get());
    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    xml_config->save(oss);
    return oss.str();
}

TEST(ConvertDictionaryAST, SimpleDictConfiguration)
{
    if (!registered)
    {
        registerDictionaries();
        registered = true;
    }

    String input = " CREATE DICTIONARY test.dict1"
                   " ("
                   "    key_column UInt64 DEFAULT 0,"
                   "    second_column UInt8 DEFAULT 1,"
                   "    third_column UInt8 DEFAULT 2"
                   " )"
                   " PRIMARY KEY key_column"
                   " SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' PASSWORD '' DB 'test' TABLE 'table_for_dict'))"
                   " LAYOUT(FLAT())"
                   " LIFETIME(MIN 1 MAX 10)"
                   " RANGE(MIN second_column MAX third_column)";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    DictionaryConfigurationPtr config = getDictionaryConfigurationFromAST(*create, getContext().context);

    /// name
    EXPECT_EQ(config->getString("dictionary.database"), "test");
    EXPECT_EQ(config->getString("dictionary.name"), "dict1");

    /// lifetime
    EXPECT_EQ(config->getInt("dictionary.lifetime.min"), 1);
    EXPECT_EQ(config->getInt("dictionary.lifetime.max"), 10);

    /// range
    EXPECT_EQ(config->getString("dictionary.structure.range_min.name"), "second_column");
    EXPECT_EQ(config->getString("dictionary.structure.range_max.name"), "third_column");
    EXPECT_EQ(config->getString("dictionary.structure.range_min.type"), "UInt8");
    EXPECT_EQ(config->getString("dictionary.structure.range_max.type"), "UInt8");


    /// source
    EXPECT_EQ(config->getString("dictionary.source.clickhouse.host"), "localhost");
    EXPECT_EQ(config->getInt("dictionary.source.clickhouse.port"), 9000);
    EXPECT_EQ(config->getString("dictionary.source.clickhouse.user"), "default");
    EXPECT_EQ(config->getString("dictionary.source.clickhouse.password"), "");
    EXPECT_EQ(config->getString("dictionary.source.clickhouse.db"), "test");
    EXPECT_EQ(config->getString("dictionary.source.clickhouse.table"), "table_for_dict");

    /// attributes and key
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("dictionary.structure", keys);

    EXPECT_EQ(keys.size(), 5); /// + ranged keys
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".name"), "second_column");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".type"), "UInt8");
    EXPECT_EQ(config->getInt("dictionary.structure." + keys[0] + ".null_value"), 1);

    EXPECT_EQ(config->getString("dictionary.structure." + keys[1] + ".name"), "third_column");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[1] + ".type"), "UInt8");
    EXPECT_EQ(config->getInt("dictionary.structure." + keys[1] + ".null_value"), 2);

    EXPECT_EQ(keys[2], "id");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[2] + ".name"), "key_column");

    /// layout
    EXPECT_TRUE(config->has("dictionary.layout.flat"));
}


TEST(ConvertDictionaryAST, TrickyAttributes)
{
    if (!registered)
    {
        registerDictionaries();
        registered = true;
    }

    String input = " CREATE DICTIONARY dict2"
                   " ("
                   "    key_column UInt64 IS_OBJECT_ID,"
                   "    second_column UInt8 HIERARCHICAL INJECTIVE,"
                   "    third_column UInt8 DEFAULT 2 EXPRESSION rand() % 100 * 77"
                   " )"
                   " PRIMARY KEY key_column"
                   " LAYOUT(hashed())"
                   " LIFETIME(MIN 1 MAX 10)"
                   " SOURCE(CLICKHOUSE(HOST 'localhost'))";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    DictionaryConfigurationPtr config = getDictionaryConfigurationFromAST(*create, getContext().context);

    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("dictionary.structure", keys);

    EXPECT_EQ(keys.size(), 3);
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".name"), "second_column");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".type"), "UInt8");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".null_value"), "");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".hierarchical"), "true");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[0] + ".injective"), "true");

    EXPECT_EQ(config->getString("dictionary.structure." + keys[1] + ".name"), "third_column");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[1] + ".type"), "UInt8");
    EXPECT_EQ(config->getInt("dictionary.structure." + keys[1] + ".null_value"), 2);
    EXPECT_EQ(config->getString("dictionary.structure." + keys[1] + ".expression"), "(rand() % 100) * 77");

    EXPECT_EQ(keys[2], "id");
    EXPECT_EQ(config->getString("dictionary.structure." + keys[2] + ".name"), "key_column");
}


TEST(ConvertDictionaryAST, ComplexKeyAndLayoutWithParams)
{
    if (!registered)
    {
        registerDictionaries();
        registered = true;
    }

    String input = " CREATE DICTIONARY dict4"
                   " ("
                   "    key_column1 String,"
                   "    key_column2 UInt64,"
                   "    third_column UInt8,"
                   "    fourth_column UInt8"
                   " )"
                   " PRIMARY KEY key_column1, key_column2"
                   " SOURCE(MYSQL())"
                   " LAYOUT(COMPLEX_KEY_CACHE(size_in_cells 50))"
                   " LIFETIME(MIN 1 MAX 10)";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    DictionaryConfigurationPtr config = getDictionaryConfigurationFromAST(*create, getContext().context);

    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("dictionary.structure.key", keys);

    EXPECT_EQ(keys.size(), 2);
    EXPECT_EQ(config->getString("dictionary.structure.key." + keys[0] + ".name"), "key_column1");
    EXPECT_EQ(config->getString("dictionary.structure.key." + keys[0] + ".type"), "String");

    EXPECT_EQ(config->getString("dictionary.structure.key." + keys[1] + ".name"), "key_column2");
    EXPECT_EQ(config->getString("dictionary.structure.key." + keys[1] + ".type"), "UInt64");

    Poco::Util::AbstractConfiguration::Keys attrs;
    config->keys("dictionary.structure", attrs);

    EXPECT_EQ(attrs.size(), 3);
    EXPECT_EQ(config->getString("dictionary.structure." + attrs[0] + ".name"), "third_column");
    EXPECT_EQ(config->getString("dictionary.structure." + attrs[0] + ".type"), "UInt8");

    EXPECT_EQ(config->getString("dictionary.structure." + attrs[1] + ".name"), "fourth_column");
    EXPECT_EQ(config->getString("dictionary.structure." + attrs[1] + ".type"), "UInt8");

    EXPECT_EQ(attrs[2], "key");

    EXPECT_EQ(config->getInt("dictionary.layout.complex_key_cache.size_in_cells"), 50);
}


TEST(ConvertDictionaryAST, ComplexSource)
{
    if (!registered)
    {
        registerDictionaries();
        registered = true;
    }

    String input = " CREATE DICTIONARY dict4"
                   " ("
                   "    key_column UInt64,"
                   "    second_column UInt8,"
                   "    third_column UInt8"
                   " )"
                   " PRIMARY KEY key_column"
                   " SOURCE(MYSQL(HOST 'localhost' PORT 9000 USER 'default' REPLICA(HOST '127.0.0.1' PRIORITY 1) PASSWORD ''))"
                   " LAYOUT(CACHE(size_in_cells 50))"
                   " LIFETIME(MIN 1 MAX 10)"
                   " RANGE(MIN second_column MAX third_column)";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    DictionaryConfigurationPtr config = getDictionaryConfigurationFromAST(*create, getContext().context);
    /// source
    EXPECT_EQ(config->getString("dictionary.source.mysql.host"), "localhost");
    EXPECT_EQ(config->getInt("dictionary.source.mysql.port"), 9000);
    EXPECT_EQ(config->getString("dictionary.source.mysql.user"), "default");
    EXPECT_EQ(config->getString("dictionary.source.mysql.password"), "");
    EXPECT_EQ(config->getString("dictionary.source.mysql.replica.host"), "127.0.0.1");
    EXPECT_EQ(config->getInt("dictionary.source.mysql.replica.priority"), 1);
}
