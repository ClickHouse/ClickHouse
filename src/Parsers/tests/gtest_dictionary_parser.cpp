#include <base/types.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

#pragma GCC diagnostic ignored "-Wunused-function"

static String astToString(IAST * ast)
{
    WriteBufferFromOwnString buf;
    dumpAST(*ast, buf);
    return buf.str();
}

/// Tests for external dictionaries DDL parser

TEST(ParserDictionaryDDL, SimpleDictionary)
{
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
    EXPECT_EQ(create->table, "dict1");
    EXPECT_EQ(create->database, "test");
    EXPECT_EQ(create->is_dictionary, true);
    EXPECT_NE(create->dictionary, nullptr);
    EXPECT_NE(create->dictionary->lifetime, nullptr);
    EXPECT_NE(create->dictionary->source, nullptr);
    EXPECT_NE(create->dictionary->layout, nullptr);
    EXPECT_NE(create->dictionary->primary_key, nullptr);
    EXPECT_NE(create->dictionary->range, nullptr);

    /// source test
    EXPECT_EQ(create->dictionary->source->name, "clickhouse");
    auto children = create->dictionary->source->elements->children;
    EXPECT_EQ(children[0]->as<ASTPair>() -> first, "host");
    EXPECT_EQ(children[0]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "localhost");

    EXPECT_EQ(children[1]->as<ASTPair>()->first, "port");
    EXPECT_EQ(children[1]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<UInt64>(), 9000);

    EXPECT_EQ(children[2]->as<ASTPair>()->first, "user");
    EXPECT_EQ(children[2]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "default");
    EXPECT_EQ(children[3]->as<ASTPair>()->first, "password");
    EXPECT_EQ(children[3]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "");

    EXPECT_EQ(children[4]->as<ASTPair>()->first, "db");
    EXPECT_EQ(children[4]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "test");

    EXPECT_EQ(children[5]->as<ASTPair>()->first, "table");
    EXPECT_EQ(children[5]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "table_for_dict");

    /// layout test
    auto * layout = create->dictionary->layout;
    EXPECT_EQ(layout->layout_type, "flat");
    EXPECT_EQ(layout->parameters->children.size(), 0);

    /// lifetime test
    auto * lifetime = create->dictionary->lifetime;

    EXPECT_EQ(lifetime->min_sec, 1);
    EXPECT_EQ(lifetime->max_sec, 10);

    /// primary key test
    auto * primary_key = create->dictionary->primary_key;

    EXPECT_EQ(primary_key->children.size(), 1);
    EXPECT_EQ(primary_key->children[0]->as<ASTIdentifier>()->name(), "key_column");

    /// range test
    auto * range = create->dictionary->range;
    EXPECT_EQ(range->min_attr_name, "second_column");
    EXPECT_EQ(range->max_attr_name, "third_column");

    /// test attributes
    EXPECT_NE(create->dictionary_attributes_list, nullptr);

    auto attributes_children = create->dictionary_attributes_list->children;
    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->name, "key_column");
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->name, "second_column");
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->name, "third_column");

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 0);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 1);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 2);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, false);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, false);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->injective, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->injective, false);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->injective, false);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);
}


TEST(ParserDictionaryDDL, AttributesWithMultipleProperties)
{
    String input = " CREATE DICTIONARY dict2"
                   " ("
                   "    key_column UInt64 IS_OBJECT_ID,"
                   "    second_column UInt8 DEFAULT 1 HIERARCHICAL INJECTIVE,"
                   "    third_column UInt8 DEFAULT 2 EXPRESSION rand() % 100 * 77"
                   " )"
                   " PRIMARY KEY key_column"
                   " SOURCE(CLICKHOUSE(HOST 'localhost'))";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    EXPECT_EQ(create->table, "dict2");
    EXPECT_EQ(create->database, "");

    /// test attributes
    EXPECT_NE(create->dictionary_attributes_list, nullptr);

    auto attributes_children = create->dictionary_attributes_list->children;
    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->name, "key_column");
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->name, "second_column");
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->name, "third_column");

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->default_value, nullptr);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 1);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 2);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);
    EXPECT_EQ(serializeAST(*attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->expression, true), "(rand() % 100) * 77");

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, true);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, false);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->injective, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->injective, true);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->injective, false);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, true);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);
}

TEST(ParserDictionaryDDL, CustomAttributePropertiesOrder)
{
    String input = " CREATE DICTIONARY dict3"
                   " ("
                   "    key_column UInt64 IS_OBJECT_ID DEFAULT 100,"
                   "    second_column UInt8 INJECTIVE HIERARCHICAL DEFAULT 1,"
                   "    third_column UInt8 EXPRESSION rand() % 100 * 77 DEFAULT 2 INJECTIVE HIERARCHICAL"
                   " )"
                   " PRIMARY KEY key_column"
                   " SOURCE(CLICKHOUSE(REPLICA(HOST '127.0.0.1' PRIORITY 1)))"
                   " LIFETIME(300)";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();

    /// test attributes
    EXPECT_NE(create->dictionary_attributes_list, nullptr);

    auto attributes_children = create->dictionary_attributes_list->children;

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->name, "key_column");
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->name, "second_column");
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->name, "third_column");

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 100);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 1);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->default_value->as<ASTLiteral>()->value.get<UInt64>(), 2);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->expression, nullptr);
    EXPECT_EQ(serializeAST(*attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->expression, true), "(rand() % 100) * 77");

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, true);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->hierarchical, true);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->injective, false);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->injective, true);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->injective, true);

    EXPECT_EQ(attributes_children[0]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, true);
    EXPECT_EQ(attributes_children[1]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);
    EXPECT_EQ(attributes_children[2]->as<ASTDictionaryAttributeDeclaration>()->is_object_id, false);

    /// lifetime test
    auto * lifetime = create->dictionary->lifetime;

    EXPECT_EQ(lifetime->min_sec, 0);
    EXPECT_EQ(lifetime->max_sec, 300);
}


TEST(ParserDictionaryDDL, NestedSource)
{
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
    EXPECT_EQ(create->table, "dict4");
    EXPECT_EQ(create->database, "");

    /// source test
    EXPECT_EQ(create->dictionary->source->name, "mysql");
    auto children = create->dictionary->source->elements->children;

    EXPECT_EQ(children[0]->as<ASTPair>()->first, "host");
    EXPECT_EQ(children[0]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "localhost");

    EXPECT_EQ(children[1]->as<ASTPair>()->first, "port");
    EXPECT_EQ(children[1]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<UInt64>(), 9000);

    EXPECT_EQ(children[2]->as<ASTPair>()->first, "user");
    EXPECT_EQ(children[2]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "default");

    EXPECT_EQ(children[3]->as<ASTPair>()->first, "replica");
    auto replica = children[3]->as<ASTPair>()->second->children;

    EXPECT_EQ(replica[0]->as<ASTPair>()->first, "host");
    EXPECT_EQ(replica[0]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "127.0.0.1");

    EXPECT_EQ(replica[1]->as<ASTPair>()->first, "priority");
    EXPECT_EQ(replica[1]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<UInt64>(), 1);

    EXPECT_EQ(children[4]->as<ASTPair>()->first, "password");
    EXPECT_EQ(children[4]->as<ASTPair>()->second->as<ASTLiteral>()->value.get<String>(), "");
}


TEST(ParserDictionaryDDL, Formatting)
{
    String input = " CREATE DICTIONARY test.dict5"
                   " ("
                   "    key_column1 UInt64 DEFAULT 1 HIERARCHICAL INJECTIVE,"
                   "    key_column2 String DEFAULT '',"
                   "    second_column UInt8 EXPRESSION intDiv(50, rand() % 1000),"
                   "    third_column UInt8"
                   " )"
                   " PRIMARY KEY key_column1, key_column2"
                   " SOURCE(MYSQL(HOST 'localhost' PORT 9000 USER 'default' REPLICA(HOST '127.0.0.1' PRIORITY 1) PASSWORD ''))"
                   " LAYOUT(CACHE(size_in_cells 50))"
                   " LIFETIME(MIN 1 MAX 10)"
                   " RANGE(MIN second_column MAX third_column)";

    ParserCreateDictionaryQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    auto str = serializeAST(*create, true);
    EXPECT_EQ(str, "CREATE DICTIONARY test.dict5 (`key_column1` UInt64 DEFAULT 1 HIERARCHICAL INJECTIVE, `key_column2` String DEFAULT '', `second_column` UInt8 EXPRESSION intDiv(50, rand() % 1000), `third_column` UInt8) PRIMARY KEY key_column1, key_column2 SOURCE(MYSQL(HOST 'localhost' PORT 9000 USER 'default' REPLICA (HOST '127.0.0.1' PRIORITY 1) PASSWORD '')) LIFETIME(MIN 1 MAX 10) LAYOUT(CACHE(SIZE_IN_CELLS 50)) RANGE(MIN second_column MAX third_column)");
}

TEST(ParserDictionaryDDL, ParseDropQuery)
{
    String input1 = "DROP DICTIONARY test.dict1";

    ParserDropQuery parser;
    ASTPtr ast1 = parseQuery(parser, input1.data(), input1.data() + input1.size(), "", 0, 0);
    ASTDropQuery * drop1 = ast1->as<ASTDropQuery>();

    EXPECT_TRUE(drop1->is_dictionary);
    EXPECT_EQ(drop1->database, "test");
    EXPECT_EQ(drop1->table, "dict1");
    auto str1 = serializeAST(*drop1, true);
    EXPECT_EQ(input1, str1);

    String input2 = "DROP DICTIONARY IF EXISTS dict2";

    ASTPtr ast2 = parseQuery(parser, input2.data(), input2.data() + input2.size(), "", 0, 0);
    ASTDropQuery * drop2 = ast2->as<ASTDropQuery>();

    EXPECT_TRUE(drop2->is_dictionary);
    EXPECT_EQ(drop2->database, "");
    EXPECT_EQ(drop2->table, "dict2");
    auto str2 = serializeAST(*drop2, true);
    EXPECT_EQ(input2, str2);
}

TEST(ParserDictionaryDDL, ParsePropertiesQueries)
{
    String input1 = "SHOW CREATE DICTIONARY test.dict1";

    ParserTablePropertiesQuery parser;
    ASTPtr ast1 = parseQuery(parser, input1.data(), input1.data() + input1.size(), "", 0, 0);
    ASTShowCreateDictionaryQuery * show1 = ast1->as<ASTShowCreateDictionaryQuery>();

    EXPECT_EQ(show1->table, "dict1");
    EXPECT_EQ(show1->database, "test");
    EXPECT_EQ(serializeAST(*show1), input1);

    String input2 = "EXISTS DICTIONARY dict2";

    ASTPtr ast2 = parseQuery(parser, input2.data(), input2.data() + input2.size(), "", 0, 0);
    ASTExistsDictionaryQuery * show2 = ast2->as<ASTExistsDictionaryQuery>();

    EXPECT_EQ(show2->table, "dict2");
    EXPECT_EQ(show2->database, "");
    EXPECT_EQ(serializeAST(*show2), input2);
}
