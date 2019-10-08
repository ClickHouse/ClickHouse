#include <iostream>

#include <Core/Types.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserDictionary.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/DumpASTNode.h>
#include <sstream>

#include <gtest/gtest.h>

using namespace DB;

String astToString(IAST * ast)
{
    std::ostringstream oss;
    dumpAST(*ast, oss);
    return oss.str();
}


TEST(ParserCreateDictionary, SimpleDictionary)
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
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
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
    auto layout = create->dictionary->layout;
    EXPECT_EQ(layout->layout_type, "flat");
    EXPECT_EQ(layout->children.size(), 0);

    /// lifetime test
    auto lifetime = create->dictionary->lifetime;

    EXPECT_EQ(lifetime->min_sec, 1);
    EXPECT_EQ(lifetime->max_sec, 10);

    /// primary key test
    auto primary_key = create->dictionary->primary_key;

    EXPECT_EQ(primary_key->children.size(), 1);
    EXPECT_EQ(primary_key->children[0]->as<ASTIdentifier>()->name, "key_column");

    /// range test
    auto range = create->dictionary->range;
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


TEST(ParserCreateDictionary, AttributesWithMultipleProperties)
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
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
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


TEST(ParserCreateDictionary, NestedSource)
{
    String input = " CREATE DICTIONARY dict3"
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
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);
    ASTCreateQuery * create = ast->as<ASTCreateQuery>();
    EXPECT_EQ(create->table, "dict3");
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
