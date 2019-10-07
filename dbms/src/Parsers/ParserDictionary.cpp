#include <Parsers/ParserDictionary.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserDictionaryAttributeDeclaration.h>

#include <Poco/String.h>

namespace DB
{


bool ParserDictionaryLifetime::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyValuePairsList key_value_pairs_p;
    ASTPtr ast_lifetime;
    if (!key_value_pairs_p.parse(pos, ast_lifetime, expected))
        return false;

    const ASTExpressionList & expr_list = ast_lifetime->as<const ASTExpressionList &>();
    if (expr_list.children.size() != 2)
        return false;

    auto res = std::make_shared<ASTDictionaryLifetime>();
    for (const auto & elem : expr_list.children)
    {
        const ASTPair & pair = elem->as<const ASTPair &>();
        const ASTLiteral * literal = dynamic_cast<const ASTLiteral *>(pair.second.get());
        if (literal == nullptr)
            return false;

        if (literal->value.getType() != Field::Types::UInt64)
            return false;

        if (pair.first == "min")
            res->min_sec = literal->value.get<UInt64>();
        else if (pair.first == "max")
            res->max_sec = literal->value.get<UInt64>();
        else
            return false;
    }

    if (!res->max_sec || !res->min_sec)
        return false;

    node = res;
    return true;
}


bool ParserDictionaryRange::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyValuePairsList key_value_pairs_p;
    ASTPtr ast_range;
    if (!key_value_pairs_p.parse(pos, ast_range, expected))
        return false;

    const ASTExpressionList & expr_list = ast_range->as<const ASTExpressionList &>();
    if (expr_list.children.size() != 2)
        return false;

    auto res = std::make_shared<ASTDictionaryRange>();
    for (const auto & elem : expr_list.children)
    {
        const ASTPair & pair = elem->as<const ASTPair &>();
        const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(pair.second.get());
        if (identifier == nullptr)
            return false;

        if (pair.first == "min")
            res->min_attr_name = identifier->name;
        else if (pair.first == "max")
            res->max_attr_name = identifier->name;
        else
            return false;
    }

    if (res->min_attr_name.empty() || res->max_attr_name.empty())
        return false;

    node = res;
    return true;
}

bool ParserDictionaryLayout::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserFunctionWithKeyValueArguments key_value_func_p;
    ASTPtr ast_func;
    if (!key_value_func_p.parse(pos, ast_func, expected))
        return false;

    const ASTFunctionWithKeyValueArguments & func = ast_func->as<const ASTFunctionWithKeyValueArguments &>();
    auto res = std::make_shared<ASTDictionaryLayout>();
    // here must be exactly one argument - layout_type
    if (func.children.size() > 1)
        return false;

    res->layout_type = func.name;
    const ASTExpressionList & type_expr_list = func.elements->as<const ASTExpressionList &>();
    // there doesn't exist a layout with more than 1 parameter
    if (type_expr_list.children.size() > 1)
        return false;

    if (type_expr_list.children.size() == 1)
    {
        const ASTPair * pair = dynamic_cast<const ASTPair *>(type_expr_list.children.at(0).get());
        // here only a pair is allowed
        if (pair == nullptr)
            return false;

        const ASTLiteral * literal = dynamic_cast<const ASTLiteral *>(pair->second.get());
        if (literal == nullptr || literal->value.getType() != Field::Types::UInt64)
            return false;
        res->parameter.emplace(pair->first, nullptr);
        res->set(res->parameter->second, literal->clone());
    }

    node = res;
    return true;
}


bool ParserDictionary::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword primary_key_keyword("PRIMARY KEY");
    ParserKeyword source_keyword("SOURCE");
    ParserKeyword lifetime_keyword("LIFETIME");
    ParserKeyword range_keyword("RANGE");
    ParserKeyword layout_keyword("LAYOUT");
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);
    ParserFunctionWithKeyValueArguments key_value_pairs_p;
    ParserList expression_list_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserDictionaryLifetime lifetime_p;
    ParserDictionaryRange range_p;
    ParserDictionaryLayout layout_p;

    ASTPtr primary_key;
    ASTPtr ast_source;
    ASTPtr ast_lifetime;
    ASTPtr ast_layout;
    ASTPtr ast_range;

    if (primary_key_keyword.ignore(pos) && !expression_list_p.parse(pos, primary_key, expected))
        return false;

    /// Exactly one of two keys should be defined
    if (!primary_key)
        return false;

    while (true)
    {
        if (!ast_source && source_keyword.ignore(pos, expected))
        {

            if (!open.ignore(pos))
                return false;

            if (!key_value_pairs_p.parse(pos, ast_source, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_lifetime && lifetime_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!lifetime_p.parse(pos, ast_lifetime, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_layout && layout_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!layout_p.parse(pos, ast_layout, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_range && range_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!range_p.parse(pos, ast_range, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        break;
    }

    auto query = std::make_shared<ASTDictionary>();
    node = query;
    if (primary_key)
        query->set(query->primary_key, primary_key);

    if (ast_source)
        query->set(query->source, ast_source);

    if (ast_lifetime)
        query->set(query->lifetime, ast_lifetime);

    if (ast_layout)
        query->set(query->layout, ast_layout);

    if (ast_range)
        query->set(query->range, ast_range);

    return true;
}


bool ParserCreateDictionaryQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_dictionary("DICTIONARY");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserIdentifier name_p;
    ParserToken s_left_paren(TokenType::OpeningRoundBracket);
    ParserToken s_right_paren(TokenType::ClosingRoundBracket);
    ParserToken s_dot(TokenType::Dot);
    ParserDictionaryAttributeDeclarationList attributes_p;
    ParserDictionary dictionary_p;


    bool if_not_exists = false;

    ASTPtr database;
    ASTPtr name;
    ASTPtr attributes;
    ASTPtr dictionary;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!s_dictionary.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        database = name;
        if (!name_p.parse(pos, name, expected))
            return false;
    }

    if (!s_left_paren.ignore(pos, expected))
        return false;

    if (!attributes_p.parse(pos, attributes, expected))
        return false;

    if (!s_right_paren.ignore(pos, expected))
        return false;

    if (!dictionary_p.parse(pos, dictionary, expected))
        return false;

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;
    query->is_dictionary = true;

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;

    query->table = typeid_cast<ASTIdentifier &>(*name).name;

    query->if_not_exists = if_not_exists;
    query->set(query->dictionary_attributes_list, attributes);
    query->set(query->dictionary, dictionary);

    return true;
}

}
