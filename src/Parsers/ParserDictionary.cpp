#include <Parsers/ParserDictionary.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserDictionaryAttributeDeclaration.h>

#include <Poco/String.h>

#include <Parsers/ParserSetQuery.h>

namespace DB
{


bool ParserDictionaryLifetime::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserLiteral literal_p;
    ParserKeyValuePairsList key_value_pairs_p;
    ASTPtr ast_lifetime;
    auto res = std::make_shared<ASTDictionaryLifetime>();

    /// simple lifetime with only maximum value e.g. LIFETIME(300)
    if (literal_p.parse(pos, ast_lifetime, expected))
    {
        auto literal = ast_lifetime->as<const ASTLiteral &>();

        if (literal.value.getType() != Field::Types::UInt64)
            return false;

        res->max_sec = literal.value.safeGet<UInt64>();
        node = res;
        return true;
    }

    if (!key_value_pairs_p.parse(pos, ast_lifetime, expected))
        return false;

    const ASTExpressionList & expr_list = ast_lifetime->as<const ASTExpressionList &>();
    if (expr_list.children.size() != 2)
        return false;

    bool initialized_max = false;
    /// should contain both min and max
    for (const auto & elem : expr_list.children)
    {
        const ASTPair & pair = elem->as<const ASTPair &>();
        const ASTLiteral * literal = pair.second->as<ASTLiteral>();
        if (literal == nullptr)
            return false;

        if (literal->value.getType() != Field::Types::UInt64)
            return false;

        if (pair.first == "min")
            res->min_sec = literal->value.safeGet<UInt64>();
        else if (pair.first == "max")
        {
            res->max_sec = literal->value.safeGet<UInt64>();
            initialized_max = true;
        }
        else
            return false;
    }

    if (!initialized_max)
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
        const ASTIdentifier * identifier = pair.second->as<ASTIdentifier>();
        if (identifier == nullptr)
            return false;

        if (pair.first == "min")
            res->min_attr_name = identifier->name();
        else if (pair.first == "max")
            res->max_attr_name = identifier->name();
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
    ParserFunctionWithKeyValueArguments key_value_func_p(/* brackets_can_be_omitted = */ true);
    ASTPtr ast_func;
    if (!key_value_func_p.parse(pos, ast_func, expected))
        return false;

    const ASTFunctionWithKeyValueArguments & func = ast_func->as<const ASTFunctionWithKeyValueArguments &>();
    auto res = std::make_shared<ASTDictionaryLayout>();
    /// here must be exactly one argument - layout_type
    if (func.children.size() > 1)
        return false;

    res->layout_type = func.name;
    res->has_brackets = func.has_brackets;
    const ASTExpressionList & type_expr_list = func.elements->as<const ASTExpressionList &>();

    /// if layout has params than brackets must be specified
    if (!type_expr_list.children.empty() && !res->has_brackets)
        return false;

    res->set(res->parameters, func.elements);

    node = res;
    return true;
}

bool ParserDictionarySettings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    SettingsChanges changes;

    while (true)
    {
        if (!changes.empty() && !s_comma.ignore(pos))
            break;

        changes.push_back(SettingChange{});

        if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTDictionarySettings>();
    query->changes = std::move(changes);

    node = query;

    return true;
}


bool ParserDictionary::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword primary_key_keyword(Keyword::PRIMARY_KEY);
    ParserKeyword source_keyword(Keyword::SOURCE);
    ParserKeyword lifetime_keyword(Keyword::LIFETIME);
    ParserKeyword range_keyword(Keyword::RANGE);
    ParserKeyword layout_keyword(Keyword::LAYOUT);
    ParserKeyword settings_keyword(Keyword::SETTINGS);
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);
    ParserFunctionWithKeyValueArguments key_value_pairs_p;
    ParserList expression_list_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserDictionaryLifetime lifetime_p;
    ParserDictionaryRange range_p;
    ParserDictionaryLayout layout_p;
    ParserDictionarySettings settings_p;

    ASTPtr primary_key;
    ASTPtr ast_source;
    ASTPtr ast_lifetime;
    ASTPtr ast_layout;
    ASTPtr ast_range;
    ASTPtr ast_settings;

    /// Primary is required to be the first in dictionary definition
    if (primary_key_keyword.ignore(pos, expected))
    {
        bool was_open = false;

        if (open.ignore(pos, expected))
            was_open = true;

        if (!expression_list_p.parse(pos, primary_key, expected))
            return false;

        if (was_open && !close.ignore(pos, expected))
            return false;
    }

    /// Loop is used to avoid strict order of dictionary properties
    while (true)
    {
        if (!ast_source && source_keyword.ignore(pos, expected))
        {

            if (!open.ignore(pos, expected))
                return false;

            if (!key_value_pairs_p.parse(pos, ast_source, expected))
                return false;

            if (!close.ignore(pos, expected))
                return false;

            continue;
        }

        if (!ast_lifetime && lifetime_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos, expected))
                return false;

            if (!lifetime_p.parse(pos, ast_lifetime, expected))
                return false;

            if (!close.ignore(pos, expected))
                return false;

            continue;
        }

        if (!ast_layout && layout_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos, expected))
                return false;

            if (!layout_p.parse(pos, ast_layout, expected))
                return false;

            if (!close.ignore(pos, expected))
                return false;

            continue;
        }

        if (!ast_range && range_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos, expected))
                return false;

            if (!range_p.parse(pos, ast_range, expected))
                return false;

            if (!close.ignore(pos, expected))
                return false;

            continue;
        }

        if (!ast_settings && settings_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos, expected))
                return false;

            if (!settings_p.parse(pos, ast_settings, expected))
                return false;

            if (!close.ignore(pos, expected))
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

    if (ast_settings)
        query->set(query->dict_settings, ast_settings);

    return true;
}

}
