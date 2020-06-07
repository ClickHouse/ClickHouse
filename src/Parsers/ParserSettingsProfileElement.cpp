#include <Parsers/ParserSettingsProfileElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{
namespace
{
    bool parseProfileNameOrID(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, bool parse_id, String & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!parse_id)
                return parseIdentifierOrStringLiteral(pos, expected, ranges, res);

            if (!ParserKeyword{"ID"}.ignore(pos, expected, ranges))
                return false;
            if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected, ranges))
                return false;
            if (!ParserStringLiteral{}.parse(pos, ast, expected, ranges))
                return false;
            String id = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected, ranges))
                return false;

            res = std::move(id);
            return true;
        });
    }


    bool parseValue(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, Field & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::Equals}.ignore(pos, expected, ranges))
                return false;

            ASTPtr ast;
            if (!ParserLiteral{}.parse(pos, ast, expected, ranges))
                return false;

            res = ast->as<ASTLiteral &>().value;
            return true;
        });
    }


    bool parseMinMaxValue(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, Field & min_value, Field & max_value)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            bool is_min_value = ParserKeyword{"MIN"}.ignore(pos, expected, ranges);
            bool is_max_value = !is_min_value && ParserKeyword{"MAX"}.ignore(pos, expected, ranges);
            if (!is_min_value && !is_max_value)
                return false;

            ParserToken{TokenType::Equals}.ignore(pos, expected, ranges);

            ASTPtr ast;
            if (!ParserLiteral{}.parse(pos, ast, expected, ranges))
                return false;

            auto min_or_max_value = ast->as<ASTLiteral &>().value;

            if (is_min_value)
                min_value = min_or_max_value;
            else
                max_value = min_or_max_value;
            return true;
        });
    }


    bool parseReadonlyOrWritableKeyword(IParserBase::Pos & pos, Expected & expected, IParser::Ranges * ranges, std::optional<bool> & readonly)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{"READONLY"}.ignore(pos, expected, ranges))
            {
                readonly = true;
                return true;
            }
            else if (ParserKeyword{"READONLY"}.ignore(pos, expected, ranges))
            {
                readonly = false;
                return true;
            }
            else
                return false;
        });
    }
}


bool ParserSettingsProfileElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    String parent_profile;
    String setting_name;
    Field value;
    Field min_value;
    Field max_value;
    std::optional<bool> readonly;

    if (ParserKeyword{"PROFILE"}.ignore(pos, expected, ranges) ||
        (enable_inherit_keyword && ParserKeyword{"INHERIT"}.ignore(pos, expected, ranges)))
    {
        if (!parseProfileNameOrID(pos, expected, ranges, id_mode, parent_profile))
            return false;
    }
    else
    {
        ASTPtr name_ast;
        if (!ParserIdentifier{}.parse(pos, name_ast, expected, ranges))
            return false;
        setting_name = getIdentifierName(name_ast);

        bool has_value_or_constraint = false;
        while (parseValue(pos, expected, ranges, value) || parseMinMaxValue(pos, expected, ranges, min_value, max_value)
               || parseReadonlyOrWritableKeyword(pos, expected, ranges, readonly))
        {
            has_value_or_constraint = true;
        }

        if (!has_value_or_constraint)
            return false;
    }

    auto result = std::make_shared<ASTSettingsProfileElement>();
    result->parent_profile = std::move(parent_profile);
    result->setting_name = std::move(setting_name);
    result->value = std::move(value);
    result->min_value = std::move(min_value);
    result->max_value = std::move(max_value);
    result->readonly = readonly;
    result->id_mode = id_mode;
    result->use_inherit_keyword = enable_inherit_keyword;
    node = result;
    return true;
}


bool ParserSettingsProfileElements::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> elements;

    if (ParserKeyword{"NONE"}.ignore(pos, expected, ranges))
    {
    }
    else
    {
        do
        {
            ASTPtr ast;
            if (!ParserSettingsProfileElement{}.useIDMode(id_mode).enableInheritKeyword(enable_inherit_keyword).parse(pos, ast, expected, ranges))
                return false;
            auto element = typeid_cast<std::shared_ptr<ASTSettingsProfileElement>>(ast);
            elements.push_back(std::move(element));
        }
        while (ParserToken{TokenType::Comma}.ignore(pos, expected, ranges));
    }

    auto result = std::make_shared<ASTSettingsProfileElements>();
    result->elements = std::move(elements);
    node = result;
    return true;
}

}
