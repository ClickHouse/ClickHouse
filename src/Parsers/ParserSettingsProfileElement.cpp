#include <Parsers/ParserSettingsProfileElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace
{
    bool parseProfileKeyword(IParserBase::Pos & pos, Expected & expected, bool use_inherit_keyword)
    {
        if (ParserKeyword{"PROFILE"}.ignore(pos, expected))
            return true;

        if (use_inherit_keyword && ParserKeyword{"INHERIT"}.ignore(pos, expected))
        {
            ParserKeyword{"PROFILE"}.ignore(pos, expected);
            return true;
        }

        return false;
    }


    bool parseProfileNameOrID(IParserBase::Pos & pos, Expected & expected, bool id_mode, String & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!id_mode)
                return parseIdentifierOrStringLiteral(pos, expected, res);

            if (!ParserKeyword{"ID"}.ignore(pos, expected))
                return false;
            if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
                return false;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            String id = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;

            res = std::move(id);
            return true;
        });
    }


    bool parseValue(IParserBase::Pos & pos, Expected & expected, Field & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::Equals}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserLiteral{}.parse(pos, ast, expected))
                return false;

            res = ast->as<ASTLiteral &>().value;
            return true;
        });
    }


    bool parseMinMaxValue(IParserBase::Pos & pos, Expected & expected, Field & min_value, Field & max_value)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            bool is_min_value = ParserKeyword{"MIN"}.ignore(pos, expected);
            bool is_max_value = !is_min_value && ParserKeyword{"MAX"}.ignore(pos, expected);
            if (!is_min_value && !is_max_value)
                return false;

            ParserToken{TokenType::Equals}.ignore(pos, expected);

            ASTPtr ast;
            if (!ParserLiteral{}.parse(pos, ast, expected))
                return false;

            auto min_or_max_value = ast->as<ASTLiteral &>().value;

            if (is_min_value)
                min_value = min_or_max_value;
            else
                max_value = min_or_max_value;
            return true;
        });
    }


    bool parseReadonlyOrWritableKeyword(IParserBase::Pos & pos, Expected & expected, std::optional<bool> & readonly)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{"READONLY"}.ignore(pos, expected))
            {
                readonly = true;
                return true;
            }
            else if (ParserKeyword{"WRITABLE"}.ignore(pos, expected))
            {
                readonly = false;
                return true;
            }
            else
                return false;
        });
    }


    bool parseSettingNameWithValueOrConstraints(
        IParserBase::Pos & pos,
        Expected & expected,
        String & setting_name,
        Field & value,
        Field & min_value,
        Field & max_value,
        std::optional<bool> & readonly)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr name_ast;
            if (!ParserCompoundIdentifier{}.parse(pos, name_ast, expected))
                return false;

            String res_setting_name = getIdentifierName(name_ast);
            Field res_value;
            Field res_min_value;
            Field res_max_value;
            std::optional<bool> res_readonly;

            bool has_value_or_constraint = false;
            while (parseValue(pos, expected, res_value) || parseMinMaxValue(pos, expected, res_min_value, res_max_value)
                   || parseReadonlyOrWritableKeyword(pos, expected, res_readonly))
            {
                has_value_or_constraint = true;
            }

            if (!has_value_or_constraint)
                return false;

            if (boost::iequals(res_setting_name, "PROFILE") && res_value.isNull() && res_min_value.isNull() && res_max_value.isNull()
                && res_readonly)
            {
                /// Ambiguity: "profile readonly" can be treated either as a profile named "readonly" or
                /// as a setting named 'profile' with the readonly constraint.
                /// So we've decided to treat it as a profile named "readonly".
                return false;
            }

            setting_name = std::move(res_setting_name);
            value = std::move(res_value);
            min_value = std::move(res_min_value);
            max_value = std::move(res_max_value);
            readonly = res_readonly;
            return true;
        });
    }


    bool parseSettingsProfileElement(IParserBase::Pos & pos,
                                     Expected & expected,
                                     bool id_mode,
                                     bool use_inherit_keyword,
                                     bool previous_element_was_parent_profile,
                                     std::shared_ptr<ASTSettingsProfileElement> & result)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String parent_profile;
            String setting_name;
            Field value;
            Field min_value;
            Field max_value;
            std::optional<bool> readonly;

            bool ok = parseSettingNameWithValueOrConstraints(pos, expected, setting_name, value, min_value, max_value, readonly);

            if (!ok && (parseProfileKeyword(pos, expected, use_inherit_keyword) || previous_element_was_parent_profile))
                ok = parseProfileNameOrID(pos, expected, id_mode, parent_profile);

            if (!ok)
                return false;

            result = std::make_shared<ASTSettingsProfileElement>();
            result->parent_profile = std::move(parent_profile);
            result->setting_name = std::move(setting_name);
            result->value = std::move(value);
            result->min_value = std::move(min_value);
            result->max_value = std::move(max_value);
            result->readonly = readonly;
            result->id_mode = id_mode;
            result->use_inherit_keyword = use_inherit_keyword;
            return true;
        });
    }
}


bool ParserSettingsProfileElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::shared_ptr<ASTSettingsProfileElement> res;
    if (!parseSettingsProfileElement(pos, expected, id_mode, use_inherit_keyword, false, res))
        return false;

    node = res;
    return true;
}


bool ParserSettingsProfileElements::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> elements;

    if (ParserKeyword{"NONE"}.ignore(pos, expected))
    {
    }
    else
    {
        bool previous_element_was_parent_profile = false;

        auto parse_element = [&]
        {
            std::shared_ptr<ASTSettingsProfileElement> element;
            if (!parseSettingsProfileElement(pos, expected, id_mode, use_inherit_keyword, previous_element_was_parent_profile, element))
                return false;

            elements.push_back(element);
            previous_element_was_parent_profile = !element->parent_profile.empty();
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_element, false))
            return false;
    }

    auto result = std::make_shared<ASTSettingsProfileElements>();
    result->elements = std::move(elements);
    node = result;
    return true;
}

}
