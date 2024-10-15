#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace
{
    bool parseProfileKeyword(IParserBase::Pos & pos, Expected & expected, bool use_inherit_keyword)
    {
        if (ParserKeyword{Keyword::PROFILE}.ignore(pos, expected))
            return true;

        if (use_inherit_keyword && ParserKeyword{Keyword::INHERIT}.ignore(pos, expected))
        {
            ParserKeyword{Keyword::PROFILE}.ignore(pos, expected);
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

            if (!ParserKeyword{Keyword::ID}.ignore(pos, expected))
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


    bool parseValue(IParserBase::Pos & pos, Expected & expected, std::optional<Field> & res)
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


    bool parseMinMaxValue(IParserBase::Pos & pos, Expected & expected, std::optional<Field> & min_value, std::optional<Field> & max_value)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            bool is_min_value = ParserKeyword{Keyword::MIN}.ignore(pos, expected);
            bool is_max_value = !is_min_value && ParserKeyword{Keyword::MAX}.ignore(pos, expected);
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


    bool parseConstraintWritabilityKeyword(IParserBase::Pos & pos, Expected & expected, std::optional<SettingConstraintWritability> & writability)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{Keyword::READONLY}.ignore(pos, expected) || ParserKeyword{Keyword::CONST}.ignore(pos, expected))
            {
                writability = SettingConstraintWritability::CONST;
                return true;
            }
            if (ParserKeyword{Keyword::WRITABLE}.ignore(pos, expected))
            {
                writability = SettingConstraintWritability::WRITABLE;
                return true;
            }
            if (ParserKeyword{Keyword::CHANGEABLE_IN_READONLY}.ignore(pos, expected))
            {
                writability = SettingConstraintWritability::CHANGEABLE_IN_READONLY;
                return true;
            }
            return false;
        });
    }


    bool parseSettingNameWithValueOrConstraints(
        IParserBase::Pos & pos,
        Expected & expected,
        String & setting_name,
        std::optional<Field> & value,
        std::optional<Field> & min_value,
        std::optional<Field> & max_value,
        std::optional<SettingConstraintWritability> & writability)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr name_ast;
            if (!ParserCompoundIdentifier{}.parse(pos, name_ast, expected))
                return false;

            String res_setting_name = getIdentifierName(name_ast);
            std::optional<Field> res_value;
            std::optional<Field> res_min_value;
            std::optional<Field> res_max_value;
            std::optional<SettingConstraintWritability> res_writability;

            bool has_value_or_constraint = false;
            while (parseValue(pos, expected, res_value) || parseMinMaxValue(pos, expected, res_min_value, res_max_value)
                   || parseConstraintWritabilityKeyword(pos, expected, res_writability))
            {
                has_value_or_constraint = true;
            }

            if (!has_value_or_constraint)
                return false;

            if (boost::iequals(res_setting_name, toStringView(Keyword::PROFILE)) && !res_value && !res_min_value && !res_max_value
                && res_writability == SettingConstraintWritability::CONST)
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
            writability = res_writability;
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
            std::optional<Field> value;
            std::optional<Field> min_value;
            std::optional<Field> max_value;
            std::optional<SettingConstraintWritability> writability;

            bool ok = parseSettingNameWithValueOrConstraints(pos, expected, setting_name, value, min_value, max_value, writability);

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
            result->writability = writability;
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

    if (ParserKeyword{Keyword::NONE}.ignore(pos, expected))
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
