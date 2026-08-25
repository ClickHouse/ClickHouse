#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/algorithm/string/predicate.hpp>
#include <base/insertAtEnd.h>


namespace DB
{
namespace
{
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


    bool parseSettingName(IParserBase::Pos & pos, Expected & expected, String & res)
    {
        ASTPtr name_ast;
        if (!ParserCompoundIdentifier{}.parse(pos, name_ast, expected))
            return false;

        res = getIdentifierName(name_ast);
        return true;
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
            String res_setting_name;
            if (!parseSettingName(pos, expected, res_setting_name))
                return false;

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


    bool parseSettingsProfileElements(IParserBase::Pos & pos,
                                      Expected & expected,
                                      bool id_mode,
                                      bool use_inherit_keyword,
                                      std::vector<std::shared_ptr<ASTSettingsProfileElement>> & res)
    {
        std::vector<std::shared_ptr<ASTSettingsProfileElement>> elements;
        bool found_none = false;

        bool expect_profiles = false;
        bool expect_settings = false;

        auto parse_element = [&]
        {
            if (ParserKeyword{Keyword::SETTINGS}.ignore(pos, expected) || ParserKeyword{Keyword::SETTING}.ignore(pos, expected))
            {
                expect_settings = true;
            }

            bool expect_settings_next = expect_settings;

            if (ParserKeyword{Keyword::PROFILES}.ignore(pos, expected) || ParserKeyword{Keyword::PROFILE}.ignore(pos, expected))
            {
                expect_profiles = true;
                expect_settings = false;
            }
            else if (use_inherit_keyword && ParserKeyword{Keyword::INHERIT}.ignore(pos, expected))
            {
                if (!ParserKeyword{Keyword::PROFILES}.ignore(pos, expected))
                    ParserKeyword{Keyword::PROFILE}.ignore(pos, expected);
                expect_profiles = true;
                expect_settings = false;
            }

            if (!expect_profiles && !expect_settings)
                return false;

            if (ParserKeyword{Keyword::NONE}.ignore(pos, expected))
            {
                found_none = true;
                expect_settings = expect_settings_next;
                return true;
            }

            if (expect_settings)
            {
                String setting_name;
                std::optional<Field> value;
                std::optional<Field> min_value;
                std::optional<Field> max_value;
                std::optional<SettingConstraintWritability> writability;
                if (parseSettingNameWithValueOrConstraints(pos, expected, setting_name, value, min_value, max_value, writability))
                {
                    auto element = std::make_shared<ASTSettingsProfileElement>();
                    element->setting_name = std::move(setting_name);
                    element->value = std::move(value);
                    element->min_value = std::move(min_value);
                    element->max_value = std::move(max_value);
                    element->writability = std::move(writability);
                    elements.push_back(element);
                    expect_profiles = false;
                    expect_settings = expect_settings_next;
                    return true;
                }
            }

            if (expect_profiles)
            {
                String profile_name;
                if (parseProfileNameOrID(pos, expected, id_mode, profile_name))
                {
                    auto element = std::make_shared<ASTSettingsProfileElement>();
                    element->parent_profile = std::move(profile_name);
                    element->id_mode = id_mode;
                    element->use_inherit_keyword = use_inherit_keyword;
                    elements.push_back(element);
                    expect_settings = expect_settings_next;
                    return true;
                }
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_element, false))
            return false;

        if (elements.empty() && !found_none)
            return false;

        res = std::move(elements);
        return true;
    }
}


bool ParserSettingsProfileElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> elements;
    if (!parseSettingsProfileElements(pos, expected, id_mode, use_inherit_keyword, elements))
        return false;

    if (elements.size() != 1)
        return false;

    node = elements[0];
    return true;
}


bool ParserSettingsProfileElements::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> elements;
    if (!parseSettingsProfileElements(pos, expected, id_mode, use_inherit_keyword, elements))
        return false;

    auto result = std::make_shared<ASTSettingsProfileElements>();
    result->elements = std::move(elements);
    node = result;
    return true;
}


bool ParserAlterSettingsProfileElements::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> add_settings;
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> modify_settings;
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> drop_settings;
    bool drop_all_settings = false;
    bool drop_all_profiles = false;

    std::vector<std::shared_ptr<ASTSettingsProfileElement>> old_style_settings;
    if (parseSettingsProfileElements(pos, expected, /* id_mode= */ false, use_inherit_keyword, old_style_settings))
    {
        /// old style: "SETTINGS ..." replaces all the settings ad profiles.
        add_settings = std::move(old_style_settings);
        drop_all_settings = true;
        drop_all_profiles = true;
    }
    else
    {
        /// new style: "MODIFY SETTINGS ..., ADD PROFILES ..., DROP PROFILES ..., DROP SETTINGS ..."
        std::string_view action;
        std::string_view target;

        auto parse_element = [&]
        {
            if (ParserKeyword{Keyword::ADD}.ignore(pos, expected))
            {
                action = "ADD";
                target = "";
            }
            else if (ParserKeyword{Keyword::DROP}.ignore(pos, expected))
            {
                /// `DROP` statements should be at the beginning of the query.
                if (!add_settings.empty() || !modify_settings.empty())
                    return false;
                action = "DROP";
                target = "";
            }
            else if (ParserKeyword{Keyword::MODIFY}.ignore(pos, expected))
            {
                action = "MODIFY";
                target = "";
            }

            if (!action.empty())
            {
                if (ParserKeyword{Keyword::ALL_PROFILES}.ignore(pos, expected))
                    target = "ALL PROFILES";
                else if (ParserKeyword{Keyword::ALL_SETTINGS}.ignore(pos, expected))
                    target = "ALL SETTINGS";
                else if (ParserKeyword{Keyword::PROFILES}.ignore(pos, expected) || ParserKeyword{Keyword::PROFILE}.ignore(pos, expected))
                    target = "PROFILES";
                else if (ParserKeyword{Keyword::SETTINGS}.ignore(pos, expected) || ParserKeyword{Keyword::SETTING}.ignore(pos, expected))
                    target = "SETTINGS";
            }

            if (target.empty())
                return false;

            if (target == "PROFILES")
            {
                auto element = std::make_shared<ASTSettingsProfileElement>();
                if (!parseProfileNameOrID(pos, expected, /* id_mode= */ false, element->parent_profile))
                    return false;
                if (action == "ADD")
                {
                    add_settings.push_back(element);
                    return true;
                }
                if (action == "DROP")
                {
                    drop_settings.push_back(element);
                    return true;
                }
                return false;
            }

            if (target == "SETTINGS")
            {
                auto element = std::make_shared<ASTSettingsProfileElement>();
                if (action == "ADD" || action == "MODIFY")
                {
                    if (!parseSettingNameWithValueOrConstraints(pos, expected, element->setting_name, element->value, element->min_value, element->max_value, element->writability))
                        return false;
                    if (action == "ADD")
                        add_settings.push_back(element);
                    else
                        modify_settings.push_back(element);
                    return true;
                }
                if (action == "DROP")
                {
                    ASTPtr name_ast;
                    if (!ParserCompoundIdentifier{}.parse(pos, name_ast, expected))
                        return false;
                    element->setting_name = getIdentifierName(name_ast);
                    drop_settings.push_back(element);
                    return true;
                }
                return false;
            }

            if (action == "DROP" && target == "ALL PROFILES" && !drop_all_profiles)
            {
                drop_all_profiles = true;
                return true;
            }

            if (action == "DROP" && target == "ALL SETTINGS" && !drop_all_settings)
            {
                drop_all_settings = true;
                return true;
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_element, false))
            return false;
    }

    if (add_settings.empty() && modify_settings.empty() && drop_settings.empty() && !drop_all_settings && !drop_all_profiles)
        return false;

    auto result = std::make_shared<ASTAlterSettingsProfileElements>();
    if (!add_settings.empty())
    {
        result->add_settings = std::make_shared<ASTSettingsProfileElements>();
        result->add_settings->elements = std::move(add_settings);
    }
    if (!modify_settings.empty())
    {
        result->modify_settings = std::make_shared<ASTSettingsProfileElements>();
        result->modify_settings->elements = std::move(modify_settings);
    }
    if (!drop_settings.empty())
    {
        result->drop_settings = std::make_shared<ASTSettingsProfileElements>();
        result->drop_settings->elements = std::move(drop_settings);
    }
    result->drop_all_settings = drop_all_settings;
    result->drop_all_profiles = drop_all_profiles;

    node = result;
    return true;
}

}
