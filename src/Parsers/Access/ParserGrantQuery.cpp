#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
    extern const int SYNTAX_ERROR;
}

namespace
{
    bool parseAccessFlags(IParser::Pos & pos, Expected & expected, AccessFlags & access_flags)
    {
        static constexpr auto is_one_of_access_type_words = [](IParser::Pos & pos_)
        {
            if (pos_->type != TokenType::BareWord)
                return false;
            std::string_view word{pos_->begin, pos_->size()};
            return !(boost::iequals(word, "ON") || boost::iequals(word, "TO") || boost::iequals(word, "FROM"));
        };

        expected.add(pos, "access type");

        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!is_one_of_access_type_words(pos))
                return false;

            String str;
            do
            {
                if (!str.empty())
                    str += " ";
                str += std::string_view(pos->begin, pos->size());
                ++pos;
            }
            while (is_one_of_access_type_words(pos));

            try
            {
                access_flags = AccessFlags{str};
            }
            catch (...)
            {
                return false;
            }

            return true;
        });
    }


    bool parseColumnNames(IParser::Pos & pos, Expected & expected, Strings & columns)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserList{std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                return false;

            Strings res_columns;
            for (const auto & child : ast->children)
                res_columns.emplace_back(getIdentifierName(child));

            if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                return false;

            columns = std::move(res_columns);
            return true;
        });
    }

    bool parseAccessFlagsWithColumns(IParser::Pos & pos, Expected & expected,
                                     std::vector<std::pair<AccessFlags, Strings>> & access_and_columns)
    {
        std::vector<std::pair<AccessFlags, Strings>> res;

        auto parse_access_and_columns = [&]
        {
            AccessFlags access_flags;
            if (!parseAccessFlags(pos, expected, access_flags))
                return false;

            Strings columns;
            parseColumnNames(pos, expected, columns);
            res.emplace_back(access_flags, std::move(columns));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_access_and_columns, false))
            return false;

        access_and_columns = std::move(res);
        return true;
    }


    bool parseElementsWithoutOptions(IParser::Pos & pos, Expected & expected, AccessRightsElements & elements)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            AccessRightsElements res_elements;

            auto parse_around_on = [&]
            {
                std::vector<std::pair<AccessFlags, Strings>> access_and_columns;
                if (!parseAccessFlagsWithColumns(pos, expected, access_and_columns))
                    return false;

                String database_name, table_name, parameter;
                bool any_database = false, any_table = false, any_parameter = false;

                size_t is_global_with_parameter = 0;
                for (const auto & elem : access_and_columns)
                {
                    if (elem.first.isGlobalWithParameter())
                        ++is_global_with_parameter;
                }

                if (!ParserKeyword{"ON"}.ignore(pos, expected))
                    return false;

                if (is_global_with_parameter && is_global_with_parameter == access_and_columns.size())
                {
                    ASTPtr parameter_ast;
                    if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                    {
                        any_parameter = true;
                    }
                    else if (ParserIdentifier{}.parse(pos, parameter_ast, expected))
                    {
                        any_parameter = false;
                        parameter = getIdentifierName(parameter_ast);
                    }
                    else
                        return false;

                    any_database = any_table = true;
                }
                else if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, database_name, any_database, table_name, any_table))
                {
                    return false;
                }

                for (auto & [access_flags, columns] : access_and_columns)
                {
                    AccessRightsElement element;
                    element.access_flags = access_flags;
                    element.any_column = columns.empty();
                    element.columns = std::move(columns);
                    element.any_database = any_database;
                    element.database = database_name;
                    element.any_table = any_table;
                    element.any_parameter = any_parameter;
                    element.table = table_name;
                    element.parameter = parameter;
                    res_elements.emplace_back(std::move(element));
                }

                return true;
            };

            if (!ParserList::parseUtil(pos, expected, parse_around_on, false))
                return false;

            elements = std::move(res_elements);
            return true;
        });
    }

    bool parseCurrentGrants(IParser::Pos & pos, Expected & expected, AccessRightsElements & elements)
    {
        if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        {
            if (!parseElementsWithoutOptions(pos, expected, elements))
                return false;

            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;
        }
        else
        {
            AccessRightsElement default_element(AccessType::ALL);

            if (!ParserKeyword{"ON"}.ignore(pos, expected))
                return false;

            String database_name, table_name;
            bool any_database = false, any_table = false;
            if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, database_name, any_database, table_name, any_table))
                return false;

            default_element.any_database = any_database;
            default_element.database = database_name;
            default_element.any_table = any_table;
            default_element.table = table_name;
            elements.push_back(std::move(default_element));
        }

        return true;
    }

    void throwIfNotGrantable(AccessRightsElements & elements)
    {
        boost::range::remove_erase_if(elements, [](AccessRightsElement & element)
        {
            if (element.empty())
                return true;
            auto old_flags = element.access_flags;
            element.eraseNonGrantable();
            if (!element.empty())
                return false;

            if (!element.any_column)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the column level", old_flags.toString());
            else if (!element.any_table)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the table level", old_flags.toString());
            else if (!element.any_database)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the database level", old_flags.toString());
            else if (!element.any_parameter)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the global with parameter level", old_flags.toString());
            else
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted", old_flags.toString());
        });
    }


    bool parseRoles(IParser::Pos & pos, Expected & expected, bool is_revoke, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().useIDMode(id_mode);
            if (is_revoke)
                roles_p.allowAll();

            ASTPtr ast;
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            return true;
        });
    }


    bool parseToGrantees(IParser::Pos & pos, Expected & expected, bool is_revoke, std::shared_ptr<ASTRolesOrUsersSet> & grantees)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{is_revoke ? "FROM" : "TO"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().allowUsers().allowCurrentUser().allowAll(is_revoke);
            if (!roles_p.parse(pos, ast, expected))
                return false;

            grantees = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (attach_mode && !ParserKeyword{"ATTACH"}.ignore(pos, expected))
        return false;

    bool is_replace = false;
    bool is_revoke = false;
    if (ParserKeyword{"REVOKE"}.ignore(pos, expected))
        is_revoke = true;
    else if (!ParserKeyword{"GRANT"}.ignore(pos, expected))
        return false;

    String cluster;
    parseOnCluster(pos, expected, cluster);

    bool grant_option = false;
    bool admin_option = false;
    if (is_revoke)
    {
        if (ParserKeyword{"GRANT OPTION FOR"}.ignore(pos, expected))
            grant_option = true;
        else if (ParserKeyword{"ADMIN OPTION FOR"}.ignore(pos, expected))
            admin_option = true;
    }

    AccessRightsElements elements;
    std::shared_ptr<ASTRolesOrUsersSet> roles;

    bool current_grants = false;
    if (!is_revoke && ParserKeyword{"CURRENT GRANTS"}.ignore(pos, expected))
    {
        current_grants = true;
        if (!parseCurrentGrants(pos, expected, elements))
            return false;
    }
    else
    {
        if (!parseElementsWithoutOptions(pos, expected, elements) && !parseRoles(pos, expected, is_revoke, attach_mode, roles))
            return false;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    std::shared_ptr<ASTRolesOrUsersSet> grantees;
    if (!parseToGrantees(pos, expected, is_revoke, grantees) && !allow_no_grantees)
        return false;

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (!is_revoke)
    {
        if (ParserKeyword{"WITH GRANT OPTION"}.ignore(pos, expected))
            grant_option = true;
        else if (ParserKeyword{"WITH ADMIN OPTION"}.ignore(pos, expected))
            admin_option = true;

        if (ParserKeyword{"WITH REPLACE OPTION"}.ignore(pos, expected))
            is_replace = true;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (grant_option && roles)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "GRANT OPTION should be specified for access types");
    if (admin_option && !elements.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "ADMIN OPTION should be specified for roles");

    if (grant_option)
    {
        for (auto & element : elements)
            element.grant_option = true;
    }


    bool replace_access = false;
    bool replace_role = false;
    if (is_replace)
    {
        if (roles)
            replace_role = true;
        else
            replace_access = true;
    }

    if (!is_revoke)
    {
        if (attach_mode)
            elements.eraseNonGrantable();
        else
            throwIfNotGrantable(elements);
    }

    auto query = std::make_shared<ASTGrantQuery>();
    node = query;

    query->is_revoke = is_revoke;
    query->attach_mode = attach_mode;
    query->cluster = std::move(cluster);
    query->access_rights_elements = std::move(elements);
    query->roles = std::move(roles);
    query->grantees = std::move(grantees);
    query->admin_option = admin_option;
    query->replace_access = replace_access;
    query->replace_granted_roles = replace_role;
    query->current_grants = current_grants;

    return true;
}
}
