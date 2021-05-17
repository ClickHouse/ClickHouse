#include <Parsers/ParserGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserRolesOrUsersSet.h>
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
                std::string_view word{pos->begin, pos->size()};
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

                if (!ParserKeyword{"ON"}.ignore(pos, expected))
                    return false;

                String database_name, table_name;
                bool any_database = false, any_table = false;
                if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, database_name, any_database, table_name, any_table))
                    return false;

                for (auto & [access_flags, columns] : access_and_columns)
                {
                    AccessRightsElement element;
                    element.access_flags = access_flags;
                    element.any_column = columns.empty();
                    element.columns = std::move(columns);
                    element.any_database = any_database;
                    element.database = database_name;
                    element.any_table = any_table;
                    element.table = table_name;
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


    void eraseNonGrantable(AccessRightsElements & elements)
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
                throw Exception(old_flags.toString() + " cannot be granted on the column level", ErrorCodes::INVALID_GRANT);
            else if (!element.any_table)
                throw Exception(old_flags.toString() + " cannot be granted on the table level", ErrorCodes::INVALID_GRANT);
            else if (!element.any_database)
                throw Exception(old_flags.toString() + " cannot be granted on the database level", ErrorCodes::INVALID_GRANT);
            else
                throw Exception(old_flags.toString() + " cannot be granted", ErrorCodes::INVALID_GRANT);
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
    if (!parseElementsWithoutOptions(pos, expected, elements) && !parseRoles(pos, expected, is_revoke, attach_mode, roles))
        return false;

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    std::shared_ptr<ASTRolesOrUsersSet> grantees;
    if (!parseToGrantees(pos, expected, is_revoke, grantees))
        return false;

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (!is_revoke)
    {
        if (ParserKeyword{"WITH GRANT OPTION"}.ignore(pos, expected))
            grant_option = true;
        else if (ParserKeyword{"WITH ADMIN OPTION"}.ignore(pos, expected))
            admin_option = true;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (grant_option && roles)
        throw Exception("GRANT OPTION should be specified for access types", ErrorCodes::SYNTAX_ERROR);
    if (admin_option && !elements.empty())
        throw Exception("ADMIN OPTION should be specified for roles", ErrorCodes::SYNTAX_ERROR);

    if (grant_option)
    {
        for (auto & element : elements)
            element.grant_option = true;
    }

    if (!is_revoke)
        eraseNonGrantable(elements);

    auto query = std::make_shared<ASTGrantQuery>();
    node = query;

    query->is_revoke = is_revoke;
    query->attach_mode = attach_mode;
    query->cluster = std::move(cluster);
    query->access_rights_elements = std::move(elements);
    query->roles = std::move(roles);
    query->grantees = std::move(grantees);
    query->admin_option = admin_option;

    return true;
}
}
