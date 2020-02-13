#include <Parsers/ParserGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserRoleList.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace
{
    bool parseAccessFlags(IParser::Pos & pos, Expected & expected, AccessFlags & access_flags)
    {
        auto is_one_of_access_type_words = [](IParser::Pos & pos_)
        {
            if (pos_->type != TokenType::BareWord)
                return false;
            std::string_view word{pos_->begin, pos_->size()};
            if (boost::iequals(word, "ON") || boost::iequals(word, "TO") || boost::iequals(word, "FROM"))
                return false;
            return true;
        };

        if (!is_one_of_access_type_words(pos))
        {
            expected.add(pos, "access type");
            return false;
        }

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

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            auto old_pos = pos;
            ++pos;
            if (pos->type == TokenType::ClosingRoundBracket)
            {
                ++pos;
                str += "()";
            }
            else
                pos = old_pos;
        }

        access_flags = AccessFlags{str};
        return true;
    }


    bool parseColumnNames(IParser::Pos & pos, Expected & expected, Strings & columns)
    {
        if (!ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
            return false;

        do
        {
            ASTPtr column_ast;
            if (!ParserIdentifier().parse(pos, column_ast, expected))
                return false;
            columns.push_back(getIdentifierName(column_ast));
        }
        while (ParserToken{TokenType::Comma}.ignore(pos, expected));

        return ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected);
    }


    bool parseDatabaseAndTableNameOrMaybeAsterisks(
        IParser::Pos & pos, Expected & expected, String & database_name, bool & any_database, String & table_name, bool & any_table)
    {
        ASTPtr ast[2];
        if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
        {
            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (!ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                    return false;

                /// *.* (any table in any database)
                any_database = true;
                any_table = true;
                return true;
            }
            else
            {
                /// * (any table in the current database)
                any_database = false;
                database_name = "";
                any_table = true;
                return true;
            }
        }
        else if (ParserIdentifier().parse(pos, ast[0], expected))
        {
            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// <database_name>.*
                    any_database = false;
                    database_name = getIdentifierName(ast[0]);
                    any_table = true;
                    return true;
                }
                else if (ParserIdentifier().parse(pos, ast[1], expected))
                {
                    /// <database_name>.<table_name>
                    any_database = false;
                    database_name = getIdentifierName(ast[0]);
                    any_table = false;
                    table_name = getIdentifierName(ast[1]);
                    return true;
                }
                else
                    return false;
            }
            else
            {
                /// <table_name>  - the current database, specified table
                any_database = false;
                database_name = "";
                table_name = getIdentifierName(ast[0]);
                return true;
            }
        }
        else
            return false;
    }
}


bool ParserGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    using Kind = ASTGrantQuery::Kind;
    Kind kind;
    if (ParserKeyword{"GRANT"}.ignore(pos, expected))
        kind = Kind::GRANT;
    else if (ParserKeyword{"REVOKE"}.ignore(pos, expected))
        kind = Kind::REVOKE;
    else
        return false;

    bool grant_option = false;
    if (kind == Kind::REVOKE)
    {
        if (ParserKeyword{"GRANT OPTION FOR"}.ignore(pos, expected))
            grant_option = true;
    }

    AccessRightsElements elements;
    do
    {
        std::vector<std::pair<AccessFlags, Strings>> access_and_columns;
        do
        {
            AccessFlags access_flags;
            if (!parseAccessFlags(pos, expected, access_flags))
                return false;

            Strings columns;
            parseColumnNames(pos, expected, columns);
            access_and_columns.emplace_back(access_flags, std::move(columns));
        }
        while (ParserToken{TokenType::Comma}.ignore(pos, expected));

        if (!ParserKeyword{"ON"}.ignore(pos, expected))
            return false;

        String database_name, table_name;
        bool any_database = false, any_table = false;
        if (!parseDatabaseAndTableNameOrMaybeAsterisks(pos, expected, database_name, any_database, table_name, any_table))
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
            elements.emplace_back(std::move(element));
        }
    }
    while (ParserToken{TokenType::Comma}.ignore(pos, expected));

    ASTPtr to_roles;
    if (kind == Kind::GRANT)
    {
        if (!ParserKeyword{"TO"}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (!ParserKeyword{"FROM"}.ignore(pos, expected))
            return false;
    }
    if (!ParserRoleList{}.parse(pos, to_roles, expected))
        return false;

    if (kind == Kind::GRANT)
    {
        if (ParserKeyword{"WITH GRANT OPTION"}.ignore(pos, expected))
            grant_option = true;
    }


    auto query = std::make_shared<ASTGrantQuery>();
    node = query;

    query->kind = kind;
    query->access_rights_elements = std::move(elements);
    query->to_roles = std::static_pointer_cast<ASTRoleList>(to_roles);
    query->grant_option = grant_option;

    return true;
}
}
