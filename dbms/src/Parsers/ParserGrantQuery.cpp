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
    bool parseRoundBrackets(IParser::Pos & pos, Expected & expected)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected)
                && ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected);
        });
    }

    bool parseAccessFlags(IParser::Pos & pos, Expected & expected, AccessFlags & access_flags)
    {
        static constexpr auto is_one_of_access_type_words = [](IParser::Pos & pos_)
        {
            if (pos_->type != TokenType::BareWord)
                return false;
            std::string_view word{pos_->begin, pos_->size()};
            if (boost::iequals(word, "ON") || boost::iequals(word, "TO") || boost::iequals(word, "FROM"))
                return false;
            return true;
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

            parseRoundBrackets(pos, expected);
            return true;
        });
    }


    bool parseColumnNames(IParser::Pos & pos, Expected & expected, Strings & columns)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
                return false;

            Strings res_columns;
            do
            {
                ASTPtr column_ast;
                if (!ParserIdentifier().parse(pos, column_ast, expected))
                    return false;
                res_columns.emplace_back(getIdentifierName(column_ast));
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                return false;

            columns = std::move(res_columns);
            return true;
        });
    }


    bool parseDatabaseAndTableNameOrMaybeAsterisks(
        IParser::Pos & pos, Expected & expected, String & database_name, bool & any_database, String & table_name, bool & any_table)
    {
        return IParserBase::wrapParseImpl(pos, [&]
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
                    database_name.clear();
                    any_table = true;
                    table_name.clear();
                    return true;
                }

                /// * (any table in the current database)
                any_database = false;
                database_name.clear();
                any_table = true;
                table_name.clear();
                return true;
            }

            if (!ParserIdentifier().parse(pos, ast[0], expected))
                return false;

            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// <database_name>.*
                    any_database = false;
                    database_name = getIdentifierName(ast[0]);
                    any_table = true;
                    table_name.clear();
                    return true;
                }

                if (!ParserIdentifier().parse(pos, ast[1], expected))
                    return false;

                /// <database_name>.<table_name>
                any_database = false;
                database_name = getIdentifierName(ast[0]);
                any_table = false;
                table_name = getIdentifierName(ast[1]);
                return true;
            }

            /// <table_name>  - the current database, specified table
            any_database = false;
            database_name.clear();
            any_table = false;
            table_name = getIdentifierName(ast[0]);
            return true;
        });
    }


    bool parseAccessRightsElements(IParser::Pos & pos, Expected & expected, AccessRightsElements & elements)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            AccessRightsElements res_elements;
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
                    res_elements.emplace_back(std::move(element));
                }
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            elements = std::move(res_elements);
            return true;
        });
    }


    bool parseToRoles(IParser::Pos & pos, Expected & expected, ASTGrantQuery::Kind kind, std::shared_ptr<ASTRoleList> & to_roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            using Kind = ASTGrantQuery::Kind;
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

            ASTPtr ast;
            if (!ParserRoleList{false, false}.parse(pos, ast, expected))
                return false;

            to_roles = typeid_cast<std::shared_ptr<ASTRoleList>>(ast);
            return true;
        });
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
    std::shared_ptr<ASTRoleList> to_roles;
    if (!parseAccessRightsElements(pos, expected, elements) && !parseToRoles(pos, expected, kind, to_roles))
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
    query->to_roles = std::move(to_roles);
    query->grant_option = grant_option;

    return true;
}
}
