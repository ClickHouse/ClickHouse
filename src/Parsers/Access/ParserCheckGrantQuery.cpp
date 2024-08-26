#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserCheckGrantQuery.h>
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
            return !(boost::iequals(word, toStringView(Keyword::ON)) || boost::iequals(word, toStringView(Keyword::TO)) || boost::iequals(word, toStringView(Keyword::FROM)));
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

                if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
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

    void throwIfNotGrantable(AccessRightsElements & elements)
    {
        std::erase_if(elements, [](AccessRightsElement & element)
        {
            if (element.empty())
                return true;
            auto old_flags = element.access_flags;
            element.eraseNonGrantable();
            if (!element.empty())
                return false;

            if (!element.any_column)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot check grant on the column level", old_flags.toString());
            else if (!element.any_table)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot check grant on the table level", old_flags.toString());
            else if (!element.any_database)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot check grant on the database level", old_flags.toString());
            else if (!element.any_parameter)
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot check grant on the global with parameter level", old_flags.toString());
            else
                throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot check grant", old_flags.toString());
        });
    }
}


bool ParserCheckGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::CHECK_GRANT}.ignore(pos, expected))
        return false;


    AccessRightsElements elements;

    if (!parseElementsWithoutOptions(pos, expected, elements) )
        return false;

    throwIfNotGrantable(elements);

    auto query = std::make_shared<ASTCheckGrantQuery>();
    node = query;

    query->access_rights_elements = std::move(elements);

    return true;
}
}
