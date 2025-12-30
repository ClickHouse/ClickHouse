#include <Parsers/Access/parseAccessRightsElements.h>

#include <Access/Common/AccessRightsElement.h>
#include <Common/re2.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

#include <boost/algorithm/string/predicate.hpp>


namespace DB
{

namespace
{
    bool parseParameterRegExp(IParser::Pos & pos, Expected & expected, String & parameter_regexp)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
                return true;

            if (!parseIdentifierOrStringLiteral(pos, expected, parameter_regexp))
                return false;

            re2::RE2::Options options;
            options.set_log_errors(false);
            if (const re2::RE2 re(parameter_regexp, options); !re.ok())
            {
                expected.add(pos, "valid regexp");
                return false;
            }

            if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                return false;

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
}

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


bool parseAccessRightsElementsWithoutOptions(IParser::Pos & pos, Expected & expected, AccessRightsElements & elements)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        AccessRightsElements res_elements;

        auto parse_around_on = [&]
        {
            std::vector<std::pair<AccessFlags, Strings>> access_and_columns;
            if (!parseAccessFlagsWithColumns(pos, expected, access_and_columns))
                return false;

            String database_name;
            String table_name;
            String parameter;
            String filter;

            size_t is_global_with_parameter = 0;
            for (const auto & elem : access_and_columns)
            {
                if (elem.first.isGlobalWithParameter())
                    ++is_global_with_parameter;
            }

            if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
                return false;

            bool wildcard = false;
            bool default_database = false;
            if (is_global_with_parameter && is_global_with_parameter == access_and_columns.size())
            {
                ASTPtr parameter_ast;
                // *[.*]
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    ParserToken{TokenType::Dot}.ignore(pos, expected);
                    ParserToken{TokenType::Asterisk}.ignore(pos, expected);
                }
                else
                {
                    if (ParserIdentifier{}.parse(pos, parameter_ast, expected))
                        parameter = getIdentifierName(parameter_ast);
                    else
                        return false;
                }

                auto add_to_expected = [&](const char * name) { expected.add(pos, name); };
                for (const auto & elem : access_and_columns)
                {
                    if (!elem.first.validateParameter(parameter, add_to_expected))
                        return false;
                }

                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                    wildcard = true;

                /// GRANT READ ON S3('s3://foo/*')
                if (!parseParameterRegExp(pos, expected, filter))
                    return false;

                /// GRANT READ ON *(foo) is prohibited.
                if ((wildcard || parameter.empty()) && !filter.empty())
                    return false;
            }
            else if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, database_name, table_name, wildcard, default_database))
                return false;

            for (auto & [access_flags, columns] : access_and_columns)
            {
                if ((wildcard || table_name.empty()) && !columns.empty())
                    return false;

                AccessRightsElement element;
                element.access_flags = access_flags;
                element.columns = std::move(columns);
                element.database = database_name;
                element.table = table_name;
                element.parameter = parameter;
                element.filter = filter;

                element.wildcard = wildcard;
                element.default_database = default_database;
                res_elements.emplace_back(std::move(element));
            }

            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_around_on, false))
            return false;

        res_elements.replaceDeprecated();
        elements = std::move(res_elements);
        return true;
    });
}

}
