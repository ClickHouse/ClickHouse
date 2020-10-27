#include <Parsers/MySQL/ASTDeclareOption.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace MySQLParser
{

template <bool recursive>
bool ParserDeclareOptionImpl<recursive>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::unordered_map<String, ASTPtr> changes;
    std::unordered_map<String, std::shared_ptr<IParser>> usage_parsers_cached;
    usage_parsers_cached.reserve(options_collection.size());

    const auto & get_parser_from_cache = [&](const char * usage_name)
    {
        auto iterator = usage_parsers_cached.find(usage_name);
        if (iterator == usage_parsers_cached.end())
            iterator = usage_parsers_cached.insert(std::make_pair(usage_name, std::make_shared<ParserKeyword>(usage_name))).first;

        return iterator->second;
    };

    do
    {
        ASTPtr value;
        bool found{false};
        for (const auto & option_describe : options_collection)
        {
            if (strlen(option_describe.usage_name) == 0)
            {
                if (option_describe.value_parser->parse(pos, value, expected))
                {
                    found = true;
                    changes.insert(std::make_pair(option_describe.option_name, value));
                    break;
                }
            }
            else if (get_parser_from_cache(option_describe.usage_name)->ignore(pos, expected))
            {
                ParserToken{TokenType::Equals}.ignore(pos, expected);

                if (!option_describe.value_parser->parse(pos, value, expected))
                    return false;

                found = true;
                changes.insert(std::make_pair(option_describe.option_name, value));
                break;
            }
        }

        if (!found)
            break;
    } while (recursive);

    if (!changes.empty())
    {
        auto options_declare = std::make_shared<ASTDeclareOptions>();
        options_declare->changes = changes;

        node = options_declare;
    }

    return !changes.empty();
}

ASTPtr ASTDeclareOptions::clone() const
{
    auto res = std::make_shared<ASTDeclareOptions>(*this);
    res->children.clear();
    res->changes.clear();

    for (const auto & [name, value] : this->changes)
        res->changes.insert(std::make_pair(name, value->clone()));

    return res;
}

bool ParserAlwaysTrue::parseImpl(IParser::Pos & /*pos*/, ASTPtr & node, Expected & /*expected*/)
{
    node = std::make_shared<ASTLiteral>(Field(UInt64(1)));
    return true;
}

bool ParserAlwaysFalse::parseImpl(IParser::Pos & /*pos*/, ASTPtr & node, Expected & /*expected*/)
{
    node = std::make_shared<ASTLiteral>(Field(UInt64(0)));
    return true;
}

bool ParserCharsetName::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected &)
{
    /// Identifier in backquotes or in double quotes
    if (pos->type == TokenType::QuotedIdentifier)
    {
        ReadBufferFromMemory buf(pos->begin, pos->size());
        String s;

        if (*pos->begin == '`')
            readBackQuotedStringWithSQLStyle(s, buf);
        else
            readDoubleQuotedStringWithSQLStyle(s, buf);

        if (s.empty()) /// Identifiers "empty string" are not allowed.
            return false;

        node = std::make_shared<ASTIdentifier>(s);
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::BareWord)
    {
        const char * begin = pos->begin;

        while (true)
        {
            if (!isWhitespaceASCII(*pos->end) && pos->type != TokenType::EndOfStream)
                ++pos;
            else
                break;
        }

        node = std::make_shared<ASTIdentifier>(String(begin, pos->end));
        ++pos;
        return true;
    }

    return false;
}

template class ParserDeclareOptionImpl<true>;
template class ParserDeclareOptionImpl<false>;

}

}
