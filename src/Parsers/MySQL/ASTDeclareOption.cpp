#include <Parsers/MySQL/ASTDeclareOption.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>

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
    node = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
    return true;
}

bool ParserAlwaysFalse::parseImpl(IParser::Pos & /*pos*/, ASTPtr & node, Expected & /*expected*/)
{
    node = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(0)));
    return true;
}

bool ParserCharsetOrCollateName::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier p_identifier;
    ParserStringLiteral p_string_literal;

    if (p_identifier.parse(pos, node, expected))
        return true;
    else
    {
        if (p_string_literal.parse(pos, node, expected))
        {
            const auto & string_value = node->as<ASTLiteral>()->value.safeGet<String>();
            node = std::make_shared<ASTIdentifier>(string_value);
            return true;
        }
    }

    return false;
}

template class ParserDeclareOptionImpl<true>;
template class ParserDeclareOptionImpl<false>;

}

}
