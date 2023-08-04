#include "Parser.h"
#include "KeeperClient.h"


namespace DB
{

bool parseKeeperArg(IParser::Pos & pos, Expected & expected, String & result)
{
    expected.add(pos, getTokenName(TokenType::BareWord));

    if (pos->type == TokenType::BareWord)
    {
        result = String(pos->begin, pos->end);
        ++pos;
        ParserToken{TokenType::Whitespace}.ignore(pos);
        return true;
    }

    bool status = parseIdentifierOrStringLiteral(pos, expected, result);
    ParserToken{TokenType::Whitespace}.ignore(pos);
    return status;
}

bool parseKeeperPath(IParser::Pos & pos, Expected & expected, String & path)
{
    expected.add(pos, "path");

    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        return parseIdentifierOrStringLiteral(pos, expected, path);

    String result;
    while (pos->type != TokenType::Whitespace && pos->type != TokenType::EndOfStream)
    {
        result.append(pos->begin, pos->end);
        ++pos;
    }
    ParserToken{TokenType::Whitespace}.ignore(pos);

    if (result.empty())
        return false;

    path = result;
    return true;
}

bool KeeperParser::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTKeeperQuery>();

    for (const auto & pair : KeeperClient::commands)
        expected.add(pos, pair.first.data());

    for (const auto & flwc : four_letter_word_commands)
        expected.add(pos, flwc.data());

    if (pos->type != TokenType::BareWord)
        return false;

    String command_name(pos->begin, pos->end);
    Command command;

    auto iter = KeeperClient::commands.find(command_name);
    if (iter == KeeperClient::commands.end())
    {
        if (command_name.size() == 4)
        {
            /// Treat it like four-letter command
            /// Since keeper server can potentially have different version we don't want to match this command with embedded list
            command = std::make_shared<FourLetterWordCommand>();
            command_name = command->getName();
            /// We also don't move the position, so the command will be parsed as an argument
        }
        else
            return false;
    }
    else
    {
        command = iter->second;
        ++pos;
        ParserToken{TokenType::Whitespace}.ignore(pos);
    }

    query->command = command_name;
    if (!command->parse(pos, query, expected))
        return false;

    ParserToken{TokenType::Whitespace}.ignore(pos);

    node = query;
    return true;
}

}
