#include <Common/ZooKeeper/KeeperClientCLI/Parser.h>
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{

bool parseKeeperArg(IParser::Pos & pos, Expected & expected, String & result)
{
    /// Parse an argument that can mix bare tokens, backslash-escaped spaces,
    /// and quoted segments — all in a single pass:
    ///
    ///   /foo\ bar       →  /foo bar       (backslash-escaped space)
    ///   "/foo bar"      →  /foo bar       (double-quoted)
    ///   '/foo bar'      →  /foo bar       (single-quoted)
    ///   /dir/"sub dir"  →  /dir/sub dir   (mixed bare + quoted)

    const char * last_end = pos->begin;

    while (!pos->isEnd())
    {
        if (pos->type == TokenType::Whitespace)
            break;

        /// A gap between consecutive tokens means whitespace was skipped
        /// by the tokenizer (skip_insignificant mode). Treat as argument boundary.
        if (pos->begin > last_end)
            break;

        /// Quoted segment: parse it inline and append the unquoted content.
        if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        {
            const char * token_end = pos->end;
            String segment;
            if (!parseIdentifierOrStringLiteral(pos, expected, segment))
                break;
            result += segment;
            last_end = token_end;
            continue;
        }

        /// Backslash: may escape the following space or another backslash.
        if (pos->type == TokenType::Error
            && pos->end == pos->begin + 1
            && *pos->begin == '\\')
        {
            const char * bs_end = pos->end;
            ++pos;

            if (pos->isEnd())
            {
                result += '\\';
                break;
            }

            /// Explicit whitespace token (skip_insignificant=false) → escaped space.
            /// Only one space is escaped; if the token spans multiple characters
            /// (e.g. `\  bar`), the rest are genuine word breaks.
            if (pos->type == TokenType::Whitespace)
            {
                result += ' ';
                if (pos->end - pos->begin > 1)
                    break;
                last_end = pos->end;
                ++pos;
                continue;
            }

            /// Gap after backslash (skip_insignificant=true) → escaped space.
            /// The tokenizer consumed whitespace between the backslash Error token
            /// and the next token. A gap of exactly one byte means `\ ` (one escaped
            /// space). A larger gap means `\  ...` — one escaped space then a word break.
            if (pos->begin > bs_end)
            {
                result += ' ';
                if (pos->begin - bs_end > 1)
                    break;
                last_end = pos->begin;
                continue;
            }

            /// \\ → literal backslash
            if (pos->type == TokenType::Error
                && pos->end == pos->begin + 1
                && *pos->begin == '\\')
            {
                result += '\\';
                last_end = pos->end;
                ++pos;
                continue;
            }

            /// \<other> → keep backslash as-is
            result += '\\';
            last_end = bs_end;
            continue;
        }

        /// Regular tokens that can be part of a bare path or argument.
        if (pos->type == TokenType::BareWord
            || pos->type == TokenType::Slash
            || pos->type == TokenType::Dot
            || pos->type == TokenType::Number
            || pos->type == TokenType::Minus)
        {
            result.append(pos->begin, pos->end);
            last_end = pos->end;
            ++pos;
            continue;
        }

        break;
    }

    ParserToken{TokenType::Whitespace}.ignore(pos);

    if (result.empty())
        return false;

    return true;
}

bool parseKeeperPath(IParser::Pos & pos, Expected & expected, String & path)
{
    expected.add(pos, "path");
    return parseKeeperArg(pos, expected, path);
}

bool KeeperParser::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTKeeperQuery>();

    for (const auto & pair : KeeperClientBase::commands)
        expected.add(pos, pair.first.data());

    for (const auto & four_letter_word_command : four_letter_word_commands)
        expected.add(pos, four_letter_word_command.data());

    if (pos->type != TokenType::BareWord)
        return false;

    String command_name(pos->begin, pos->end);
    Command command;

    auto iter = KeeperClientBase::commands.find(command_name);
    if (iter == KeeperClientBase::commands.end())
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
