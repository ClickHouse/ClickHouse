#include <Parsers/TokenIterator.h>
#include <Parsers/parseIntervalKind.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace
{

bool isWord(const Token & token, std::string_view word)
{
    return token.type == TokenType::BareWord && equalsCaseInsensitive(std::string_view(token.begin, token.size()), word);
}

bool isIntervalKind(const Token & token)
{
    if (token.type != TokenType::BareWord)
        return false;

    Tokens tokens(token.begin, token.end);
    IParser::Pos pos(tokens, 0, 0);
    Expected expected;
    IntervalKind kind;
    return parseIntervalKind(pos, expected, kind) && pos->isEnd();
}

bool isOpeningBracket(const Token & token)
{
    return token.type == TokenType::OpeningRoundBracket || token.type == TokenType::OpeningSquareBracket
        || token.type == TokenType::OpeningCurlyBrace;
}

bool isClosingBracket(const Token & token)
{
    return token.type == TokenType::ClosingRoundBracket || token.type == TokenType::ClosingSquareBracket
        || token.type == TokenType::ClosingCurlyBrace;
}

}

bool Tokens::followedInSameBrackets(size_t index, KeywordWithTerminator keyword)
{
    const bool is_case = keyword == KeywordWithTerminator::Case;
    auto & answers = is_case ? end_follows_case : kind_follows_interval;
    if (auto it = answers.find(index); it != answers.end())
        return it->second;

    const std::string_view keyword_word = is_case ? "CASE" : "INTERVAL";
    auto is_terminator = [&](const Token & token) { return is_case ? isWord(token, "END") : isIntervalKind(token); };

    /// The occurrences of the keyword that are not answered yet, by bracket depth relative to `index`.
    std::vector<std::vector<size_t>> pending{{index}};
    auto answer = [&](std::vector<size_t> & occurrences, bool value)
    {
        for (size_t occurrence : occurrences)
            answers[occurrence] = value;
        occurrences.clear();
    };

    for (size_t i = index + 1; !pending.empty(); ++i)
    {
        const Token & token = peek(i);
        if (token.isEnd() || token.isError() || token.type == TokenType::Semicolon)
        {
            /// The expression cannot go past this token. Stopping at the end of the statement also keeps the scan
            /// linear for scripts: they are parsed statement by statement, each with new `Tokens` over the rest.
            for (auto & occurrences : pending)
                answer(occurrences, false);
            break;
        }

        if (isOpeningBracket(token))
        {
            pending.emplace_back();
        }
        else if (isClosingBracket(token))
        {
            answer(pending.back(), false);
            pending.pop_back();
        }
        else if (is_terminator(token))
        {
            answer(pending.back(), true);
            if (pending.size() == 1)
                break;
        }
        else if (isWord(token, keyword_word))
        {
            pending.back().push_back(i);
        }
    }

    return answers.at(index);
}

UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin)
{
    /// We have just two kind of parentheses: () and [].
    UnmatchedParentheses stack;

    /// We have to iterate through all tokens until the end to avoid false positive "Unmatched parentheses" error
    /// when parser failed in the middle of the query.
    for (TokenIterator it = begin; it.isValid(); ++it)
    {
        if (it->type == TokenType::OpeningRoundBracket || it->type == TokenType::OpeningSquareBracket)
        {
            stack.push_back(*it);
        }
        else if (it->type == TokenType::ClosingRoundBracket || it->type == TokenType::ClosingSquareBracket)
        {
            if (stack.empty())
            {
                /// Excessive closing bracket.
                stack.push_back(*it);
                return stack;
            }
            if ((stack.back().type == TokenType::OpeningRoundBracket && it->type == TokenType::ClosingRoundBracket)
                || (stack.back().type == TokenType::OpeningSquareBracket && it->type == TokenType::ClosingSquareBracket))
            {
                /// Valid match.
                stack.pop_back();
            }
            else
            {
                /// Closing bracket type doesn't match opening bracket type.
                stack.push_back(*it);
                return stack;
            }
        }
    }

    /// If stack is not empty, we have unclosed brackets.
    return stack;
}

}
