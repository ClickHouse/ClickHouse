#include <Parsers/TokenIterator.h>


namespace DB
{

const Token & Tokens::lexUntil(size_t index)
{
    while (true)
    {
        if (index < data.size())
        {
            max_pos = std::max(max_pos, index);
            return data[index];
        }

        if (!data.empty() && data.back().isEnd())
        {
            max_pos = data.size() - 1;
            return data.back();
        }

        Token token = lexer.nextToken();

        if (!skip_insignificant || token.isSignificant())
            data.emplace_back(token);
    }
}

UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin, const char * statement_end)
{
    /// We have just two kind of parentheses: () and [].
    UnmatchedParentheses stack;

    /// We have to iterate through all tokens until the end to avoid false positive "Unmatched parentheses" error
    /// when parser failed in the middle of the query.
    for (TokenIterator it = begin; it.isValid(); ++it)
    {
        if (it->type == TokenType::Semicolon && stack.empty() && it->begin >= statement_end)
            break;

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
