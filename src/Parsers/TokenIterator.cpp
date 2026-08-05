#include <Parsers/TokenIterator.h>


namespace DB
{

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
