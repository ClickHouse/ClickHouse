#pragma once

#include <Core/Defines.h>
#include <Parsers/Lexer.h>

#include <cassert>
#include <vector>


namespace DB
{

/** Parser operates on lazy stream of tokens.
  * It could do lookaheads of any depth.
  */

/** Used as an input for parsers.
  * All whitespace and comment tokens are transparently skipped if `skip_insignificant`.
  */
class Tokens
{
private:
    std::vector<Token> data;
    size_t max_pos = 0;
    Lexer lexer;
    bool skip_insignificant;

public:
    Tokens(const char * begin, const char * end, size_t max_query_size = 0, bool skip_insignificant_ = true)
        : lexer(begin, end, max_query_size), skip_insignificant(skip_insignificant_)
    {
    }

    const Token & operator[] (size_t index)
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

    const Token & max()
    {
        if (data.empty())
            return (*this)[0];
        return data[max_pos];
    }

    void reset()
    {
        max_pos = 0;
    }
};


/// To represent position in a token stream.
class TokenIterator
{
private:
    Tokens * tokens;
    size_t index = 0;

public:
    explicit TokenIterator(Tokens & tokens_) : tokens(&tokens_) {}

    ALWAYS_INLINE const Token & get() { return (*tokens)[index]; }
    ALWAYS_INLINE const Token & operator*() { return get(); }
    ALWAYS_INLINE const Token * operator->() { return &get(); }

    ALWAYS_INLINE TokenIterator & operator++()
    {
        ++index;
        return *this;
    }
    ALWAYS_INLINE TokenIterator & operator--()
    {
        --index;
        return *this;
    }

    ALWAYS_INLINE bool operator<(const TokenIterator & rhs) const { return index < rhs.index; }
    ALWAYS_INLINE bool operator<=(const TokenIterator & rhs) const { return index <= rhs.index; }
    ALWAYS_INLINE bool operator==(const TokenIterator & rhs) const { return index == rhs.index; }
    ALWAYS_INLINE bool operator!=(const TokenIterator & rhs) const { return index != rhs.index; }

    ALWAYS_INLINE bool isValid() { return get().type < TokenType::EndOfStream; }

    /// Rightmost token we had looked.
    ALWAYS_INLINE const Token & max() { return tokens->max(); }
};


/// Returns positions of unmatched parentheses.
using UnmatchedParentheses = std::vector<Token>;
UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin);

}
