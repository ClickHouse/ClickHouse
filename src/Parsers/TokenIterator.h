#pragma once

#include <Core/Defines.h>
#include <Parsers/Lexer.h>
#include <base/defines.h>

#include <string_view>
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

    /// A syntax error is reported at the rightmost token read so far (see `max`). A parser that
    /// reads ahead only to decide how to parse saves this position before the lookahead and
    /// restores it afterwards, so the lookahead does not move the reported error. Every access to
    /// a token marks it read, so after the restore, the parser must not access a token that only
    /// the lookahead reached (keep what it needs from such a token before the restore).
    size_t getMaxPos() const { return max_pos; }

    void restoreMaxPos(size_t saved_max_pos)
    {
        chassert(saved_max_pos <= max_pos);
        max_pos = saved_max_pos;
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

    /// See `Tokens::getMaxPos` and `Tokens::restoreMaxPos`.
    ALWAYS_INLINE size_t getMaxPos() const { return tokens->getMaxPos(); }
    ALWAYS_INLINE void restoreMaxPos(size_t saved_max_pos) { tokens->restoreMaxPos(saved_max_pos); }
};


/** The query text spanned by the tokens `[begin, end)`, exactly as the user wrote it.
  *
  * Whitespace and comments are part of the result when they fall inside the range, and are never
  * part of it at the edges, because the iterator does not stop on them.
  *
  * Parsers use this for error messages, and to hand `astText` something to fall back on when the
  * build has no formatter to ask.
  */
inline std::string_view textBetween(TokenIterator begin, TokenIterator end)
{
    chassert(begin < end);
    --end;
    return {begin->begin, static_cast<size_t>(end->end - begin->begin)};
}

/// Returns positions of unmatched parentheses.
using UnmatchedParentheses = std::vector<Token>;
UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin);

}
