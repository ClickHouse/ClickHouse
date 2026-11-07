#pragma once

#include <Core/Defines.h>
#include <Parsers/Lexer.h>
#include <base/defines.h>

#include <string_view>
#include <unordered_map>
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

    /// The answers of `followedInSameBrackets` by the index of the keyword token, one map per kind of keyword.
    std::unordered_map<size_t, bool> end_follows_case;
    std::unordered_map<size_t, bool> kind_follows_interval;

    /// Returns the token at `index` without moving `max`, which tells where the parser stopped.
    const Token & peek(size_t index)
    {
        while (true)
        {
            if (index < data.size())
                return data[index];

            if (!data.empty() && data.back().isEnd())
                return data.back();

            Token token = lexer.nextToken();

            if (!skip_insignificant || token.isSignificant())
                data.emplace_back(token);
        }
    }

public:
    Tokens(const char * begin, const char * end, size_t max_query_size = 0, bool skip_insignificant_ = true)
        : lexer(begin, end, max_query_size), skip_insignificant(skip_insignificant_)
    {
    }

    const Token & operator[] (size_t index)
    {
        const Token & token = peek(index);
        max_pos = std::max(max_pos, std::min(index, data.size() - 1));
        return token;
    }

    /// A keyword that begins an expression which must contain a certain keyword at its own bracket depth:
    /// `CASE` ... `END`, and `INTERVAL` <expr> <kind>.
    enum class KeywordWithTerminator : uint8_t
    {
        Case,
        Interval,
    };

    /// Whether the token at `index` is followed by the terminator of `keyword` (`END` for `CASE`, an interval kind
    /// for `INTERVAL`) in the same brackets: after it, outside nested brackets, and before the brackets around it close,
    /// and before the end of the statement. If it is not, the token cannot begin that expression. Does not move `max`.
    ///
    /// The scan also answers for every other occurrence of `keyword` it passes, so all the occurrences in
    /// a query are answered in time linear in the length of the query.
    bool followedInSameBrackets(size_t index, KeywordWithTerminator keyword);

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

    /// See `Tokens::followedInSameBrackets`.
    bool followedInSameBrackets(Tokens::KeywordWithTerminator keyword) { return tokens->followedInSameBrackets(index, keyword); }

    /// Rightmost token we had looked.
    ALWAYS_INLINE const Token & max() { return tokens->max(); }
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
