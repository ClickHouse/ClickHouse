#pragma once

#include <Core/Defines.h>
#include <Parsers/Lexer.h>

#include <algorithm>
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
        /// Pre-size the token buffer to avoid repeated geometric reallocations while lexing.
        /// Each reallocation relocates all previously lexed Token structs, which is a large
        /// fraction of parse time for queries with huge literals (e.g. a 10k-element `IN [...]`).
        /// A significant token is at least ~3 bytes on average for dense numeric literals, so
        /// (bytes to lex) / 4 covers the bulk of them.
        ///
        /// The reserve must be bounded by the bytes the lexer will actually consume for the
        /// *current* statement, not the whole begin..end range: call sites such as parseQuery
        /// (multi-statement) and ParserInsertQuery (`INSERT ... FORMAT <name>\n<data>`) pass an
        /// `end` far past the current statement, so a short header in front of a large payload
        /// must not pre-reserve for payload it will never lex.
        ///
        /// We cannot know that boundary here without lexing, so the reserve is bounded two ways:
        ///  - by max_query_size when set (the lexer stops at begin + max_query_size), and
        ///  - unconditionally by a hard ceiling max_reserve_tokens.
        /// The hard ceiling is what makes this safe. max_query_size is 0 on some paths
        /// (e.g. formatQuery, which parses one statement at a time out of the whole editor
        /// buffer) and can be raised by the user above the remaining buffer size; in both cases
        /// the max_query_size bound collapses back to end - begin. The ceiling caps the up-front
        /// allocation at a constant regardless of buffer size or max_query_size
        /// (65536 tokens * sizeof(Token) ~= 1.5 MiB), so inline INSERT data and large multi-query
        /// scripts can never trigger a large reserve. A genuinely huge single statement grows
        /// geometrically past the ceiling, which is correct and keeps most of the benefit (the
        /// first, most expensive relocations are still avoided).
        size_t lex_bytes = end > begin ? static_cast<size_t>(end - begin) : 0;
        if (max_query_size != 0)
            lex_bytes = std::min<size_t>(lex_bytes, max_query_size);
        static constexpr size_t max_reserve_tokens = DBMS_DEFAULT_MAX_QUERY_SIZE / 4;
        data.reserve(std::min<size_t>(lex_bytes / 4 + 16, max_reserve_tokens));
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
