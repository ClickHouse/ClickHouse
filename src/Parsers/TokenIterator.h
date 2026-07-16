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
        /// Bound the estimate by the bytes the lexer may actually consume, not the whole
        /// begin..end range: the lexer stops at begin + max_query_size, and call sites such as
        /// parseQuery (multi-statement) and ParserInsertQuery (`INSERT ... FORMAT`) pass an `end`
        /// far past the current statement. Without this bound a short header before a large
        /// payload/script would reserve up to the cap (4M tokens ~= 96 MiB) up front.
        ///
        /// max_query_size == 0 means "unlimited" (e.g. formatQuery in the client parses one
        /// statement at a time out of the whole editor buffer with max_query_size=0). There the
        /// begin..end bound is useless again, so fall back to a conservative default cap instead
        /// of the whole buffer, keeping the reserve small for a short statement in front of a
        /// large payload. A single genuinely large query in that mode just falls back to geometric
        /// growth, which is correct, only slightly slower.
        size_t bound = max_query_size != 0 ? max_query_size : DBMS_DEFAULT_MAX_QUERY_SIZE;
        size_t lex_bytes = end > begin ? static_cast<size_t>(end - begin) : 0;
        lex_bytes = std::min<size_t>(lex_bytes, bound);
        static constexpr size_t max_reserve = 4 * 1024 * 1024;
        data.reserve(std::min<size_t>(lex_bytes / 4 + 16, max_reserve));
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
