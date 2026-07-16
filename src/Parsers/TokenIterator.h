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

    /// Small initial capacity so short statements (single-statement headers, empty/trivial
    /// queries) never allocate more than a few hundred bytes.
    static constexpr size_t initial_reserve_tokens = 16;
    /// Growth factor applied to the token buffer when it fills up. Larger than the standard
    /// std::vector doubling so the number of reallocations (each relocating all previously lexed
    /// tokens) is roughly halved for queries with huge literals, while keeping the buffer's peak
    /// over-allocation bounded to a small multiple of the tokens actually stored.
    static constexpr size_t token_buffer_growth_factor = 4;
    /// Cap on how many tokens a single growth step may add, so one reallocation cannot over-reserve
    /// by an unbounded amount even for very large statements (65536 tokens * sizeof(Token) ~= 1.5 MiB).
    static constexpr size_t max_reserve_step = DBMS_DEFAULT_MAX_QUERY_SIZE / 4;

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
            {
                /// Grow the token buffer to avoid repeated reallocations while lexing. Each
                /// reallocation relocates all previously lexed Token structs, which is a large
                /// fraction of parse time for queries with huge literals (e.g. a 10k-element
                /// `IN [...]`).
                ///
                /// The growth is driven purely by the number of tokens observed so far, never by
                /// the byte length of the input or the buffer we were handed. A byte estimate
                /// cannot know the token density (a large comment or a single large string literal
                /// is many bytes but only a couple of stored tokens) and cannot know the current
                /// statement's boundary (parseQuery and ParserInsertQuery pass an `end` far past
                /// the text the parser will actually request tokens for). Growing on the stored
                /// token count sidesteps both: peak capacity is always within a small factor of the
                /// tokens actually kept, so token-sparse statements and short headers in front of
                /// large payloads never over-allocate, while dense literals still get large,
                /// infrequent reservations.
                if (data.size() == data.capacity())
                {
                    size_t new_capacity = data.empty()
                        ? initial_reserve_tokens
                        : data.capacity() + std::min<size_t>(data.capacity() * (token_buffer_growth_factor - 1), max_reserve_step);
                    data.reserve(new_capacity);
                }

                data.emplace_back(token);
            }
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
