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
    /// Geometric growth factor applied to the token buffer when it fills up. Larger than the
    /// standard std::vector doubling so the number of reallocations (each relocating all previously
    /// lexed tokens) is a few times smaller for queries with huge literals, while keeping the
    /// buffer's peak over-allocation bounded to this small multiple of the tokens actually stored.
    /// The growth stays geometric for arbitrarily large statements, so the total relocation cost is
    /// O(N) in the number of tokens (never fixed-step, which would be O(N^2)).
    static constexpr size_t token_buffer_growth_factor = 4;

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
                /// large payloads never over-allocate.
                ///
                /// The step stays geometric (multiply the capacity) for arbitrarily large
                /// statements rather than switching to a fixed increment past some threshold, so
                /// the total relocation cost for a truly large literal is O(N), matching or beating
                /// the standard vector doubling.
                if (data.size() == data.capacity())
                {
                    size_t new_capacity = data.empty()
                        ? initial_reserve_tokens
                        : data.capacity() * token_buffer_growth_factor;
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
