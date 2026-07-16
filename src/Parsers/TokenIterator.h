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

    /// Bytes the lexer may consume, i.e. the size of the current statement's window.
    size_t lex_bytes = 0;
    /// The first byte the lexer starts from, used to measure how much we have consumed so far.
    const char * lex_begin = nullptr;
    /// Whether the one-shot density-based reserve has already run.
    bool token_buffer_sized = false;

    /// Number of significant tokens to observe before extrapolating the buffer size from the
    /// measured token density. Small enough that the reallocations to reach it are cheap
    /// (relocating <= this many Tokens), large enough for a stable density estimate.
    static constexpr size_t reserve_sample_tokens = 1024;
    /// Hard safety ceiling on the reserve, independent of any input size
    /// (65536 tokens * sizeof(Token) ~= 1.5 MiB).
    static constexpr size_t max_reserve_tokens = DBMS_DEFAULT_MAX_QUERY_SIZE / 4;

public:
    Tokens(const char * begin, const char * end, size_t max_query_size = 0, bool skip_insignificant_ = true)
        : lexer(begin, end, max_query_size), skip_insignificant(skip_insignificant_)
    {
        /// The token buffer is grown to avoid repeated geometric reallocations while lexing.
        /// Each reallocation relocates all previously lexed Token structs, which is a large
        /// fraction of parse time for queries with huge literals (e.g. a 10k-element `IN [...]`).
        ///
        /// We do NOT pre-size from the byte count here. A byte estimate cannot know the token
        /// density: on the common `skip_insignificant` path a large block comment or a single
        /// large string literal is many bytes but only a couple of stored tokens, so any
        /// bytes/4-style reserve over-allocates for such token-sparse statements. It also cannot
        /// know the current statement's boundary: parseQuery (multi-statement) and
        /// ParserInsertQuery (`INSERT ... FORMAT <name>\n<data>`) pass an `end` far past the text
        /// the parser will actually request tokens for, so a short header in front of a large
        /// payload would reserve for payload that is never lexed.
        ///
        /// Instead the reserve is staged (see operator[]): the vector grows geometrically for the
        /// first `reserve_sample_tokens` significant tokens (cheap, since those arrays are small),
        /// then a single reserve extrapolates the final size from the *measured* token density
        /// over the bytes consumed so far. Statements that never accumulate that many tokens
        /// (short headers, token-sparse inputs) never trigger a large reserve at all.
        lex_bytes = end > begin ? static_cast<size_t>(end - begin) : 0;
        if (max_query_size != 0)
            lex_bytes = std::min<size_t>(lex_bytes, max_query_size);
        lex_begin = begin;
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
                data.emplace_back(token);

                /// One-shot density-based reserve once we have a representative sample. The
                /// measured density (stored tokens per byte consumed) already accounts for
                /// skipped comments/whitespace and for token-sparse literals, so this neither
                /// over-allocates for sparse input nor under-allocates for dense literals.
                if (!token_buffer_sized && data.size() >= reserve_sample_tokens)
                {
                    token_buffer_sized = true;
                    size_t bytes_consumed = token.end > lex_begin ? static_cast<size_t>(token.end - lex_begin) : 0;
                    if (bytes_consumed > 0)
                    {
                        size_t estimated_total = data.size() * lex_bytes / bytes_consumed;
                        if (estimated_total > data.size())
                            data.reserve(std::min<size_t>(estimated_total, max_reserve_tokens));
                    }
                }
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
