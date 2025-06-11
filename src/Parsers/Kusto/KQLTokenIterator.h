#pragma once

#include <Core/Defines.h>
#include <Parsers/Kusto/KQLLexer.h>

#include <cassert>
#include <vector>


namespace DB
{

/** Parser operates on lazy stream of tokens.
  * It could do lookaheads of any depth.
  */

/** Used as an input for parsers.
  * All whitespace and comment tokens are transparently skipped.
  */
class KQLTokens
{
private:
    std::vector<KQLToken> data;
    std::size_t last_accessed_index = 0;

public:
    KQLTokens(const char * begin, const char * end, size_t max_query_size = 0, bool skip_insignificant = true);

    ALWAYS_INLINE inline const KQLToken & operator[](size_t index)
    {
        assert(index < data.size());
        last_accessed_index = std::max(last_accessed_index, index);
        return data[index];
    }

    ALWAYS_INLINE inline const KQLToken & max() { return data[last_accessed_index]; }
};


/// To represent position in a token stream.
class KQLTokenIterator
{
private:
    KQLTokens * tokens;
    size_t index = 0;

public:
    explicit KQLTokenIterator(KQLTokens & tokens_) : tokens(&tokens_) {}

    ALWAYS_INLINE const KQLToken & get() { return (*tokens)[index]; }
    ALWAYS_INLINE const KQLToken & operator*() { return get(); }
    ALWAYS_INLINE const KQLToken * operator->() { return &get(); }

    ALWAYS_INLINE KQLTokenIterator & operator++()
    {
        ++index;
        return *this;
    }
    ALWAYS_INLINE KQLTokenIterator & operator--()
    {
        --index;
        return *this;
    }

    ALWAYS_INLINE bool operator<(const KQLTokenIterator & rhs) const { return index < rhs.index; }
    ALWAYS_INLINE bool operator<=(const KQLTokenIterator & rhs) const { return index <= rhs.index; }
    ALWAYS_INLINE bool operator==(const KQLTokenIterator & rhs) const { return index == rhs.index; }
    ALWAYS_INLINE bool operator!=(const KQLTokenIterator & rhs) const { return index != rhs.index; }

    ALWAYS_INLINE bool isValid() { return get().type < KQLTokenType::EndOfStream; }

    /// Rightmost token we had looked.
    ALWAYS_INLINE const KQLToken & max() { return tokens->max(); }
};


/// Returns positions of unmatched parentheses.
using KQLUnmatchedParentheses = std::vector<KQLToken>;
KQLUnmatchedParentheses checkKQLUnmatchedParentheses(KQLTokenIterator begin);

}
