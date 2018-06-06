#pragma once

#include <Common/StringSearcher.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <ext/range.h>
#include <stdint.h>
#include <string.h>


/** Search for a substring in a string by Volnitsky's algorithm
  * http://volnitsky.com/project/str_search/
  *
  * `haystack` and `needle` can contain zero bytes.
  *
  * Algorithm:
  * - if the `needle` is too small or too large, or too small `haystack`, use std::search or memchr;
  * - when initializing, fill in an open-addressing linear probing hash table of the form
  *    hash from the bigram of needle -> the position of this bigram in needle + 1.
  *    (one is added only to distinguish zero offset from an empty cell)
  * - the keys are not stored in the hash table, only the values are stored;
  * - bigrams can be inserted several times if they occur in the needle several times;
  * - when searching, take from haystack bigram, which should correspond to the last bigram of needle (comparing from the end);
  * - look for it in the hash table, if found - get the offset from the hash table and compare the string bytewise;
  * - if it did not match, we check the next cell of the hash table from the collision resolution chain;
  * - if not found, skip to haystack almost the size of the needle bytes;
  *
  * Unaligned memory access is used.
  */


namespace DB
{


/// @todo store lowercase needle to speed up in case there are numerous occurrences of bigrams from needle in haystack
template <typename CRTP>
class VolnitskyBase
{
protected:
    using Offset = UInt8;    /// Offset in the needle. For the basic algorithm, the length of the needle must not be greater than 255.
    using Ngram = UInt16;    /// n-gram (2 bytes).

    const UInt8 * const needle;
    const size_t needle_size;
    const UInt8 * const needle_end = needle + needle_size;
    /// For how long we move, if the n-gram from haystack is not found in the hash table.
    const size_t step = needle_size - sizeof(Ngram) + 1;

    /** max needle length is 255, max distinct ngrams for case-sensitive is (255 - 1), case-insensitive is 4 * (255 - 1)
      *  storage of 64K ngrams (n = 2, 128 KB) should be large enough for both cases */
    static const size_t hash_size = 64 * 1024;    /// Fits into the L2 cache (of common Intel CPUs).
    Offset hash[hash_size];    /// Hash table.

    /// min haystack size to use main algorithm instead of fallback
    static constexpr auto min_haystack_size_for_algorithm = 20000;
    const bool fallback; /// Do we need to use the fallback algorithm.

public:
    /** haystack_size_hint - the expected total size of the haystack for `search` calls. Optional (zero means unspecified).
      * If you specify it small enough, the fallback algorithm will be used,
      *  since it is considered that it's useless to waste time initializing the hash table.
      */
    VolnitskyBase(const char * const needle, const size_t needle_size, size_t haystack_size_hint = 0)
    : needle{reinterpret_cast<const UInt8 *>(needle)}, needle_size{needle_size},
      fallback{
          needle_size < 2 * sizeof(Ngram)
          || needle_size >= std::numeric_limits<Offset>::max()
          || (haystack_size_hint && haystack_size_hint < min_haystack_size_for_algorithm)}
    {
        if (fallback)
            return;

        memset(hash, 0, sizeof(hash));

        /// int is used here because unsigned can't be used with condition like `i >= 0`, unsigned always >= 0
        for (auto i = static_cast<int>(needle_size - sizeof(Ngram)); i >= 0; --i)
            self().putNGram(this->needle + i, i + 1, this->needle);
    }


    /// If not found, the end of the haystack is returned.
    const UInt8 * search(const UInt8 * const haystack, const size_t haystack_size) const
    {
        if (needle_size == 0)
            return haystack;

        const auto haystack_end = haystack + haystack_size;

        if (needle_size == 1 || fallback || haystack_size <= needle_size)
            return self().search_fallback(haystack, haystack_end);

        /// Let's "apply" the needle to the haystack and compare the n-gram from the end of the needle.
        const auto * pos = haystack + needle_size - sizeof(Ngram);
        for (; pos <= haystack_end - needle_size; pos += step)
        {
            /// We look at all the cells of the hash table that can correspond to the n-gram from haystack.
            for (size_t cell_num = toNGram(pos) % hash_size; hash[cell_num];
                 cell_num = (cell_num + 1) % hash_size)
            {
                /// When found - compare bytewise, using the offset from the hash table.
                const auto res = pos - (hash[cell_num] - 1);

                if (self().compare(res))
                    return res;
            }
        }

        /// The remaining tail.
        return self().search_fallback(pos - step + 1, haystack_end);
    }

    const char * search(const char * haystack, size_t haystack_size) const
    {
        return reinterpret_cast<const char *>(search(reinterpret_cast<const UInt8 *>(haystack), haystack_size));
    }

protected:
    CRTP & self() { return static_cast<CRTP &>(*this); }
    const CRTP & self() const { return const_cast<VolnitskyBase *>(this)->self(); }

    static const Ngram & toNGram(const UInt8 * const pos)
    {
        return *reinterpret_cast<const Ngram *>(pos);
    }

    void putNGramBase(const Ngram ngram, const int offset)
    {
        /// Put the offset for the n-gram in the corresponding cell or the nearest free cell.
        size_t cell_num = ngram % hash_size;

        while (hash[cell_num])
            cell_num = (cell_num + 1) % hash_size; /// Search for the next free cell.

        hash[cell_num] = offset;
    }

    void putNGramASCIICaseInsensitive(const UInt8 * const pos, const int offset)
    {
        struct Chars
        {
            UInt8 c0;
            UInt8 c1;
        };

        union
        {
            Ngram n;
            Chars chars;
        };

        n = toNGram(pos);

        const auto c0_al = isAlphaASCII(chars.c0);
        const auto c1_al = isAlphaASCII(chars.c1);

        if (c0_al && c1_al)
        {
            /// 4 combinations: AB, aB, Ab, ab
            putNGramBase(n, offset);
            chars.c0 = alternateCaseIfAlphaASCII(chars.c0);
            putNGramBase(n, offset);
            chars.c1 = alternateCaseIfAlphaASCII(chars.c1);
            putNGramBase(n, offset);
            chars.c0 = alternateCaseIfAlphaASCII(chars.c0);
            putNGramBase(n, offset);
        }
        else if (c0_al)
        {
            /// 2 combinations: A1, a1
            putNGramBase(n, offset);
            chars.c0 = alternateCaseIfAlphaASCII(chars.c0);
            putNGramBase(n, offset);
        }
        else if (c1_al)
        {
            /// 2 combinations: 0B, 0b
            putNGramBase(n, offset);
            chars.c1 = alternateCaseIfAlphaASCII(chars.c1);
            putNGramBase(n, offset);
        }
        else
            /// 1 combination: 01
            putNGramBase(n, offset);
    }
};


template <bool CaseSensitive, bool ASCII> struct VolnitskyImpl;

/// Case sensitive comparison
template <bool ASCII> struct VolnitskyImpl<true, ASCII> : VolnitskyBase<VolnitskyImpl<true, ASCII>>
{
    VolnitskyImpl(const char * const needle, const size_t needle_size, const size_t haystack_size_hint = 0)
        : VolnitskyBase<VolnitskyImpl<true, ASCII>>{needle, needle_size, haystack_size_hint},
          fallback_searcher{needle, needle_size}
    {
    }

    void putNGram(const UInt8 * const pos, const int offset, const UInt8 * const /*begin*/)
    {
        this->putNGramBase(this->toNGram(pos), offset);
    }

    bool compare(const UInt8 * const pos) const
    {
        /// @todo: maybe just use memcmp for this case and rely on internal SSE optimization as in case with memcpy?
        return fallback_searcher.compare(pos);
    }

    const UInt8 * search_fallback(const UInt8 * const haystack, const UInt8 * const haystack_end) const
    {
        return fallback_searcher.search(haystack, haystack_end);
    }

    ASCIICaseSensitiveStringSearcher fallback_searcher;
};

/// Case-insensitive ASCII
template <> struct VolnitskyImpl<false, true> : VolnitskyBase<VolnitskyImpl<false, true>>
{
    VolnitskyImpl(const char * const needle, const size_t needle_size, const size_t haystack_size_hint = 0)
        : VolnitskyBase{needle, needle_size, haystack_size_hint}, fallback_searcher{needle, needle_size}
    {
    }

    void putNGram(const UInt8 * const pos, const int offset, const UInt8 * const /*begin*/)
    {
        putNGramASCIICaseInsensitive(pos, offset);
    }

    bool compare(const UInt8 * const pos) const
    {
        return fallback_searcher.compare(pos);
    }

    const UInt8 * search_fallback(const UInt8 * const haystack, const UInt8 * const haystack_end) const
    {
        return fallback_searcher.search(haystack, haystack_end);
    }

    ASCIICaseInsensitiveStringSearcher fallback_searcher;
};

/// Case-sensitive UTF-8
template <> struct VolnitskyImpl<false, false> : VolnitskyBase<VolnitskyImpl<false, false>>
{
    VolnitskyImpl(const char * const needle, const size_t needle_size, const size_t haystack_size_hint = 0)
        : VolnitskyBase{needle, needle_size, haystack_size_hint}, fallback_searcher{needle, needle_size}
    {
    }

    void putNGram(const UInt8 * const pos, const int offset, const UInt8 * const begin)
    {
        struct Chars
        {
            UInt8 c0;
            UInt8 c1;
        };

        union
        {
            Ngram n;
            Chars chars;
        };

        n = toNGram(pos);

        if (isascii(chars.c0) && isascii(chars.c1))
        {
            putNGramASCIICaseInsensitive(pos, offset);
        }
        else
        {
            /** n-gram (in the case of n = 2)
              *  can be entirely located within one code point,
              *  or intersect with two code points.
              *
              * In the first case, you need to consider up to two alternatives - this code point in upper and lower case,
              *  and in the second case - up to four alternatives - fragments of two code points in all combinations of cases.
              *
              * It does not take into account the dependence of the case-transformation from the locale (for example - Turkish `Ii`)
              *  as well as composition / decomposition and other features.
              *
              * It also does not work if characters with lower and upper cases are represented by different number of bytes or code points.
              */

            using Seq = UInt8[6];

            static const Poco::UTF8Encoding utf8;

            if (UTF8::isContinuationOctet(chars.c1))
            {
                /// ngram is inside a sequence
                auto seq_pos = pos;
                UTF8::syncBackward(seq_pos, begin);

                const auto u32 = utf8.convert(seq_pos);
                const auto l_u32 = Poco::Unicode::toLower(u32);
                const auto u_u32 = Poco::Unicode::toUpper(u32);

                /// symbol is case-independent
                if (l_u32 == u_u32)
                    putNGramBase(n, offset);
                else
                {
                    /// where is the given ngram in respect to the start of UTF-8 sequence?
                    const auto seq_ngram_offset = pos - seq_pos;

                    Seq seq;

                    /// put ngram for lowercase
                    utf8.convert(l_u32, seq, sizeof(seq));
                    chars.c0 = seq[seq_ngram_offset];
                    chars.c1 = seq[seq_ngram_offset + 1];
                    putNGramBase(n, offset);

                    /// put ngram for uppercase
                    utf8.convert(u_u32, seq, sizeof(seq));
                    chars.c0 = seq[seq_ngram_offset];
                    chars.c1 = seq[seq_ngram_offset + 1];
                    putNGramBase(n, offset);
                }
            }
            else
            {
                /// ngram is on the boundary of two sequences
                /// first sequence may start before u_pos if it is not ASCII
                auto first_seq_pos = pos;
                UTF8::syncBackward(first_seq_pos, begin);
                /// where is the given ngram in respect to the start of first UTF-8 sequence?
                const auto seq_ngram_offset = pos - first_seq_pos;

                const auto first_u32 = utf8.convert(first_seq_pos);
                const auto first_l_u32 = Poco::Unicode::toLower(first_u32);
                const auto first_u_u32 = Poco::Unicode::toUpper(first_u32);

                /// second sequence always start immediately after u_pos
                auto second_seq_pos = pos + 1;

                const auto second_u32 = utf8.convert(second_seq_pos);    /// TODO This assumes valid UTF-8 or zero byte after needle.
                const auto second_l_u32 = Poco::Unicode::toLower(second_u32);
                const auto second_u_u32 = Poco::Unicode::toUpper(second_u32);

                /// both symbols are case-independent
                if (first_l_u32 == first_u_u32 && second_l_u32 == second_u_u32)
                {
                    putNGramBase(n, offset);
                }
                else if (first_l_u32 == first_u_u32)
                {
                    /// first symbol is case-independent
                    Seq seq;

                    /// put ngram for lowercase
                    utf8.convert(second_l_u32, seq, sizeof(seq));
                    chars.c1 = seq[0];
                    putNGramBase(n, offset);

                    /// put ngram from uppercase, if it is different
                    utf8.convert(second_u_u32, seq, sizeof(seq));
                    if (chars.c1 != seq[0])
                    {
                        chars.c1 = seq[0];
                        putNGramBase(n, offset);
                    }
                }
                else if (second_l_u32 == second_u_u32)
                {
                    /// second symbol is case-independent
                    Seq seq;

                    /// put ngram for lowercase
                    utf8.convert(first_l_u32, seq, sizeof(seq));
                    chars.c0 = seq[seq_ngram_offset];
                    putNGramBase(n, offset);

                    /// put ngram for uppercase, if it is different
                    utf8.convert(first_u_u32, seq, sizeof(seq));
                    if (chars.c0 != seq[seq_ngram_offset])
                    {
                        chars.c0 = seq[seq_ngram_offset];
                        putNGramBase(n, offset);
                    }
                }
                else
                {
                    Seq first_l_seq;
                    Seq first_u_seq;
                    Seq second_l_seq;
                    Seq second_u_seq;

                    utf8.convert(first_l_u32, first_l_seq, sizeof(first_l_seq));
                    utf8.convert(first_u_u32, first_u_seq, sizeof(first_u_seq));
                    utf8.convert(second_l_u32, second_l_seq, sizeof(second_l_seq));
                    utf8.convert(second_u_u32, second_u_seq, sizeof(second_u_seq));

                    auto c0l = first_l_seq[seq_ngram_offset];
                    auto c0u = first_u_seq[seq_ngram_offset];
                    auto c1l = second_l_seq[0];
                    auto c1u = second_u_seq[0];

                    /// ngram for ll
                    chars.c0 = c0l;
                    chars.c1 = c1l;
                    putNGramBase(n, offset);

                    if (c0l != c0u)
                    {
                        /// ngram for Ul
                        chars.c0 = c0u;
                        chars.c1 = c1l;
                        putNGramBase(n, offset);
                    }

                    if (c1l != c1u)
                    {
                        /// ngram for lU
                        chars.c0 = c0l;
                        chars.c1 = c1u;
                        putNGramBase(n, offset);
                    }

                    if (c0l != c0u && c1l != c1u)
                    {
                        /// ngram for UU
                        chars.c0 = c0u;
                        chars.c1 = c1u;
                        putNGramBase(n, offset);
                    }
                }
            }
        }
    }

    bool compare(const UInt8 * const pos) const
    {
        return fallback_searcher.compare(pos);
    }

    const UInt8 * search_fallback(const UInt8 * const haystack, const UInt8 * const haystack_end) const
    {
        return fallback_searcher.search(haystack, haystack_end);
    }

    UTF8CaseInsensitiveStringSearcher fallback_searcher;
};


using Volnitsky = VolnitskyImpl<true, true>;
using VolnitskyUTF8 = VolnitskyImpl<true, false>;    /// exactly same as Volnitsky
using VolnitskyCaseInsensitive = VolnitskyImpl<false, true>;    /// ignores non-ASCII bytes
using VolnitskyCaseInsensitiveUTF8 = VolnitskyImpl<false, false>;


}
