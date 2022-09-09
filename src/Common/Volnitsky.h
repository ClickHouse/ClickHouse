#pragma once

#include <algorithm>
#include <vector>
#include <stdint.h>
#include <string.h>
#include <base/types.h>
#include <Poco/Unicode.h>
#include <Common/StringSearcher.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <base/StringRef.h>
#include <base/unaligned.h>

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
  * MultiVolnitsky - search for multiple substrings in a string:
  * - Add bigrams to hash table with string index. Then the usual Volnitsky search is used.
  * - We are adding while searching, limiting the number of fallback searchers and the total number of added bigrams
  */


namespace DB
{
namespace VolnitskyTraits
{
    using Offset = UInt8; /// Offset in the needle. For the basic algorithm, the length of the needle must not be greater than 255.
    using Id = UInt8; /// Index of the string (within the array of multiple needles), must not be greater than 255.
    using Ngram = UInt16; /// n-gram (2 bytes).

    /** Fits into the L2 cache (of common Intel CPUs).
     * This number is extremely good for compilers as it is numeric_limits<Uint16>::max() and there are optimizations with movzwl and other instructions with 2 bytes
     */
    static constexpr size_t hash_size = 64 * 1024;

    /// min haystack size to use main algorithm instead of fallback
    static constexpr size_t min_haystack_size_for_algorithm = 20000;

    static inline bool isFallbackNeedle(const size_t needle_size, size_t haystack_size_hint = 0)
    {
        return needle_size < 2 * sizeof(Ngram) || needle_size >= std::numeric_limits<Offset>::max()
            || (haystack_size_hint && haystack_size_hint < min_haystack_size_for_algorithm);
    }

    static inline Ngram toNGram(const UInt8 * const pos) { return unalignedLoad<Ngram>(pos); }

    template <typename Callback>
    static inline bool putNGramASCIICaseInsensitive(const UInt8 * pos, int offset, Callback && putNGramBase)
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

        return true;
    }

    template <typename Callback>
    static inline bool putNGramUTF8CaseInsensitive(
        const UInt8 * pos, int offset, const UInt8 * begin, size_t size, Callback && putNGramBase)
    {
        const UInt8 * end = begin + size;

        struct Chars
        {
            UInt8 c0;
            UInt8 c1;
        };

        union
        {
            VolnitskyTraits::Ngram n;
            Chars chars;
        };

        n = toNGram(pos);

        if (isascii(chars.c0) && isascii(chars.c1))
        {
            return putNGramASCIICaseInsensitive(pos, offset, putNGramBase);
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

            if (UTF8::isContinuationOctet(chars.c1))
            {
                /// ngram is inside a sequence
                const auto * seq_pos = pos;
                UTF8::syncBackward(seq_pos, begin);

                auto u32 = UTF8::convertUTF8ToCodePoint(seq_pos, end - seq_pos);
                /// Invalid UTF-8
                if (!u32)
                {
                    putNGramBase(n, offset);
                }
                else
                {
                    int l_u32 = Poco::Unicode::toLower(*u32);
                    int u_u32 = Poco::Unicode::toUpper(*u32);

                    /// symbol is case-independent
                    if (l_u32 == u_u32)
                    {
                        putNGramBase(n, offset);
                    }
                    else
                    {
                        /// where is the given ngram in respect to the start of UTF-8 sequence?
                        size_t seq_ngram_offset = pos - seq_pos;

                        Seq seq_l;
                        size_t length_l = UTF8::convertCodePointToUTF8(l_u32, seq_l, sizeof(seq_l));

                        Seq seq_r;
                        size_t length_r = UTF8::convertCodePointToUTF8(u_u32, seq_r, sizeof(seq_r));

                        if (length_l != length_r)
                            return false;

                        assert(length_l >= 2 && length_r >= 2);

                        chars.c0 = seq_l[seq_ngram_offset];
                        chars.c1 = seq_l[seq_ngram_offset + 1];
                        putNGramBase(n, offset);

                        chars.c0 = seq_r[seq_ngram_offset]; //-V519
                        chars.c1 = seq_r[seq_ngram_offset + 1]; //-V519
                        putNGramBase(n, offset);

                    }
                }
            }
            else
            {
                /// ngram is on the boundary of two sequences
                /// first sequence may start before u_pos if it is not ASCII
                const auto * first_seq_pos = pos;
                UTF8::syncBackward(first_seq_pos, begin);
                /// where is the given ngram in respect to the start of first UTF-8 sequence?
                size_t seq_ngram_offset = pos - first_seq_pos;

                auto first_u32 = UTF8::convertUTF8ToCodePoint(first_seq_pos, end - first_seq_pos);
                int first_l_u32 = 0;
                int first_u_u32 = 0;

                if (first_u32)
                {
                    first_l_u32 = Poco::Unicode::toLower(*first_u32);
                    first_u_u32 = Poco::Unicode::toUpper(*first_u32);
                }

                /// second sequence always start immediately after u_pos
                const auto * second_seq_pos = pos + 1;

                auto second_u32 = UTF8::convertUTF8ToCodePoint(second_seq_pos, end - second_seq_pos);
                int second_l_u32 = 0;
                int second_u_u32 = 0;

                if (second_u32)
                {
                    second_l_u32 = Poco::Unicode::toLower(*second_u32);
                    second_u_u32 = Poco::Unicode::toUpper(*second_u32);
                }

                /// both symbols are case-independent
                if (first_l_u32 == first_u_u32 && second_l_u32 == second_u_u32)
                {
                    putNGramBase(n, offset);
                }
                else if (first_l_u32 == first_u_u32)
                {
                    /// first symbol is case-independent
                    Seq seq_l;
                    size_t size_l = UTF8::convertCodePointToUTF8(second_l_u32, seq_l, sizeof(seq_l));

                    Seq seq_u;
                    size_t size_u = UTF8::convertCodePointToUTF8(second_u_u32, seq_u, sizeof(seq_u));

                    if (size_l != size_u)
                        return false;

                    assert(size_l >= 1 && size_u >= 1);
                    chars.c1 = seq_l[0];
                    putNGramBase(n, offset);

                    /// put ngram from uppercase, if it is different
                    if (chars.c1 != seq_u[0])
                    {
                        chars.c1 = seq_u[0];
                        putNGramBase(n, offset);
                    }
                }
                else if (second_l_u32 == second_u_u32)
                {
                    /// second symbol is case-independent

                    Seq seq_l;
                    size_t size_l = UTF8::convertCodePointToUTF8(first_l_u32, seq_l, sizeof(seq_l));
                    Seq seq_u;
                    size_t size_u = UTF8::convertCodePointToUTF8(first_u_u32, seq_u, sizeof(seq_u));

                    if (size_l != size_u)
                        return false;

                    assert(size_l > seq_ngram_offset && size_u > seq_ngram_offset);

                    chars.c0 = seq_l[seq_ngram_offset];
                    putNGramBase(n, offset);

                    /// put ngram for uppercase, if it is different
                    if (chars.c0 != seq_u[seq_ngram_offset])
                    {
                        chars.c0 = seq_u[seq_ngram_offset];
                        putNGramBase(n, offset);
                    }
                }
                else
                {
                    Seq first_l_seq;
                    Seq first_u_seq;
                    Seq second_l_seq;
                    Seq second_u_seq;

                    size_t size_first_l = UTF8::convertCodePointToUTF8(first_l_u32, first_l_seq, sizeof(first_l_seq));
                    size_t size_first_u = UTF8::convertCodePointToUTF8(first_u_u32, first_u_seq, sizeof(first_u_seq));
                    size_t size_second_l = UTF8::convertCodePointToUTF8(second_l_u32, second_l_seq, sizeof(second_l_seq));
                    size_t size_second_u = UTF8::convertCodePointToUTF8(second_u_u32, second_u_seq, sizeof(second_u_seq));
                    if (size_first_l != size_first_u || size_second_l != size_second_u)
                        return false;

                    assert(size_first_l > seq_ngram_offset);
                    assert(size_first_u > seq_ngram_offset);
                    assert(size_second_l > 0);
                    assert(size_second_u > 0);

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
                        chars.c1 = c1l; //-V1048
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
        return true;
    }

    template <bool CaseSensitive, bool ASCII, typename Callback>
    static inline bool putNGram(const UInt8 * pos, int offset, [[maybe_unused]] const UInt8 * begin, size_t size, Callback && putNGramBase)
    {
        if constexpr (CaseSensitive)
        {
            putNGramBase(toNGram(pos), offset);
            return true;
        }
        else if constexpr (ASCII)
        {
            return putNGramASCIICaseInsensitive(pos, offset, std::forward<Callback>(putNGramBase));
        }
        else
        {
            return putNGramUTF8CaseInsensitive(pos, offset, begin, size, std::forward<Callback>(putNGramBase));
        }
    }
}


/// @todo store lowercase needle to speed up in case there are numerous occurrences of bigrams from needle in haystack
template <bool CaseSensitive, bool ASCII, typename FallbackSearcher>
class VolnitskyBase
{
protected:
    const UInt8 * needle;
    size_t needle_size;
    const UInt8 * needle_end = needle + needle_size;
    /// For how long we move, if the n-gram from haystack is not found in the hash table.
    size_t step = needle_size - sizeof(VolnitskyTraits::Ngram) + 1;

    /** max needle length is 255, max distinct ngrams for case-sensitive is (255 - 1), case-insensitive is 4 * (255 - 1)
      *  storage of 64K ngrams (n = 2, 128 KB) should be large enough for both cases */
    std::unique_ptr<VolnitskyTraits::Offset[]> hash; /// Hash table.

    bool fallback; /// Do we need to use the fallback algorithm.

    FallbackSearcher fallback_searcher;

public:
    using Searcher = FallbackSearcher;

    /** haystack_size_hint - the expected total size of the haystack for `search` calls. Optional (zero means unspecified).
      * If you specify it small enough, the fallback algorithm will be used,
      *  since it is considered that it's useless to waste time initializing the hash table.
      */
    VolnitskyBase(const char * const needle_, const size_t needle_size_, size_t haystack_size_hint = 0)
        : needle{reinterpret_cast<const UInt8 *>(needle_)}
        , needle_size{needle_size_}
        , fallback{VolnitskyTraits::isFallbackNeedle(needle_size, haystack_size_hint)}
        , fallback_searcher{needle_, needle_size}
    {
        if (fallback || fallback_searcher.force_fallback)
            return;

        hash = std::unique_ptr<VolnitskyTraits::Offset[]>(new VolnitskyTraits::Offset[VolnitskyTraits::hash_size]{});

        auto callback = [this](const VolnitskyTraits::Ngram ngram, const int offset) { return this->putNGramBase(ngram, offset); };
        /// ssize_t is used here because unsigned can't be used with condition like `i >= 0`, unsigned always >= 0
        /// And also adding from the end guarantees that we will find first occurrence because we will lookup bigger offsets first.
        for (auto i = static_cast<ssize_t>(needle_size - sizeof(VolnitskyTraits::Ngram)); i >= 0; --i)
        {
            bool ok = VolnitskyTraits::putNGram<CaseSensitive, ASCII>(needle + i, i + 1, needle, needle_size, callback);

            /** `putNGramUTF8CaseInsensitive` does not work if characters with lower and upper cases
              * are represented by different number of bytes or code points.
              * So, use fallback if error occurred.
              */
            if (!ok)
            {
                fallback_searcher.force_fallback = true;
                hash = nullptr;
                return;
            }
        }
    }


    /// If not found, the end of the haystack is returned.
    const UInt8 * search(const UInt8 * const haystack, const size_t haystack_size) const
    {
        if (needle_size == 0)
            return haystack;

        const auto * haystack_end = haystack + haystack_size;

        if (fallback || haystack_size <= needle_size || fallback_searcher.force_fallback)
            return fallback_searcher.search(haystack, haystack_end);

        /// Let's "apply" the needle to the haystack and compare the n-gram from the end of the needle.
        const auto * pos = haystack + needle_size - sizeof(VolnitskyTraits::Ngram);
        for (; pos <= haystack_end - needle_size; pos += step)
        {
            /// We look at all the cells of the hash table that can correspond to the n-gram from haystack.
            for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size; hash[cell_num];
                 cell_num = (cell_num + 1) % VolnitskyTraits::hash_size)
            {
                /// When found - compare bytewise, using the offset from the hash table.
                const auto * res = pos - (hash[cell_num] - 1);

                /// pointer in the code is always padded array so we can use pagesafe semantics
                if (fallback_searcher.compare(haystack, haystack_end, res))
                    return res;
            }
        }

        return fallback_searcher.search(pos - step + 1, haystack_end);
    }

    const char * search(const char * haystack, size_t haystack_size) const
    {
        return reinterpret_cast<const char *>(search(reinterpret_cast<const UInt8 *>(haystack), haystack_size));
    }

protected:
    void putNGramBase(const VolnitskyTraits::Ngram ngram, const int offset)
    {
        /// Put the offset for the n-gram in the corresponding cell or the nearest free cell.
        size_t cell_num = ngram % VolnitskyTraits::hash_size;

        while (hash[cell_num])
            cell_num = (cell_num + 1) % VolnitskyTraits::hash_size; /// Search for the next free cell.

        hash[cell_num] = offset;
    }
};


template <bool CaseSensitive, bool ASCII, typename FallbackSearcher>
class MultiVolnitskyBase
{
private:
    /// needles and their offsets
    const std::vector<std::string_view> & needles;


    /// fallback searchers
    std::vector<size_t> fallback_needles;
    std::vector<FallbackSearcher> fallback_searchers;

    /// because std::pair<> is not POD
    struct OffsetId
    {
        VolnitskyTraits::Id id;
        VolnitskyTraits::Offset off;
    };

    std::unique_ptr<OffsetId[]> hash;

    /// step for each bunch of strings
    size_t step;

    /// last index of offsets that was not processed
    size_t last;

    /// limit for adding to hashtable. In worst case with case insentive search, the table will be filled at most as half
    static constexpr size_t small_limit = VolnitskyTraits::hash_size / 8;

public:
    explicit MultiVolnitskyBase(const std::vector<std::string_view> & needles_) : needles{needles_}, step{0}, last{0}
    {
        fallback_searchers.reserve(needles.size());
        hash = std::unique_ptr<OffsetId[]>(new OffsetId[VolnitskyTraits::hash_size]);   /// No zero initialization, it will be done later.
    }

    /**
     * This function is needed to initialize hash table
     * Returns `true` if there is nothing to initialize
     * and `false` if we have something to initialize and initializes it.
     * This function is a kind of fallback if there are many needles.
     * We actually destroy the hash table and initialize it with uninitialized needles
     * and search through the haystack again.
     * The actual usage of this function is like this:
     * while (hasMoreToSearch())
     * {
     *     search inside the haystack with the known needles
     * }
     */
    bool hasMoreToSearch()
    {
        if (last == needles.size())
            return false;

        memset(hash.get(), 0, VolnitskyTraits::hash_size * sizeof(OffsetId));
        fallback_needles.clear();
        step = std::numeric_limits<size_t>::max();

        size_t buf = 0;
        size_t size = needles.size();

        for (; last < size; ++last)
        {
            const char * cur_needle_data = needles[last].data();
            const size_t cur_needle_size = needles[last].size();

            /// save the indices of fallback searchers
            if (VolnitskyTraits::isFallbackNeedle(cur_needle_size))
            {
                fallback_needles.push_back(last);
            }
            else
            {
                /// put all bigrams
                auto callback = [this](const VolnitskyTraits::Ngram ngram, const int offset)
                {
                    return this->putNGramBase(ngram, offset, this->last);
                };

                buf += cur_needle_size - sizeof(VolnitskyTraits::Ngram) + 1;

                /// this is the condition when we actually need to stop and start searching with known needles
                if (buf > small_limit)
                    break;

                step = std::min(step, cur_needle_size - sizeof(VolnitskyTraits::Ngram) + 1);
                for (auto i = static_cast<int>(cur_needle_size - sizeof(VolnitskyTraits::Ngram)); i >= 0; --i)
                {
                    VolnitskyTraits::putNGram<CaseSensitive, ASCII>(
                        reinterpret_cast<const UInt8 *>(cur_needle_data) + i,
                        i + 1,
                        reinterpret_cast<const UInt8 *>(cur_needle_data),
                        cur_needle_size,
                        callback);
                }
            }
            fallback_searchers.emplace_back(cur_needle_data, cur_needle_size);
        }
        return true;
    }

    inline bool searchOne(const UInt8 * haystack, const UInt8 * haystack_end) const
    {
        const size_t fallback_size = fallback_needles.size();
        for (size_t i = 0; i < fallback_size; ++i)
            if (fallback_searchers[fallback_needles[i]].search(haystack, haystack_end) != haystack_end)
                return true;

        /// check if we have one non empty volnitsky searcher
        if (step != std::numeric_limits<size_t>::max())
        {
            const auto * pos = haystack + step - sizeof(VolnitskyTraits::Ngram);
            for (; pos <= haystack_end - sizeof(VolnitskyTraits::Ngram); pos += step)
            {
                for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size; hash[cell_num].off;
                     cell_num = (cell_num + 1) % VolnitskyTraits::hash_size)
                {
                    if (pos >= haystack + hash[cell_num].off - 1)
                    {
                        const auto res = pos - (hash[cell_num].off - 1);
                        const size_t ind = hash[cell_num].id;
                        if (res + needles[ind].size() <= haystack_end && fallback_searchers[ind].compare(haystack, haystack_end, res))
                            return true;
                    }
                }
            }
        }
        return false;
    }

    inline size_t searchOneFirstIndex(const UInt8 * haystack, const UInt8 * haystack_end) const
    {
        const size_t fallback_size = fallback_needles.size();

        size_t answer = std::numeric_limits<size_t>::max();

        for (size_t i = 0; i < fallback_size; ++i)
            if (fallback_searchers[fallback_needles[i]].search(haystack, haystack_end) != haystack_end)
                answer = std::min(answer, fallback_needles[i]);

        /// check if we have one non empty volnitsky searcher
        if (step != std::numeric_limits<size_t>::max())
        {
            const auto * pos = haystack + step - sizeof(VolnitskyTraits::Ngram);
            for (; pos <= haystack_end - sizeof(VolnitskyTraits::Ngram); pos += step)
            {
                for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size; hash[cell_num].off;
                     cell_num = (cell_num + 1) % VolnitskyTraits::hash_size)
                {
                    if (pos >= haystack + hash[cell_num].off - 1)
                    {
                        const auto res = pos - (hash[cell_num].off - 1);
                        const size_t ind = hash[cell_num].id;
                        if (res + needles[ind].size() <= haystack_end && fallback_searchers[ind].compare(haystack, haystack_end, res))
                            answer = std::min(answer, ind);
                    }
                }
            }
        }

        /*
        * if nothing was found, answer + 1 will be equal to zero and we can
        * assign it into the result because we need to return the position starting with one
        */
        return answer + 1;
    }

    template <typename CountCharsCallback>
    inline UInt64 searchOneFirstPosition(const UInt8 * haystack, const UInt8 * haystack_end, const CountCharsCallback & count_chars) const
    {
        const size_t fallback_size = fallback_needles.size();

        UInt64 answer = std::numeric_limits<UInt64>::max();

        for (size_t i = 0; i < fallback_size; ++i)
            if (auto pos = fallback_searchers[fallback_needles[i]].search(haystack, haystack_end); pos != haystack_end)
                answer = std::min<UInt64>(answer, pos - haystack);

        /// check if we have one non empty volnitsky searcher
        if (step != std::numeric_limits<size_t>::max())
        {
            const auto * pos = haystack + step - sizeof(VolnitskyTraits::Ngram);
            for (; pos <= haystack_end - sizeof(VolnitskyTraits::Ngram); pos += step)
            {
                for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size; hash[cell_num].off;
                     cell_num = (cell_num + 1) % VolnitskyTraits::hash_size)
                {
                    if (pos >= haystack + hash[cell_num].off - 1)
                    {
                        const auto res = pos - (hash[cell_num].off - 1);
                        const size_t ind = hash[cell_num].id;
                        if (res + needles[ind].size() <= haystack_end && fallback_searchers[ind].compare(haystack, haystack_end, res))
                            answer = std::min<UInt64>(answer, res - haystack);
                    }
                }
            }
        }
        if (answer == std::numeric_limits<UInt64>::max())
            return 0;
        return count_chars(haystack, haystack + answer);
    }

    template <typename CountCharsCallback, typename AnsType>
    inline void searchOneAll(const UInt8 * haystack, const UInt8 * haystack_end, AnsType * answer, const CountCharsCallback & count_chars) const
    {
        const size_t fallback_size = fallback_needles.size();
        for (size_t i = 0; i < fallback_size; ++i)
        {
            const UInt8 * ptr = fallback_searchers[fallback_needles[i]].search(haystack, haystack_end);
            if (ptr != haystack_end)
                answer[fallback_needles[i]] = count_chars(haystack, ptr);
        }

        /// check if we have one non empty volnitsky searcher
        if (step != std::numeric_limits<size_t>::max())
        {
            const auto * pos = haystack + step - sizeof(VolnitskyTraits::Ngram);
            for (; pos <= haystack_end - sizeof(VolnitskyTraits::Ngram); pos += step)
            {
                for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size; hash[cell_num].off;
                     cell_num = (cell_num + 1) % VolnitskyTraits::hash_size)
                {
                    if (pos >= haystack + hash[cell_num].off - 1)
                    {
                        const auto * res = pos - (hash[cell_num].off - 1);
                        const size_t ind = hash[cell_num].id;
                        if (answer[ind] == 0
                            && res + needles[ind].size() <= haystack_end
                            && fallback_searchers[ind].compare(haystack, haystack_end, res))
                            answer[ind] = count_chars(haystack, res);
                    }
                }
            }
        }
    }

    void putNGramBase(const VolnitskyTraits::Ngram ngram, const int offset, const size_t num)
    {
        size_t cell_num = ngram % VolnitskyTraits::hash_size;

        while (hash[cell_num].off)
            cell_num = (cell_num + 1) % VolnitskyTraits::hash_size;

        hash[cell_num] = {static_cast<VolnitskyTraits::Id>(num), static_cast<VolnitskyTraits::Offset>(offset)};
    }
};


using Volnitsky = VolnitskyBase<true, true, ASCIICaseSensitiveStringSearcher>;
using VolnitskyUTF8 = VolnitskyBase<true, false, ASCIICaseSensitiveStringSearcher>; /// exactly same as Volnitsky
using VolnitskyCaseInsensitive = VolnitskyBase<false, true, ASCIICaseInsensitiveStringSearcher>; /// ignores non-ASCII bytes
using VolnitskyCaseInsensitiveUTF8 = VolnitskyBase<false, false, UTF8CaseInsensitiveStringSearcher>;

using VolnitskyCaseSensitiveToken = VolnitskyBase<true, true, ASCIICaseSensitiveTokenSearcher>;
using VolnitskyCaseInsensitiveToken = VolnitskyBase<false, true, ASCIICaseInsensitiveTokenSearcher>;

using MultiVolnitsky = MultiVolnitskyBase<true, true, ASCIICaseSensitiveStringSearcher>;
using MultiVolnitskyUTF8 = MultiVolnitskyBase<true, false, ASCIICaseSensitiveStringSearcher>;
using MultiVolnitskyCaseInsensitive = MultiVolnitskyBase<false, true, ASCIICaseInsensitiveStringSearcher>;
using MultiVolnitskyCaseInsensitiveUTF8 = MultiVolnitskyBase<false, false, UTF8CaseInsensitiveStringSearcher>;


}
