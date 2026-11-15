#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSimilarity.h>
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>
#include <Common/TargetSpecific.h>
#include <Common/UTF8Helpers.h>
#include <Common/iota.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <bit>
#include <climits>
#include <chrono>
#include <cstdint>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif
#if USE_X86_64_MULTITARGET_CODE
#    include <immintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Op>
struct FunctionStringDistanceImpl
{
    using ResultType = typename Op::ResultType;

    static void constantConstant(const String & haystack, const String & needle, ResultType & res)
    {
        res = Op::process(haystack.data(), haystack.size(), needle.data(), needle.size());
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<ResultType> & res,
        size_t input_rows_count)
    {
        const char * haystack = reinterpret_cast<const char *>(haystack_data.data());
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = Op::process(
                haystack + haystack_offsets[i - 1],
                haystack_offsets[i] - haystack_offsets[i - 1],
                needle + needle_offsets[i - 1],
                needle_offsets[i] - needle_offsets[i - 1]);
        }
    }

    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<ResultType> & res,
        size_t input_rows_count)
    {
        const char * haystack_data = haystack.data();
        size_t haystack_size = haystack.size();
        const char * needle = reinterpret_cast<const char *>(needle_data.data());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = Op::process(haystack_data, haystack_size,
                needle + needle_offsets[i - 1], needle_offsets[i] - needle_offsets[i - 1]);
        }
    }

    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const String & needle,
        PaddedPODArray<ResultType> & res,
        size_t input_rows_count)
    {
        constantVector(needle, data, offsets, res, input_rows_count);
    }

};

struct ByteHammingDistanceImpl
{
    using ResultType = UInt64;
    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        UInt64 res = 0;
        const char * haystack_end = haystack + haystack_size;
        const char * needle_end = needle + needle_size;

#ifdef __SSE4_2__
        static constexpr auto mode = _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY;

        const char * haystack_end16 = haystack + haystack_size / 16 * 16;
        const char * needle_end16 = needle + needle_size / 16 * 16;

        for (; haystack < haystack_end16 && needle < needle_end16; haystack += 16, needle += 16)
        {
            __m128i s1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
            __m128i s2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(needle));
            auto result_mask = _mm_cmpestrm(s1, 16, s2, 16, mode);
            const __m128i mask_hi = _mm_unpackhi_epi64(result_mask, result_mask);
            res += _mm_popcnt_u64(_mm_cvtsi128_si64(result_mask)) + _mm_popcnt_u64(_mm_cvtsi128_si64(mask_hi));
        }
#endif
        for (; haystack != haystack_end && needle != needle_end; ++haystack, ++needle)
            res += *haystack != *needle;

        res = res + (haystack_end - haystack) + (needle_end - needle);
        return res;
    }
};

template <typename UTF8Consumer>
void parseUTF8String(const char * __restrict data, size_t size, UTF8Consumer && utf8_consumer)
{
    const char * end = data + size;
    while (data < end)
    {
        size_t len = UTF8::seqLength(*data);
        if (len == 1)
        {
            utf8_consumer(static_cast<UInt32>(*data));
            ++data;
        }
        else
        {
            auto code_point = UTF8::convertUTF8ToCodePoint(data, end - data);
            if (code_point.has_value())
            {
                utf8_consumer(code_point.value());
                data += len;
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Illegal UTF-8 sequence, while processing '{}'", std::string_view(data, end - data));
            }
        }
    }
}

template <bool is_utf8>
struct ByteJaccardIndexImpl
{

    /// For byte strings (and ASCII chars of UTF8) use an array
    struct ScratchASCII
    {
        using CalcType = UInt8;
        constexpr static size_t max_size = std::numeric_limits<unsigned char>::max() + 1;
        UInt8 haystack_set[max_size]{};
        UInt8 needle_set[max_size]{};
    };

    /// For UTF8 use a hash set for wider codepoints
    struct ScratchUTF8
    {
        using CalcType = UInt32;
        using Set = HashSet<UInt32, DefaultHash<UInt32>>;
        Set haystack_utf8_set;
        Set needle_utf8_set;
    };

    using ScratchType = std::conditional_t<is_utf8, ScratchUTF8, ScratchASCII>;

    using ResultType = Float64;
    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        if (haystack_size == 0 || needle_size == 0)
            return 0;

        const char * haystack_end = haystack + haystack_size;
        const char * needle_end = needle + needle_size;

        ScratchType scratch;

        if constexpr (is_utf8)
        {
            parseUTF8String(
                haystack,
                haystack_size,
                [&](UInt32 data) { scratch.haystack_utf8_set.insert(data); });
            parseUTF8String(
                needle, needle_size, [&](UInt32 data) { scratch.needle_utf8_set.insert(data); });
        }
        else
        {
            while (haystack < haystack_end)
            {
                scratch.haystack_set[static_cast<unsigned char>(*haystack)] = 1;
                ++haystack;
            }
            while (needle < needle_end)
            {
                scratch.needle_set[static_cast<unsigned char>(*needle)] = 1;
                ++needle;
            }
        }

        using CalcType = typename ScratchType::CalcType;

        CalcType intersection = 0;
        CalcType union_size = 0;

        if constexpr (is_utf8)
        {
            const auto & small = (scratch.haystack_utf8_set.size() < scratch.needle_utf8_set.size()) ? scratch.haystack_utf8_set : scratch.needle_utf8_set;
            const auto & large = (scratch.haystack_utf8_set.size() < scratch.needle_utf8_set.size()) ? scratch.needle_utf8_set : scratch.haystack_utf8_set;

            for (auto it = small.begin(); it != small.end(); ++it)
            {
                const auto & key = it->getKey();
                if (large.has(key))
                    ++intersection;
            }
            union_size = static_cast<UInt32>(scratch.haystack_utf8_set.size() + scratch.needle_utf8_set.size() - intersection);
        }
        else
        {
            for (size_t i = 0; i < ScratchType::max_size; ++i)
            {
                intersection += (scratch.haystack_set[i] & scratch.needle_set[i]);
                union_size += (scratch.haystack_set[i] | scratch.needle_set[i]);
            }
        }

        return static_cast<ResultType>(intersection) / static_cast<ResultType>(union_size);
    }
};

static constexpr size_t max_string_size = 1u << 16;

template<bool is_utf8, typename WorkType = UInt64>
struct ByteEditDistanceMyersImpl
{
    using SymbolT = std::conditional_t<is_utf8, UInt32, UInt8>;
    using Word = WorkType;

    struct ScratchASCII
    {
        Word ascii_masks[256]{};
    };
    struct ScratchUTF8
    {
        HashMapWithStackMemory<UInt32, Word, DefaultHash<UInt32>, 6> unicode_masks;
    };

    using ScratchT = std::conditional_t<is_utf8, ScratchUTF8, ScratchASCII>;

    static UInt32 distance(const SymbolT* __restrict haystack, UInt32 haystack_len, const SymbolT* __restrict needle, UInt32 needle_len)
    {
        ScratchT scratch;

        if constexpr (!is_utf8)
        {
            for (size_t i = 0; i < needle_len; ++i)
                scratch.ascii_masks[needle[i]] |= Word(1) << i;
        }
        else
        {
            for (size_t i = 0; i < needle_len; ++i)
                scratch.unicode_masks[needle[i]] = scratch.unicode_masks[needle[i]] | (Word(1) << i);
        }

        Word VP = ~Word(0);
        Word VN = 0;
        uint32_t score = needle_len;

        const Word highest_bit = Word(1) << (needle_len - 1);

        // From: Bit-Parallel Approximate String Matching Algorithms with Transposition (Heikki Hyyro)
        for (size_t i = 0; i < haystack_len; ++i)
        {
            // PM = pattern match mask for current text symbol
            // PM[j] = 1 iff needle[j] == haystack[i]
            Word PM;
            if constexpr (!is_utf8)
            {
                PM = scratch.ascii_masks[haystack[i]];
            }
            else
            {
                auto it = scratch.unicode_masks.find(haystack[i]);
                PM = (it == scratch.unicode_masks.end()) ? Word(0) : it->getMapped();
            }

            // X combines matches and vertical negative deltas
            Word X = PM | VN;
            // D0: diagonal zero-delta vector
            // D0[j] = 1 iff D[i][j] == D[i-1][j-1]
            Word D0 = (((X & VP) + VP) ^ VP) | X;
            // HP: horizontal positive deltas
            // HP[j] = 1 iff D[i][j] - D[i][j-1] == +1
            Word HP = VN | ~(D0 | VP);
            // HN: horizontal negative deltas
            // HN[j] = 1 iff D[i][j] - D[i][j-1] == -1
            Word HN = VP & D0;

            // Update the edit distance score using the last pattern position (corresponds to D[m][i])
            if (HP & highest_bit) ++score;
            if (HN & highest_bit) --score;

            // Update vertical positive deltas for next column
            VP = (HN << 1) | ~(D0 | ((HP << 1) | 1));
            // Update vertical negative deltas for next column
            VN = ((HP << 1) | 1) & D0;
        }

        return score;
    }
};

template <bool is_utf8, typename Word = UInt64>
struct BlockMyersEditDistance
{
    struct PatternMasksASCII
    {
        static constexpr size_t W = sizeof(Word) * 8;
        // Needles up to stack_blocks * W chars keep the pattern table in inline storage,
        // avoiding a per-row heap allocation in the hot edit-distance loop. Longer needles
        // spill to a tracked allocation.
        static constexpr size_t stack_blocks = 8;
        UInt32 num_blocks = 0;
        // flat: tbl[c * num_blocks + k] = bitmask of positions of char c in block k
        PODArrayWithStackMemory<Word, 256 * stack_blocks * sizeof(Word)> tbl;

        void build(const UInt8 * needle, UInt32 m)
        {
            num_blocks = (m + W - 1) / W;
            tbl.assign(static_cast<size_t>(num_blocks) * 256, Word(0));
            for (UInt32 i = 0; i < m; ++i)
                tbl[needle[i] * num_blocks + i / W] |= Word(1) << (i % W);
        }

        const Word * operator[](UInt8 c) const { return tbl.data() + c * num_blocks; }
    };

    struct PatternMasksUTF8
    {
        static constexpr size_t W = sizeof(Word) * 8;
        UInt32 num_blocks = 0;
        using Map = HashMapWithStackMemory<UInt32, UInt32, DefaultHash<UInt32>, 6>;
        Map offset_map;
        VectorWithMemoryTracking<Word> arena; // flat: arena[offset_map[c] + k] = bitmask of positions of char c in block k

        void build(const UInt32 * needle, UInt32 m)
        {
            num_blocks = (m + W - 1) / W;

            // Pass 1: count unique codepoints and assign offsets
            UInt32 num_unique = 0;
            for (UInt32 i = 0; i < m; ++i)
            {
                Map::LookupResult it = nullptr;
                bool inserted = false;
                offset_map.emplace(needle[i], it, inserted);
                if (inserted)
                {
                    it->getMapped() = num_unique++ * num_blocks;
                }
            }

            arena.assign(num_unique * num_blocks, Word(0));

            // Pass 2: fill masks
            for (UInt32 i = 0; i < m; ++i)
                arena[offset_map[needle[i]] + i / W] |= Word(1) << (i % W);
        }

        const Word * operator[](UInt32 c) const
        {
            const auto * it = offset_map.find(c);
            return (it == offset_map.end()) ? nullptr : arena.data() + it->getMapped();
        }
    };

    using SymbolT = std::conditional_t<is_utf8, UInt32, UInt8>;
    static constexpr size_t W = sizeof(Word) * 8;

    // Requires haystack_len and needle_len > 0
    static UInt32 distance(
        const SymbolT * __restrict haystack, UInt32 haystack_len,
        const SymbolT * __restrict needle,   UInt32 needle_len)
    {
        const UInt32 num_blocks   = (needle_len + W - 1) / W;
        const UInt32 last         = num_blocks - 1;
        const UInt32 bits_in_last = needle_len - last * W;   // in [1, W]
        const Word last_mask  = (bits_in_last == W)
                                    ? ~Word(0)
                                    : (Word(1) << bits_in_last) - Word(1);

        // score_mask selects the bit corresponding to the last character
        // of the needle (position m-1). Its state determines whether
        // the edit distance increases or decreases for this column.
        const Word score_mask = Word(1) << (bits_in_last - 1);

        using PatternMasks = std::conditional_t<is_utf8, PatternMasksUTF8, PatternMasksASCII>;
        PatternMasks PM_tbl;
        PM_tbl.build(needle, needle_len);

        // Inline storage for needles up to stack_limit * W = 8192 chars keeps these
        // per-row buffers off the heap, avoiding a per-row allocation and memory-tracker
        // atomics in the hot loop. Longer needles spill to a tracked allocation.
        constexpr size_t stack_limit = 128;
        using StackVec = PODArrayWithStackMemory<Word, stack_limit * sizeof(Word)>;

        // As in the non-blocked version, but this time per block:
        // VP all ones, VN all zeros encodes that the first column's edit distances equal the row index (0,1,2,...,m).
        StackVec VP;
        StackVec VN;
        VP.assign(static_cast<size_t>(num_blocks), ~Word(0));
        VN.assign(static_cast<size_t>(num_blocks), Word(0));
        UInt32 score = needle_len;

        // HP/HN/D0 per-block scratch, allocated once and reused across columns.
        StackVec hp_arr;
        StackVec hn_arr;
        StackVec d0_arr;
        hp_arr.resize(num_blocks);
        hn_arr.resize(num_blocks);
        d0_arr.resize(num_blocks);

        for (UInt32 col = 0; col < haystack_len; ++col)
        {
            const SymbolT ch = haystack[col];
            const Word *  pm = PM_tbl[ch]; // may be nullptr (no match anywhere)

            // Pass 1: compute D0, HP, HN per block (cannot update VP/VN until we have HP/HN for all blocks).
            // We store HP/HN/D0 in scratch arrays because we cannot overwrite VP/VN yet (we still need their old values).
            // Carry from the addition within block k propagates to block k+1.
            // carry = 0 initially (there is no carry into block 0).
            Word add_carry = 0;

            for (UInt32 k = 0; k < num_blocks; ++k)
            {
                const Word pm_k = pm ? pm[k] : Word(0);
                const Word vp   = VP[k];
                const Word vn   = VN[k];

                Word X  = pm_k | vn;
                Word Y  = X & vp;

                // Blocked addition: sum = Y + vp + add_carry
                // carry_out = overflow of the Word addition
                Word s1 = Y + vp;
                Word c1 = (s1 < Y) ? Word(1) : Word(0);
                Word s2 = s1 + add_carry;
                Word c2 = (s2 < s1) ? Word(1) : Word(0);
                add_carry = c1 | c2;  // single-bit carry out

                Word D0 = (s2 ^ vp) | X;
                Word HP = vn  | ~(D0 | vp);
                Word HN = vp & D0;

                hp_arr[k] = HP;
                hn_arr[k] = HN;
                d0_arr[k] = D0;
            }

            // Update the edit distance score from the last block
            if (hp_arr[last] & score_mask) ++score;
            if (hn_arr[last] & score_mask) --score;

            // Pass 2: shift HP/HN left by 1 (cross-block) and update VP/VN
            // In the single-word version this is just: (HP << 1) | 1.
            // With multiple blocks, the shift must carry across block boundaries:
            //   - For block 0: shift left and insert 1 as the lowest bit.
            //   - For block k > 0: shift left and insert the top bit (MSB)
            //     from the previous block.
            // HN is shifted the same way, except its first inserted bit is 0.
            Word hp_carry_in = Word(1); // the "+1" in the original formula
            Word hn_carry_in = Word(0);

            // For each block:
            // 1) Shift HP and HN left by one bit.
            //    - The highest bit becomes the carry into the next block.
            //    - The lowest bit comes from the previous block (hp_carry_in / hn_carry_in).
            // 2) In the last block, mask off bits beyond the pattern length
            //    so we don’t affect unused bits.
            // 3) Update VP and VN using the standard Myers formulas.
            // 4) Pass the outgoing carry to the next block.
            for (UInt32 k = 0; k < num_blocks; ++k)
            {
                Word HP = hp_arr[k];
                Word HN = hn_arr[k];
                Word D0 = d0_arr[k];

                // Capture the bit that will shift into the next block
                Word hp_carry_out = HP >> (W - 1);
                Word hn_carry_out = HN >> (W - 1);

                Word HP_sh = (HP << 1) | hp_carry_in;
                Word HN_sh = (HN << 1) | hn_carry_in;

                // Mask last block: bits above needle_len - 1 must stay 0/1
                // VP is initialised to ~0 so we must keep top bits of last block as 1.
                // The mask only applies to HP_sh and HN_sh (and D0 for last block).
                if (k == last)
                {
                    HP_sh &= last_mask;
                    HN_sh &= last_mask;
                    D0    &= last_mask;
                }

                // VP/VN update (same formula as single-block)
                VP[k] = HN_sh | ~(D0 | HP_sh);
                VN[k] = HP_sh & D0;

                hp_carry_in = hp_carry_out;
                hn_carry_in = hn_carry_out;
            }
        }

        return score;
    }
};


template <bool is_utf8>
struct ByteEditDistanceDPImpl
{
    using WorkingType = UInt32;
    using SymbolT = std::conditional_t<is_utf8, UInt32, UInt8>;

    static UInt32 distance(const SymbolT * __restrict haystack, UInt32 haystack_size, const SymbolT * __restrict needle, UInt32 needle_size)
    {
        PaddedPODArray<WorkingType> distances0(haystack_size + 1);
        PaddedPODArray<WorkingType> distances1(haystack_size + 1);
        auto * __restrict prev = distances0.data();
        auto * __restrict curr = distances1.data();

        WorkingType substitution = 0;
        WorkingType insertion = 0;
        WorkingType deletion = 0;

        iota(distances0.data(), haystack_size + 1, WorkingType(0));

        const auto has_tail = (haystack_size % 2) == 1;

        for (size_t pos_needle = 0; pos_needle < needle_size; ++pos_needle)
        {
            curr[0] = static_cast<WorkingType>(pos_needle + 1);

            const auto needle_char = needle[pos_needle];

            size_t pos_haystack = 0;
            for (; pos_haystack + 1 < haystack_size; pos_haystack += 2)
            {
                // First:
                deletion = prev[pos_haystack + 1] + 1;
                insertion = curr[pos_haystack] + 1;
                substitution = prev[pos_haystack];

                if (needle_char != *(haystack + pos_haystack))
                    substitution += 1;

                curr[pos_haystack + 1] = std::min({deletion, substitution, insertion});
                // Second:
                deletion = prev[pos_haystack + 2] + 1;
                insertion = curr[pos_haystack + 1] + 1;
                substitution = prev[pos_haystack + 1];

                if (needle_char != *(haystack + pos_haystack + 1))
                    substitution += 1;
                curr[pos_haystack + 2] = std::min({deletion, substitution, insertion});
            }
            if (has_tail)
            {
                deletion = prev[pos_haystack + 1] + 1;
                insertion = curr[pos_haystack] + 1;
                substitution = prev[pos_haystack];
                if (needle_char != *(haystack + pos_haystack))
                    substitution += 1;
                curr[pos_haystack + 1] = std::min({deletion, substitution, insertion});
            }
            std::swap(curr, prev);
        }

        return prev[haystack_size];
    }
};

template <bool is_utf8>
struct ByteEditDistanceImpl
{
    using ResultType = UInt64;
    using WorkingType = UInt32;
    using SymbolT = std::conditional_t<is_utf8, UInt32, UInt8>;

    /// If `max_length` is set, it gets the length of the longer string, counted as for the distance.
    static ResultType process(
        const char * __restrict haystack,
        size_t haystack_size,
        const char * __restrict needle,
        size_t needle_size,
        size_t * max_length = nullptr)
    {
        /// The distance to an empty string is the length of the other string: its size in bytes,
        /// or its number of code points for the UTF8 variant (validated as in the general case).
        if (haystack_size == 0 || needle_size == 0)
        {
            size_t length = haystack_size + needle_size;
            if constexpr (is_utf8)
            {
                length = 0;
                parseUTF8String(haystack_size ? haystack : needle, haystack_size + needle_size, [&](UInt32) { ++length; });
            }
            if (max_length)
                *max_length = length;
            return length;
        }

        /// Safety threshold against DoS, since we use two arrays to calculate the distance.
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function editDistance, should be at most {}",
                max_string_size);

        PaddedPODArray<UInt32> haystack_utf8;
        PaddedPODArray<UInt32> needle_utf8;
        if constexpr (is_utf8)
        {
            parseUTF8String(haystack, haystack_size, [&](UInt32 data) { haystack_utf8.push_back(data); });
            parseUTF8String(needle, needle_size, [&](UInt32 data) { needle_utf8.push_back(data); });
            haystack_size = haystack_utf8.size();
            needle_size = needle_utf8.size();
        }
        if (needle_size > haystack_size)
        {
            std::swap(needle_size, haystack_size);
            if constexpr (is_utf8)
            {
                std::swap(needle_utf8, haystack_utf8);
            }
            else
            {
                std::swap(needle, haystack);
            }
        }
        if (max_length)
            *max_length = haystack_size;

        auto dispatchMyers = [&]<typename F>()
        {
            if constexpr (is_utf8)
            {
                return F::distance(
                    haystack_utf8.data(), static_cast<UInt32>(haystack_size), needle_utf8.data(), static_cast<UInt32>(needle_size));
            }
            else
            {
                return F::distance(
                    reinterpret_cast<const SymbolT *>(haystack),
                    static_cast<UInt32>(haystack_size),
                    reinterpret_cast<const SymbolT *>(needle),
                    static_cast<UInt32>(needle_size));
            }
        };

        constexpr auto cutoff_size = []{
            if constexpr (!is_utf8)
                return 8192;
            else
                return 4096;
        }();

        if (needle_size <= sizeof(UInt64) * CHAR_BIT)
            return dispatchMyers.template operator()<ByteEditDistanceMyersImpl<is_utf8, UInt64>>();
        else if (needle_size <= sizeof(UInt128) * CHAR_BIT)
            return dispatchMyers.template operator()<ByteEditDistanceMyersImpl<is_utf8, UInt128>>();
        else if (needle_size <= sizeof(UInt256) * CHAR_BIT)
            return dispatchMyers.template operator()<ByteEditDistanceMyersImpl<is_utf8, UInt256>>();
        else if (needle_size <= cutoff_size)
            return dispatchMyers.template operator()<BlockMyersEditDistance<is_utf8, UInt64>>();
        else
        {
            if constexpr (is_utf8)
            {
                return ByteEditDistanceDPImpl<is_utf8>::distance(
                    haystack_utf8.data(), static_cast<UInt32>(haystack_size), needle_utf8.data(), static_cast<UInt32>(needle_size));
            }
            else
            {
                return ByteEditDistanceDPImpl<is_utf8>::distance(
                    reinterpret_cast<const SymbolT *>(haystack),
                    static_cast<UInt32>(haystack_size),
                    reinterpret_cast<const SymbolT *>(needle),
                    static_cast<UInt32>(needle_size));
            }
        }
    }
};

/// Normalized Levenshtein similarity: 1 - editDistance / max(length1, length2),
/// with lengths in bytes, or in code points for the UTF8 variant. Two empty strings are equal (1).
template <bool is_utf8>
struct ByteEditDistanceRatioImpl
{
    using ResultType = Float64;

    static ResultType process(const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Safety threshold against DoS, as for editDistance. It protects the distance computation, which does not run
        /// when one string is empty.
        if (haystack_size != 0 && needle_size != 0 && (haystack_size > max_string_size || needle_size > max_string_size))
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function editDistanceRatio, should be at most {}",
                max_string_size);

        /// The length comes from the same parse as the distance, so they count the same symbols: a byte from 0x80 to 0xBF
        /// without a lead byte is one symbol in both. A non-empty string against an empty one gives 0.
        size_t max_length = 0;
        const UInt64 distance = ByteEditDistanceImpl<is_utf8>::process(haystack, haystack_size, needle, needle_size, &max_length);
        if (max_length == 0)
            return 1;

        return 1 - static_cast<Float64>(distance) / static_cast<Float64>(max_length);
    }
};

/// Length of the longest common subsequence, bit-parallel: one bit per needle symbol, 64 symbols per word.
/// From: Bit-Parallel LCS-length Computation Revisited (Heikki Hyyro, 2004).
/// For each haystack symbol c: U = S & PM[c], S = (S + U) | (S - U). The zero bits of S count the LCS.
/// U is a subset of S, so S - U never borrows and only the addition carries from word to word.
template <bool is_utf8, typename Word = UInt64>
struct LongestCommonSubsequenceImpl
{
    using SymbolT = std::conditional_t<is_utf8, UInt32, UInt8>;
    static constexpr size_t W = sizeof(Word) * 8;

    /// Requires needle_len > 0
    static UInt32 length(const SymbolT * __restrict haystack, UInt32 haystack_len, const SymbolT * __restrict needle, UInt32 needle_len)
    {
        using PatternMasks = std::conditional_t<is_utf8,
            typename BlockMyersEditDistance<is_utf8, Word>::PatternMasksUTF8,
            typename BlockMyersEditDistance<is_utf8, Word>::PatternMasksASCII>;
        PatternMasks pm_table;
        pm_table.build(needle, needle_len);

        const UInt32 num_blocks = static_cast<UInt32>((needle_len + W - 1) / W);
        constexpr size_t stack_limit = 128;
        PODArrayWithStackMemory<Word, stack_limit * sizeof(Word)> s;
        s.assign(static_cast<size_t>(num_blocks), ~Word(0));

        for (UInt32 i = 0; i < haystack_len; ++i)
        {
            const Word * pm = pm_table[haystack[i]];
            if (!pm)
                continue; /// The symbol does not occur in the needle, S stays the same.

            Word carry = 0;
            for (UInt32 k = 0; k < num_blocks; ++k)
            {
                const Word u = s[k] & pm[k];
                const Word sum = s[k] + u;
                const Word with_carry = sum + carry;
                carry = (sum < u) | (with_carry < sum);
                s[k] = with_carry | (s[k] - u);
            }
        }

        /// Bits above needle_len in the last word do not belong to the needle.
        const size_t bits_in_last = needle_len - (num_blocks - 1) * W;
        UInt32 lcs = 0;
        for (UInt32 k = 0; k < num_blocks; ++k)
        {
            Word zeros = ~s[k];
            if (k == num_blocks - 1 && bits_in_last < W)
                zeros &= (Word(1) << bits_in_last) - 1;
            lcs += static_cast<UInt32>(std::popcount(zeros));
        }
        return lcs;
    }
};

/// Normalized indel similarity, as in Python's Levenshtein.ratio: 2 * LCS / (length1 + length2),
/// i.e. 1 - (insertions + deletions) / (length1 + length2). Two empty strings are equal (1).
template <bool is_utf8>
struct ByteStringSimilarityRatioImpl
{
    using ResultType = Float64;
    using SymbolT = std::conditional_t<is_utf8, UInt32, UInt8>;

    static ResultType process(const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Safety threshold against DoS, as for editDistance. It protects the LCS computation, which does not run
        /// when one string is empty.
        if (haystack_size != 0 && needle_size != 0 && (haystack_size > max_string_size || needle_size > max_string_size))
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function stringSimilarityRatio, should be at most {}",
                max_string_size);

        PaddedPODArray<UInt32> haystack_utf8;
        PaddedPODArray<UInt32> needle_utf8;
        const SymbolT * haystack_symbols = nullptr;
        const SymbolT * needle_symbols = nullptr;
        if constexpr (is_utf8)
        {
            parseUTF8String(haystack, haystack_size, [&](UInt32 code_point) { haystack_utf8.push_back(code_point); });
            parseUTF8String(needle, needle_size, [&](UInt32 code_point) { needle_utf8.push_back(code_point); });
            haystack_symbols = haystack_utf8.data();
            haystack_size = haystack_utf8.size();
            needle_symbols = needle_utf8.data();
            needle_size = needle_utf8.size();
        }
        else
        {
            haystack_symbols = reinterpret_cast<const SymbolT *>(haystack);
            needle_symbols = reinterpret_cast<const SymbolT *>(needle);
        }

        const size_t total_length = haystack_size + needle_size;
        if (total_length == 0)
            return 1;
        if (haystack_size == 0 || needle_size == 0)
            return 0;

        /// The bit vector runs over the shorter string.
        if (needle_size > haystack_size)
        {
            std::swap(haystack_symbols, needle_symbols);
            std::swap(haystack_size, needle_size);
        }

        const UInt32 lcs = LongestCommonSubsequenceImpl<is_utf8>::length(
            haystack_symbols, static_cast<UInt32>(haystack_size), needle_symbols, static_cast<UInt32>(needle_size));
        /// Computed as 1 - indel / total rather than 2 * lcs / total to give bit-for-bit the same value as Python.
        const size_t indel_distance = total_length - 2 * static_cast<size_t>(lcs);
        return 1 - static_cast<Float64>(indel_distance) / static_cast<Float64>(total_length);
    }
};

struct ByteDamerauLevenshteinDistanceImpl
{
    using ResultType = UInt64;

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function damerauLevenshteinDistance, should be at most {}", max_string_size);

        /// Shortcuts:

        if (haystack_size == 0)
            return needle_size;

        if (needle_size == 0)
            return haystack_size;

        if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0)
            return 0;

        /// Implements the algorithm for optimal string alignment distance from
        /// https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Optimal_string_alignment_distance

        /// Dynamically allocate memory for the 2D array
        /// Allocating a 2D array, for convenience starts is an array of pointers to the start of the rows.
        VectorWithMemoryTracking<int> d((needle_size + 1) * (haystack_size + 1));
        VectorWithMemoryTracking<int *> starts(haystack_size + 1);

        /// Setting the pointers in starts to the beginning of (needle_size + 1)-long intervals.
        /// Also initialize the row values based on the mentioned algorithm.
        for (size_t i = 0; i <= haystack_size; ++i)
        {
            starts[i] = d.data() + (needle_size + 1) * i;
            starts[i][0] = static_cast<int>(i);
        }

        for (size_t j = 0; j <= needle_size; ++j)
        {
            starts[0][j] = static_cast<int>(j);
        }

        for (size_t i = 1; i <= haystack_size; ++i)
        {
            for (size_t j = 1; j <= needle_size; ++j)
            {
                int cost = (haystack[i - 1] == needle[j - 1]) ? 0 : 1;
                starts[i][j] = std::min(
                    {starts[i - 1][j] + 1, /// deletion
                     starts[i][j - 1] + 1, /// insertion
                     starts[i - 1][j - 1] + cost} /// substitution
                );
                if (i > 1 && j > 1 && haystack[i - 1] == needle[j - 2] && haystack[i - 2] == needle[j - 1])
                    starts[i][j] = std::min(starts[i][j], starts[i - 2][j - 2] + 1); /// transposition
            }
        }

        return starts[haystack_size][needle_size];
    }
};

/// AVX2-accelerated greedy Jaro matcher. For each i in s1 (ascending), we
/// find the smallest unmatched j in [i-d, i+d] with s2[j] == s1[i]. Two
/// kernels split by needle length:
///   * `|needle| <= 64`: hoist all of `needle` into one or two `__m256i`
///     registers. Per s1 char: broadcast/cmpeq/movemask -> 64-bit eq
///     mask; AND with window AND ~matched_s2; ctzll picks the smallest j.
///   * `|needle|  > 64`: slide a 4xAVX2 (= 128 byte) window over needle
///     with a single branch per 128 bytes. The 128-bit eq mask is AND-ed
///     with ~matched_s2 loaded at the current bit offset.
/// `matched_s1` is recorded in encounter order; `matched_s2` is a bitmap
/// walked via ctzll/blsr in the transposition pass. Result is byte-
/// identical to the scalar reference.
///
/// Wrapped in `DECLARE_X86_64_V3_SPECIFIC_CODE` so the helpers are compiled
/// with the AVX2 target attribute regardless of the default build flags and
/// selected at runtime via `isArchSupported(TargetArch::x86_64_v3)`. The
/// `apply_to=function` pragma ensures the lambda inside `jaroScan` also
/// inherits the attribute.
#if USE_X86_64_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(

/// Mask covering bit positions [lo, hi). Caller guarantees 0 <= lo <= hi <= 64.
inline UInt64 jaroWindowMask(int lo, int hi)
{
    const UInt64 hi_mask = (hi == 64) ? ~UInt64{0} : ((UInt64{1} << hi) - 1);
    const UInt64 lo_mask = (UInt64{1} << lo) - 1;
    return hi_mask & ~lo_mask;
}

inline double jaroFinishScore(int matches, int trans2, int s1_len, int s2_len)
{
    if (matches == 0)
        return 0.0;
    const double m = static_cast<double>(matches);
    const double t = static_cast<double>(trans2) * 0.5;
    return (1.0 / 3.0) * (m / s1_len + m / s2_len + (m - t) / m);
}

static double jaroSmall(const unsigned char * s1, int s1_len,
                        const unsigned char * s2, int s2_len,
                        int max_range)
{
    alignas(32) unsigned char buf[64] = {};
    memcpy(buf, s2, s2_len);
    const __m256i s2v0 = _mm256_load_si256(reinterpret_cast<const __m256i *>(buf));
    const __m256i s2v1 = _mm256_load_si256(reinterpret_cast<const __m256i *>(buf + 32));
    const UInt64 valid = (s2_len == 64) ? ~UInt64{0} : ((UInt64{1} << s2_len) - 1);

    /// matches <= s2_len <= 64, so the encounter list fits on the stack.
    int idx_buf[64];
    int matches = 0;
    UInt64 matched_s2 = 0;

    for (int i = 0; i < s1_len; ++i)
    {
        const int lo = std::max(0, i - max_range);
        const int hi = std::min(s2_len, i + max_range + 1);
        if (lo >= hi)
            continue;
        const __m256i target = _mm256_set1_epi8(static_cast<char>(s1[i]));
        const UInt32 e0 = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(target, s2v0)));
        const UInt32 e1 = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(target, s2v1)));
        const UInt64 eq = UInt64(e0) | (UInt64(e1) << 32);
        const UInt64 cand = eq & jaroWindowMask(lo, hi) & valid & ~matched_s2;
        if (cand)
        {
            const int j = static_cast<int>(getTrailingZeroBitsUnsafe(cand));
            matched_s2 |= UInt64{1} << j;
            idx_buf[matches++] = i;
        }
    }

    int trans2 = 0;
    UInt64 b = matched_s2;
    for (int k = 0; k < matches; ++k)
    {
        const int j = static_cast<int>(getTrailingZeroBitsUnsafe(b));
        b &= b - 1;
        trans2 += (s1[idx_buf[k]] != s2[j]);
    }
    return jaroFinishScore(matches, trans2, s1_len, s2_len);
}

/// Read 64 bits from a packed bitmap starting at an arbitrary bit offset.
/// Caller must provide a sentinel word past the last live position so this
/// can blindly load `bits[word + 1]`.
inline UInt64 jaroBits64At(const UInt64 * bits, int bit_off)
{
    const int word = bit_off >> 6;
    const int sh   = bit_off & 63;
    const UInt64 lo = bits[word] >> sh;
    const UInt64 hi = (sh == 0) ? 0 : (bits[word + 1] << (64 - sh));
    return lo | hi;
}

static double jaroScan(const unsigned char * s1, int s1_len,
                       const unsigned char * s2, int s2_len,
                       int max_range)
{
    const int s2_words = (s2_len + 63) / 64;
    /// One sentinel word past the end so jaroBits64At can load [word + 1].
    PaddedPODArray<UInt64> matched_s2_bits(s2_words + 1, 0);
    PaddedPODArray<int> idx_buf(s1_len);
    int matches = 0;

    /// Returns the smallest unmatched j in [lo, hi) with s2[j] == c, or -1.
    auto findMatch = [&](unsigned char c, int lo, int hi) -> int
    {
        const __m256i target = _mm256_set1_epi8(static_cast<char>(c));
        int j = lo;

        /// 128-byte unroll: one branch per 128 bytes keeps mispredict cost
        /// bounded even for randomized inputs.
        while (j + 128 <= hi)
        {
            const __m256i c0 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2 + j));
            const __m256i c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2 + j + 32));
            const __m256i c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2 + j + 64));
            const __m256i c3 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2 + j + 96));
            const UInt32 e0 = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c0, target)));
            const UInt32 e1 = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, target)));
            const UInt32 e2 = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c2, target)));
            const UInt32 e3 = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c3, target)));
            const UInt64 eq_lo = UInt64(e0) | (UInt64(e1) << 32);
            const UInt64 eq_hi = UInt64(e2) | (UInt64(e3) << 32);
            const UInt64 cand_lo = eq_lo & ~jaroBits64At(matched_s2_bits.data(), j);
            const UInt64 cand_hi = eq_hi & ~jaroBits64At(matched_s2_bits.data(), j + 64);
            if (cand_lo | cand_hi)
                return cand_lo
                    ? j      + static_cast<int>(getTrailingZeroBitsUnsafe(cand_lo))
                    : j + 64 + static_cast<int>(getTrailingZeroBitsUnsafe(cand_hi));
            j += 128;
        }
        while (j + 32 <= hi)
        {
            const __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(s2 + j));
            UInt32 eq_mask = static_cast<UInt32>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, target)));
            eq_mask &= ~static_cast<UInt32>(jaroBits64At(matched_s2_bits.data(), j) & 0xFFFFFFFFu);
            if (eq_mask)
                return j + static_cast<int>(getTrailingZeroBitsUnsafe(eq_mask));
            j += 32;
        }
        for (; j < hi; ++j)
        {
            const UInt64 bit = UInt64{1} << (j & 63);
            if (s2[j] == c && !(matched_s2_bits[j >> 6] & bit))
                return j;
        }
        return -1;
    };

    for (int i = 0; i < s1_len; ++i)
    {
        const int lo = std::max(0, i - max_range);
        const int hi = std::min(s2_len, i + max_range + 1);
        if (lo >= hi)
            continue;

        const int jj = findMatch(s1[i], lo, hi);
        if (jj >= 0)
        {
            matched_s2_bits[jj >> 6] |= UInt64{1} << (jj & 63);
            idx_buf[matches++] = i;
        }
    }

    int trans2 = 0;
    int wb = 0;
    UInt64 b = matched_s2_bits[0];
    for (int k = 0; k < matches; ++k)
    {
        while (!b)
        {
            ++wb;
            if (wb >= s2_words)
                return jaroFinishScore(matches, trans2, s1_len, s2_len);
            b = matched_s2_bits[wb];
        }
        const int j = (wb << 6) + static_cast<int>(getTrailingZeroBitsUnsafe(b));
        b &= b - 1;
        trans2 += (s1[idx_buf[k]] != s2[j]);
    }
    return jaroFinishScore(matches, trans2, s1_len, s2_len);
}

) // DECLARE_X86_64_V3_SPECIFIC_CODE
#endif

struct ByteJaroSimilarityImpl
{
    using ResultType = Float64;

    /// Scalar reference implementation, used as fallback when AVX2 is not
    /// available at runtime (or at compile time, for non-x86 builds).
    static ResultType processScalar(
        const char * __restrict haystack,
        const char * __restrict needle,
        int s1len, int s2len, int max_range)
    {
        VectorWithMemoryTracking<int> s1_matching(s1len, -1);
        VectorWithMemoryTracking<int> s2_matching(s2len, -1);

        /// Calculate matching characters
        size_t matching_characters = 0;
        for (int i = 0; i < s1len; i++)
        {
            /// Matching window
            const int min_index = std::max(i - max_range, 0);
            const int max_index = std::min(i + max_range + 1, s2len);
            for (int j = min_index; j < max_index; j++)
            {
                if (s2_matching[j] == -1 && haystack[i] == needle[j])
                {
                    s1_matching[i] = i;
                    s2_matching[j] = j;
                    matching_characters++;
                    break;
                }
            }
        }

        if (matching_characters == 0)
            return 0.0;

        /// Transpositions (one-way only)
        double transpositions = 0.0;
        for (size_t i = 0, s1i = 0, s2i = 0; i < matching_characters; i++)
        {
            while (s1_matching[s1i] == -1)
                s1i++;
            while (s2_matching[s2i] == -1)
                s2i++;
            if (haystack[s1i] != needle[s2i])
                transpositions += 0.5;
            s1i++;
            s2i++;
        }

        const double m = static_cast<double>(matching_characters);
        return 1.0 / 3.0 * (m / static_cast<double>(s1len)
                          + m / static_cast<double>(s2len)
                          + (m - transpositions) / m);
    }

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        /// Shortcuts:

        if (haystack_size == 0 || needle_size == 0)
            return 0.0;

        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function jaroSimilarity, should be at most {}", max_string_size);

        if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0)
            return 1.0;

        const int s1len = static_cast<int>(haystack_size);
        const int s2len = static_cast<int>(needle_size);

        /// Window size to search for matches in the other string
        const int max_range = std::max(0, std::max(s1len, s2len) / 2 - 1);

#if USE_X86_64_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v3))
        {
            const auto * s1 = reinterpret_cast<const unsigned char *>(haystack);
            const auto * s2 = reinterpret_cast<const unsigned char *>(needle);
            /// `jaroSmall` hoists the second string into two AVX2 registers, so it requires that
            /// string to be at most 64 bytes. Which of the two arguments ends up here as the
            /// needle is not under our control: `FunctionStringDistanceImpl::vectorConstant`
            /// forwards `jaroSimilarity(column, const)` through `constantVector(const, column)`,
            /// so the row value can arrive as the needle. Jaro similarity is symmetric - both the
            /// matching window `max_range` and the final score are - so pick the kernel by the
            /// shorter of the two strings rather than by the argument position, and swap the
            /// arguments when the needle is the longer one.
            if (s2len <= 64)
                return TargetSpecific::x86_64_v3::jaroSmall(s1, s1len, s2, s2len, max_range);
            if (s1len <= 64)
                return TargetSpecific::x86_64_v3::jaroSmall(s2, s2len, s1, s1len, max_range);
            return TargetSpecific::x86_64_v3::jaroScan(s1, s1len, s2, s2len, max_range);
        }
#endif
        return processScalar(haystack, needle, s1len, s2len, max_range);
    }
};

struct ByteJaroWinklerSimilarityImpl
{
    using ResultType = Float64;

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size, const char * __restrict needle, size_t needle_size)
    {
        static constexpr int max_prefix_length = 4;
        static constexpr double scaling_factor =  0.1;
        static constexpr double boost_threshold = 0.7;

        if (haystack_size == 0 || needle_size == 0)
            return 0.0;

        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function jaroWinklerSimilarity, should be at most {}", max_string_size);

        const int s1len = static_cast<int>(haystack_size);
        const int s2len = static_cast<int>(needle_size);

        ResultType jaro_winkler_similarity = ByteJaroSimilarityImpl::process(haystack, haystack_size, needle, needle_size);

        if (jaro_winkler_similarity > boost_threshold)
        {
            const int common_length = std::min({max_prefix_length, s1len, s2len});
            int common_prefix = 0;
            while (common_prefix < common_length && haystack[common_prefix] == needle[common_prefix])
                common_prefix++;

            jaro_winkler_similarity += common_prefix * scaling_factor * (1.0 - jaro_winkler_similarity);
        }
        return jaro_winkler_similarity;
    }
};

template <bool is_utf8>
struct ByteAffineGapDistanceImpl
{
    using ResultType = Float64;
    
    static constexpr size_t max_string_size = 1000000; // 1MB safety limit
    static constexpr long double max_complexity = 1e9L;
    static constexpr double default_gap_open = 1.0;    // Default penalty for opening a gap
    static constexpr double default_gap_extend = 0.5;  // Default penalty for extending a gap
    static constexpr double default_mismatch = 1.0;    // Default penalty for substitution
    static constexpr double big_value = std::numeric_limits<double>::infinity(); // Large but not infinity to avoid overflow

    static ResultType process(
        const char * __restrict haystack, size_t haystack_size,
        const char * __restrict needle, size_t needle_size,
        double gap_open = default_gap_open,
        double gap_extend = default_gap_extend,
        double mismatch = default_mismatch)
    {
        /// Safety threshold against DoS
        if (haystack_size > max_string_size || needle_size > max_string_size)
        {
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "The string size is too big for function affineGap, should be at most {}", max_string_size);
        }

        if (static_cast<long double>(haystack_size) * static_cast<long double>(needle_size) > max_complexity)
        {
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "Complexity of the operation is too high, should be at most {}", max_complexity);
        }


        /// Shortcuts:
        if (haystack_size == 0)
            return static_cast<double>(needle_size) * gap_open;

        if (needle_size == 0)
            return static_cast<double>(haystack_size) * gap_open;

        if (haystack_size == needle_size && memcmp(haystack, needle, haystack_size) == 0)
            return 0.0;
            
        /// For UTF-8 strings, convert to code points first
        VectorWithMemoryTracking<UInt32> haystack_codepoints;
        VectorWithMemoryTracking<UInt32> needle_codepoints;
        
        if constexpr (is_utf8)
        {
            parseUTF8String(haystack, haystack_size, [&](UInt32 code_point) {
                haystack_codepoints.push_back(code_point);
            });
            
            parseUTF8String(needle, needle_size, [&](UInt32 code_point) {
                needle_codepoints.push_back(code_point);
            });
            
            haystack_size = haystack_codepoints.size();
            needle_size = needle_codepoints.size();
        }
        
        /// Three dynamic programming matrices for affine gap alignment:
        /// This algorithm uses three matrices to track different alignment states:
        /// 1. m_matrix[i][j]: Represents the optimal score when aligning positions i and j
        ///    with a match/mismatch (no gap). This is the main alignment matrix.
        /// 2. x_matrix[i][j]: Represents the optimal score when aligning position i with a gap
        ///    in the second sequence (needle). This tracks deletions in the first sequence.
        /// 3. y_matrix[i][j]: Represents the optimal score when aligning position j with a gap
        ///    in the first sequence (haystack). This tracks insertions in the second sequence.

        VectorWithMemoryTracking<VectorWithMemoryTracking<double>> m_matrix(haystack_size + 1, VectorWithMemoryTracking<double>(needle_size + 1, big_value));
        VectorWithMemoryTracking<VectorWithMemoryTracking<double>> x_matrix(haystack_size + 1, VectorWithMemoryTracking<double>(needle_size + 1, big_value));
        VectorWithMemoryTracking<VectorWithMemoryTracking<double>> y_matrix(haystack_size + 1, VectorWithMemoryTracking<double>(needle_size + 1, big_value));
        
        m_matrix[0][0] = 0.0;
        x_matrix[0][0] = gap_open;
        y_matrix[0][0] = gap_open;
        
        /// Initialize first column: represents aligning haystack with empty needle
        /// For each position i, the cost is gap_open + (i-1)*gap_extend
        for (size_t i = 1; i <= haystack_size; ++i)
        {
            m_matrix[i][0] = big_value;      // Invalid transition - can't match with empty string
            x_matrix[i][0] = gap_open + gap_extend * static_cast<double>(i - 1);  // Cost of i-1 gap extensions
            y_matrix[i][0] = big_value;      // Invalid transition
        }
        
        /// Initialize first row: represents aligning needle with empty haystack
        /// For each position j, the cost is gap_open + (j-1)*gap_extend
        for (size_t j = 1; j <= needle_size; ++j)
        {
            m_matrix[0][j] = big_value;      // Invalid transition - can't match with empty string
            x_matrix[0][j] = big_value;      // Invalid transition
            y_matrix[0][j] = gap_open + gap_extend * static_cast<double>(j - 1);  // Cost of j-1 gap extensions
        }
        
        /// Fill in the matrices using dynamic programming
        /// For each position (i,j), we consider three possible alignments:
        /// 1. Match/mismatch: align characters at positions i and j
        /// 2. Deletion: align character at i with a gap in needle
        /// 3. Insertion: align character at j with a gap in haystack
        for (size_t i = 1; i <= haystack_size; ++i)
        {
            for (size_t j = 1; j <= needle_size; ++j)
            {
                /// Check if characters at positions i-1 and j-1 match
                bool match = false;
                if constexpr (is_utf8)
                    match = haystack_codepoints[i-1] == needle_codepoints[j-1];
                else
                    match = haystack[i-1] == needle[j-1];
                
                /// Cost of match/mismatch: 0 for match, mismatch penalty for mismatch
                double match_cost = match ? 0.0 : mismatch;
                
                /// Update match/mismatch matrix (m_matrix):
                /// 1. Extend previous match/mismatch
                /// 2. Extend previous deletion with a match/mismatch
                /// 3. Extend previous insertion with a match/mismatch
                m_matrix[i][j] = m_matrix[i-1][j-1] + match_cost;
                m_matrix[i][j] = std::min(m_matrix[i][j], x_matrix[i-1][j-1] + match_cost);
                m_matrix[i][j] = std::min(m_matrix[i][j], y_matrix[i-1][j-1] + match_cost);
                
                /// Update deletion matrix (x_matrix):
                /// 1. Start a new deletion from a match/mismatch
                /// 2. Extend an existing deletion
                x_matrix[i][j] = m_matrix[i-1][j] + gap_open;
                x_matrix[i][j] = std::min(x_matrix[i][j], x_matrix[i-1][j] + gap_extend);
                
                /// Update insertion matrix (y_matrix):
                /// 1. Start a new insertion from a match/mismatch
                /// 2. Extend an existing insertion
                y_matrix[i][j] = m_matrix[i][j-1] + gap_open;
                y_matrix[i][j] = std::min(y_matrix[i][j], y_matrix[i][j-1] + gap_extend);
            }
        }
        
        /// Return minimum of the three matrices at the bottom-right corner
        return std::min({m_matrix[haystack_size][needle_size],
                         x_matrix[haystack_size][needle_size],
                         y_matrix[haystack_size][needle_size]});
    }
};

template <bool utf8_enabled>
struct ByteSmashSimilarityImpl
{
    using ResultType = Float64;
    static constexpr double big_value = std::numeric_limits<double>::infinity();
    static constexpr long double max_complexity = 1e10L;
    static constexpr size_t max_string_size = 100000;

    static ResultType process(
        const char* __restrict haystack, size_t haystack_size,
        const char* __restrict needle, size_t needle_size)
    {
        if (haystack_size > max_string_size || needle_size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "String size too large for smashSimilarity");

        // Convert input strings to UTF-8 code points if needed
        VectorWithMemoryTracking<UInt32> haystack_codepoints;
        VectorWithMemoryTracking<UInt32> needle_codepoints;
        
        if constexpr (utf8_enabled)
        {
            try
            {
                parseUTF8String(haystack, haystack_size, [&](UInt32 code_point) {
                    haystack_codepoints.push_back(code_point);
                });
                parseUTF8String(needle, needle_size, [&](UInt32 code_point) {
                    needle_codepoints.push_back(code_point);
                });
                
                // Create string views from code points
                std::string_view long_str(reinterpret_cast<const char*>(haystack_codepoints.data()),
                                       haystack_codepoints.size() * sizeof(UInt32));
                std::string_view short_str(reinterpret_cast<const char*>(needle_codepoints.data()),
                                        needle_codepoints.size() * sizeof(UInt32));
                
                VectorWithMemoryTracking<UInt32> long_codepoints = haystack_codepoints;
                VectorWithMemoryTracking<UInt32> short_codepoints = needle_codepoints;
                
                if (long_str.size() < short_str.size())
                {
                    std::swap(long_str, short_str);
                    std::swap(long_codepoints, short_codepoints);
                }
                
                return processStrings(long_str, short_str, long_codepoints);
            }
            catch (const Exception & e)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid UTF-8 sequence in input strings: {}", e.message());
            }
        }
        else
        {
            std::string_view long_str(haystack, haystack_size);
            std::string_view short_str(needle, needle_size);
            if (long_str.size() < short_str.size())
                std::swap(long_str, short_str);
            
            return processStrings(long_str, short_str, haystack_codepoints);
        }
    }

private:
    static ResultType processStrings(
        const std::string_view& long_str,
        const std::string_view& short_str,
        const VectorWithMemoryTracking<UInt32>& codepoints)
    {
        if (long_str.empty() && short_str.empty())
            return 1.0;

        if (long_str.empty() || short_str.empty())
            return 0.0;

        VectorWithMemoryTracking<std::string_view> words;
        size_t pos = 0;

        // Split into words
        while (pos < long_str.size())
        {
            // Skip spaces
            while (pos < long_str.size() && isSpace(long_str, pos, codepoints))
                pos++;
                
            size_t word_start = pos;
            
            // Find word end
            while (pos < long_str.size() && !isSpace(long_str, pos, codepoints))
                pos++;
                
            if (pos > word_start)
            {
                words.emplace_back(long_str.substr(word_start, pos - word_start));
            }
        }

        const int64_t m = words.size();
        const int64_t n = short_str.size();

        size_t max_word_length = 0;
        for (const auto& word : words)
        {
            max_word_length = std::max(max_word_length, word.size());
        }

        long double smash_complexity = static_cast<long double>(m) * static_cast<long double>(n) * static_cast<long double>(n - 1) / 2;
        if (smash_complexity > max_complexity)
        {
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "String size too large for smashSimilarity");
        }


        if (m == 0 || n == 0)
            return 0.0;

        /// Dynamic programming matrix for SMASH similarity:
        /// dp[i][j] represents the minimum distance to match the first i words
        /// with the first j characters of the short string.

        VectorWithMemoryTracking<VectorWithMemoryTracking<double>> dp(m + 1, VectorWithMemoryTracking<double>(n + 1, std::numeric_limits<double>::infinity()));
        dp[0][0] = 0.0;

        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(5);

        for (int64_t i = 1; i <= m; ++i)
        {
            for (int64_t j = 1; j <= n; ++j)
            {
                for (int64_t k = i - 1; k < j; ++k)
                {
                    auto current_time = std::chrono::steady_clock::now();
                    if (std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time) > timeout)
                    {
                        throw Exception(
                            ErrorCodes::TOO_LARGE_STRING_SIZE,
                            "Operation timeout: SMASH similarity calculation took more than 5 seconds");
                    }
                    
                    /// Get the current word and the substring to compare it with
                    auto current_word = words[i - 1];
                    auto substring = short_str.substr(k, j - k);

                    /// Calculate the distance between the word and substring
                    /// using affine gap distance for more accurate comparison
                    double dist = calculateDistance(current_word, substring);
                    
                    /// Update the dp matrix with the new distance
                    /// dp[i][j] = min(dp[i][j], dp[i-1][k] + dist)
                    double new_dist = dp[i - 1][k] + dist;
                    dp[i][j] = std::min(new_dist, dp[i][j]);
                }
            }
        }

        const double max_distance = 10.0;
        double similarity = 1.0 - (dp[m][n] / max_distance);
        return std::clamp(similarity, 0.0, 1.0);
    }

    static bool isSpace(const std::string_view& str, size_t pos, const VectorWithMemoryTracking<UInt32>& codepoints)
    {
        if constexpr (utf8_enabled)
        {
            size_t code_point_idx = pos / sizeof(UInt32);
            if (code_point_idx >= codepoints.size())
                return false;
                
            UInt32 code_point = codepoints[code_point_idx];
            return code_point == ' ' || code_point == '\t' || code_point == '\n' ||
                   code_point == '\r' || code_point == '\v' || code_point == '\f';
        }
        else
        {
            unsigned char c = static_cast<unsigned char>(str[pos]);
            return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
        }
    }

    static double calculateDistance(std::string_view word, std::string_view substr)
    {
        auto is_subsequence = [](std::string_view shorter, std::string_view longer)
        {
            if (shorter.empty()) return true;
            if (longer.empty()) return false;
            
            size_t j = 0;
            for (size_t i = 0; i < longer.size() && j < shorter.size(); i++)
            {
                if (longer[i] == shorter[j])
                    j++;
            }
            return j == shorter.size();
        };

        if (is_subsequence(substr, word) || is_subsequence(word, substr))
            return 0.0;

        if constexpr (utf8_enabled)
        {
            return ByteAffineGapDistanceImpl<true>::process(
                word.data(), word.size(),
                substr.data(), substr.size(),
                0.7, // gap_open - reduced to be less punitive for typos
                0.3, // gap_extend - reduced to be less punitive for typos
                1.0  // mismatch
            );
        }
        else
        {
            return ByteAffineGapDistanceImpl<false>::process(
                word.data(), word.size(),
                substr.data(), substr.size(),
                0.7, // gap_open - reduced to be less punitive for typos
                0.3, // gap_extend - reduced to be less punitive for typos
                1.0  // mismatch
            );
        }
    }
};

#ifndef STRING_SIMILARITY_GTEST_UNIT_TEST

struct NameByteHammingDistance
{
    static constexpr auto name = "byteHammingDistance";
};
using FunctionByteHammingDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteHammingDistanceImpl>, NameByteHammingDistance>;

struct NameEditDistance
{
    static constexpr auto name = "editDistance";
};
using FunctionEditDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceImpl<false>>, NameEditDistance>;
struct NameEditDistanceUTF8
{
    static constexpr auto name = "editDistanceUTF8";
};
using FunctionEditDistanceUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceImpl<true>>, NameEditDistanceUTF8>;

struct NameEditDistanceRatio
{
    static constexpr auto name = "editDistanceRatio";
};
using FunctionEditDistanceRatio = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceRatioImpl<false>>, NameEditDistanceRatio>;
struct NameEditDistanceRatioUTF8
{
    static constexpr auto name = "editDistanceRatioUTF8";
};
using FunctionEditDistanceRatioUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteEditDistanceRatioImpl<true>>, NameEditDistanceRatioUTF8>;

struct NameStringSimilarityRatio
{
    static constexpr auto name = "stringSimilarityRatio";
};
using FunctionStringSimilarityRatio = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteStringSimilarityRatioImpl<false>>, NameStringSimilarityRatio>;
struct NameStringSimilarityRatioUTF8
{
    static constexpr auto name = "stringSimilarityRatioUTF8";
};
using FunctionStringSimilarityRatioUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteStringSimilarityRatioImpl<true>>, NameStringSimilarityRatioUTF8>;

struct NameDamerauLevenshteinDistance
{
    static constexpr auto name = "damerauLevenshteinDistance";
};
using FunctionDamerauLevenshteinDistance = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteDamerauLevenshteinDistanceImpl>, NameDamerauLevenshteinDistance>;

struct NameJaccardIndex
{
    static constexpr auto name = "stringJaccardIndex";
};
using FunctionStringJaccardIndex = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaccardIndexImpl<false>>, NameJaccardIndex>;

struct NameJaccardIndexUTF8
{
    static constexpr auto name = "stringJaccardIndexUTF8";
};
using FunctionStringJaccardIndexUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaccardIndexImpl<true>>, NameJaccardIndexUTF8>;

struct NameJaroSimilarity
{
    static constexpr auto name = "jaroSimilarity";
};
using FunctionJaroSimilarity = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaroSimilarityImpl>, NameJaroSimilarity>;

struct NameJaroWinklerSimilarity
{
    static constexpr auto name = "jaroWinklerSimilarity";
};
using FunctionJaroWinklerSimilarity = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteJaroWinklerSimilarityImpl>, NameJaroWinklerSimilarity>;

struct NameSmashSimilarity
{
    static constexpr auto name = "smashSimilarity";
};
using FunctionSmashSimilarity = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteSmashSimilarityImpl<false>>, NameSmashSimilarity>;

struct NameSmashSimilarityUTF8
{
    static constexpr auto name = "smashSimilarityUTF8";
};
using FunctionSmashSimilarityUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteSmashSimilarityImpl<true>>, NameSmashSimilarityUTF8>;

struct NameAffineGap
{
    static constexpr auto name = "affineGap";
};
using FunctionAffineGap = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteAffineGapDistanceImpl<false>>, NameAffineGap>;

struct NameAffineGapUTF8
{
    static constexpr auto name = "affineGapUTF8";
};
using FunctionAffineGapUTF8 = FunctionsStringSimilarity<FunctionStringDistanceImpl<ByteAffineGapDistanceImpl<true>>, NameAffineGapUTF8>;

REGISTER_FUNCTION(StringDistance)
{
    FunctionDocumentation::Description description_hamming = R"(
Calculates the [hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) between two byte strings.
)";
    FunctionDocumentation::Syntax syntax_hamming = "byteHammingDistance(s1, s2)";
    FunctionDocumentation::Arguments arguments_hamming = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_hamming = {"Returns the Hamming distance between the two strings.", {"UInt64"}};
    FunctionDocumentation::Examples examples_hamming = {
    {
        "Usage example",
        "SELECT byteHammingDistance('karolin', 'kathrin')",
        R"(
┌─byteHammingDistance('karolin', 'kathrin')─┐
│                                         3 │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_hamming = {description_hamming, syntax_hamming, arguments_hamming, {}, returned_value_hamming, examples_hamming, introduced_in, category};

    FunctionDocumentation::Description description_edit = R"(
Calculates the [edit distance](https://en.wikipedia.org/wiki/Edit_distance) between two byte strings.
)";
    FunctionDocumentation::Syntax syntax_edit = "editDistance(s1, s2)";
    FunctionDocumentation::Arguments arguments_edit = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_edit = {"Returns the edit distance between the two strings.", {"UInt64"}};
    FunctionDocumentation::Examples examples_edit = {
    {
        "Usage example",
        "SELECT editDistance('clickhouse', 'mouse')",
        R"(
┌─editDistance('clickhouse', 'mouse')─┐
│                                   6 │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_edit = {description_edit, syntax_edit, arguments_edit, {}, returned_value_edit, examples_edit, introduced_in, category};

    FunctionDocumentation::Description description_edit_utf8 = R"(
Calculates the [edit distance](https://en.wikipedia.org/wiki/Edit_distance) between two UTF8 strings.
)";
    FunctionDocumentation::Syntax syntax_edit_utf8 = "editDistanceUTF8(s1, s2)";
    FunctionDocumentation::Arguments arguments_edit_utf8 = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_edit_utf8 = {"Returns the edit distance between the two UTF8 strings.", {"UInt64"}};
    FunctionDocumentation::Examples examples_edit_utf8 = {
    {
        "Usage example",
        "SELECT editDistanceUTF8('我是谁', '我是我') AS distance",
        R"(
┌─distance─┐
│        1 │
└──────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_utf8 = {24, 6};
    FunctionDocumentation documentation_edit_utf8 = {description_edit_utf8, syntax_edit_utf8, arguments_edit_utf8, {}, returned_value_edit_utf8, examples_edit_utf8, introduced_in_utf8, category};

    FunctionDocumentation::Description description_edit_ratio = R"(
Calculates the normalized [edit distance](https://en.wikipedia.org/wiki/Edit_distance) similarity between two byte strings:
`1 - editDistance(s1, s2) / max(length(s1), length(s2))`.
Returns a value between `0` (the edit distance equals the length of the longer string, as for `'ab'` and `'ba'`) and `1` (equal strings). Two empty strings are equal.
)";
    FunctionDocumentation::Syntax syntax_edit_ratio = "editDistanceRatio(s1, s2)";
    FunctionDocumentation::Arguments arguments_edit_ratio = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_edit_ratio = {"Returns the normalized edit distance similarity between the two strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_edit_ratio = {
    {
        "Usage example",
        "SELECT editDistanceRatio('kitten', 'sitting')",
        R"(
┌─editDistanceRatio('kitten', 'sitting')─┐
│                     0.5714285714285714 │
└────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ratio = {26, 11};
    FunctionDocumentation documentation_edit_ratio = {description_edit_ratio, syntax_edit_ratio, arguments_edit_ratio, {}, returned_value_edit_ratio, examples_edit_ratio, introduced_in_ratio, category};

    FunctionDocumentation::Description description_edit_ratio_utf8 = R"(
Like [`editDistanceRatio`](#editDistanceRatio) but for UTF8-encoded strings: the edit distance and the lengths are counted in code points.
)";
    FunctionDocumentation::Syntax syntax_edit_ratio_utf8 = "editDistanceRatioUTF8(s1, s2)";
    FunctionDocumentation::Arguments arguments_edit_ratio_utf8 = {
        {"s1", "First input UTF8 string.", {"String"}},
        {"s2", "Second input UTF8 string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_edit_ratio_utf8 = {"Returns the normalized edit distance similarity between the two UTF8 strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_edit_ratio_utf8 = {
    {
        "Usage example",
        "SELECT editDistanceRatioUTF8('我是谁', '我是我') AS ratio",
        R"(
┌──────────────ratio─┐
│ 0.6666666666666667 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_edit_ratio_utf8 = {description_edit_ratio_utf8, syntax_edit_ratio_utf8, arguments_edit_ratio_utf8, {}, returned_value_edit_ratio_utf8, examples_edit_ratio_utf8, introduced_in_ratio, category};

    FunctionDocumentation::Description description_similarity_ratio = R"(
Calculates the normalized indel similarity between two byte strings, the same value as Python's `Levenshtein.ratio`:
`2 * LCS / (length(s1) + length(s2))`, where `LCS` is the length of the [longest common subsequence](https://en.wikipedia.org/wiki/Longest_common_subsequence).
Equivalently, `1 - (insertions + deletions) / (length(s1) + length(s2))`.
Returns a value between `0` (nothing in common) and `1` (equal strings). Two empty strings are equal.
)";
    FunctionDocumentation::Syntax syntax_similarity_ratio = "stringSimilarityRatio(s1, s2)";
    FunctionDocumentation::Arguments arguments_similarity_ratio = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_similarity_ratio = {"Returns the normalized indel similarity between the two strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_similarity_ratio = {
    {
        "Usage example",
        "SELECT stringSimilarityRatio('kitten', 'sitting')",
        R"(
┌─stringSimilarityRatio('kitten', 'sitting')─┐
│                         0.6153846153846154 │
└────────────────────────────────────────────┘
        )"
    },
    {
        "Fuzzy matching of names",
        "SELECT name FROM (SELECT arrayJoin(['Equinix', 'Equinix Inc.', 'Digital Realty']) AS name) WHERE stringSimilarityRatio(lower(name), 'equinix inc') > 0.7",
        R"(
┌─name─────────┐
│ Equinix      │
│ Equinix Inc. │
└──────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_similarity_ratio = {description_similarity_ratio, syntax_similarity_ratio, arguments_similarity_ratio, {}, returned_value_similarity_ratio, examples_similarity_ratio, introduced_in_ratio, category};

    FunctionDocumentation::Description description_similarity_ratio_utf8 = R"(
Like [`stringSimilarityRatio`](#stringSimilarityRatio) but for UTF8-encoded strings: the lengths are counted in code points.
)";
    FunctionDocumentation::Syntax syntax_similarity_ratio_utf8 = "stringSimilarityRatioUTF8(s1, s2)";
    FunctionDocumentation::Arguments arguments_similarity_ratio_utf8 = {
        {"s1", "First input UTF8 string.", {"String"}},
        {"s2", "Second input UTF8 string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_similarity_ratio_utf8 = {"Returns the normalized indel similarity between the two UTF8 strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_similarity_ratio_utf8 = {
    {
        "Usage example",
        "SELECT stringSimilarityRatioUTF8('我是谁', '我是我') AS ratio",
        R"(
┌──────────────ratio─┐
│ 0.6666666666666667 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_similarity_ratio_utf8 = {description_similarity_ratio_utf8, syntax_similarity_ratio_utf8, arguments_similarity_ratio_utf8, {}, returned_value_similarity_ratio_utf8, examples_similarity_ratio_utf8, introduced_in_ratio, category};

    FunctionDocumentation::Description description_damerau = R"(
Calculates the [Damerau-Levenshtein distance](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance) between two byte strings.
)";
    FunctionDocumentation::Syntax syntax_damerau = "damerauLevenshteinDistance(s1, s2)";
    FunctionDocumentation::Arguments arguments_damerau = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_damerau = {"Returns the Damerau-Levenshtein distance between the two strings.", {"UInt64"}};
    FunctionDocumentation::Examples examples_damerau = {
    {
        "Usage example",
        "SELECT damerauLevenshteinDistance('clickhouse', 'mouse')",
        R"(
┌─damerauLevenshteinDistance('clickhouse', 'mouse')─┐
│                                                 6 │
└───────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_damerau = {24, 1};
    FunctionDocumentation documentation_damerau = {description_damerau, syntax_damerau, arguments_damerau, {}, returned_value_damerau, examples_damerau, introduced_in_damerau, category};

    FunctionDocumentation::Description description_jaccard = R"(
Calculates the [Jaccard similarity index](https://en.wikipedia.org/wiki/Jaccard_index) between two byte strings.
)";
    FunctionDocumentation::Syntax syntax_jaccard = "stringJaccardIndex(s1, s2)";
    FunctionDocumentation::Arguments arguments_jaccard = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_jaccard = {"Returns the Jaccard similarity index between the two strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_jaccard = {
    {
        "Usage example",
        "SELECT stringJaccardIndex('clickhouse', 'mouse')",
        R"(
┌─stringJaccardIndex('clickhouse', 'mouse')─┐
│                                       0.4 │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_jaccard = {23, 11};
    FunctionDocumentation documentation_jaccard = {description_jaccard, syntax_jaccard, arguments_jaccard, {}, returned_value_jaccard, examples_jaccard, introduced_in_jaccard, category};

    FunctionDocumentation::Description description_jaccard_utf8 = R"(
Like [`stringJaccardIndex`](#stringJaccardIndex) but for UTF8-encoded strings.
)";
    FunctionDocumentation::Syntax syntax_jaccard_utf8 = "stringJaccardIndexUTF8(s1, s2)";
    FunctionDocumentation::Arguments arguments_jaccard_utf8 = {
        {"s1", "First input UTF8 string.", {"String"}},
        {"s2", "Second input UTF8 string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_jaccard_utf8 = {"Returns the Jaccard similarity index between the two UTF8 strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_jaccard_utf8 = {
    {
        "Usage example",
        "SELECT stringJaccardIndexUTF8('我爱你', '我也爱你') AS jaccard_index",
        R"(
┌─jaccard_index─┐
│          0.75 │
└───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_jaccard_utf8 = {23, 11};
    FunctionDocumentation documentation_jaccard_utf8 = {description_jaccard_utf8, syntax_jaccard_utf8, arguments_jaccard_utf8, {}, returned_value_jaccard_utf8, examples_jaccard_utf8, introduced_in_jaccard_utf8, category};

    FunctionDocumentation::Description description_jaro = R"(
Calculates the [Jaro similarity](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance#Jaro_similarity) between two byte strings.
Returns `0` if either string is empty.
)";
    FunctionDocumentation::Syntax syntax_jaro = "jaroSimilarity(s1, s2)";
    FunctionDocumentation::Arguments arguments_jaro = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_jaro = {"Returns the Jaro similarity between the two strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_jaro = {
    {
        "Usage example",
        "SELECT jaroSimilarity('clickhouse', 'click')",
        R"(
┌─jaroSimilarity('clickhouse', 'click')─┐
│                    0.8333333333333333 │
└───────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_jaro = {24, 1};
    FunctionDocumentation documentation_jaro = {description_jaro, syntax_jaro, arguments_jaro, {}, returned_value_jaro, examples_jaro, introduced_in_jaro, category};

    FunctionDocumentation::Description description_jaro_winkler = R"(
Calculates the [Jaro-Winkler similarity](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance) between two byte strings.
Returns `0` if either string is empty.
)";
    FunctionDocumentation::Syntax syntax_jaro_winkler = "jaroWinklerSimilarity(s1, s2)";
    FunctionDocumentation::Arguments arguments_jaro_winkler = {
        {"s1", "First input string.", {"String"}},
        {"s2", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_jaro_winkler = {"Returns the Jaro-Winkler similarity between the two strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_jaro_winkler = {
    {
        "Usage example",
        "SELECT jaroWinklerSimilarity('clickhouse', 'click')",
        R"(
┌─jaroWinklerSimilarity('clickhouse', 'click')─┐
│                           0.8999999999999999 │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_jaro_winkler = {24, 1};
    FunctionDocumentation documentation_jaro_winkler = {description_jaro_winkler, syntax_jaro_winkler, arguments_jaro_winkler, {}, returned_value_jaro_winkler, examples_jaro_winkler, introduced_in_jaro_winkler, category};

    FunctionDocumentation::Description description_smash = R"(
Calculates the SMASH (Smart String Similarity) between two byte strings. This function is designed to handle word-based string comparisons with intelligent gap handling and subsequence matching.

It is suitable for matching manually entered strings (acronyms, abbreviations, typos), e.g., police rosters, medical records, location names.

The algorithm splits the longer string into words, compares each word with substrings of the shorter string using the `affineGap` algorithm, handles subsequence matching for partial word matches, and normalizes the result to the `[0, 1]` range.

Time complexity: O(m * n^3 * l) where m is the number of words in the longer string, n is the length of the shorter string and l is the size of the largest word. Space complexity: O(m * n).
)";
    FunctionDocumentation::Syntax syntax_smash = "smashSimilarity(haystack, needle)";
    FunctionDocumentation::Arguments arguments_smash = {
        {"haystack", "First input string.", {"String"}},
        {"needle", "Second input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_smash = {"Returns a value between 0 and 1, where 1 indicates identical strings and 0 indicates completely different strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_smash = {
    {
        "Usage example",
        "SELECT smashSimilarity('hello', 'Hello')",
        R"(
0.9
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_smash = {26, 11};
    FunctionDocumentation documentation_smash = {description_smash, syntax_smash, arguments_smash, {}, returned_value_smash, examples_smash, introduced_in_smash, category};

    FunctionDocumentation::Description description_smash_utf8 = R"(
Calculates the SMASH similarity between two UTF-8 encoded strings. This is the UTF-8 aware version of `smashSimilarity` that correctly handles multi-byte characters.
)";
    FunctionDocumentation::Syntax syntax_smash_utf8 = "smashSimilarityUTF8(haystack, needle)";
    FunctionDocumentation::Arguments arguments_smash_utf8 = {
        {"haystack", "First input UTF-8 string.", {"String"}},
        {"needle", "Second input UTF-8 string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_smash_utf8 = {"Returns a value between 0 and 1, where 1 indicates identical strings and 0 indicates completely different strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_smash_utf8 = {
    {
        "Usage example",
        "SELECT smashSimilarityUTF8('тест', 'тест')",
        R"(
1
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_smash_utf8 = {26, 11};
    FunctionDocumentation documentation_smash_utf8 = {description_smash_utf8, syntax_smash_utf8, arguments_smash_utf8, {}, returned_value_smash_utf8, examples_smash_utf8, introduced_in_smash_utf8, category};

    FunctionDocumentation::Description description_affine_gap = R"(
Calculates the affine gap distance between two byte strings.

Time complexity: O(m * n) where m and n are the lengths of the input strings. Space complexity: O(m * n) for the three dynamic programming matrices.
)";
    FunctionDocumentation::Syntax syntax_affine_gap = "affineGap(string1, string2)";
    FunctionDocumentation::Arguments arguments_affine_gap = {
        {"string1", "First string to compare.", {"String"}},
        {"string2", "Second string to compare.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_affine_gap = {"Affine gap distance between the strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_affine_gap = {
    {
        "Usage example",
        "SELECT affineGap('hello', 'helo')",
        R"(
1
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_affine_gap = {26, 11};
    FunctionDocumentation documentation_affine_gap = {description_affine_gap, syntax_affine_gap, arguments_affine_gap, {}, returned_value_affine_gap, examples_affine_gap, introduced_in_affine_gap, category};

    FunctionDocumentation::Description description_affine_gap_utf8 = R"(
Calculates the affine gap distance between two UTF-8 strings.

Time complexity: O(m * n) where m and n are the lengths of the input strings (in code points). Space complexity: O(m * n) for the three dynamic programming matrices.
)";
    FunctionDocumentation::Syntax syntax_affine_gap_utf8 = "affineGapUTF8(string1, string2)";
    FunctionDocumentation::Arguments arguments_affine_gap_utf8 = {
        {"string1", "First UTF-8 string to compare.", {"String"}},
        {"string2", "Second UTF-8 string to compare.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_affine_gap_utf8 = {"Affine gap distance between the UTF-8 strings.", {"Float64"}};
    FunctionDocumentation::Examples examples_affine_gap_utf8 = {
    {
        "Usage example",
        "SELECT affineGapUTF8('тест', 'тест')",
        R"(
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_affine_gap_utf8 = {26, 11};
    FunctionDocumentation documentation_affine_gap_utf8 = {description_affine_gap_utf8, syntax_affine_gap_utf8, arguments_affine_gap_utf8, {}, returned_value_affine_gap_utf8, examples_affine_gap_utf8, introduced_in_affine_gap_utf8, category};

    factory.registerFunction<FunctionByteHammingDistance>(documentation_hamming);
    factory.registerAlias("mismatches", NameByteHammingDistance::name);

    factory.registerFunction<FunctionEditDistance>(documentation_edit);
    factory.registerAlias("levenshteinDistance", NameEditDistance::name);

    factory.registerFunction<FunctionEditDistanceUTF8>(documentation_edit_utf8);
    factory.registerAlias("levenshteinDistanceUTF8", NameEditDistanceUTF8::name);
    factory.registerAlias("levenshtein_distance", NameEditDistanceUTF8::name, FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionEditDistanceRatio>(documentation_edit_ratio);
    factory.registerAlias("levenshteinRatio", NameEditDistanceRatio::name);

    factory.registerFunction<FunctionEditDistanceRatioUTF8>(documentation_edit_ratio_utf8);
    factory.registerAlias("levenshteinRatioUTF8", NameEditDistanceRatioUTF8::name);

    factory.registerFunction<FunctionStringSimilarityRatio>(documentation_similarity_ratio);
    factory.registerFunction<FunctionStringSimilarityRatioUTF8>(documentation_similarity_ratio_utf8);

    factory.registerFunction<FunctionDamerauLevenshteinDistance>(documentation_damerau);

    factory.registerFunction<FunctionStringJaccardIndex>(documentation_jaccard);
    factory.registerFunction<FunctionStringJaccardIndexUTF8>(documentation_jaccard_utf8);

    factory.registerFunction<FunctionJaroSimilarity>(documentation_jaro);

    factory.registerFunction<FunctionJaroWinklerSimilarity>(documentation_jaro_winkler);

    factory.registerFunction<FunctionSmashSimilarity>(documentation_smash);
    factory.registerFunction<FunctionSmashSimilarityUTF8>(documentation_smash_utf8);
    factory.registerFunction<FunctionAffineGap>(documentation_affine_gap);
    factory.registerFunction<FunctionAffineGapUTF8>(documentation_affine_gap_utf8);
}

#endif // STRING_SIMILARITY_GTEST_UNIT_TEST
}
