#include <Storages/MergeTree/TextIndexPhraseSearch.h>

#include <Common/Exception.h>
#include <Common/TargetSpecific.h>

#include <algorithm>
#include <bit>
#include <cstdlib>
#include <limits>
#include <string_view>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace DB
{

/// Roaringish phrase search: intersect two terms' sorted position lists (key = (doc_id << 32) | group,
/// plus a 32-bit bitmap of positions within the group). Two match cases for positional distance `shift`:
///   within-group : matching keys, bitmap = (lhs_bitmap << shift) & rhs_bitmap.
///   boundary     : a match straddling a 32-position group boundary (rhs key = lhs key + 1),
///                  bitmap = (lhs_bitmap >> (BITMAP_BITS - shift)) & rhs_bitmap. `key + 1` stays in the
///                  same document since group <= 0x07FFFFFF (positions are UInt32).
///
/// The within-group case is the vectorized hot loop (AVX2 / AVX-512, scalar fallback + tail). The
/// boundary case only fires for lhs buckets whose top `shift` bits are set, so those are gathered into a
/// small "carry" list and intersected with rhs separately; the two sorted runs are merged. Only the
/// sorted-key comparison is vectorized; the bitmap overlap and emission stay scalar. `CH_PHRASE_KERNEL`
/// (scalar / v3 / v4) forces a specific kernel for testing and benchmarking.

namespace
{

/// Within-group overlap for a matching key; the UInt32 shift drops the high bits (the carry path).
ALWAYS_INLINE UInt32 matchBitmap(UInt32 lhs_bitmap, UInt32 rhs_bitmap, UInt32 shift)
{
    return (lhs_bitmap << shift) & rhs_bitmap;
}

/// Scalar within-group intersection from (i, j) to the end: the standalone scalar kernel (i = j = 0)
/// and the tail of every SIMD kernel.
ALWAYS_INLINE void scalarIntersect(
    const PositionList & lhs,
    const PositionList & rhs,
    size_t i,
    size_t j,
    UInt32 shift,
    PositionList & out)
{
    const size_t n = lhs.size();
    const size_t m = rhs.size();
    while (i < n && j < m)
    {
        const UInt64 lhs_key = lhs.keys[i];
        const UInt64 rhs_key = rhs.keys[j];
        if (lhs_key < rhs_key)
        {
            ++i;
            continue;
        }
        if (lhs_key > rhs_key)
        {
            ++j;
            continue;
        }
        if (const UInt32 bitmap = matchBitmap(lhs.bitmap[i], rhs.bitmap[j], shift))
            out.pushBackKey(rhs_key, bitmap);
        ++i;
        ++j;
    }
}

/// Emit matches for one 8x8 block. Keys are unique, so matches form a monotone bijection: the k-th set
/// bit of m1 pairs with the k-th of m2. AVX-512 block kernel only.
[[maybe_unused]] ALWAYS_INLINE void emitBlockMatches(
    const PositionList & lhs,
    const PositionList & rhs,
    size_t i,
    size_t j,
    unsigned m1,
    unsigned m2,
    UInt32 shift,
    PositionList & out)
{
    while (m1)
    {
        const int ki = std::countr_zero(m1);
        const int kj = std::countr_zero(m2);
        if (const UInt32 bitmap = matchBitmap(lhs.bitmap[i + ki], rhs.bitmap[j + kj], shift))
            out.pushBackKey(rhs.keys[j + kj], bitmap);
        m1 &= m1 - 1;
        m2 &= m2 - 1;
    }
}

void intersectWithinScalar(const PositionList & lhs, const PositionList & rhs, UInt32 shift, PositionList & out)
{
    scalarIntersect(lhs, rhs, 0, 0, shift, out);
}

/// Gather lhs buckets that carry into the next group (top `shift` bits set) as a sorted run of
/// (key = lhs key + 1, bitmap = carried bits), replacing a second full scan with an O(lhs) filter.
void buildCarryList(const PositionList & lhs, UInt32 shift, PositionList & carry)
{
    const size_t n = lhs.size();
    const UInt32 drop = RoaringishEntry::BITMAP_BITS - shift;
    carry.reserve(n / 16 + 16);
    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 overflow = lhs.bitmap[i] >> drop;
        if (overflow)
            carry.pushBackKey(lhs.keys[i] + 1, overflow); /// group + 1, same doc (group <= 0x07FFFFFF)
    }
}

/// Probe each carry key against rhs with a galloping (moving lower_bound) search, emitting boundary matches.
void intersectCarry(const PositionList & carry, const PositionList & rhs, PositionList & out)
{
    const UInt64 * const base = rhs.keys.data();
    const UInt64 * const end = base + rhs.size();
    const UInt64 * cur = base;
    const size_t c = carry.size();
    for (size_t i = 0; i < c; ++i)
    {
        const UInt64 key = carry.keys[i];
        cur = std::lower_bound(cur, end, key);
        if (cur == end)
            break;
        if (*cur == key)
        {
            if (const UInt32 bitmap = carry.bitmap[i] & rhs.bitmap[static_cast<size_t>(cur - base)])
                out.pushBackKey(key, bitmap);
        }
    }
}

#if USE_MULTITARGET_CODE
enum class PhraseKernel
{
    Auto,
    Scalar,
    V3,
    V4,
};

PhraseKernel readKernelFromEnv()
{
    const char * env = std::getenv("CH_PHRASE_KERNEL"); /// NOLINT(concurrency-mt-unsafe)
    if (env == nullptr)
        return PhraseKernel::Auto;

    const std::string_view value(env);
    if (value == "scalar")
        return PhraseKernel::Scalar;
    if (value == "v3" || value == "avx2")
        return PhraseKernel::V3;
    if (value == "v4" || value == "avx512")
        return PhraseKernel::V4;
    return PhraseKernel::Auto;
}

PhraseKernel chosenKernel()
{
    static const PhraseKernel kernel = readKernelFromEnv();
    return kernel;
}
#endif

}

DECLARE_X86_64_V3_SPECIFIC_CODE(

/// AVX2 within-group intersection: broadcast the lhs key, compare 4 rhs keys per step (skip kernel).
static void intersectWithin(const DB::PositionList & lhs, const DB::PositionList & rhs, UInt32 shift, DB::PositionList & out)
{
    size_t i = 0;
    size_t j = 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();

    while (i < n && j + 4 <= m)
    {
        const UInt64 lhs_key = lhs.keys[i];

        /// lhs key past this block of rhs keys: skip it.
        if (lhs_key > rhs.keys[j + 3])
        {
            j += 4;
            continue;
        }

        const __m256i v_lhs_key = _mm256_set1_epi64x(static_cast<long long>(lhs_key));
        const __m256i v_rhs_keys = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&rhs.keys[j]));
        const __m256i v_cmp = _mm256_cmpeq_epi64(v_lhs_key, v_rhs_keys);
        const int mask = _mm256_movemask_pd(_mm256_castsi256_pd(v_cmp));

        if (mask)
        {
            /// Keys are unique per list: at most one match.
            const int k = std::countr_zero(static_cast<unsigned>(mask));
            if (const UInt32 bitmap = DB::matchBitmap(lhs.bitmap[i], rhs.bitmap[j + k], shift))
                out.pushBackKey(rhs.keys[j + k], bitmap);
            ++i;
            j += k + 1;
        }
        else
        {
            ++i;
        }
    }

    DB::scalarIntersect(lhs, rhs, i, j, shift, out);
}

) // DECLARE_X86_64_V3_SPECIFIC_CODE

DECLARE_X86_64_V4_SPECIFIC_CODE(

/// AVX-512 within-group intersection: like the AVX2 kernel but 8 rhs keys per step (skip kernel).
static void intersectWithinSkip(const DB::PositionList & lhs, const DB::PositionList & rhs, UInt32 shift, DB::PositionList & out)
{
    size_t i = 0;
    size_t j = 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();

    while (i < n && j + 8 <= m)
    {
        const UInt64 lhs_key = lhs.keys[i];

        if (lhs_key > rhs.keys[j + 7])
        {
            j += 8;
            continue;
        }

        const __m512i v_lhs_key = _mm512_set1_epi64(static_cast<long long>(lhs_key));
        const __m512i v_rhs_keys = _mm512_loadu_si512(reinterpret_cast<const void *>(&rhs.keys[j]));
        const __mmask8 mask = _mm512_cmpeq_epi64_mask(v_lhs_key, v_rhs_keys);

        if (mask)
        {
            const int k = std::countr_zero(static_cast<unsigned>(mask));
            if (const UInt32 bitmap = DB::matchBitmap(lhs.bitmap[i], rhs.bitmap[j + k], shift))
                out.pushBackKey(rhs.keys[j + k], bitmap);
            ++i;
            j += k + 1;
        }
        else
        {
            ++i;
        }
    }

    DB::scalarIntersect(lhs, rhs, i, j, shift, out);
}

/// AVX-512 8x8 block intersection (emulated VP2INTERSECT: permutexvar-broadcast each rhs key, compare
/// the 8 lhs keys). High throughput for comparable-size lists; advances the side with the smaller max.
static void intersectWithinBlock(const DB::PositionList & lhs, const DB::PositionList & rhs, UInt32 shift, DB::PositionList & out)
{
    size_t i = 0;
    size_t j = 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();

    while (i + 8 <= n && j + 8 <= m)
    {
        const __m512i v_lhs_keys = _mm512_loadu_si512(reinterpret_cast<const void *>(&lhs.keys[i]));
        const __m512i v_rhs_keys = _mm512_loadu_si512(reinterpret_cast<const void *>(&rhs.keys[j]));

        __mmask8 m1 = 0;
        __mmask8 m2 = 0;
        __mmask8 match;

        /// Unrolled to avoid a lambda across the target-attribute boundary.
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(0), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 0);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(1), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 1);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(2), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 2);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(3), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 3);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(4), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 4);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(5), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 5);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(6), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 6);
        match = _mm512_cmpeq_epi64_mask(v_lhs_keys, _mm512_permutexvar_epi64(_mm512_set1_epi64(7), v_rhs_keys));
        m1 |= match; if (match) m2 |= (1 << 7);

        if (m1)
            DB::emitBlockMatches(lhs, rhs, i, j, m1, m2, shift, out);

        const UInt64 last_lhs = lhs.keys[i + 7];
        const UInt64 last_rhs = rhs.keys[j + 7];
        if (last_lhs < last_rhs)
            i += 8;
        else if (last_lhs > last_rhs)
            j += 8;
        else
        {
            i += 8;
            j += 8;
        }
    }

    DB::scalarIntersect(lhs, rhs, i, j, shift, out);
}

) // DECLARE_X86_64_V4_SPECIFIC_CODE

namespace
{

/// Run the within-group intersection with the best available (or forced) kernel.
void dispatchIntersectWithin(const PositionList & lhs, const PositionList & rhs, UInt32 shift, PositionList & out)
{
#if USE_MULTITARGET_CODE
    const PhraseKernel kernel = chosenKernel();
    const bool want_v4 = kernel == PhraseKernel::V4 || kernel == PhraseKernel::Auto;
    if (want_v4 && isArchSupported(TargetArch::x86_64_v4))
    {
        /// Skip (galloping) kernel when lhs is much smaller than rhs; block (8x8) kernel otherwise.
        if (rhs.size() > lhs.size() * 10)
            TargetSpecific::x86_64_v4::intersectWithinSkip(lhs, rhs, shift, out);
        else
            TargetSpecific::x86_64_v4::intersectWithinBlock(lhs, rhs, shift, out);
        return;
    }

    const bool want_v3 = kernel == PhraseKernel::V3 || kernel == PhraseKernel::Auto;
    if (want_v3 && isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::intersectWithin(lhs, rhs, shift, out);
        return;
    }
#endif

    intersectWithinScalar(lhs, rhs, shift, out);
}

}

PositionList TextIndexPhraseSearch::intersect(const PositionList & lhs, const PositionList & rhs, UInt32 shift)
{
    PositionList result;
    if (lhs.empty() || rhs.empty() || shift == 0)
        return result;

    chassert(shift < RoaringishEntry::BITMAP_BITS);

    /// Within-group matches: one (vectorized) sorted-key intersection pass.
    PositionList within;
    within.reserve(std::min(lhs.size(), rhs.size()));
    dispatchIntersectWithin(lhs, rhs, shift, within);

    /// Boundary (group-straddling) matches: intersect the carry list with rhs (a plain bitmap AND, i.e.
    /// shift 0), reusing the vectorized kernels; gallop only when the carry list is tiny relative to rhs.
    PositionList carry;
    buildCarryList(lhs, shift, carry);
    PositionList boundary;
    if (!carry.empty())
    {
        if (rhs.size() > carry.size() * 256)
            intersectCarry(carry, rhs, boundary);
        else
            dispatchIntersectWithin(carry, rhs, /*shift=*/0, boundary);
    }

    chassert(std::is_sorted(within.keys.begin(), within.keys.end()));
    chassert(std::is_sorted(boundary.keys.begin(), boundary.keys.end()));

    /// Merge the two sorted runs, OR-ing bitmaps when a key appears in both.
    result.reserve(within.size() + boundary.size());
    size_t i = 0;
    size_t j = 0;
    const size_t wn = within.size();
    const size_t bn = boundary.size();
    while (i < wn && j < bn)
    {
        const UInt64 wk = within.keys[i];
        const UInt64 bk = boundary.keys[j];
        if (wk < bk)
        {
            result.pushBackKey(wk, within.bitmap[i]);
            ++i;
        }
        else if (bk < wk)
        {
            result.pushBackKey(bk, boundary.bitmap[j]);
            ++j;
        }
        else
        {
            result.pushBackKey(wk, within.bitmap[i] | boundary.bitmap[j]);
            ++i;
            ++j;
        }
    }
    while (i < wn)
    {
        result.pushBackKey(within.keys[i], within.bitmap[i]);
        ++i;
    }
    while (j < bn)
    {
        result.pushBackKey(boundary.keys[j], boundary.bitmap[j]);
        ++j;
    }

    return result;
}

PaddedPODArray<UInt32> TextIndexPhraseSearch::phraseSearch(const std::vector<PositionList> & position_lists)
{
    if (position_lists.empty())
        return {};

    if (position_lists.size() == 1)
        return extractDocIds(position_lists[0]);

    /// Chain pairwise intersections with shift=1 (consecutive tokens):
    ///   ((list[0] ∩ list[1]) ∩ list[2]) ...
    PositionList current = intersect(position_lists[0], position_lists[1], 1);

    for (size_t k = 2; k < position_lists.size(); ++k)
    {
        if (current.empty())
            return {};
        current = intersect(current, position_lists[k], 1);
    }

    if (current.empty())
        return {};

    return extractDocIds(current);
}

PaddedPODArray<UInt32> TextIndexPhraseSearch::extractDocIds(const PositionList & pl)
{
    PaddedPODArray<UInt32> doc_ids;
    doc_ids.reserve(pl.size());

    UInt32 prev_doc = std::numeric_limits<UInt32>::max();
    for (size_t i = 0; i < pl.size(); ++i)
    {
        const UInt32 doc = PositionList::keyToDoc(pl.keys[i]);
        if (doc != prev_doc)
        {
            doc_ids.push_back(doc);
            prev_doc = doc;
        }
    }

    return doc_ids;
}

}
