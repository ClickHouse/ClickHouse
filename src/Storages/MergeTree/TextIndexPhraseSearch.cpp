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

/// Roaringish phrase search via two-phase sorted-key intersection.
///
/// A phrase term's positions are decoded into a PositionList: a sorted, unique lane of 64-bit keys
/// `(doc_id << 32) | group` and a parallel lane of 32-bit bitmaps (bit b => position group*32 + b).
/// Phrase adjacency of two terms with positional distance `shift` reduces to intersecting the two
/// sorted key lanes and, on a key match, testing a shifted bitmap overlap:
///   within-group : bitmap = (lhs_bitmap << shift) & rhs_bitmap                     (same key)
///   boundary     : bitmap = (lhs_bitmap >> (BITMAP_BITS - shift)) & rhs_bitmap     (rhs key = lhs key + 1)
/// The boundary phase catches matches that straddle a 32-position group boundary; `lhs key + 1`
/// stays within the same document because group = position / 32 <= 0x07FFFFFF (positions are UInt32).
///
/// Only the sorted-key intersection is vectorized (broadcast-compare / 8x8 block compare); the bitmap
/// overlap and the match emission stay scalar. The scan is the same shape as a vectorized set
/// intersection, so it maps cleanly onto AVX2 (x86_64_v3) and AVX-512 (x86_64_v4), with a scalar
/// fallback and a scalar tail for the sub-vector remainder. The active kernel is chosen at runtime
/// from the CPU's supported ISA; the `CH_PHRASE_KERNEL` environment variable (scalar / v3 / v4) can
/// force a specific kernel for testing and benchmarking.

namespace
{

/// Bitmap overlap for a matching key. `boundary` selects the group-straddling high bits.
ALWAYS_INLINE UInt32 matchBitmap(UInt32 lhs_bitmap, UInt32 rhs_bitmap, UInt32 shift, bool boundary)
{
    const UInt64 shifted = static_cast<UInt64>(lhs_bitmap) << shift;
    const UInt32 selected = boundary ? static_cast<UInt32>(shifted >> 32) : static_cast<UInt32>(shifted);
    return selected & rhs_bitmap;
}

/// Scalar two-pointer intersection from (i, j) to the end. Serves as both the standalone scalar
/// kernel (called with i = j = 0) and the tail of every SIMD kernel. Emits a strictly increasing,
/// unique run of (key, bitmap) into `out`.
ALWAYS_INLINE void scalarIntersect(
    const PositionList & lhs,
    const PositionList & rhs,
    size_t i,
    size_t j,
    UInt32 shift,
    bool boundary,
    PositionList & out)
{
    const UInt64 add = boundary ? 1 : 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();
    while (i < n && j < m)
    {
        const UInt64 lhs_key = lhs.keys[i] + add;
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
        if (const UInt32 bitmap = matchBitmap(lhs.bitmap[i], rhs.bitmap[j], shift, boundary))
            out.pushBackKey(rhs_key, bitmap);
        ++i;
        ++j;
    }
}

/// Emit matches for one 8x8 block, given lane masks m1 (lhs lanes that matched) and m2 (rhs lanes
/// that matched). Keys are unique per list, so the matches form a monotone bijection: the k-th set
/// bit of m1 pairs with the k-th set bit of m2. Only used by the AVX-512 block kernel.
[[maybe_unused]] ALWAYS_INLINE void emitBlockMatches(
    const PositionList & lhs,
    const PositionList & rhs,
    size_t i,
    size_t j,
    unsigned m1,
    unsigned m2,
    UInt32 shift,
    bool boundary,
    PositionList & out)
{
    while (m1)
    {
        const int ki = std::countr_zero(m1);
        const int kj = std::countr_zero(m2);
        if (const UInt32 bitmap = matchBitmap(lhs.bitmap[i + ki], rhs.bitmap[j + kj], shift, boundary))
            out.pushBackKey(rhs.keys[j + kj], bitmap);
        m1 &= m1 - 1;
        m2 &= m2 - 1;
    }
}

void intersectPhaseScalar(const PositionList & lhs, const PositionList & rhs, UInt32 shift, bool boundary, PositionList & out)
{
    scalarIntersect(lhs, rhs, 0, 0, shift, boundary, out);
}

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

}

DECLARE_X86_64_V3_SPECIFIC_CODE(

/// AVX2 broadcast-compare intersection: broadcast the (boundary-adjusted) lhs key and compare it
/// against 4 rhs keys per iteration. Skip-oriented: cheap per step, galloping when lhs is the
/// smaller list.
static void intersectPhaseSkip(const DB::PositionList & lhs, const DB::PositionList & rhs, UInt32 shift, bool boundary, DB::PositionList & out)
{
    size_t i = 0;
    size_t j = 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();
    const UInt64 add = boundary ? 1 : 0;

    while (i < n && j + 4 <= m)
    {
        const UInt64 lhs_key = lhs.keys[i] + add;

        /// The current lhs key is beyond the next 4 rhs keys: skip them.
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
            if (const UInt32 bitmap = DB::matchBitmap(lhs.bitmap[i], rhs.bitmap[j + k], shift, boundary))
                out.pushBackKey(rhs.keys[j + k], bitmap);
            ++i;
            j += k + 1;
        }
        else
        {
            ++i;
        }
    }

    DB::scalarIntersect(lhs, rhs, i, j, shift, boundary, out);
}

) // DECLARE_X86_64_V3_SPECIFIC_CODE

DECLARE_X86_64_V4_SPECIFIC_CODE(

/// AVX-512 broadcast-compare intersection: like the AVX2 kernel but 8 rhs keys per iteration.
/// Preferred when one list is much larger than the other (galloping over the smaller one).
static void intersectPhaseSkip(const DB::PositionList & lhs, const DB::PositionList & rhs, UInt32 shift, bool boundary, DB::PositionList & out)
{
    size_t i = 0;
    size_t j = 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();
    const UInt64 add = boundary ? 1 : 0;

    while (i < n && j + 8 <= m)
    {
        const UInt64 lhs_key = lhs.keys[i] + add;

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
            if (const UInt32 bitmap = DB::matchBitmap(lhs.bitmap[i], rhs.bitmap[j + k], shift, boundary))
                out.pushBackKey(rhs.keys[j + k], bitmap);
            ++i;
            j += k + 1;
        }
        else
        {
            ++i;
        }
    }

    DB::scalarIntersect(lhs, rhs, i, j, shift, boundary, out);
}

/// AVX-512 8x8 block intersection emulating VP2INTERSECT with Foundation/BW instructions
/// (broadcast each of the 8 rhs keys with permutexvar and compare against the 8 lhs keys).
/// High throughput for lists of comparable size. Advances the side with the smaller block maximum.
static void intersectPhaseBlock(const DB::PositionList & lhs, const DB::PositionList & rhs, UInt32 shift, bool boundary, DB::PositionList & out)
{
    size_t i = 0;
    size_t j = 0;
    const size_t n = lhs.size();
    const size_t m = rhs.size();
    const UInt64 add = boundary ? 1 : 0;
    const __m512i v_add = _mm512_set1_epi64(static_cast<long long>(add));

    while (i + 8 <= n && j + 8 <= m)
    {
        const __m512i v_lhs_keys = _mm512_add_epi64(_mm512_loadu_si512(reinterpret_cast<const void *>(&lhs.keys[i])), v_add);
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
            DB::emitBlockMatches(lhs, rhs, i, j, m1, m2, shift, boundary, out);

        const UInt64 last_lhs = lhs.keys[i + 7] + add;
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

    DB::scalarIntersect(lhs, rhs, i, j, shift, boundary, out);
}

) // DECLARE_X86_64_V4_SPECIFIC_CODE

namespace
{

/// Run one intersection phase with the best available (or forced) kernel.
void dispatchIntersectPhase(const PositionList & lhs, const PositionList & rhs, UInt32 shift, bool boundary, PositionList & out)
{
    const PhraseKernel kernel = chosenKernel();

#if USE_MULTITARGET_CODE
    const bool want_v4 = kernel == PhraseKernel::V4 || kernel == PhraseKernel::Auto;
    if (want_v4 && isArchSupported(TargetArch::x86_64_v4))
    {
        /// Skip (galloping) kernel when lhs is much smaller than rhs; block (8x8) kernel otherwise.
        if (rhs.size() > lhs.size() * 10)
            TargetSpecific::x86_64_v4::intersectPhaseSkip(lhs, rhs, shift, boundary, out);
        else
            TargetSpecific::x86_64_v4::intersectPhaseBlock(lhs, rhs, shift, boundary, out);
        return;
    }

    const bool want_v3 = kernel == PhraseKernel::V3 || kernel == PhraseKernel::Auto;
    if (want_v3 && isArchSupported(TargetArch::x86_64_v3))
    {
        TargetSpecific::x86_64_v3::intersectPhaseSkip(lhs, rhs, shift, boundary, out);
        return;
    }
#endif

    intersectPhaseScalar(lhs, rhs, shift, boundary, out);
}

}

PositionList TextIndexPhraseSearch::intersect(const PositionList & lhs, const PositionList & rhs, UInt32 shift)
{
    PositionList result;
    if (lhs.empty() || rhs.empty() || shift == 0)
        return result;

    chassert(shift < RoaringishEntry::BITMAP_BITS);

    /// Two independent passes over the sorted key lanes, each producing a strictly increasing,
    /// unique run of (key, bitmap): the within-group phase and the boundary-crossing phase.
    PositionList within;
    PositionList boundary;
    within.reserve(std::min(lhs.size(), rhs.size()));

    dispatchIntersectPhase(lhs, rhs, shift, /*boundary=*/false, within);
    dispatchIntersectPhase(lhs, rhs, shift, /*boundary=*/true, boundary);

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
