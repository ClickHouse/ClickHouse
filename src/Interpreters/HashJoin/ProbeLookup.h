#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/RowRefs.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <base/types.h>

#include <type_traits>
#include <vector>

namespace DB
{

/** Splits the probe into a pass that records where each key landed and a pass that emits from those
  * records, so a later lookup's cache miss overlaps an earlier one. It costs writing and re-reading
  * the records, so each probe site decides for itself.
  */

/// Measured flat from 256 to 65536 rows, so not a tuning knob.
inline constexpr size_t PROBE_BATCH_ROWS = 8192;

template <typename Selector>
ALWAYS_INLINE size_t selectorIndexAt(const Selector & selector, size_t k)
{
    if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Selector>)
        return selector[k];
    else if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Indexes>)
        return selector.getData()[k];
    else
        return selector.first + k;
}

/// An occupied cell is never all-zero, so zero can mean "no match".

template <typename Mapped>
inline constexpr bool probe_mapped_fits_word
    = std::is_same_v<std::remove_const_t<Mapped>, RowRef> || std::is_same_v<std::remove_const_t<Mapped>, RowRefList>;

template <typename Mapped>
requires probe_mapped_fits_word<Mapped>
ALWAYS_INLINE UInt64 mappedWordOf(const Mapped & mapped)
{
    if constexpr (std::is_same_v<std::remove_const_t<Mapped>, RowRefList>)
        return mapped.word;
    else
        return mapped.encode();
}

template <typename Mapped>
requires probe_mapped_fits_word<Mapped>
ALWAYS_INLINE Mapped mappedFromWord(UInt64 word)
{
    if constexpr (std::is_same_v<Mapped, RowRefList>)
        return RowRefList::fromWord(word);
    else
        return RowRef::fromWord(word);
}

/** Indexed by position within the batch. `found[j]` is zero for a miss or a skipped row, else the
  * mapped value by value where it fits in the word and a pointer into the cell where it does not
  * (ASOF) - copying on visit is what keeps the emit pass off the hash table.
  */
struct ProbeOutcomes
{
    /// `scratch`, or `LazyOutput::row_refs` directly - see `outputIsProbeOutcomes`.
    UInt64 * found = nullptr;
    PODArray<UInt64> offset;
    PODArray<UInt64> scratch;

    void useScratch(size_t rows, bool need_flags)
    {
        scratch.resize(rows);
        found = scratch.data();
        if (need_flags)
            offset.resize(rows);
    }

    /// `external` must hold `rows` and must not move before the emit pass has consumed them.
    void useExternal(UInt64 * external, size_t rows, bool need_flags)
    {
        found = external;
        if (need_flags)
            offset.resize(rows);
    }
};

template <bool need_flags>
struct RecordOutcomeSink
{
    ProbeOutcomes & outcomes;

    ALWAYS_INLINE void miss(size_t j, size_t /* row */) { outcomes.found[j] = 0; }

    template <typename FindResult>
    ALWAYS_INLINE void result(size_t j, size_t row, size_t /* ind */, const FindResult & find_result)
    {
        using Mapped = std::remove_reference_t<decltype(std::declval<FindResult &>().getMapped())>;

        if (!find_result.isFound())
        {
            miss(j, row);
            return;
        }

        auto & mapped = find_result.getMapped();
        if constexpr (probe_mapped_fits_word<Mapped>)
            outcomes.found[j] = mappedWordOf(mapped);
        else
            outcomes.found[j] = reinterpret_cast<UInt64>(&mapped);

        if constexpr (need_flags)
            outcomes.offset[j] = find_result.getOffset();
    }
};

/** One `findKey` per row, in order, writing every slot in `[0, count)`. `prefetch_at` takes the
  * absolute row so look-ahead can cross the batch boundary.
  *
  * `skip_data == nullptr` branches into one of two outlined loops rather than being a template
  * parameter: making it one, or inlining `runImpl`, cost over 3% on String keys.
  */
struct SequentialLookup
{
    template <bool need_flags, typename KeyGetter, typename Map, typename Selector, typename PrefetchAt>
    static void run(
        KeyGetter & key_getter,
        const Map & map,
        const Selector & selector,
        const UInt8 * skip_data,
        Arena & pool,
        size_t begin,
        size_t count,
        PrefetchAt && prefetch_at,
        ProbeOutcomes & outcomes)
    {
        RecordOutcomeSink<need_flags> sink{outcomes};
        if (skip_data == nullptr)
            runImpl</*with_skip=*/false, need_flags>(
                key_getter, map, selector, skip_data, pool, begin, count, prefetch_at, sink);
        else
            runImpl</*with_skip=*/true, need_flags>(
                key_getter, map, selector, skip_data, pool, begin, count, prefetch_at, sink);
    }

    template <bool with_skip, bool need_flags, typename KeyGetter, typename Map, typename Selector, typename PrefetchAt>
    NO_INLINE static void runImpl(
        KeyGetter & key_getter,
        const Map & map,
        const Selector & selector,
        const UInt8 * skip_data [[maybe_unused]],
        Arena & pool,
        size_t begin,
        size_t count,
        PrefetchAt && prefetch_at,
        RecordOutcomeSink<need_flags> & sink)
    {
        for (size_t j = 0; j < count; ++j)
        {
            prefetch_at(begin + j);

            const size_t ind = selectorIndexAt(selector, begin + j);

            if constexpr (with_skip)
            {
                if (skip_data[ind])
                {
                    sink.miss(j, begin + j);
                    continue;
                }
            }

            sink.result(j, begin + j, ind, key_getter.findKey(map, ind, pool));
        }
    }
};


}
