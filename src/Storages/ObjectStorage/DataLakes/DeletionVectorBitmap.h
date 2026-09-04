#pragma once

#include <base/sort.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <Common/HashTable/SmallTable.h>

#include <memory>

/// Include this header last, because it is an auto-generated dump of questionable
/// garbage that breaks the build (e.g. it changes _POSIX_C_SOURCE).
#include <roaring/roaring64.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** The row numbers deleted from a single data file, as described by a Delta Lake deletion
  * vector or by Iceberg position deletes. Row numbers are relative to the data file and are
  * 64-bit, because that is the width both formats hand us: Iceberg declares `pos` as `long`,
  * and the Delta kernel returns `u64`.
  *
  * A handful of values are kept in a fixed-size array; beyond that the set switches to a
  * roaring bitmap. The serialized form is byte-for-byte identical to that of
  * `RoaringBitmapWithSmallSet<size_t, 32>`, which this type used to be, because the cluster
  * function protocol carries deletion vectors from the initiator to the workers and the two
  * sides may run different versions.
  *
  * The roaring representation is croaring's native 64-bit bitmap rather than
  * `roaring::Roaring64Map`. Both implement the same portable format, but only the former can
  * hand out its values in batches. That matters for `forEachInRange`, which enumerates the
  * deleted rows of one chunk: reading them one value at a time costs several times more,
  * because every value crosses into the library and back out through the callback.
  */
class DeletionVectorBitmap : private boost::noncopyable
{
public:
    void add(UInt64 value);

    UInt64 size() const;

    bool contains(UInt64 value) const;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);

    /// Calls `callback(row_number)` for every deleted row number in [range_begin, range_end),
    /// in ascending order and without duplicates.
    template <typename F>
    void forEachInRange(UInt64 range_begin, UInt64 range_end, F && callback) const
    {
        if (range_begin >= range_end)
            return;

        if (!bitmap)
        {
            /// The small set is unordered, so the matches have to be collected and sorted first.
            UInt64 buffer[small_set_size];
            size_t count = 0;
            for (const auto & x : small)
            {
                const UInt64 value = x.getValue();
                if (value >= range_begin && value < range_end)
                    buffer[count++] = value;
            }

            ::sort(buffer, buffer + count);
            for (size_t i = 0; i < count; ++i)
                callback(buffer[i]);
            return;
        }

        IteratorPtr iterator(roaring::api::roaring64_iterator_create(bitmap.get()));
        if (!roaring::api::roaring64_iterator_move_equalorlarger(iterator.get(), range_begin))
            return;

        /// The batch is read into a local buffer instead of being consumed value by value: that
        /// keeps both the call into the library and the callback out of the per-value path.
        UInt64 buffer[batch_size];
        while (true)
        {
            const UInt64 count = roaring::api::roaring64_iterator_read(iterator.get(), buffer, batch_size);
            for (UInt64 i = 0; i < count; ++i)
            {
                /// The iterator has no upper bound, so the tail of the last batch may overshoot.
                if (buffer[i] >= range_end)
                    return;
                callback(buffer[i]);
            }

            if (count < batch_size)
                return;
        }
    }

private:
    /// Must match `BitmapKind` in AggregateFunctionGroupBitmapData.h: the serialized form is
    /// shared with `RoaringBitmapWithSmallSet`.
    enum Kind : UInt8
    {
        SmallKind = 0,
        BitmapKind = 1,
    };

    /// Must match the `small_set_size` of the `RoaringBitmapWithSmallSet` this type replaced,
    /// because it decides which of the two serialized forms is written.
    static constexpr UInt8 small_set_size = 32;

    /// Large enough that the per-batch costs vanish, small enough to stay on the stack.
    static constexpr UInt64 batch_size = 256;

    struct BitmapDeleter
    {
        void operator()(roaring::api::roaring64_bitmap_t * value) const noexcept
        {
            roaring::api::roaring64_bitmap_free(value);
        }
    };
    using BitmapPtr = std::unique_ptr<roaring::api::roaring64_bitmap_t, BitmapDeleter>;

    struct IteratorDeleter
    {
        void operator()(roaring::api::roaring64_iterator_t * value) const noexcept
        {
            roaring::api::roaring64_iterator_free(value);
        }
    };
    using IteratorPtr = std::unique_ptr<roaring::api::roaring64_iterator_t, IteratorDeleter>;

    void toLarge();

    SmallSet<UInt64, small_set_size> small;
    BitmapPtr bitmap;

    /// Remembers the leaf the previous `add` landed in, which is what makes inserting a run of
    /// nearby row numbers cheap. It points into `bitmap`, so it has to be cleared whenever
    /// `bitmap` is replaced, and every insertion has to go through the `_bulk` entry point.
    roaring::api::roaring64_bulk_context_t bulk_context{};
};

}
