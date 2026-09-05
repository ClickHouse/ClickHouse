#pragma once

#include <array>
#include <memory>
#include <base/sort.h>
#include <boost/noncopyable.hpp>
#include <Common/HashTable/SmallTable.h>
#include <Common/PODArray.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/ReadHelpersArena.h>

// Include this header last, because it is an auto-generated dump of questionable
// garbage that breaks the build (e.g. it changes _POSIX_C_SOURCE).
// TODO: find out what it is. On github, they have proper interface headers like
// this one: https://github.com/RoaringBitmap/CRoaring/blob/master/include/roaring/roaring.h
#include <roaring/roaring.hh>
#include <roaring/roaring64map.hh>


namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LARGE_ARRAY_SIZE;
extern const int INCORRECT_DATA;
extern const int NOT_IMPLEMENTED;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

enum BitmapKind
{
    Small = 0,
    Bitmap = 1
};


/**
  * For a small number of values - an array of fixed size "on the stack".
  * For large, roaring bitmap is allocated.
  * For a description of the roaring_bitmap_t, see: https://github.com/RoaringBitmap/CRoaring
  */
template <typename T, UInt8 small_set_size>
class RoaringBitmapWithSmallSet : private boost::noncopyable
{
private:
    using UnsignedT = std::make_unsigned_t<T>;
    SmallSet<T, small_set_size> small;
    using ValueBuffer = VectorWithMemoryTracking<T>;
    using RoaringBitmap = std::conditional_t<sizeof(T) >= 8, roaring::Roaring64Map, roaring::Roaring>;
    using Value = std::conditional_t<sizeof(T) >= 8, UInt64, UInt32>;
    std::shared_ptr<RoaringBitmap> roaring_bitmap;

    void toLarge()
    {
        roaring_bitmap = std::make_shared<RoaringBitmap>();
        for (const auto & x : small)
            roaring_bitmap->add(static_cast<Value>(x.getValue()));
        small.clear();
    }

public:
    bool isLarge() const { return roaring_bitmap != nullptr; }
    bool isSmall() const { return roaring_bitmap == nullptr; }

    void add(T value)
    {
        if (isSmall())
        {
            if (small.find(value) == small.end())
            {
                if (!small.full())
                {
                    small.insert(value);
                }
                else
                {
                    toLarge();
                    roaring_bitmap->add(static_cast<Value>(value));
                }
            }
        }
        else
        {
            roaring_bitmap->add(static_cast<Value>(value));
        }
    }

    UInt64 size() const
    {
        if (isSmall())
            return small.size();
        return roaring_bitmap->cardinality();
    }

    void merge(const RoaringBitmapWithSmallSet & r1)
    {
        if (r1.isLarge())
        {
            if (isSmall())
                toLarge();

            *roaring_bitmap |= *r1.roaring_bitmap;
        }
        else
        {
            for (const auto & x : r1.small)
                add(x.getValue());
        }
    }

    void read(DB::ReadBuffer & in)
    {
        UInt8 kind = 0;
        readBinary(kind, in);

        if (BitmapKind::Small == kind)
        {
            small.read(in);
        }
        else if (BitmapKind::Bitmap == kind)
        {
            size_t size = 0;
            readVarUInt(size, in);

            static constexpr size_t max_size = 100_GiB;

            if (size == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect size (0) in groupBitmap.");
            if (size > max_size)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in groupBitmap (maximum: {})", max_size);

            /// TODO: this is unnecessary copying - it will be better to read and deserialize in one pass.
            /// A `String` is counted against the memory tracker but not refused by it, so a large legitimate bitmap still loads.
            String buf;
            readStringGrowing(buf, size, in);

            roaring_bitmap = std::make_shared<RoaringBitmap>(RoaringBitmap::readSafe(buf.data(), size));
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown type of roaring bitmap");
    }

    void write(DB::WriteBuffer & out) const
    {
        UInt8 kind = isLarge() ? BitmapKind::Bitmap : BitmapKind::Small;
        writeBinary(kind, out);

        if (BitmapKind::Small == kind)
        {
            small.write(out);
        }
        else if (BitmapKind::Bitmap == kind)
        {
            std::unique_ptr<RoaringBitmap> bitmap = std::make_unique<RoaringBitmap>(*roaring_bitmap);
            bitmap->runOptimize();
            auto size = bitmap->getSizeInBytes();
            writeVarUInt(size, out);
            std::unique_ptr<char[]> buf(new char[size]);
            bitmap->write(buf.get());
            out.write(buf.get(), size);
        }
    }

    /**
     * Get a new RoaringBitmap from elements of small
     */
    std::shared_ptr<RoaringBitmap> getNewRoaringBitmapFromSmall() const
    {
        std::shared_ptr<RoaringBitmap> ret = std::make_shared<RoaringBitmap>();
        for (const auto & x : small)
            ret->add(static_cast<Value>(x.getValue()));
        return ret;
    }

    /**
     * Computes the intersection between two bitmaps
     */
    void rb_and(const RoaringBitmapWithSmallSet & r1) /// NOLINT
    {
        ValueBuffer buffer;
        if (isSmall() && r1.isSmall())
        {
            // intersect
            for (const auto & x : small)
                if (r1.small.find(x.getValue()) != r1.small.end())
                    buffer.push_back(x.getValue());

            // Clear out the original values
            small.clear();

            for (const auto & value : buffer)
                small.insert(value);

            buffer.clear();
        }
        else if (isSmall() && r1.isLarge())
        {
            for (const auto & x : small)
            {
                if (r1.roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                    buffer.push_back(x.getValue());
            }

            // Clear out the original values
            small.clear();

            for (const auto & value : buffer)
                small.insert(value);

            buffer.clear();
        }
        else
        {
            std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.roaring_bitmap;
            *roaring_bitmap &= *new_rb;
        }
    }

    /**
     * Computes the union between two bitmaps.
     */
    void rb_or(const RoaringBitmapWithSmallSet & r1)  /// NOLINT
    {
        merge(r1); /// NOLINT
    }

    /**
     * Computes the symmetric difference (xor) between two bitmaps.
     */
    void rb_xor(const RoaringBitmapWithSmallSet & r1) /// NOLINT
    {
        if (isSmall())
            toLarge();

        std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.roaring_bitmap;
        *roaring_bitmap ^= *new_rb;
    }

    /**
     * Computes the difference (andnot) between two bitmaps
     */
    void rb_andnot(const RoaringBitmapWithSmallSet & r1) /// NOLINT
    {
        ValueBuffer buffer;
        if (isSmall() && r1.isSmall())
        {
            // subtract
            for (const auto & x : small)
                if (r1.small.find(x.getValue()) == r1.small.end())
                    buffer.push_back(x.getValue());

            // Clear out the original values
            small.clear();

            for (const auto & value : buffer)
                small.insert(value);

            buffer.clear();
        }
        else if (isSmall() && r1.isLarge())
        {
            for (const auto & x : small)
            {
                if (!r1.roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                    buffer.push_back(x.getValue());
            }

            // Clear out the original values
            small.clear();

            for (const auto & value : buffer)
                small.insert(value);

            buffer.clear();
        }
        else
        {
            std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.roaring_bitmap;
            *roaring_bitmap -= *new_rb;
        }
    }

    /**
     * Computes the cardinality of the intersection between two bitmaps.
     */
    UInt64 rb_and_cardinality(const RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        UInt64 ret = 0;
        if (isSmall() && r1.isSmall())
        {
            for (const auto & x : small)
                if (r1.small.find(x.getValue()) != r1.small.end())
                    ++ret;
        }
        else if (isSmall() && r1.isLarge())
        {
            for (const auto & x : small)
            {
                if (r1.roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                    ++ret;
            }
        }
        else if (r1.isSmall())
        {
            for (const auto & x : r1.small)
            {
                if (roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                    ++ret;
            }
        }
        else if constexpr (sizeof(T) < 8)
        {
            ret = roaring_bitmap->and_cardinality(*r1.roaring_bitmap);
        }
        else
        {
            /// Roaring64Map exposes no and_cardinality, so the intersection must be materialized.
            ret = (*roaring_bitmap & *r1.roaring_bitmap).cardinality();
        }
        return ret;
    }

    /**
     * Computes the cardinality of the union between two bitmaps.
     */
    UInt64 rb_or_cardinality(const RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        UInt64 c1 = size();
        UInt64 c2 = r1.size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 + c2 - inter;
    }

    /**
     * Computes the cardinality of the symmetric difference (andnot) between two bitmaps.
     */
    UInt64 rb_xor_cardinality(const RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        UInt64 c1 = size();
        UInt64 c2 = r1.size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 + c2 - 2 * inter;
    }

    /**
     * Computes the cardinality of the difference (andnot) between two bitmaps.
     */
    UInt64 rb_andnot_cardinality(const RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        UInt64 c1 = size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 - inter;
    }

    /**
     * Return 1 if the two bitmaps contain the same elements.
     */
    UInt8 rb_equals(const RoaringBitmapWithSmallSet & r1) /// NOLINT
    {
        if (isSmall())
            toLarge();

        std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.roaring_bitmap;
        return *roaring_bitmap == *new_rb;
    }

    /**
     * Check whether two bitmaps intersect.
     * Intersection with an empty set is always 0 (consistent with hasAny).
     */
    UInt8 rb_intersect(const RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        if (isSmall())
        {
            if (r1.isSmall())
            {
                for (const auto & x : r1.small)
                    if (small.find(x.getValue()) != small.end())
                        return 1;
            }
            else
            {
                for (const auto & x : small)
                {
                    if (r1.roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                        return 1;
                }
            }
        }
        else if (r1.isSmall())
        {
            for (const auto & x : r1.small)
            {
                if (roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                    return 1;
            }
        }
        else if constexpr (sizeof(T) < 8)
        {
            if (roaring_bitmap->intersect(*r1.roaring_bitmap))
                return 1;
        }
        else
        {
            /// Roaring64Map exposes no intersect, so the intersection must be materialized.
            if ((*roaring_bitmap & *r1.roaring_bitmap).cardinality() > 0)
                return 1;
        }

        return 0;
    }

    /**
     * Check whether the argument is the subset of this set.
     * Empty set is a subset of any other set (consistent with hasAll).
     * It's used in subset and currently only support comparing same type
     */
    UInt8 rb_is_subset(const RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        if (isSmall())
        {
            if (r1.isSmall())
            {
                for (const auto & x : r1.small)
                    if (small.find(x.getValue()) == small.end())
                        return 0;
            }
            else
            {
                UInt64 r1_size = r1.size();

                // A bigger set can not be a subset of ours.
                if (r1_size > small.size())
                    return 0;

                // This is a rare case with a small number of elements on
                // both sides: r1 was promoted to large for some reason and
                // it is still not larger than our small set.
                // If r1 is our subset then our size must be equal to
                // r1_size + number of not found elements, if this sum becomes
                // greater then r1 is not a subset.
                for (const auto & x : small)
                {
                    if (!r1.roaring_bitmap->contains(static_cast<Value>(x.getValue())) && ++r1_size > small.size())
                        return 0;
                }
            }
        }
        else if (r1.isSmall())
        {
            for (const auto & x : r1.small)
            {
                if (!roaring_bitmap->contains(static_cast<Value>(x.getValue())))
                    return 0;
            }
        }
        else
        {
            if (!r1.roaring_bitmap->isSubset(*roaring_bitmap))
                return 0;
        }
        return 1;
    }

    /**
     * Check whether this bitmap contains the argument.
     */
    UInt8 rb_contains(UInt64 x) const /// NOLINT
    {
        if constexpr (!std::is_same_v<T, UInt64>)
        {
            if (x > static_cast<UInt64>(std::numeric_limits<UnsignedT>::max()))
                return 0;
        }

        /// Cast as T so narrow signed values retain a consistent, sign-extended Value through promotion.
        if (isSmall())
            return small.find(static_cast<T>(x)) != small.end();

        return roaring_bitmap->contains(static_cast<Value>(static_cast<T>(x)));
    }

    /**
     * Convert elements to integer array, return number of elements
     */
    template <typename Element>
    UInt64 rb_to_array(PaddedPODArray<Element> & res) const /// NOLINT
    {
        UInt64 count = 0;
        if (isSmall())
        {
            for (const auto & x : small)
            {
                res.emplace_back(x.getValue());
                ++count;
            }
        }
        else
        {
            for (auto it = roaring_bitmap->begin(); it != roaring_bitmap->end(); ++it)
            {
                res.emplace_back(static_cast<Element>(*it));
                ++count;
            }
        }
        return count;
    }

    /**
     * Return new set with specified value range (not including range_end).
     * Element values are compared as UnsignedT.
     */
    UInt64 rb_range(UInt64 range_start, UInt64 range_end, RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        UInt64 count = 0;
        if (range_start >= range_end)
            return count;
        if (isSmall())
        {
            for (const auto & x : small)
            {
                T val = x.getValue();
                if (static_cast<UnsignedT>(val) >= range_start && static_cast<UnsignedT>(val) < range_end)
                {
                    r1.add(val);
                    ++count;
                }
            }
        }
        else
        {
            for (auto it = roaring_bitmap->begin(); it != roaring_bitmap->end(); ++it)
            {
                /// Narrow signed values are stored sign-extended in UInt32, compare in UnsignedT domain.
                const UInt64 uv = static_cast<UnsignedT>(*it);
                if (uv < range_start)
                    continue;

                if (uv < range_end)
                {
                    r1.add(static_cast<T>(*it));
                    ++count;
                }
                else
                    break;
            }
        }
        return count;
    }

    /**
     * Return new set of the smallest `limit` values (as UnsignedT) which are no less than `range_start`.
     */
    UInt64 rb_limit(UInt64 range_start, UInt64 limit, RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        if (limit == 0)
            return 0;

        if (isSmall())
        {
            VectorWithMemoryTracking<T> answer;
            for (const auto & x : small)
            {
                T val = x.getValue();
                if (static_cast<UnsignedT>(val) >= range_start)
                {
                    answer.push_back(val);
                }
            }
            if (limit < answer.size())
            {
                ::nth_element(
                    answer.begin(),
                    answer.begin() + limit,
                    answer.end(),
                    [](const T & lhs, const T & rhs) { return static_cast<UnsignedT>(lhs) < static_cast<UnsignedT>(rhs); });
                answer.resize(limit);
            }

            for (const auto & elem : answer)
                r1.add(elem);
            return answer.size();
        }

        UInt64 count = 0;
        for (auto it = roaring_bitmap->begin(); it != roaring_bitmap->end(); ++it)
        {
            /// Narrow signed values are stored sign-extended in UInt32, compare in UnsignedT domain.
            const UInt64 uv = static_cast<UnsignedT>(*it);
            if (uv < range_start)
                continue;

            if (count < limit)
            {
                r1.add(static_cast<T>(*it));
                ++count;
            }
            else
                break;
        }
        return count;
    }

    UInt64 rb_offset_limit(UInt64 offset, UInt64 limit, RoaringBitmapWithSmallSet & r1) const /// NOLINT
    {
        if (limit == 0 || offset >= size())
            return 0;

        if (isSmall())
        {
            /// The small set holds at most `small_set_size` elements, so no allocation is needed.
            std::array<T, small_set_size> values{};
            size_t num_values = 0;
            for (const auto & x : small)
                values[num_values++] = x.getValue();

            /// `offset` is below the size, checked above.
            const UInt64 count = std::min(limit, static_cast<UInt64>(num_values) - offset);

            /// Only the first `offset + count` elements have to be in order.
            ::partial_sort(
                values.begin(),
                values.begin() + (offset + count),
                values.begin() + num_values,
                [](T lhs, T rhs) { return static_cast<UnsignedT>(lhs) < static_cast<UnsignedT>(rhs); });

            for (UInt64 i = offset; i < offset + count; ++i)
                r1.add(values[i]);
            return count;
        }

        UInt64 count = 0;
        UInt64 offset_count = 0;
        auto it = roaring_bitmap->begin();
        for (; it != roaring_bitmap->end() && offset_count < offset; ++it)
            ++offset_count;

        for (; it != roaring_bitmap->end() && count < limit; ++it, ++count)
            r1.add(static_cast<T>(*it));
        return count;
    }

    UInt64 rb_min() const /// NOLINT
    {
        if (isSmall())
        {
            if (small.empty())
            {
                if constexpr (sizeof(T) >= 8)
                    return std::numeric_limits<UInt64>::max();
                return std::numeric_limits<UInt32>::max();
            }
            auto min_val = std::numeric_limits<UnsignedT>::max();
            for (const auto & x : small)
            {
                UnsignedT val = x.getValue();
                if (val < min_val)
                    min_val = val;
            }
            return min_val;
        }
        /// CRoaring returns UINT32_MAX / UINT64_MAX for an empty bitmap.
        if (roaring_bitmap->isEmpty())
        {
            if constexpr (sizeof(T) >= 8)
                return std::numeric_limits<UInt64>::max();
            return std::numeric_limits<UInt32>::max();
        }
        /// Narrow signed values are stored sign-extended in UInt32; truncate to UnsignedT to match the small-set.
        return static_cast<UnsignedT>(roaring_bitmap->minimum());
    }

    UInt64 rb_max() const /// NOLINT
    {
        if (isSmall())
        {
            if (small.empty())
                return 0;
            UnsignedT max_val = 0;
            for (const auto & x : small)
            {
                UnsignedT val = x.getValue();
                if (val > max_val)
                    max_val = val;
            }
            return max_val;
        }
        /// Narrow signed values are stored sign-extended in UInt32; truncate to UnsignedT to match the small-set.
        return static_cast<UnsignedT>(roaring_bitmap->maximum());
    }

    /**
     * Replace value.
     * It's used in transform, from/to are interpreted in the UnsignedT domain.
     */
    void rb_replace(const UInt64 * from_vals, const UInt64 * to_vals, size_t num) /// NOLINT
    {
        if (isSmall())
            toLarge();

        constexpr UInt64 max_element = std::is_same_v<T, UInt64>
            ? std::numeric_limits<UInt64>::max()
            : static_cast<UInt64>(std::numeric_limits<UnsignedT>::max());

        for (size_t i = 0; i < num; ++i)
        {
            if (from_vals[i] == to_vals[i])
                continue;

            /// Rejected regardless of whether the replacement applies, so that the outcome depends
            /// on the arguments and not on the contents of the bitmap. Storing such a value would
            /// silently truncate it to an unrelated element.
            if (to_vals[i] > max_element)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Replacement value {} does not fit into the bitmap element type, the maximum is {}",
                    to_vals[i],
                    max_element);

            /// The bitmap cannot hold this value, so there is nothing to replace. Consistent with
            /// `bitmapContains`, which reports such a value as absent instead of failing.
            if (from_vals[i] > max_element)
                continue;

            /// Cast through T so narrow signed values match sign-extended storage (e.g. Int8 255 -> -1 -> 0xFFFFFFFF).
            if (roaring_bitmap->removeChecked(static_cast<Value>(static_cast<T>(from_vals[i]))))
                roaring_bitmap->add(static_cast<Value>(static_cast<T>(to_vals[i])));
        }
    }

    /**
     * Convert container elements to UInt32 array, using the passed base instead of container_id as the high 16 bits.
     * Return the number of elements.
     */
    inline UInt16 container_to_uint32_array(const UInt16 & container_id, const UInt32 & base, PaddedPODArray<UInt32> & res) const /// NOLINT
    {
        if (sizeof(T) >= 8)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported Roaring64Map");
        }
        if (isSmall())
        {
            SetWithMemoryTracking<UInt32> values;
            for (const auto & x : small)
                if ((static_cast<UInt32>(x.getValue()) >> 16) == container_id)
                    values.insert((static_cast<UInt32>(x.getValue()) & 0xFFFFu) + base);
            UInt16 num_added = 0;
            for (const auto & value : values)
                res[num_added++] = value;
            return num_added;
        }
        else
        {
            if (res.size() < 65536)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "PaddedPODArray<UInt32> res size must >= 65536");
            auto * rb32 = reinterpret_cast<roaring::Roaring *>(roaring_bitmap.get());
            auto * ra = &rb32->roaring.high_low_container;
            int idx = roaring::internal::ra_get_index(ra, container_id);
            if (idx < 0)
                return 0;
            return static_cast<UInt16>(roaring::internal::container_to_uint32_array(res.data(), ra->containers[idx], ra->typecodes[idx], base));
        }
    }

    /**
     * Return the cardinality of a container.
     */
    inline UInt16 ra_get_container_cardinality(const UInt16 & container_id) const /// NOLINT
    {
        if (sizeof(T) >= 8)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Roaring64Map");
        }
        if (isSmall())
        {
            int num_added = 0;
            for (const auto & x : small)
            {
                if (static_cast<UInt32>(x.getValue()) >> 16 == container_id)
                    num_added++;
            }
            return static_cast<UInt16>(num_added);
        }
        else
        {
            auto * rb32 = reinterpret_cast<roaring::Roaring *>(roaring_bitmap.get());
            auto * ra = &rb32->roaring.high_low_container;
            int idx = roaring::internal::ra_get_index(ra, container_id);
            if (idx < 0)
                return 0;
            return static_cast<UInt16>(roaring::internal::container_get_cardinality(ra->containers[idx], ra->typecodes[idx]));
        }
    }

    /**
     * Collects all containers' ID with cardinality > 0.
     *  For small sets, iterates elements' high bits.
     *  For larger ones, extracts from Roaring bitmap keys.
     * Returns sorted containers' ID.
     */
    inline SetWithMemoryTracking<UInt16> ra_get_all_container_ids() /// NOLINT
    {
        if (sizeof(T) >= 8)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Roaring64Map");
        }
        SetWithMemoryTracking<UInt16> container_ids;
        if (isSmall())
        {
            for (const auto & x : small)
                container_ids.insert(static_cast<UInt32>(x.getValue()) >> 16);
        }
        else
        {
            auto * rb32 = reinterpret_cast<roaring::Roaring *>(roaring_bitmap.get());
            auto * ra = &rb32->roaring.high_low_container;
            for (int i = 0; i < ra->size; ++i)
            {
                container_ids.insert(ra->keys[i]);
            }
        }
        return container_ids;
    }

    /**
     * Return bitmap container by container ID. Returns pointer or nullptr.
     * Handles Roaring32 structure, throws for SmallSet/Roaring64Map cases.
     */
    inline roaring::internal::container_t * ra_get_container(const UInt16 & container_id, uint8_t * typecode) const /// NOLINT
    {
        if (sizeof(T) >= 8)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Roaring64Map");
        }
        if (isSmall())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported SmallSet in ra_get_container");
        }
        else
        {
            auto * rb32 = reinterpret_cast<roaring::Roaring *>(roaring_bitmap.get());
            auto * ra = &rb32->roaring.high_low_container;
            int idx = roaring::internal::ra_get_index(ra, container_id);
            if (idx < 0)
                return nullptr;
            return roaring::internal::ra_get_container_at_index(ra, static_cast<uint16_t>(idx), typecode);
        }
    }

    /**
     * Performs container-level AND operation for specified container ID.
     */
    inline roaring::internal::container_t *
    container_and(const RoaringBitmapWithSmallSet<T, small_set_size> * rhs, const UInt16 & container_id, uint8_t * result_type) const /// NOLINT
    {
        if (isSmall() || rhs->isSmall())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported SmallSet");
        uint8_t type1 = 0;
        uint8_t type2 = 0;
        roaring::internal::container_t * c1 = this->ra_get_container(container_id, &type1);
        roaring::internal::container_t * c2 = rhs->ra_get_container(container_id, &type2);
        if (!c1 || !c2)
        {
            return nullptr;
        }
        roaring::internal::container_t * c = roaring::internal::container_and(c1, type1, c2, type2, result_type);
        if (roaring::internal::container_nonzero_cardinality(c, *result_type))
        {
            return c;
        }
        roaring::internal::container_free(c, *result_type);
        return nullptr;
    }

    /**
     * Executes AND operation between two objects and convert the result to UInt32 array. Return number of result elements.
     * The upper 16 bits of each element in the result use base instead of container_id.
     */
    inline UInt16 container_and_to_uint32_array( /// NOLINT
        const RoaringBitmapWithSmallSet * rhs, const UInt16 & container_id, const UInt32 & base, PaddedPODArray<UInt32> * output) const
    {
        if (sizeof(T) >= 8)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Roaring64Map");
        if (output->size() < 65536)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "PaddedPODArray<UInt32> * output size must >= 65536");
        const bool lhs_small = this->isSmall();
        const bool rhs_small = rhs->isSmall();

        if (lhs_small && rhs_small)
        {
            /// Case 1: Both are small sets
            SetWithMemoryTracking<T> lhs_values;
            for (const auto & lhs_value : small)
                lhs_values.insert(lhs_value.getValue());
            UInt32 num_added = 0;
            for (const auto & rhs_value : rhs->small)
            {
                if (static_cast<UInt32>(rhs_value.getValue()) >> 16 == container_id and lhs_values.count(rhs_value.getValue()) > 0)
                    (*output)[num_added++] = (static_cast<UInt32>(rhs_value.getValue()) & 0xFFFFu) + base;
            }
            return static_cast<UInt16>(num_added);
        }
        else if (lhs_small || rhs_small)
        {
            /// Case 2: One is small set and the other is a roaring bitmap
            const auto & small_set = lhs_small ? *this : *rhs;
            const auto & large_bm = lhs_small ? *rhs : *this;

            uint8_t large_bm_c_typecode = 0;
            roaring::internal::container_t * large_bm_c = large_bm.ra_get_container(container_id, &large_bm_c_typecode);
            if (!large_bm_c)
                return 0;
            int num_added = 0;
            for (const auto ele : small_set.small)
            {
                const UInt32 value = static_cast<UInt32>(ele.getValue());
                if ((value >> 16) != container_id)
                    continue;
                UInt32 low_16bits = value & 0xFFFFu;
                if (roaring::internal::container_contains(large_bm_c, static_cast<uint16_t>(low_16bits), large_bm_c_typecode))
                    (*output)[num_added++] = low_16bits + base;
            }
            return static_cast<UInt16>(num_added);
        }
        else
        {
            /// Case 3: Both are roaring bitmaps
            uint8_t result_type = 0;
            roaring::internal::container_t * c = this->container_and(rhs, container_id, &result_type);
            if (!c)
                return 0;
            UInt16 result_size = static_cast<UInt16>(roaring::internal::container_to_uint32_array(output->data(), c, result_type, base));
            roaring::internal::container_free(c, result_type);
            return result_size;
        }
    }

    /**
     * Update the container corresponding to container_id to ctn. The parameter type is the type of ctn.
     */
    inline void ra_set_container(roaring::internal::container_t * ctn, const UInt16 & container_id, const uint8_t & type) /// NOLINT
    {
        if (sizeof(T) >= 8)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Roaring64Map");
        if (isSmall())
            toLarge();

        auto * rb32 = reinterpret_cast<roaring::Roaring *>(roaring_bitmap.get());
        auto ctn_idx = roaring::internal::ra_get_index(&rb32->roaring.high_low_container, container_id);

        UInt32 card = type == ARRAY_CONTAINER_TYPE ? reinterpret_cast<roaring::internal::array_container_t *>(ctn)->cardinality
                                                   : reinterpret_cast<roaring::internal::bitset_container_t *>(ctn)->cardinality;
        if (card == 0)
        {
            if (ctn_idx >= 0)
            {
                roaring::internal::ra_remove_at_index_and_free(&rb32->roaring.high_low_container, ctn_idx);
            }
            roaring::internal::container_free(ctn, type);
            return;
        }
        if (type == ARRAY_CONTAINER_TYPE)
        {
            array_container_shrink_to_fit(reinterpret_cast<roaring::internal::array_container_t *>(ctn));
        }
        if (ctn_idx >= 0)
        {
            uint8_t old_type = 0;
            auto * c = roaring::internal::ra_get_container_at_index(&rb32->roaring.high_low_container, static_cast<uint16_t>(ctn_idx), &old_type);
            roaring::internal::container_free(c, old_type);
            roaring::internal::ra_set_container_at_index(&rb32->roaring.high_low_container, ctn_idx, ctn, type);
            return;
        }
        roaring::internal::ra_insert_new_key_value_at(&rb32->roaring.high_low_container, -ctn_idx - 1, container_id, ctn, type);
    }
};

template <typename T>
struct AggregateFunctionGroupBitmapData
{
    // If false, all bitmap operations will be treated as merge to initialize the state
    bool init = false;
    RoaringBitmapWithSmallSet<T, 32> roaring_bitmap_with_small_set;
    static const char * name() { return "groupBitmap"; }
};


}
