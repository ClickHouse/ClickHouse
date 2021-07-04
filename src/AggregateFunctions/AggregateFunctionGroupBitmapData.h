#pragma once

#include <algorithm>
#include <memory>
#include <boost/noncopyable.hpp>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/HashTable/SmallTable.h>
#include <Common/PODArray.h>

// Include this header last, because it is an auto-generated dump of questionable
// garbage that breaks the build (e.g. it changes _POSIX_C_SOURCE).
// TODO: find out what it is. On github, they have proper interface headers like
// this one: https://github.com/RoaringBitmap/CRoaring/blob/master/include/roaring/roaring.h
#include <roaring.hh>
#include <roaring64map.hh>


namespace DB
{

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
    using ValueBuffer = std::vector<T>;
    using RoaringBitmap = std::conditional_t<sizeof(T) >= 8, roaring::Roaring64Map, roaring::Roaring>;
    using Value = std::conditional_t<sizeof(T) >= 8, UInt64, UInt32>;
    std::shared_ptr<RoaringBitmap> rb = nullptr;

    void toLarge()
    {
        rb = std::make_unique<RoaringBitmap>();
        for (const auto & x : small)
            rb->add(static_cast<Value>(x.getValue()));
        small.clear();
    }

public:
    bool isLarge() const { return rb != nullptr; }

    bool isSmall() const { return rb == nullptr; }

    void add(T value)
    {
        if (isSmall())
        {
            if (small.find(value) == small.end())
            {
                if (!small.full())
                    small.insert(value);
                else
                {
                    toLarge();
                    rb->add(static_cast<Value>(value));
                }
            }
        }
        else
        {
            rb->add(static_cast<Value>(value));
        }
    }

    UInt64 size() const
    {
        if (isSmall())
            return small.size();
        else
            return rb->cardinality();
    }

    void merge(const RoaringBitmapWithSmallSet & r1)
    {
        if (r1.isLarge())
        {
            if (isSmall())
                toLarge();

            *rb |= *r1.rb;
        }
        else
        {
            for (const auto & x : r1.small)
                add(x.getValue());
        }
    }

    void read(DB::ReadBuffer & in)
    {
        UInt8 kind;
        readBinary(kind, in);
        if (BitmapKind::Small == kind)
        {
            small.read(in);
        }
        else if (BitmapKind::Bitmap == kind)
        {
            size_t size;
            readVarUInt(size, in);
            std::unique_ptr<char[]> buf(new char[size]);
            in.readStrict(buf.get(), size);
            rb = std::make_unique<RoaringBitmap>(RoaringBitmap::read(buf.get()));
        }
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
            auto size = rb->getSizeInBytes();
            writeVarUInt(size, out);
            std::unique_ptr<char[]> buf(new char[size]);
            rb->write(buf.get());
            out.write(buf.get(), size);
        }
    }

    /**
     * Get a new RoaringBitmap from elements of small
     */
    std::shared_ptr<RoaringBitmap> getNewRoaringBitmapFromSmall() const
    {
        std::shared_ptr<RoaringBitmap> ret = std::make_unique<RoaringBitmap>();
        for (const auto & x : small)
            ret->add(static_cast<Value>(x.getValue()));
        return ret;
    }

    /**
     * Computes the intersection between two bitmaps
     */
    void rb_and(const RoaringBitmapWithSmallSet & r1)
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
                if (r1.rb->contains(static_cast<Value>(x.getValue())))
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
            std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.rb;
            *rb &= *new_rb;
        }
    }

    /**
     * Computes the union between two bitmaps.
     */
    void rb_or(const RoaringBitmapWithSmallSet & r1) { merge(r1); }

    /**
     * Computes the symmetric difference (xor) between two bitmaps.
     */
    void rb_xor(const RoaringBitmapWithSmallSet & r1)
    {
        if (isSmall())
            toLarge();

        std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.rb;
        *rb ^= *new_rb;
    }

    /**
     * Computes the difference (andnot) between two bitmaps
     */
    void rb_andnot(const RoaringBitmapWithSmallSet & r1)
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
                if (!r1.rb->contains(static_cast<Value>(x.getValue())))
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
            std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.rb;
            *rb -= *new_rb;
        }
    }

    /**
     * Computes the cardinality of the intersection between two bitmaps.
     */
    UInt64 rb_and_cardinality(const RoaringBitmapWithSmallSet & r1) const
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
                if (r1.rb->contains(static_cast<Value>(x.getValue())))
                    ++ret;
            }
        }
        else
        {
            std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.rb;
            ret = (*rb & *new_rb).cardinality();
        }
        return ret;
    }

    /**
     * Computes the cardinality of the union between two bitmaps.
    */
    UInt64 rb_or_cardinality(const RoaringBitmapWithSmallSet & r1) const
    {
        UInt64 c1 = size();
        UInt64 c2 = r1.size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 + c2 - inter;
    }

    /**
     * Computes the cardinality of the symmetric difference (andnot) between two bitmaps.
    */
    UInt64 rb_xor_cardinality(const RoaringBitmapWithSmallSet & r1) const
    {
        UInt64 c1 = size();
        UInt64 c2 = r1.size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 + c2 - 2 * inter;
    }

    /**
     * Computes the cardinality of the difference (andnot) between two bitmaps.
     */
    UInt64 rb_andnot_cardinality(const RoaringBitmapWithSmallSet & r1) const
    {
        UInt64 c1 = size();
        UInt64 inter = rb_and_cardinality(r1);
        return c1 - inter;
    }

    /**
     * Return 1 if the two bitmaps contain the same elements.
     */
    UInt8 rb_equals(const RoaringBitmapWithSmallSet & r1)
    {
        if (isSmall())
            toLarge();

        std::shared_ptr<RoaringBitmap> new_rb = r1.isSmall() ? r1.getNewRoaringBitmapFromSmall() : r1.rb;
        return *rb == *new_rb;
    }

    /**
     * Check whether two bitmaps intersect.
     * Intersection with an empty set is always 0 (consistent with hasAny).
     */
    UInt8 rb_intersect(const RoaringBitmapWithSmallSet & r1) const
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
                    if (r1.rb->contains(static_cast<Value>(x.getValue())))
                        return 1;
                }
            }
        }
        else if (r1.isSmall())
        {
            for (const auto & x : r1.small)
            {
                if (rb->contains(static_cast<Value>(x.getValue())))
                    return 1;
            }
        }
        else
        {
            if ((*rb & *r1.rb).cardinality() > 0)
                return 1;
        }

        return 0;
    }

    /**
     * Check whether the argument is the subset of this set.
     * Empty set is a subset of any other set (consistent with hasAll).
     * It's used in subset and currently only support comparing same type
     */
    UInt8 rb_is_subset(const RoaringBitmapWithSmallSet & r1) const
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
                    if (!r1.rb->contains(static_cast<Value>(x.getValue())) && ++r1_size > small.size())
                        return 0;
                }
            }
        }
        else if (r1.isSmall())
        {
            for (const auto & x : r1.small)
            {
                if (!rb->contains(static_cast<Value>(x.getValue())))
                    return 0;
            }
        }
        else
        {
            if (!r1.rb->isSubset(*rb))
                return 0;
        }
        return 1;
    }

    /**
     * Check whether this bitmap contains the argument.
     */
    UInt8 rb_contains(UInt64 x) const
    {
        if (isSmall())
            return small.find(x) != small.end();
        else
            return rb->contains(static_cast<Value>(x));
    }

    /**
     * Remove value
     */
    void rb_remove(UInt64 x)
    {
        if (isSmall())
            toLarge();

        rb->remove(x);
    }

    /**
     * compute (in place) the negation of the roaring bitmap within a specified
     * interval: [range_start, range_end). The number of negated values is
     * range_end - range_start.
     * Areas outside the range are passed through unchanged.
     */
    void rb_flip(UInt64 begin, UInt64 end)
    {
        if (isSmall())
            toLarge();

        rb->flip(begin, end);
    }

    /**
     * returns the number of integers that are smaller or equal to offsetid.
     */
    UInt64 rb_rank(UInt64 x)
    {
        if (isSmall())
            toLarge();

        return rb->rank(x);
    }

    /**
     * Convert elements to integer array, return number of elements
     */
    template <typename Element>
    UInt64 rb_to_array(PaddedPODArray<Element> & res) const
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
            for (auto it = rb->begin(); it != rb->end(); ++it)
            {
                res.emplace_back(*it);
                ++count;
            }
        }
        return count;
    }

    /**
     * Return new set with specified range (not include the range_end)
     * It's used in subset and currently only support UInt32
     */
    UInt64 rb_range(UInt64 range_start, UInt64 range_end, RoaringBitmapWithSmallSet & r1) const
    {
        UInt64 count = 0;
        if (range_start >= range_end)
            return count;
        if (isSmall())
        {
            for (const auto & x : small)
            {
                T val = x.getValue();
                if (UInt32(val) >= range_start && UInt32(val) < range_end)
                {
                    r1.add(val);
                    ++count;
                }
            }
        }
        else
        {
            for (auto it = rb->begin(); it != rb->end(); ++it)
            {
                if (*it < range_start)
                    continue;

                if (*it < range_end)
                {
                    r1.add(*it);
                    ++count;
                }
                else
                    break;
            }
        }
        return count;
    }

    /**
     * Return new set of the smallest `limit` values in set which is no less than `range_start`.
     * It's used in subset and currently only support UInt32
     */
    UInt64 rb_limit(UInt64 range_start, UInt64 limit, RoaringBitmapWithSmallSet & r1) const
    {
        if (limit == 0)
            return 0;

        if (isSmall())
        {
            std::vector<T> answer;
            for (const auto & x : small)
            {
                T val = x.getValue();
                if (UInt32(val) >= range_start)
                {
                    answer.push_back(val);
                }
            }
            if (limit < answer.size())
            {
                std::nth_element(answer.begin(), answer.begin() + limit, answer.end());
                answer.resize(limit);
            }

            for (const auto & elem : answer)
                r1.add(elem);
            return answer.size();
        }
        else
        {
            UInt64 count = 0;
            for (auto it = rb->begin(); it != rb->end(); ++it)
            {
                if (*it < range_start)
                    continue;

                if (count < limit)
                {
                    r1.add(*it);
                    ++count;
                }
                else
                    break;
            }
            return count;
        }
    }

    UInt64 rb_min() const
    {
        if (isSmall())
        {
            if (small.empty())
                return 0;
            auto min_val = std::numeric_limits<UnsignedT>::max();
            for (const auto & x : small)
            {
                UnsignedT val = x.getValue();
                if (val < min_val)
                    min_val = val;
            }
            return min_val;
        }
        else
            return rb->minimum();
    }

    UInt64 rb_max() const
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
        else
            return rb->maximum();
    }

    /**
     * Replace value.
     * It's used in transform and currently can only support UInt32
     */
    void rb_replace(const UInt64 * from_vals, const UInt64 * to_vals, size_t num)
    {
        if (isSmall())
            toLarge();

        for (size_t i = 0; i < num; ++i)
        {
            if (from_vals[i] == to_vals[i])
                continue;
            bool changed = rb->removeChecked(static_cast<Value>(from_vals[i]));
            if (changed)
                rb->add(static_cast<Value>(to_vals[i]));
        }
    }
};

template <typename T>
struct AggregateFunctionGroupBitmapData
{
    // If false, all bitmap operations will be treated as merge to initialize the state
    bool init = false;
    RoaringBitmapWithSmallSet<T, 32> rbs;
    static const char * name() { return "groupBitmap"; }
};


}
