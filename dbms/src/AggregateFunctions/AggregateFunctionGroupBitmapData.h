#pragma once

#include <roaring.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/noncopyable.hpp>
#include <roaring.hh>
#include <Common/HashTable/SmallTable.h>
#include <Common/PODArray.h>

namespace DB
{
/**
  * For a small number of values - an array of fixed size "on the stack".
  * For large, roaring_bitmap_t is allocated.
  * For a description of the roaring_bitmap_t, see: https://github.com/RoaringBitmap/CRoaring
  */
template <typename T, UInt8 small_set_size>
class RoaringBitmapWithSmallSet : private boost::noncopyable
{
private:
    using Small = SmallSet<T, small_set_size>;
    using ValueBuffer = std::vector<T>;
    Small small;
    roaring_bitmap_t * rb = nullptr;

    void toLarge()
    {
        rb = roaring_bitmap_create();

        for (const auto & x : small)
            roaring_bitmap_add(rb, x.getValue());
    }

public:
    bool isLarge() const { return rb != nullptr; }

    bool isSmall() const { return rb == nullptr; }

    ~RoaringBitmapWithSmallSet()
    {
        if (isLarge())
            roaring_bitmap_free(rb);
    }

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
                    roaring_bitmap_add(rb, value);
                }
            }
        }
        else
            roaring_bitmap_add(rb, value);
    }

    UInt64 size() const { return isSmall() ? small.size() : roaring_bitmap_get_cardinality(rb); }

    void merge(const RoaringBitmapWithSmallSet & r1)
    {
        if (r1.isLarge())
        {
            if (isSmall())
                toLarge();

            roaring_bitmap_or_inplace(rb, r1.rb);
        }
        else
        {
            for (const auto & x : r1.small)
                add(x.getValue());
        }
    }

    void read(DB::ReadBuffer & in)
    {
        bool is_large;
        readBinary(is_large, in);

        if (is_large)
        {
            toLarge();
            UInt32 cardinality;
            readBinary(cardinality, in);
            db_roaring_bitmap_add_many(in, rb, cardinality);
        }
        else
            small.read(in);
    }

    void write(DB::WriteBuffer & out) const
    {
        writeBinary(isLarge(), out);

        if (isLarge())
        {
            UInt32 cardinality = roaring_bitmap_get_cardinality(rb);
            writePODBinary(cardinality, out);
            db_ra_to_uint32_array(out, &rb->high_low_container);
        }
        else
            small.write(out);
    }


    roaring_bitmap_t * getRb() const { return rb; }

    Small & getSmall() const { return small; }

    /**
     * Get a new roaring_bitmap_t from elements of small
     */
    roaring_bitmap_t * getNewRbFromSmall() const
    {
        roaring_bitmap_t * smallRb = roaring_bitmap_create();
        for (const auto & x : small)
            roaring_bitmap_add(smallRb, x.getValue());
        return smallRb;
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
                if (roaring_bitmap_contains(r1.rb, x.getValue()))
                    buffer.push_back(x.getValue());

            // Clear out the original values
            small.clear();

            for (const auto & value : buffer)
                small.insert(value);

            buffer.clear();
        }
        else
        {
            roaring_bitmap_t * rb1 = r1.isSmall() ? r1.getNewRbFromSmall() : r1.getRb();
            roaring_bitmap_and_inplace(rb, rb1);
            if (r1.isSmall())
                roaring_bitmap_free(rb1);
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
        roaring_bitmap_t * rb1 = r1.isSmall() ? r1.getNewRbFromSmall() : r1.getRb();
        roaring_bitmap_xor_inplace(rb, rb1);
        if (r1.isSmall())
            roaring_bitmap_free(rb1);
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
                if (!roaring_bitmap_contains(r1.rb, x.getValue()))
                    buffer.push_back(x.getValue());

            // Clear out the original values
            small.clear();

            for (const auto & value : buffer)
                small.insert(value);

            buffer.clear();
        }
        else
        {
            roaring_bitmap_t * rb1 = r1.isSmall() ? r1.getNewRbFromSmall() : r1.getRb();
            roaring_bitmap_andnot_inplace(rb, rb1);
            if (r1.isSmall())
                roaring_bitmap_free(rb1);
        }
    }

    /**
     * Computes the cardinality of the intersection between two bitmaps.
     */
    UInt64 rb_and_cardinality(const RoaringBitmapWithSmallSet & r1) const
    {
        UInt64 retSize = 0;
        if (isSmall() && r1.isSmall())
        {
            for (const auto & x : small)
                if (r1.small.find(x.getValue()) != r1.small.end())
                    retSize++;
        }
        else if (isSmall() && r1.isLarge())
        {
            for (const auto & x : small)
                if (roaring_bitmap_contains(r1.rb, x.getValue()))
                    retSize++;
        }
        else
        {
            roaring_bitmap_t * rb1 = r1.isSmall() ? r1.getNewRbFromSmall() : r1.getRb();
            retSize = roaring_bitmap_and_cardinality(rb, rb1);
            if (r1.isSmall())
                roaring_bitmap_free(rb1);
        }
        return retSize;
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
        roaring_bitmap_t * rb1 = r1.isSmall() ? r1.getNewRbFromSmall() : r1.getRb();
        UInt8 is_true = roaring_bitmap_equals(rb, rb1);
        if (r1.isSmall())
            roaring_bitmap_free(rb1);
        return is_true;
    }

    /**
     * Check whether two bitmaps intersect.
     */
    UInt8 rb_intersect(const RoaringBitmapWithSmallSet & r1)
    {
        if (isSmall())
            toLarge();
        roaring_bitmap_t * rb1 = r1.isSmall() ? r1.getNewRbFromSmall() : r1.getRb();
        UInt8 is_true = roaring_bitmap_intersect(rb, rb1);
        if (r1.isSmall())
            roaring_bitmap_free(rb1);
        return is_true;
    }

    /**
     * Remove value
     */
    void rb_remove(UInt64 offsetid)
    {
        if (isSmall())
            toLarge();
        roaring_bitmap_remove(rb, offsetid);
    }

    /**
     * compute (in place) the negation of the roaring bitmap within a specified
     * interval: [range_start, range_end). The number of negated values is
     * range_end - range_start.
     * Areas outside the range are passed through unchanged.
     */
    void rb_flip(UInt64 offsetstart, UInt64 offsetend)
    {
        if (isSmall())
            toLarge();
        roaring_bitmap_flip_inplace(rb, offsetstart, offsetend);
    }

    /**
     * returns the number of integers that are smaller or equal to offsetid.
     */
    UInt64 rb_rank(UInt64 offsetid)
    {
        if (isSmall())
            toLarge();
        return roaring_bitmap_rank(rb, offsetid);
    }

    /**
     * Convert elements to integer array, return number of elements
     */
    template <typename Element>
    UInt64 rb_to_array(PaddedPODArray<Element> & res_data) const
    {
        UInt64 count = 0;
        if (isSmall())
        {
            for (const auto & x : small)
            {
                res_data.emplace_back(x.getValue());
                count++;
            }
        }
        else
        {
            roaring_uint32_iterator_t iterator;
            roaring_init_iterator(rb, &iterator);
            while (iterator.has_value)
            {
                res_data.emplace_back(iterator.current_value);
                roaring_advance_uint32_iterator(&iterator);
                count++;
            }
        }
        return count;
    }

private:
    /// To read and write the DB Buffer directly, migrate code from CRoaring
    void db_roaring_bitmap_add_many(DB::ReadBuffer & dbBuf, roaring_bitmap_t * r, size_t n_args)
    {
        void * container = NULL; // hold value of last container touched
        uint8_t typecode = 0; // typecode of last container touched
        uint32_t prev = 0; // previous valued inserted
        size_t i = 0; // index of value
        int containerindex = 0;
        if (n_args == 0)
            return;
        uint32_t val;
        readBinary(val, dbBuf);
        container = containerptr_roaring_bitmap_add(r, val, &typecode, &containerindex);
        prev = val;
        i++;
        for (; i < n_args; i++)
        {
            readBinary(val, dbBuf);
            if (((prev ^ val) >> 16) == 0)
            { // no need to seek the container, it is at hand
                // because we already have the container at hand, we can do the
                // insertion
                // automatically, bypassing the roaring_bitmap_add call
                uint8_t newtypecode = typecode;
                void * container2 = container_add(container, val & 0xFFFF, typecode, &newtypecode);
                // rare instance when we need to
                if (container2 != container)
                {
                    // change the container type
                    container_free(container, typecode);
                    ra_set_container_at_index(&r->high_low_container, containerindex, container2, newtypecode);
                    typecode = newtypecode;
                    container = container2;
                }
            }
            else
            {
                container = containerptr_roaring_bitmap_add(r, val, &typecode, &containerindex);
            }
            prev = val;
        }
    }

    void db_ra_to_uint32_array(DB::WriteBuffer & dbBuf, roaring_array_t * ra) const
    {
        size_t ctr = 0;
        for (Int32 i = 0; i < ra->size; ++i)
        {
            Int32 num_added = db_container_to_uint32_array(dbBuf, ra->containers[i], ra->typecodes[i], ((UInt32)ra->keys[i]) << 16);
            ctr += num_added;
        }
    }

    UInt32 db_container_to_uint32_array(DB::WriteBuffer & dbBuf, const void * container, UInt8 typecode, UInt32 base) const
    {
        container = container_unwrap_shared(container, &typecode);
        switch (typecode)
        {
            case BITSET_CONTAINER_TYPE_CODE:
                return db_bitset_container_to_uint32_array(dbBuf, (const bitset_container_t *)container, base);
            case ARRAY_CONTAINER_TYPE_CODE:
                return db_array_container_to_uint32_array(dbBuf, (const array_container_t *)container, base);
            case RUN_CONTAINER_TYPE_CODE:
                return db_run_container_to_uint32_array(dbBuf, (const run_container_t *)container, base);
        }
        return 0;
    }

    UInt32 db_bitset_container_to_uint32_array(DB::WriteBuffer & dbBuf, const bitset_container_t * cont, UInt32 base) const
    {
        return (UInt32)db_bitset_extract_setbits(dbBuf, cont->array, BITSET_CONTAINER_SIZE_IN_WORDS, base);
    }

    size_t db_bitset_extract_setbits(DB::WriteBuffer & dbBuf, UInt64 * bitset, size_t length, UInt32 base) const
    {
        UInt32 outpos = 0;
        for (size_t i = 0; i < length; ++i)
        {
            UInt64 w = bitset[i];
            while (w != 0)
            {
                UInt64 t = w & (~w + 1); // on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
                UInt32 r = __builtin_ctzll(w); // on x64, should compile to TZCNT
                UInt32 val = r + base;
                writePODBinary(val, dbBuf);
                outpos++;
                w ^= t;
            }
            base += 64;
        }
        return outpos;
    }

    int db_array_container_to_uint32_array(DB::WriteBuffer & dbBuf, const array_container_t * cont, UInt32 base) const
    {
        UInt32 outpos = 0;
        for (Int32 i = 0; i < cont->cardinality; ++i)
        {
            const UInt32 val = base + cont->array[i];
            writePODBinary(val, dbBuf);
            outpos++;
        }
        return outpos;
    }

    int db_run_container_to_uint32_array(DB::WriteBuffer & dbBuf, const run_container_t * cont, UInt32 base) const
    {
        UInt32 outpos = 0;
        for (Int32 i = 0; i < cont->n_runs; ++i)
        {
            UInt32 run_start = base + cont->runs[i].value;
            UInt16 le = cont->runs[i].length;
            for (Int32 j = 0; j <= le; ++j)
            {
                UInt32 val = run_start + j;
                writePODBinary(val, dbBuf);
                outpos++;
            }
        }
        return outpos;
    }
};

template <typename T>
struct AggregateFunctionGroupBitmapData
{
    RoaringBitmapWithSmallSet<T, 32> rbs;
    static const char * name() { return "groupBitmap"; }
};


}
