#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_AVAILABLE_DATA;
}


/** Compact array for data storage, size `content_width`, in bits, of which is
  * less than one byte. Instead of storing each value in a separate
  * bytes, which leads to a waste of 37.5% of the space for content_width = 5, CompactArray stores
  * adjacent `content_width`-bit values in the byte array, that is actually CompactArray
  * simulates an array of `content_width`-bit values.
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class CompactArray final
{
public:
    class Reader;
    class Locus;

    CompactArray() = default;

    UInt8 ALWAYS_INLINE operator[](BucketIndex bucket_index) const
    {
        Locus locus(bucket_index);

        if (locus.index_l == locus.index_r)
            return locus.read(bitset[locus.index_l]);
        else
            return locus.read(bitset[locus.index_l], bitset[locus.index_r]);
    }

    Locus ALWAYS_INLINE operator[](BucketIndex bucket_index)
    {
        Locus locus(bucket_index);

        locus.content_l = &bitset[locus.index_l];

        if (locus.index_l == locus.index_r)
            locus.content_r = locus.content_l;
        else
            locus.content_r = &bitset[locus.index_r];

        return locus;
    }

private:
    /// number of bytes in bitset
    static constexpr size_t BITSET_SIZE = (static_cast<size_t>(bucket_count) * content_width + 7) / 8;
    UInt8 bitset[BITSET_SIZE] = { 0 };
};

/** A class for sequentially reading cells from a compact array on a disk.
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class CompactArray<BucketIndex, content_width, bucket_count>::Reader final
{
public:
    explicit Reader(ReadBuffer & in_)
        : in(in_)
    {
    }

    Reader(const Reader &) = delete;
    Reader & operator=(const Reader &) = delete;

    bool next()
    {
        if (current_bucket_index == bucket_count)
        {
            is_eof = true;
            return false;
        }

        locus.init(current_bucket_index);

        if (current_bucket_index == 0)
        {
            in.readStrict(reinterpret_cast<char *>(&value_l), 1);
            ++read_count;
        }
        else
            value_l = value_r;

        if (locus.index_l != locus.index_r)
        {
            if (read_count == BITSET_SIZE)
                fits_in_byte = true;
            else
            {
                fits_in_byte = false;
                in.readStrict(reinterpret_cast<char *>(&value_r), 1);
                ++read_count;
            }
        }
        else
        {
            fits_in_byte = true;
            value_r = value_l;
        }

        ++current_bucket_index;

        return true;
    }

    /** Return the current cell number and the corresponding content.
      */
    inline std::pair<BucketIndex, UInt8> get() const
    {
        if ((current_bucket_index == 0) || is_eof)
            throw Exception("No available data.", ErrorCodes::NO_AVAILABLE_DATA);

        if (fits_in_byte)
            return std::make_pair(current_bucket_index - 1, locus.read(value_l));
        else
            return std::make_pair(current_bucket_index - 1, locus.read(value_l, value_r));
    }

private:
    ReadBuffer & in;
    /// The physical location of the current cell.
    Locus locus;
    /// The current position in the file as a cell number.
    BucketIndex current_bucket_index = 0;
    /// The number of bytes read.
    size_t read_count = 0;
    /// The content in the current position.
    UInt8 value_l = 0;
    UInt8 value_r = 0;
    ///
    bool is_eof = false;
    /// Does the cell fully fit into one byte?
    bool fits_in_byte = false;
};

/** TODO This code looks very suboptimal.
  *
  * The `Locus` structure contains the necessary information to find for each cell
  * the corresponding byte and offset, in bits, from the beginning of the cell. Since in general
  * case the size of one byte is not divisible by the size of one cell, cases possible
  * when one cell overlaps two bytes. Therefore, the `Locus` structure contains two
  * pairs (index, offset).
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class CompactArray<BucketIndex, content_width, bucket_count>::Locus final
{
    friend class CompactArray;
    friend class CompactArray::Reader;

public:
    ALWAYS_INLINE operator UInt8() const /// NOLINT
    {
        if (content_l == content_r)
            return read(*content_l);
        else
            return read(*content_l, *content_r);
    }

    Locus ALWAYS_INLINE & operator=(UInt8 content)
    {
        if ((index_l == index_r) || (index_l == (BITSET_SIZE - 1)))
        {
            /// The cell completely fits into one byte.
            *content_l &= ~(((1 << content_width) - 1) << offset_l);
            *content_l |= content << offset_l;
        }
        else
        {
            /// The cell overlaps two bytes.
            size_t left = 8 - offset_l;

            *content_l &= ~(((1 << left) - 1) << offset_l);
            *content_l |= (content & ((1 << left) - 1)) << offset_l;

            *content_r &= ~((1 << offset_r) - 1);
            *content_r |= content >> left;
        }

        return *this;
    }

private:
    Locus() = default;

    explicit Locus(BucketIndex bucket_index)
    {
        init(bucket_index);
    }

    void ALWAYS_INLINE init(BucketIndex bucket_index)
    {
        /// offset in bits to the leftmost bit
        size_t l = static_cast<size_t>(bucket_index) * content_width;

        /// offset of byte that contains the leftmost bit
        index_l = l / 8;

        /// offset in bits to the leftmost bit at that byte
        offset_l = l % 8;

        /// offset of byte that contains the rightmost bit
        index_r = (l + content_width - 1) / 8;

        /// offset in bits to the next to the rightmost bit at that byte; or zero if the rightmost bit is the rightmost bit in that byte.
        offset_r = (l + content_width) % 8;

        content_l = nullptr;
        content_r = nullptr;
    }

    UInt8 ALWAYS_INLINE read(UInt8 value_l) const
    {
        /// The cell completely fits into one byte.
        return (value_l >> offset_l) & ((1 << content_width) - 1);
    }

    UInt8 ALWAYS_INLINE read(UInt8 value_l, UInt8 value_r) const
    {
        /// The cell overlaps two bytes.
        return ((value_l >> offset_l) & ((1 << (8 - offset_l)) - 1))
            | ((value_r & ((1 << offset_r) - 1)) << (8 - offset_l));
    }

    size_t index_l;
    size_t offset_l;
    size_t index_r;
    size_t offset_r;

    UInt8 * content_l;
    UInt8 * content_r;

    /// Checks
    static_assert((content_width > 0) && (content_width < 8), "Invalid parameter value");
    static_assert(bucket_count <= (std::numeric_limits<size_t>::max() / content_width), "Invalid parameter value");
};

}

