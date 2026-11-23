#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <base/arithmeticOverflow.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Common/Exception.h>

#include <roaring/roaring.hh>

#include <optional>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

inline UInt64 positionFromKeyAndSubPosition(UInt32 key, UInt32 sub_position)
{
    return (static_cast<UInt64>(key) << 32) | static_cast<UInt64>(sub_position);
}

roaring::Roaring readRoaringPortableSafe(const char * data, size_t size, Int32 key);

template <typename OnPosition>
void deserializeRoaringPositionBitmap(std::string_view bytes, std::optional<UInt64> declared_cardinality, OnPosition && on_position)
{
    if (bytes.size() < sizeof(Int64))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap is too small");

    const char * ptr = bytes.data();
    size_t remaining = bytes.size();

    /// Iceberg deletion-vector roaring layout stores count and keys as little-endian.
    const Int64 bitmap_count = unalignedLoadLittleEndian<Int64>(ptr);
    ptr += sizeof(Int64);
    remaining -= sizeof(Int64);

    if (bitmap_count < 0 || bitmap_count > std::numeric_limits<Int32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector bitmap count: {}", bitmap_count);

    Int32 last_key = -1;
    Int32 remaining_count = static_cast<Int32>(bitmap_count);
    UInt64 running_cardinality = 0;

    while (remaining_count > 0)
    {
        if (remaining < sizeof(Int32))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap is truncated while reading key");

        const Int32 key = unalignedLoadLittleEndian<Int32>(ptr);
        ptr += sizeof(Int32);
        remaining -= sizeof(Int32);

        if (key < 0 || key > DELETION_VECTOR_MAX_KEY)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid deletion vector bitmap key: {}", key);
        if (key <= last_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap keys must be sorted in ascending order");

        auto bitmap = readRoaringPortableSafe(ptr, remaining, key);

        const size_t bitmap_size = bitmap.getSizeInBytes(/*portable=*/true);
        if (bitmap_size > remaining)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector roaring bitmap at key {} exceeds blob size", key);

        const UInt64 bitmap_cardinality = bitmap.cardinality();
        UInt64 new_running_cardinality = 0;
        if (common::addOverflow(running_cardinality, bitmap_cardinality, new_running_cardinality))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector cardinality overflows UInt64");

        if (declared_cardinality.has_value() && new_running_cardinality > *declared_cardinality)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Deletion vector cardinality {} exceeds declared cardinality {}",
                new_running_cardinality,
                *declared_cardinality);

        running_cardinality = new_running_cardinality;

        for (UInt32 sub_position : bitmap)
        {
            const UInt64 position = positionFromKeyAndSubPosition(static_cast<UInt32>(key), sub_position);
            if (position > static_cast<UInt64>(DELETION_VECTOR_MAX_POSITION))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector position {} is out of supported range", position);
            on_position(position);
        }

        ptr += bitmap_size;
        remaining -= bitmap_size;
        last_key = key;
        --remaining_count;
    }

    if (remaining != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deletion vector bitmap has {} trailing bytes", remaining);

    if (declared_cardinality.has_value() && running_cardinality != *declared_cardinality)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Deletion vector cardinality {} does not match deserialized row count {}",
            *declared_cardinality,
            running_cardinality);
}

}
