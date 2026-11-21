#include <Storages/ObjectStorage/DataLakes/DeletionVectorBitmap.h>

#include <base/unit.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int TOO_LARGE_ARRAY_SIZE;
}

void DeletionVectorBitmap::toLarge()
{
    bitmap.reset(roaring::api::roaring64_bitmap_create());
    bulk_context = {};
    for (const auto & x : small)
        roaring::api::roaring64_bitmap_add_bulk(bitmap.get(), &bulk_context, x.getValue());
    small.clear();
}

void DeletionVectorBitmap::add(UInt64 value)
{
    if (bitmap)
    {
        roaring::api::roaring64_bitmap_add_bulk(bitmap.get(), &bulk_context, value);
        return;
    }

    if (small.find(value) != small.end())
        return;

    if (!small.full())
    {
        small.insert(value);
        return;
    }

    toLarge();
    roaring::api::roaring64_bitmap_add_bulk(bitmap.get(), &bulk_context, value);
}

UInt64 DeletionVectorBitmap::size() const
{
    if (!bitmap)
        return small.size();
    return roaring::api::roaring64_bitmap_get_cardinality(bitmap.get());
}

bool DeletionVectorBitmap::contains(UInt64 value) const
{
    if (!bitmap)
        return small.find(value) != small.end();
    return roaring::api::roaring64_bitmap_contains(bitmap.get(), value);
}

void DeletionVectorBitmap::write(WriteBuffer & out) const
{
    const UInt8 kind = bitmap ? BitmapKind : SmallKind;
    writeBinary(kind, out);

    if (!bitmap)
    {
        small.write(out);
        return;
    }

    /// `runOptimize` rewrites the containers in place, so it runs on a copy: the same deletion
    /// vector is shared by the tasks handed out to the workers and must not be mutated here.
    BitmapPtr optimized(roaring::api::roaring64_bitmap_copy(bitmap.get()));
    roaring::api::roaring64_bitmap_run_optimize(optimized.get());

    const size_t size = roaring::api::roaring64_bitmap_portable_size_in_bytes(optimized.get());
    writeVarUInt(size, out);

    /// TODO: this is unnecessary copying - it will be better to serialize directly into `out`.
    std::unique_ptr<char[]> buf(new char[size]);
    roaring::api::roaring64_bitmap_portable_serialize(optimized.get(), buf.get());
    out.write(buf.get(), size);
}

void DeletionVectorBitmap::read(ReadBuffer & in)
{
    UInt8 kind = 0;
    readBinary(kind, in);

    if (kind == SmallKind)
    {
        bitmap.reset();
        bulk_context = {};
        small.read(in);
        return;
    }

    if (kind != BitmapKind)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown type of roaring bitmap");

    size_t size = 0;
    readVarUInt(size, in);

    static constexpr size_t max_size = 100_GiB;

    if (size == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect size (0) in deletion vector.");
    if (size > max_size)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in deletion vector (maximum: {})", max_size);

    /// TODO: this is unnecessary copying - it will be better to read and deserialize in one pass.
    std::unique_ptr<char[]> buf(new char[size]);
    in.readStrict(buf.get(), size);

    BitmapPtr deserialized(roaring::api::roaring64_bitmap_portable_deserialize_safe(buf.get(), size));
    if (!deserialized)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize the deletion vector");

    /// Deserialization only checks that it stays inside the buffer. Until the structure itself is
    /// validated, croaring does not promise that using the bitmap is free of memory corruption.
    const char * reason = nullptr;
    if (!roaring::api::roaring64_bitmap_internal_validate(deserialized.get(), &reason))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Deletion vector is malformed: {}", reason ? reason : "unknown reason");

    small.clear();
    bitmap = std::move(deserialized);
    bulk_context = {};
}

}
