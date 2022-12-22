#pragma once

#include <base/types.h>

#include <Storages/MergeTree/IDataPartStorage.h>

#include <roaring.hh>

namespace DB
{
class DeleteBitmap
{
public:
    using RoaringBitmap = roaring::Roaring;

    DeleteBitmap();
    explicit DeleteBitmap(UInt64 version_);
    DeleteBitmap(const DeleteBitmap & rhs);
    DeleteBitmap(UInt64 version_, const std::vector<UInt32> & dels);

    void addDels(const std::vector<UInt32> & dels);
    std::shared_ptr<DeleteBitmap> addDelsAsNewVersion(UInt64 version_, const std::vector<UInt32> & dels);

    void setVersion(UInt64 version_) { version = version_; }

    bool deleted(UInt32 row) { return data && data->contains(row); }

    UInt32 rangeCardinality(size_t range_start, size_t range_end);

    size_t cardinality() const;

    void serialize(MutableDataPartStoragePtr data_part_storage) const;
    void deserialize(MutableDataPartStoragePtr data_part_storage);

    DeleteBitmap & operator=(const DeleteBitmap & rhs);

private:
    UInt64 version = 0;
    std::shared_ptr<RoaringBitmap> data = nullptr;
};
using DeleteBitmapPtr = std::shared_ptr<DeleteBitmap>;
}
