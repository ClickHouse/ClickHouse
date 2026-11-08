#pragma once

#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DeltaLake
{

/// A data file of a Delta Lake table with the size that its action in the log records.
struct DataObjectInfo : public DB::ObjectInfo
{
    DataObjectInfo(std::string path_, int64_t size_)
        : DB::ObjectInfo(DB::RelativePathWithMetadata(std::move(path_)))
        , size(size_)
    {
    }

    std::shared_ptr<DB::ObjectInfo> clone() const override { return std::make_shared<DataObjectInfo>(*this); }

    std::optional<size_t> getFileSizeHint() const override
    {
        if (size > 0)
            return static_cast<size_t>(size);
        return std::nullopt;
    }

    const int64_t size;
};

}
