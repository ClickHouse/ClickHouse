#pragma once
#include <Storages/ObjectStorage/Iterators/ObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB {

// Object info for DeltaLake and Hudi storages, but not for Iceberg (Iceberg has its own object info)
struct ObjectInfoDataLake : public DB::ObjectInfoOneFile
{
    explicit ObjectInfoDataLake(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : ObjectInfoOneFile(std::move(relative_path_), std::move(metadata_))
    {
    }
    std::optional<DataLakeObjectMetadata> & getDataLakeMetadata() override { return data_lake_metadata; }

    const std::optional<DataLakeObjectMetadata> & getDataLakeMetadata() const override { return data_lake_metadata; }

    void setDataLakeMetadata(std::optional<DataLakeObjectMetadata> metadata) override { data_lake_metadata = std::move(metadata); }

    bool hasPositionDeleteTransformer() const override { return false; }

    bool suitableForNumsRowCache() const override { return false; }

private:
    std::optional<DataLakeObjectMetadata> data_lake_metadata;
};



}
