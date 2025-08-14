#pragma once
#include <Storages/ObjectStorage/Iterators/ObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB {

struct ObjectInfoDataLake : public DB::ObjectInfoOneFile
{
    explicit ObjectInfoDataLake(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : ObjectInfoOneFile(std::move(relative_path_), std::move(metadata_))
    {
    }
    std::optional<DataLakeObjectMetadata>& getDataLakeMetadata() override  {
        return data_lake_metadata;
    }
    void setDataLakeMetadata(std::optional<DataLakeObjectMetadata> metadata) override {
        data_lake_metadata = std::move(metadata);
    }
private:
    std::optional<DataLakeObjectMetadata> data_lake_metadata;
};

struct ObjectInfoDeltaLake : public ObjectInfoDataLake
{
    explicit ObjectInfoDeltaLake(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : ObjectInfoDataLake(std::move(relative_path_), std::move(metadata_))
    {
    }
    bool hasPositionDeleteTransformer() const override { return false; }
private:
    std::optional<DataLakeObjectMetadata> data_lake_metadata;
};

}
