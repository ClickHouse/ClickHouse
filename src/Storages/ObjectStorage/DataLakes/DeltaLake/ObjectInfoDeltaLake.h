
#include <Storages/ObjectStorage/Iterators/ObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB {

struct ObjectInfoDeltaLake : public DB::ObjectInfoPlain
{
    std::optional<DataLakeObjectMetadata> data_lake_metadata;
    explicit ObjectInfoDeltaLake(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : ObjectInfoPlain(std::move(relative_path_), std::move(metadata_))
    {
    }
    std::optional<DataLakeObjectMetadata> getDataLakeMetadata() const override
    {
        return data_lake_metadata;
    }

};

}
