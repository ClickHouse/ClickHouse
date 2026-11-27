#pragma once

#include "config.h"

#if USE_HIVE

#include <Storages/Cache/IRemoteFileMetadata.h>
namespace DB
{

class StorageHiveMetadata : public IRemoteFileMetadata
{
public:
    StorageHiveMetadata() = default;

    explicit StorageHiveMetadata(
        const String & schema_,
        const String & cluster_,
        const String & remote_path_,
        size_t file_size_,
        UInt64 last_modification_timestamp_)
        : schema(schema_), cluster(cluster_)
    {
        remote_path = remote_path_;
        file_size = file_size_;
        last_modification_timestamp = last_modification_timestamp_;
    }

    ~StorageHiveMetadata() override = default;

    [[nodiscard]] String getName() const override { return "StorageHiveMetadata"; }
    [[nodiscard]] String getSchema() const { return schema; }
    [[nodiscard]] String getCluster() const { return cluster; }

    [[nodiscard]] String toString() const override;
    bool fromString(const String & buf) override;
    [[nodiscard]] String getVersion() const override;
private:
    String schema;
    String cluster;
};

}
#endif
