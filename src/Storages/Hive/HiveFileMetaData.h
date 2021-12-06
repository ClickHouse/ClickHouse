#pragma once
#include <IO/RemoteFileMetaDataBase.h>
namespace DB
{
class HiveFileMetaData : public RemoteFileMetaDataBase
{
public:
    HiveFileMetaData() = default;
    HiveFileMetaData(const String & schema_,
            const String & cluster_,
            const String & remote_path_,
            size_t file_size_,
            UInt64 last_modification_timestamp_):
        RemoteFileMetaDataBase(schema_, cluster_, remote_path_, file_size_, last_modification_timestamp_){}
    ~HiveFileMetaData() override;

    String getName() const override { return "HiveFileMetaData"; }

    String toString() const override;
    bool fromString(const String &buf) override;
    String getVersion() const override;
};
}
