#pragma once

#include <Core/Types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>

namespace DB
{
struct MaxMindDBSettings;
class TemporaryFileOnDisk;
class StorageObjectStorageConfiguration;
struct StorageID;

struct MaxMindDBFile
{
    String path;
    String version;
    std::unique_ptr<TemporaryFileOnDisk> cache_file;

    MaxMindDBFile();
    ~MaxMindDBFile();
};

/// Source configuration is accessed only during construction and by the refresh task.
class MaxMindDBSource : public WithContext
{
public:
    MaxMindDBSource(ASTs & arguments, ContextPtr context_, const StorageID & table_id, bool check_access);
    ~MaxMindDBSource();

    /// A null result means the source version is unchanged.
    std::unique_ptr<MaxMindDBFile> load(const String & current_version, const MaxMindDBSettings & settings) const;
    void checkVersion(const MaxMindDBFile & file) const;

private:
    ReadSettings read_settings;
    String local_path;
    std::shared_ptr<StorageObjectStorageConfiguration> configuration;
    ObjectStoragePtr object_storage;
};
}
