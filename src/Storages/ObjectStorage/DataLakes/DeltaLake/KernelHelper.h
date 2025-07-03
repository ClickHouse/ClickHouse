#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace ffi
{
struct EngineBuilder;
}

namespace DeltaLake
{

/**
 * A helper class to manage different storage types,
 * their data location, authentication, connection.
 */
class IKernelHelper
{
public:
    virtual ~IKernelHelper() = default;

    /// Returns path to table metadata in object storage with object store location.
    /// Example: "s3://bucket/path/to/table/data".
    virtual const std::string & getTableLocation() const = 0;

    /// Returns only data path.
    /// Example: "path/to/table/data"
    /// (while full location would be "s3://bucket/path/to/table/data")
    virtual const std::string & getDataPath() const = 0;

    /// Create "EngineBuilder" which allows to work with
    /// delta-kernel-rs ffi api and performs all interactions
    /// with object storage layer.
    virtual ffi::EngineBuilder * createBuilder() const = 0;
};

using KernelHelperPtr = std::shared_ptr<IKernelHelper>;

}

namespace DB
{

/// Create an instance of IKernelHelper from passed ConfigurationPtr.
/// Depending on the type of the passed StorageObjectStorage::IConfiguration object,
/// it would create S3KernelHelper, AzureKernelHelper, etc.
DeltaLake::KernelHelperPtr getKernelHelper(
    const StorageObjectStorage::ConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage);

}

#endif
