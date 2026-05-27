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

    /// Create `EngineBuilder` which allows working with the
    /// `delta-kernel-rs` FFI API and performs all interactions
    /// with the object storage layer.
    ///
    /// Allocates the builder via `ffi::get_engine_builder` and
    /// invokes `configureBuilder` to set storage-specific options.
    /// If `configureBuilder` throws, the builder is cleaned up
    /// (consumed via `ffi::builder_build` plus `ffi::free_engine`,
    /// because the Rust FFI does not expose a `free_engine_builder`)
    /// before the exception is rethrown.
    ffi::EngineBuilder * createBuilder() const;

protected:
    /// Override to set storage-specific options on the builder.
    /// Default implementation sets no options.
    virtual void configureBuilder(ffi::EngineBuilder * builder) const { (void)builder; }
};

using KernelHelperPtr = std::shared_ptr<IKernelHelper>;

}

namespace DB
{

/// Create an instance of IKernelHelper from passed ConfigurationPtr.
/// Depending on the type of the passed StorageObjectStorage::IConfiguration object,
/// it would create S3KernelHelper, AzureKernelHelper, etc.
DeltaLake::KernelHelperPtr getKernelHelper(
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage);

}

#endif
