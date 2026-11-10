#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <string>
#include <utility>
#include <vector>

namespace ffi
{
struct EngineBuilder;
}

#if USE_AZURE_BLOB_STORAGE
namespace DB::AzureBlobStorage
{
struct ConnectionParams;
}
#endif

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

    /// Hash of current credentials; override for providers with rotating sessions.
    virtual DB::UInt128 getCredentialsFingerprint() const { return {}; }

    /// Invokes the underlying ObjectStorage's catalog-vended credentials refresh callback
    /// (Glue / Unity / REST). Returns true if a refresh happened. Used by the kernel's
    /// `ExpiredToken` recovery path — the kernel's Rust object_store can't refresh on its
    /// own, and vended creds are static in the C++ client until this callback fires.
    virtual bool refreshCredentials() { return false; }
};

using KernelHelperPtr = std::shared_ptr<IKernelHelper>;

#if USE_AZURE_BLOB_STORAGE
/// Computes the ordered list of delta-kernel-rs object_store builder options
/// (the name/value pairs later passed to `ffi::set_builder_option`) for the given
/// Azure connection params. Extracted from `AzureKernelHelper::createBuilder` so that
/// the option-selection logic - in particular, that `azure_storage_account_name` is
/// always emitted, including on the vended-credentials / SAS path used by Unity catalog -
/// is unit-testable without the delta-kernel FFI.
std::vector<std::pair<std::string, std::string>> getAzureBuilderOptions(
    const DB::AzureBlobStorage::ConnectionParams & connection_params);
#endif

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
