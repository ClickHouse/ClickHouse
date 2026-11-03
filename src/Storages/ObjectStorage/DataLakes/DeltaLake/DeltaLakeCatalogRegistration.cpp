#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeCatalogRegistration.h>

#if USE_PARQUET && USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Databases/DataLake/Common.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <optional>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void registerDeltaTableInCatalog(
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration_ptr,
    bool if_not_exists,
    const StorageID & table_id)
{
    auto kernel_helper = getKernelHelper(configuration_ptr, object_storage);
    /// Register the full URI (`s3://…`, `file://…`) the kernel reads, not the scheme-less `getRawPath().path`.
    const auto location = kernel_helper->getTableLocation();

    /// Register the exact Delta schema read from the `_delta_log`, for both a fresh create and an attach, so
    /// the catalog stays consistent with the log through a single serializer. A one-off snapshot is read here
    /// because registration runs during CREATE, before any `DeltaLakeMetadataDeltaKernel` (and its cache) exists.
    auto snapshot = std::make_shared<DeltaLake::TableSnapshot>(
        /* version */ std::nullopt, kernel_helper, object_storage, getLogger("DeltaLakeCatalogRegistration"));
    /// Column mapping carries per-field metadata that the raw-schema helper drops, so the registered schema would differ from the `_delta_log`.
    if (!snapshot->getPhysicalNamesMap().empty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Registering a DeltaLake table with column mapping into a catalog is not supported "
            "(its physical-name metadata cannot yet be preserved in the catalog schema)");
    Poco::JSON::Array::Ptr fields = snapshot->getRawDeltaSchemaFields();

    Poco::JSON::Object::Ptr metadata_content = new Poco::JSON::Object;
    metadata_content->set("location", location);
    metadata_content->set("fields", fields);

    const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());

    /// Do not roll back commit 0 on failure: a generic catalog error is ambiguous (a racing server may have already registered our `_delta_log`), so we keep the log and surface the error rather than risk corrupting that entry.
    try
    {
        catalog->createTable(namespace_name, table_name, location, metadata_content);
    }
    catch (...)
    {
        /// For `IF NOT EXISTS`, treat a concurrently-registered entry as a no-op only when it points at the same location; a racing create at a different path is a real conflict (reporting success would orphan our `_delta_log`).
        if (if_not_exists)
        {
            try
            {
                auto strip_trailing_slash = [](std::string s)
                {
                    while (!s.empty() && s.back() == '/')
                        s.pop_back();
                    return s;
                };
                DataLake::TableMetadata existing;
                existing.withLocation();
                if (catalog->tryGetTableMetadata(namespace_name, table_name, existing)
                    && strip_trailing_slash(existing.getLocation()) == strip_trailing_slash(location))
                {
                    LOG_DEBUG(
                        getLogger("DeltaLakeCatalogRegistration"),
                        "Table {}.{} is already registered at the same location; treating IF NOT EXISTS create as a no-op",
                        namespace_name, table_name);
                    return;
                }
            }
            catch (...)
            {
                /// The original create error is the important one; a failed probe must not mask it.
                LOG_DEBUG(
                    getLogger("DeltaLakeCatalogRegistration"),
                    "Could not confirm an existing catalog entry for {}.{} while handling IF NOT EXISTS; surfacing the original create error",
                    namespace_name, table_name);
            }
        }
        throw;
    }
}

}

#endif
