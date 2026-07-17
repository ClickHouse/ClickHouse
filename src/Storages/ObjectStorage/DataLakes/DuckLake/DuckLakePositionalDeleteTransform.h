#pragma once

#include "config.h"

#if USE_PARQUET

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Formats/FormatSettings.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeDataObjectInfo.h>

namespace DB
{

/// Applies DuckLake positional deletes to one data file.
/// DuckLake binds each delete file to exactly one data file (ducklake_delete_file.data_file_id),
/// so unlike Iceberg no file_path filtering is needed: every `pos` value in the bound delete
/// files is a row position in this data file. The actual filtering is delegated to the shared
/// DeletionVectorTransform (same as DeltaLake/Iceberg).
class DuckLakePositionalDeleteTransform : public ISimpleTransform
{
public:
    DuckLakePositionalDeleteTransform(
        const SharedHeader & header_,
        ObjectStoragePtr object_storage_,
        DuckLakeDataObjectInfoPtr object_info_,
        const std::optional<FormatSettings> & format_settings_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        ContextPtr context_);

    String getName() const override { return "DuckLakePositionalDeleteTransform"; }

    void transform(Chunk & chunk) override;

private:
    ObjectStoragePtr object_storage;
    DuckLakeDataObjectInfoPtr object_info;
    std::optional<FormatSettings> format_settings;
    FormatParserSharedResourcesPtr parser_shared_resources;
    ContextPtr context;
    LoggerPtr log = getLogger("DuckLakePositionalDeleteTransform");

    DataLakeObjectMetadata::ExcludedRows excluded_rows;

    void initialize();
};

}

#endif
