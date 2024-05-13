#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include "PartitionColumns.h"

namespace DB
{

template <typename Configuration, typename MetadataReadHelper>
struct DeltaLakeMetadataParser
{
public:
    DeltaLakeMetadataParser<Configuration, MetadataReadHelper>(const Configuration & configuration, ContextPtr context);

    Strings getFiles() { return data_files; }

    NamesAndTypesList getTableSchema() const { return schema; }

    DataLakePartitionColumns getPartitionColumns() const { return partition_columns; }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const { return column_name_to_physical_name; }

private:
    struct Impl;
    std::shared_ptr<Impl> impl;
    NamesAndTypesList schema;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
    Strings data_files;
};

}
