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

private:
    struct Impl;
    std::shared_ptr<Impl> impl;
    NamesAndTypesList schema;
    DataLakePartitionColumns partition_columns;
    Strings data_files;
};

}
