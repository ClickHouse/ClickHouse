#pragma once
#include "config.h"

#if USE_AVRO

#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
namespace Iceberg
{

struct ManifestListFileEntry
{
    ManifestFileIterator manifest_file;
    Int64 added_sequence_number;
};

using ManifestList = std::vector<ManifestListFileEntry>;


using ManifestListsStorage = std::map<String, ManifestList>;
using ManifestListIterator = IteratorWrapper<ManifestList>;

struct IcebergSnapshot
{
    ManifestListIterator manifest_list_iterator;
};
}

#endif
