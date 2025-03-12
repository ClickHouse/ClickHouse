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
    IcebergSnapshot(ManifestListIterator manifest_list_iterator_, Int64 snapshot_id_)
        : manifest_list_iterator(manifest_list_iterator_)
        , snapshot_id(snapshot_id_)
    {
    }

    ManifestListIterator getManifestListIterator() const { return manifest_list_iterator; }
    Int64 getSnapshotId() const { return snapshot_id; }

    ManifestListIterator manifest_list_iterator;
    Int64 snapshot_id;
};
}

#endif
