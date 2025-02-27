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

template <typename CreationFunction>
using ManifestListIterator = IteratorWrapper<ManifestList>;

struct IcebergSnapshot
{
public:
    explicit IcebergSnapshot(const ManifestListIterator & manifest_list_iterator_, Int64 snapshot_id_, Int32 schema_id_)
        : manifest_list_iterator(manifest_list_iterator_)
        , snapshot_id(snapshot_id_)
        , schema_id(schema_id_)
    {
    }

    const ManifestList & getManifestList() const { return *manifest_list_iterator; }
    const String & getManifestListName() const { return manifest_list_iterator.getName(); }
    Int64 getSnapshotId() const { return snapshot_id; }
    Int32 getSchemaId() const { return schema_id; }

private:
    ManifestListIterator manifest_list_iterator;
    Int64 snapshot_id;
    Int32 schema_id;
};
}

#endif
