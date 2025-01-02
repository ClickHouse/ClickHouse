#pragma once
#include "config.h"

#if USE_AVRO
#    include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
namespace Iceberg
{

class ManifestList
{
public:
    explicit ManifestList(Int64 snapshot_id_, std::vector<ManifestFileEntry> manifest_files_)
        : manifest_files(std::move(manifest_files_)), snapshot_id(snapshot_id_)
    {
    }
    const std::vector<ManifestFileEntry> & getManifestFiles() const { return manifest_files; }
    Int64 getShapshotId() const { return snapshot_id; }

private:
    std::vector<ManifestFileEntry> manifest_files;
    Int64 snapshot_id;
};

using ManifestListsByName = std::map<String, ManifestList>;

class IcebergSnapshot
{
public:
    explicit IcebergSnapshot(const ManifestListsByName::const_iterator & reference_) : reference(reference_) { }

    const ManifestList & getManifestList() const { return reference->second; }
    const String & getName() const { return reference->first; }


private:
    ManifestListsByName::const_iterator reference;
};

}

#endif
