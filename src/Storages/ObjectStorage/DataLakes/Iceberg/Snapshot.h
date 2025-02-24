#pragma once
#include "config.h"

#if USE_AVRO

#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
namespace Iceberg
{

class ManifestList
{
public:
    explicit ManifestList(std::vector<ManifestFileEntry> manifest_files_) : manifest_files(std::move(manifest_files_)) { }
    const std::vector<ManifestFileEntry> & getManifestFiles() const { return manifest_files; }

private:
    std::vector<ManifestFileEntry> manifest_files;
};

using ManifestListsByName = std::map<String, ManifestList>;

class IcebergSnapshot
{
public:
    explicit IcebergSnapshot(const ManifestListsByName::const_iterator & reference_, int64_t snapshot_id_)
        : reference(reference_)
        , snapshot_id(snapshot_id_)
    {
    }

    const ManifestList & getManifestList() const { return reference->second; }
    const String & getName() const { return reference->first; }


private:
    ManifestListsByName::const_iterator reference;
    int64_t snapshot_id;
};

}

#endif
