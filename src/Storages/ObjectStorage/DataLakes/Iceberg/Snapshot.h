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

// class ManifestList
// {
// public:
//     explicit ManifestList(std::vector<ManifestListFileEntry> manifest_files_) : manifest_files(std::move(manifest_files_)) { }
//     const std::vector<ManifestListFileEntry> & getManifestFiles() const { return manifest_files; }

// private:
//     std::vector<ManifestListFileEntry> manifest_files;
// };

using ManifestListsStorage = std::map<String, ManifestList>;
using ManifestListIterator = IteratorWrapper<ManifestList>;

struct IcebergSnapshot
{
    ManifestListIterator manifest_list_iterator;
};
}

#endif
