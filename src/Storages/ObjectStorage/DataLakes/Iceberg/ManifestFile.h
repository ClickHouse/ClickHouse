#pragma once

#include "config.h"

#include <cstdint>
#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IteratorWrapper.h>

namespace Iceberg
{

struct ManifestFileContentImpl;

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,
};

enum class FileContentType : uint8_t
{
    DATA = 0,
    POSITION_DELETES = 1,
    EQUALITY_DELETES = 2,
};

struct DataFileEntry
{
    String file_name;
};

using FileEntry = std::variant<DataFileEntry>; // In the future we will add PositionalDeleteFileEntry and EqualityDeleteFileEntry here

struct ManifestFileEntry
{
    ManifestEntryStatus status;
    Int64 added_sequence_number;
    std::unordered_map<Int32, DB::Range> partition_ranges;

    FileEntry file;

    std::vector<DB::Range> getPartitionRanges(const std::vector<Int32> & partition_columns_ids) const;
};

struct PartitionColumnInfo
{
    PartitionTransform transform;
    Int32 source_id;
};


class ManifestFileContent
{
public:
    explicit ManifestFileContent(std::unique_ptr<ManifestFileContentImpl> impl_);

    const std::vector<ManifestFileEntry> & getFiles() const;
    Int32 getSchemaId() const;
    const std::vector<PartitionColumnInfo> & getPartitionColumnInfos() const;
    Int32 getPartitionSpecId() const;


private:
    std::unique_ptr<ManifestFileContentImpl> impl;
};


using ManifestFilesStorage = std::map<String, ManifestFileContent>;
using ManifestFileIterator = IteratorWrapper<ManifestFileContent>;
}

#endif
