#pragma once

#include "config.h"

#if USE_AVRO

#include <cstdint>
#include <Common/Exception.h>

namespace Iceberg
{

struct ManifestFileContentImpl;

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,

};

enum class DataFileContent : uint8_t
{
    DATA = 0,
    POSITION_DELETES = 1,
    EQUALITY_DELETES = 2,
};

struct DataFileEntry
{
    String data_file_name;
    ManifestEntryStatus status;
    DataFileContent content;
};


class ManifestFileContent
{
public:
    explicit ManifestFileContent(std::unique_ptr<ManifestFileContentImpl> impl_);

    const std::vector<DataFileEntry> & getDataFiles() const;
    Int32 getSchemaId() const;

private:
    std::unique_ptr<ManifestFileContentImpl> impl;
};


using ManifestFilesByName = std::map<String, ManifestFileContent>;

struct ManifestFileEntry
{
    explicit ManifestFileEntry(const ManifestFilesByName::const_iterator & reference_) : reference(reference_) { }
    const ManifestFileContent & getContent() const { return reference->second; }
    const String & getName() const { return reference->first; }


private:
    ManifestFilesByName::const_iterator reference;
};

}

#endif
