#pragma once

#include <Core/Block.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MaxMindDB/MaxMindDBSource.h>

#include <maxminddb.h>

#include <functional>
#include <string_view>

namespace DB
{
/// A generation owns both the mapping and, for remote sources, its immutable cache file.
class MaxMindDBGeneration
{
public:
    static constexpr auto metadata_column_name = "_mmdb_metadata";
    static DataTypePtr getMetadataType();
    static bool isMetadataColumn(std::string_view name);

    explicit MaxMindDBGeneration(std::unique_ptr<MaxMindDBFile> file_);
    ~MaxMindDBGeneration();
    MaxMindDBGeneration(const MaxMindDBGeneration &) = delete;
    MaxMindDBGeneration & operator=(const MaxMindDBGeneration &) = delete;

    ColumnsDescription inferSchema() const;
    void validateSchema(const ColumnsDescription & columns) const;

    const MMDB_s & database() const { return mmdb; }
    const MaxMindDBFile & sourceFile() const { return *file; }
    ColumnPtr getMetadataColumn(const String & name) const;

private:
    void forEachRecord(const std::function<void(MMDB_entry_s)> & visitor) const;

    std::unique_ptr<MaxMindDBFile> file;
    MMDB_s mmdb{};
    ColumnPtr metadata_column;
};

/// Prepared once per stream, with paths pointing at its own stable strings.
class MaxMindDBColumnDecoder
{
public:
    explicit MaxMindDBColumnDecoder(const NamesAndTypesList & columns);
    ~MaxMindDBColumnDecoder();
    void startBlock(size_t rows);
    void append(MMDB_entry_s entry, MutableColumns & columns);
    Block sampleBlock() const;

private:
    struct Column;
    std::vector<std::unique_ptr<Column>> projections;

};

using MaxMindDBGenerationPtr = std::shared_ptr<const MaxMindDBGeneration>;
}
