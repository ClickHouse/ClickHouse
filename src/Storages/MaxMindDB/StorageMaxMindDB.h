#pragma once

#include "config.h"

#if USE_MAXMINDDB
#    include <memory>
#    include <maxminddb.h>
#    include <Core/ColumnsWithTypeAndName.h>
#    include <Interpreters/IKeyValueEntity.h>
#    include <Storages/IStorage.h>
#    include <Common/JSONBuilder.h>

namespace DB
{
class Context;

/// Wrapper for MaxMind DB storage.
class StorageMaxMindDB final : public IStorage, public IKeyValueEntity, WithContext
{
    friend class EmbeddedRocksDBSink;

public:
    StorageMaxMindDB(
        const StorageID & table_id_,
        const StorageInMemoryMetadata & metadata,
        ContextPtr context_,
        const String & primary_key_,
        String mmdb_file_path_);

    ~StorageMaxMindDB() override;

    std::string getName() const override { return "MaxMindDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & node, ContextPtr /*query_context*/, const StorageMetadataPtr & /*metadata_snapshot*/) const override
    {
        return node->getColumnName() == primary_key;
    }

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {mmdb_file_path}; }

    Names getPrimaryKey() const override { return {primary_key}; }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const override;

    Block getSampleBlock(const Names &) const override;

    /// Return chunk with data for given serialized keys.
    /// If out_null_map is passed, fill it with 1/0 depending on key was/wasn't found. Result chunk may contain default values.
    /// If out_null_map is not passed. Not found rows excluded from result chunk.
    Chunk getBySerializedKeys(const std::vector<std::string> & keys, PaddedPODArray<UInt8> * out_null_map) const;

private:
    void checkColumns(const ColumnsDescription & columns) const;
    void initDB();
    bool lookupDB(const std::string & key, std::string & value) const;
    void finalizeDB();

    std::unique_ptr<MMDB_s> mmdb_ptr;
    const String primary_key;
    const String mmdb_file_path;

    const FormatSettings format_settings;
    JSONBuilder::FormatSettings json_format_settings;
};
}

#endif
