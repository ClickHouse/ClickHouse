#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage.h>
#include <Storages/MaxMindDB/MaxMindDBGeneration.h>
#include <Storages/MaxMindDB/MaxMindDBSettings.h>
#include <Common/MultiVersion.h>

namespace DB
{
class StorageMaxMindDB final : public IStorage, public IKeyValueEntity, public WithContext
{
public:
    StorageMaxMindDB(
        const StorageID & table_id,
        std::unique_ptr<MaxMindDBSource> source_,
        std::unique_ptr<MaxMindDBSettings> settings_,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        const String & comment,
        const ASTPtr & primary_key,
        ContextPtr context_);
    ~StorageMaxMindDB() override;

    String getName() const override { return "MaxMindDB"; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsTruncate() const override { return false; }

    void startup() override;
    void shutdown(bool is_drop) override;
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata, ContextPtr) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum,
        size_t max_block_size,
        size_t num_streams) override;

    Names getPrimaryKey() const override { return {"ip"}; }
    std::shared_ptr<const IKeyValueEntity> getLookupSnapshot() const override;
    Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets) const override;
    Block getSampleBlock(const Names & required_columns) const override;

private:
    void refresh();

    std::unique_ptr<MaxMindDBSource> source;
    std::unique_ptr<MaxMindDBSettings> settings;
    MultiVersion<MaxMindDBGeneration> generation;
    UInt64 refresh_interval_ms;
    BackgroundSchedulePoolTaskHolder refresh_task;
    LoggerPtr log;
};
}
