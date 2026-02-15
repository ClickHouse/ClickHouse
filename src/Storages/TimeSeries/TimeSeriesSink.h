#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Logger.h>

#include <memory>
#include <vector>


namespace DB
{

class StorageTimeSeries;
struct TimeSeriesSettings;

/// Sink for inserting data directly into a TimeSeries table via INSERT statements.
/// Accepts blocks with the TimeSeries table's columns (id, timestamp, value, metric_name, tags, etc.)
/// and splits them into appropriate blocks for the inner data and tags tables.
class TimeSeriesSink : public SinkToStorage, private WithContext
{
public:
    TimeSeriesSink(
        StorageTimeSeries & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_);

    String getName() const override { return "TimeSeriesSink"; }

    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    /// Split the incoming block into blocks for the data and tags inner tables.
    void splitBlock(const Block & block);

    /// Insert accumulated blocks into target tables.
    void insertToTargetTables();

    StorageTimeSeries & storage;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<const TimeSeriesSettings> time_series_settings;

    /// Accumulated blocks for each target table.
    std::vector<std::pair<ViewTarget::Kind, Block>> accumulated_blocks;

    LoggerPtr log;
};

}
