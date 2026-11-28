#pragma once

#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/StorageID.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/BlockIO.h>

#include <memory>


namespace DB
{

class PushingPipelineExecutor;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Writes the data into another table by executing a nested `INSERT` query into it, in the context of the outer query.
/// Unlike writing into the sink of that table directly, it applies the defaults and the constraints of that table
/// and pushes the data to its dependent materialized views.
///
/// It is used by the storages that do not store the data themselves but forward the inserts into another table
/// (`Alias`, `Merge`), see `IStorage::insertsThroughNestedQuery`.
class NestedInsertSink final : public SinkToStorage, WithContext
{
public:
    /// `header` is the structure of the incoming data, `columns_to_insert` are its columns written into the target table.
    /// They are converted to the types of the columns with the same names in the target table.
    /// With `explicit_column_list`, they are listed in the nested query, and the target table fills in its other
    /// columns with their defaults. Otherwise, they must be all non-materialized columns of the target table.
    NestedInsertSink(
        SharedHeader header,
        StorageID target_table_id_,
        Names columns_to_insert_,
        bool explicit_column_list_,
        ContextPtr context_,
        bool async_insert_);

    ~NestedInsertSink() override;

    String getName() const override { return "NestedInsertSink"; }

    void onStart() override;
    void consume(Chunk & chunk) override;
    void onFinish() override;
    void onPipelineFinished() override;
    void onException(std::exception_ptr) override;

private:
    const StorageID target_table_id;
    const Names columns_to_insert;
    const bool explicit_column_list;
    const bool async_insert;

    /// The structure of the data pushed into the nested query before the conversion to the types of the target table.
    Block insert_header;
    /// Converts `insert_header` to the header of the nested query, if they differ.
    ExpressionActionsPtr converting_actions;

    BlockIO block_io;
    std::unique_ptr<PushingPipelineExecutor> executor;
    /// Memoizes the deduplication data hashes across the sibling chunks of one source block, so a
    /// row-count-changing view fanned out into many chunks does not re-hash the source per chunk.
    DeduplicationInfo::DataHashCache data_hash_cache;
};

}
