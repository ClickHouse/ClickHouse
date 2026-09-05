#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class Context;


/** Implements storage for the system table One.
  * The table contains a single column of dummy UInt8 and a single row with a value of 0.
  * Used when the table is not specified in the query.
  * Analog of the DUAL table in Oracle and MySQL.
  */
class StorageSystemOne final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemOne(const StorageID & table_id_);

    std::string getName() const override { return "SystemOne"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }

    bool supportsTransactions() const override { return true; }
};

class ReadFromSystemOneStep final : public ISourceStep
{
public:
    ReadFromSystemOneStep(
        const Names & column_names_,
        const StorageSnapshotPtr & storage_snapshot_
    );

    ReadFromSystemOneStep(const ReadFromSystemOneStep &) = default;
    ReadFromSystemOneStep(ReadFromSystemOneStep &&) = default;

    String getName() const override { return "ReadFromSystemOne"; }

    /// Serialized under the already-registered "ReadFromStorage" name rather than under `getName`:
    /// `ReadFromStorageStep` holds a complete `system.one` codec, and its decoder is what reads the
    /// bytes written by `serialize` below. `getName` is deliberately left alone (`EXPLAIN` output and
    /// `optimizeJoin.cpp` match on it).
    /// Note: the wire name is consumed only by `QueryPlan::serialize`. The other users of
    /// `getSerializationName` (hash-table-statistics cache keys, join runtime-filter fingerprints)
    /// dispatch on `SourceStepWithFilter` / `ITransformingStep` / `JoinStepLogical`; this step derives
    /// from `ISourceStep` only, so they cannot see it. Revisit if that ever changes.
    String getSerializationName() const override { return "ReadFromStorage"; }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<ReadFromSystemOneStep>(*this);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
};

}
