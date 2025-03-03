#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

class Context;


/** IStorageSystemOneBlock is base class for system tables whose all columns can be synchronously fetched.
  *
  * Client class need to provide columns_description.
  * IStorageSystemOneBlock during read will create result columns in same order as in columns_description
  * and pass it with fillData method.
  *
  * Client also must override fillData and fill result columns.
  *
  * If subclass want to support virtual columns, it should override getVirtuals method of IStorage interface.
  * IStorageSystemOneBlock will add virtuals columns at the end of result columns of fillData method.
  */
class IStorageSystemOneBlock : public IStorage
{
protected:
    /// If this method uses `predicate`, getFilterSampleBlock() must list all columns to which
    /// it's applied. (Otherwise there'll be a LOGICAL_ERROR "Not-ready Set is passed" on subqueries.)
    virtual void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const = 0;

    /// Columns to which fillData() applies the `predicate`.
    virtual Block getFilterSampleBlock() const
    {
        return {};
    }

    virtual bool supportsColumnsMask() const { return false; }

    friend class ReadFromSystemOneBlock;

public:
    explicit IStorageSystemOneBlock(const StorageID & table_id_, ColumnsDescription columns_description) : IStorage(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(std::move(columns_description));
        setInMemoryMetadata(storage_metadata);
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }

    static NamesAndAliases getNamesAndAliases() { return {}; }
};

class ReadFromSystemOneBlock : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemOneBlock"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemOneBlock(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<IStorageSystemOneBlock> storage_,
        std::vector<UInt8> columns_mask_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , columns_mask(std::move(columns_mask_))
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<IStorageSystemOneBlock> storage;
    std::vector<UInt8> columns_mask;
    std::optional<ActionsDAG> filter;
};

}
