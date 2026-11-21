#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Interpreters/ActionsDAG.h>

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
class IStorageSystemOneBlock : public StorageWithCommonVirtualColumns
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

    /// Access checks that must reject the query even when no row is ever read. `fillData` is not the
    /// place for them: it can run lazily, and a consumer that closes the read port before asking for
    /// a chunk (`LIMIT 0`) never enters it.
    virtual void checkAccessRights(ContextPtr /*context*/) const {}

    virtual bool supportsColumnsMask() const { return false; }

    friend class ReadFromSystemOneBlock;

public:
    explicit IStorageSystemOneBlock(const StorageID & table_id_, ColumnsDescription columns_description) : StorageWithCommonVirtualColumns(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(std::move(columns_description));
        storage_metadata.setVirtuals(createVirtuals());
        setInMemoryMetadata(storage_metadata);
    }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
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

}
