#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>

namespace DB
{

class TablesBlockSource : public ISource
{
public:
    TablesBlockSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr tables_,
        ContextPtr context_)
        : ISource(std::move(header))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
    {
        size_t size = tables_->size();
        tables.reserve(size);
        for (size_t idx = 0; idx < size; ++idx)
            tables.insert(tables_->getDataAt(idx).toString());
    }

    String getName() const override { return "Tables"; }

protected:
    Chunk generate() override;

private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    NameSet tables;
    size_t database_idx = 0;
    DatabaseTablesIteratorPtr tables_it;
    ContextPtr context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;
};

class DetachedTablesBlockSource : public ISource
{
public:
    DetachedTablesBlockSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr detached_tables_,
        ContextPtr context_)
        : ISource(std::move(header))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
    {
        size_t size = detached_tables_->size();
        detached_tables.reserve(size);
        for (size_t idx = 0; idx < size; ++idx)
        {
            detached_tables.insert(detached_tables_->getDataAt(idx).toString());
        }
    }

    String getName() const override { return "DetachedTables"; }

protected:
    Chunk generate() override;

private:
    const std::vector<UInt8> columns_mask;
    const UInt64 max_block_size;
    const ColumnPtr databases;
    NameSet detached_tables;
    DatabaseDetachedTablesSnapshotIteratorPtr detached_tables_it;
    ContextPtr context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;

    void fillResultColumnsByDetachedTableIterator(MutableColumns & result_columns) const;
};
}
