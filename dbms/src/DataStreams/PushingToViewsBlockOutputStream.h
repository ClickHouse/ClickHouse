#pragma once

#include <DataStreams/copyData.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageMaterializedView.h>


namespace DB
{


/** Writes data to the specified table and to all dependent materialized views.
  */
class PushingToViewsBlockOutputStream : public IBlockOutputStream
{
public:
    PushingToViewsBlockOutputStream(String database, String table, const Context & context_, const ASTPtr & query_ptr_)
        : context(context_), query_ptr(query_ptr_)
    {
        storage = context.getTable(database, table);

        /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
          * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
          *  but it's clear that here is not the best place for this functionality.
          */
        addTableLock(storage->lockStructure(true));

        Dependencies dependencies = context.getDependencies(database, table);
        for (const auto & database_table : dependencies)
            views.emplace_back(
                dynamic_cast<const StorageMaterializedView &>(*context.getTable(database_table.first, database_table.second)).getInnerQuery(),
                std::make_shared<PushingToViewsBlockOutputStream>(database_table.first, database_table.second, context, ASTPtr()));

        output = storage->write(query_ptr, context.getSettingsRef());
    }

    void write(const Block & block) override
    {
        for (auto & view : views)
        {
            BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
            InterpreterSelectQuery select(view.first, context, QueryProcessingStage::Complete, 0, from);
            BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
            copyData(*data, *view.second);
        }

        if (output)
            output->write(block);
    }

    void flush() override
    {
        if (output)
            output->flush();
    }

    void writePrefix() override
    {
        if (output)
            output->writePrefix();
    }

    void writeSuffix() override
    {
        if (output)
            output->writeSuffix();
    }

private:
    StoragePtr storage;
    BlockOutputStreamPtr output;
    Context context;
    ASTPtr query_ptr;
    std::vector<std::pair<ASTPtr, BlockOutputStreamPtr>> views;
};


}
