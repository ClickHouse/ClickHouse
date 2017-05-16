#pragma once

#include <DataStreams/copyData.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageView.h>


namespace DB
{


/** Writes data to the specified table, recursively being called from all dependent views.
  * If the view is not materialized, then the data is not written to it, only redirected further.
  */
class PushingToViewsBlockOutputStream : public IBlockOutputStream
{
public:
    PushingToViewsBlockOutputStream(String database, String table, const Context & context_, ASTPtr query_ptr_)
        : context(context_), query_ptr(query_ptr_)
    {
        storage = context.getTable(database, table);

        /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
          * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
          *  but it's clear that here is not the best place for this functionality.
          */
        addTableLock(storage->lockStructure(true));

        Dependencies dependencies = context.getDependencies(database, table);
        for (size_t i = 0; i < dependencies.size(); ++i)
        {
            children.push_back(std::make_shared<PushingToViewsBlockOutputStream>(dependencies[i].first, dependencies[i].second, context, ASTPtr()));
            queries.push_back(dynamic_cast<StorageView &>(*context.getTable(dependencies[i].first, dependencies[i].second)).getInnerQuery());
        }

        if (storage->getName() != "View")
            output = storage->write(query_ptr, context.getSettingsRef());
    }

    void write(const Block & block) override
    {
        for (size_t i = 0; i < children.size(); ++i)
        {
            BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
            InterpreterSelectQuery select(queries[i], context, QueryProcessingStage::Complete, 0, from);
            BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
            copyData(*data, *children[i]);
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
    std::vector<BlockOutputStreamPtr> children;
    std::vector<ASTPtr> queries;
};


}
