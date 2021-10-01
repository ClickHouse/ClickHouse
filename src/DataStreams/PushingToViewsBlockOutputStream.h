#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Common/Stopwatch.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>

namespace Poco
{
class Logger;
};

namespace DB
{

class ReplicatedMergeTreeBlockOutputStream;
class Context;


/** Writes data to the specified table and to all dependent materialized views.
  */
class PushingToViewsBlockOutputStream : public IBlockOutputStream
{
public:
    PushingToViewsBlockOutputStream(
        const StoragePtr & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Context & context_,
        const ASTPtr & query_ptr_,
        bool no_destination = false);

    Block getHeader() const override;
    void write(const Block & block) override;

    void flush() override;
    void writePrefix() override;
    void writeSuffix() override;

private:
    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;
    BlockOutputStreamPtr output;
    ReplicatedMergeTreeBlockOutputStream * replicated_output = nullptr;
    Poco::Logger * log;

    const Context & context;
    ASTPtr query_ptr;
    Stopwatch main_watch;

    struct ViewInfo
    {
        ASTPtr query;
        StorageID table_id;
        BlockOutputStreamPtr out;
        std::exception_ptr exception;
        UInt64 elapsed_ms = 0;
    };

    std::vector<ViewInfo> views;
    std::unique_ptr<Context> select_context;
    std::unique_ptr<Context> insert_context;

    void process(const Block & block, ViewInfo & view);
};


}
