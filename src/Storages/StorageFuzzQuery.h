#pragma once

#include <Storages/IStorage.h>
#include <Storages/StorageConfiguration.h>
#include <Common/randomSeed.h>
#include <Common/QueryFuzzer.h>

#include "config.h"

namespace DB
{

class NamedCollection;

class StorageFuzzQuery final : public IStorage
{
public:
    struct Configuration : public StatelessTableEngineConfiguration
    {
        String query;
        UInt64 max_query_length = 500;
        UInt64 random_seed = randomSeed();
    };

    StorageFuzzQuery(
        const StorageID & table_id_, const ColumnsDescription & columns_, const String & comment_, const Configuration & config_);

    std::string getName() const override { return "FuzzQuery"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static StorageFuzzQuery::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);

private:
    const Configuration config;
};


class FuzzQuerySource : public ISource
{
public:
    FuzzQuerySource(
        UInt64 block_size_, Block block_header_, const StorageFuzzQuery::Configuration & config_, ASTPtr query_)
        : ISource(block_header_)
        , block_size(block_size_)
        , block_header(std::move(block_header_))
        , config(config_)
        , query(query_)
        , fuzzer(config_.random_seed)
    {
    }

    String getName() const override { return "FuzzQuery"; }

protected:
    Chunk generate() override
    {
        Columns columns;
        columns.reserve(block_header.columns());
        for (const auto & col : block_header)
        {
            chassert(col.type->getTypeId() == TypeIndex::String);
            columns.emplace_back(createColumn());
        }

        return {std::move(columns), block_size};
    }

private:
    ColumnPtr createColumn();

    UInt64 block_size;
    Block block_header;

    StorageFuzzQuery::Configuration config;
    ASTPtr query;

    QueryFuzzer fuzzer;
};

}
