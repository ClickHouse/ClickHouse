#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/StorageConfiguration.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Common/randomSeed.h>
#include <Common/QueryFuzzer.h>
#include <Processors/ISource.h>

namespace DB
{

class Pipe;
class Chunk;

class NamedCollection;

class StorageFuzzQuery final : public StorageWithCommonVirtualColumns
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

    static VirtualColumnsDescription createVirtuals();

    using StorageWithCommonVirtualColumns::read;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    /// `mode` controls whether the contract validation in `getConfiguration` is enforced.
    /// On `CREATE` / `SECONDARY_CREATE` we reject impossible configurations (e.g.
    /// `max_query_length` smaller than the formatted base query). On `ATTACH` and
    /// stronger we skip those checks so existing tables with previously-tolerated
    /// configurations keep loading after an upgrade — required by the ClickHouse
    /// backward-compatibility rule that new validation must not break existing objects.
    static StorageFuzzQuery::Configuration getConfiguration(
        ASTs & engine_args,
        ContextPtr local_context,
        LoadingStrictnessLevel mode = LoadingStrictnessLevel::CREATE);

private:
    const Configuration config;
};


class FuzzQuerySource : public ISource
{
public:
    FuzzQuerySource(
        UInt64 block_size_, SharedHeader block_header_, const StorageFuzzQuery::Configuration & config_, ASTPtr query_)
        : ISource(block_header_)
        , block_size(block_size_)
        , block_header(block_header_)
        , config(config_)
        , query(query_)
        , fuzzer(config_.random_seed)
    {
    }

    String getName() const override { return "FuzzQuery"; }

protected:
    Chunk generate() override
    {
        if (isCancelled())
            return {};

        Columns columns;
        columns.reserve(block_header->columns());
        for (const auto & col : *block_header)
        {
            chassert(col.type->getTypeId() == TypeIndex::String);
            auto column = createColumn();

            /// `createColumn` returns a partial column on cancellation. Discard the
            /// partial chunk by returning an empty one — the pipeline treats this as
            /// end-of-stream and stops pulling. This avoids `Chunk` size validation
            /// failures and gives `KILL QUERY` an immediate response.
            if (column->size() != block_size)
                return {};

            columns.emplace_back(std::move(column));
        }

        return {std::move(columns), block_size};
    }

private:
    ColumnPtr createColumn();

    UInt64 block_size;
    SharedHeader block_header;

    StorageFuzzQuery::Configuration config;
    ASTPtr query;

    QueryFuzzer fuzzer;
};

}
