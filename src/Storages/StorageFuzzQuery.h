#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/StorageConfiguration.h>
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
        /// Without it, every read uses a new seed
        std::optional<UInt64> random_seed;
    };

    StorageFuzzQuery(
        const StorageID & table_id_, const ColumnsDescription & columns_, const String & comment_, const Configuration & config_);

    std::string getName() const override { return "FuzzQuery"; }

    bool supportsTruncate() const override { return false; }

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

    static StorageFuzzQuery::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);

private:
    const Configuration config;
};


class FuzzQuerySource final : public ISource
{
public:
    FuzzQuerySource(
        UInt64 block_size_, SharedHeader block_header_, const StorageFuzzQuery::Configuration & config_, ASTPtr query_, UInt64 seed_)
        : ISource(block_header_)
        , block_size(block_size_)
        , block_header(block_header_)
        , config(config_)
        , query(query_)
        , fuzzer(seed_)
    {
    }

    String getName() const override { return "FuzzQuery"; }

    /// Every call generates a block: only a consumer that stops reading ends the source.
    bool isUnboundedSource() const override { return true; }

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
            columns.emplace_back(createColumn());
        }

        /// `createColumn` may exit early on cancellation and emit fewer rows than
        /// `block_size`; with multiple columns each call observes cancellation
        /// independently and may produce different counts. Take the smallest size
        /// and truncate the rest so the resulting `Chunk` has consistent row counts.
        size_t actual_rows = block_size;
        for (const auto & column : columns)
            actual_rows = std::min(actual_rows, column->size());

        if (actual_rows == 0)
            return {};

        for (auto & column : columns)
            if (column->size() > actual_rows)
                column = column->cut(0, actual_rows);

        return {std::move(columns), actual_rows};
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
