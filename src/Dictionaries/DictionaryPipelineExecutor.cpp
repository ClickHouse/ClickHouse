#include <Dictionaries/DictionaryPipelineExecutor.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

DictionaryPipelineExecutor::DictionaryPipelineExecutor(QueryPipeline & pipeline_, bool async)
    : async_executor(async ? std::make_unique<PullingAsyncPipelineExecutor>(pipeline_) : nullptr)
    , executor(async ? nullptr : std::make_unique<PullingPipelineExecutor>(pipeline_))
{
}

bool DictionaryPipelineExecutor::pull(Block & block)
{
    bool has_data = pullImpl(block);

    /// A source reading `MergeTree` parts with automatic `LowCardinality` serialization produces
    /// non-native `ColumnLowCardinality` columns for `String` and `FixedString` attributes. Dictionaries
    /// assume that a column matches its data type (e.g. they append blocks with `insertRangeFrom`), so the
    /// encoding is materialized here, at the common boundary of every dictionary load, instead of in
    /// each dictionary. Other special representations (e.g. `Sparse`) are left to the consumers.
    if (has_data)
        for (auto & column : block)
            column.column = recursiveRemoveNonNativeLowCardinality(column.column);

    return has_data;
}

bool DictionaryPipelineExecutor::pullImpl(Block & block)
{
    if (async_executor)
    {
        while (true)
        {
            bool has_data = async_executor->pull(block);
            if (has_data && block.empty())
                continue;
            return has_data;
        }
    }
    else if (executor)
        return executor->pull(block);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DictionaryPipelineExecutor is not initialized");
}

DictionaryPipelineExecutor::~DictionaryPipelineExecutor() = default;

}
