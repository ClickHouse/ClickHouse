#include "DictionarySourceHelpers.h"
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include "DictionaryStructure.h"
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

/// For simple key

Block blockForIds(
    const DictionaryStructure & dict_struct,
    const std::vector<UInt64> & ids)
{
    auto column = ColumnUInt64::create(ids.size());
    memcpy(column->getData().data(), ids.data(), ids.size() * sizeof(ids.front()));

    Block block{{std::move(column), std::make_shared<DataTypeUInt64>(), (*dict_struct.id).name}};

    return block;
}

/// For composite key

Block blockForKeys(
    const DictionaryStructure & dict_struct,
    const Columns & key_columns,
    const std::vector<size_t> & requested_rows)
{
    Block block;

    for (size_t i = 0, size = key_columns.size(); i < size; ++i)
    {
        const ColumnPtr & source_column = key_columns[i];
        size_t column_rows_size = source_column->size();

        PaddedPODArray<UInt8> filter(column_rows_size, false);

        for (size_t idx : requested_rows)
            filter[idx] = true;

        auto filtered_column = source_column->filter(filter, requested_rows.size());

        block.insert({filtered_column, (*dict_struct.key)[i].type, (*dict_struct.key)[i].name});
    }

    return block;
}


SettingsChanges readSettingsFromDictionaryConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".settings"))
        return {};

    const auto prefix = config_prefix + ".settings";

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(prefix, config_keys);

    SettingsChanges changes;

    for (const std::string & key : config_keys)
    {
        const auto value = config.getString(prefix + "." + key);
        changes.emplace_back(key, value);
    }

    return changes;
}


ContextMutablePtr copyContextAndApplySettingsFromDictionaryConfig(
    const ContextPtr & context, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    auto context_copy = Context::createCopy(context);
    auto changes = readSettingsFromDictionaryConfig(config, config_prefix);
    context_copy->applySettingsChanges(changes);
    return context_copy;
}

static Block transformHeader(Block header, Block block_to_add)
{
    for (Int64 i = static_cast<Int64>(block_to_add.columns() - 1); i >= 0; --i)
        header.insert(0, block_to_add.getByPosition(i).cloneEmpty());

    return header;
}

TransformWithAdditionalColumns::TransformWithAdditionalColumns(
    Block block_to_add_, const Block & header)
    : ISimpleTransform(header, transformHeader(header, block_to_add_), true)
    , block_to_add(std::move(block_to_add_))
{
}

void TransformWithAdditionalColumns::transform(Chunk & chunk)
{
    if (chunk)
    {
        auto num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        auto cut_block = block_to_add.cloneWithCutColumns(current_range_index, num_rows);

        if (cut_block.rows() != num_rows)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Number of rows in block to add after cut must equal to number of rows in block from inner stream");

        for (Int64 i = static_cast<Int64>(cut_block.columns() - 1); i >= 0; --i)
            columns.insert(columns.begin(), cut_block.getByPosition(i).column);

        current_range_index += num_rows;
        chunk.setColumns(std::move(columns), num_rows);
    }
}

String TransformWithAdditionalColumns::getName() const
{
    return "TransformWithAdditionalColumns";
}

DictionaryPipelineExecutor::DictionaryPipelineExecutor(QueryPipeline & pipeline_, bool async)
    : async_executor(async ? std::make_unique<PullingAsyncPipelineExecutor>(pipeline_) : nullptr)
    , executor(async ? nullptr : std::make_unique<PullingPipelineExecutor>(pipeline_))
{}

bool DictionaryPipelineExecutor::pull(Block & block)
{
    if (async_executor)
    {
        while (true)
        {
            bool has_data = async_executor->pull(block);
            if (has_data && !block)
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
