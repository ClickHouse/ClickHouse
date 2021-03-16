#include "DictionarySourceHelpers.h"
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include "DictionaryStructure.h"
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>

namespace DB
{

void formatBlock(BlockOutputStreamPtr & out, const Block & block)
{
    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
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

        block.insert({std::move(filtered_column), (*dict_struct.key)[i].type, (*dict_struct.key)[i].name});
    }

    return block;
}

Context copyContextAndApplySettings(
    const std::string & config_prefix,
    const Context & context,
    const Poco::Util::AbstractConfiguration & config)
{
    Context local_context(context);
    if (config.has(config_prefix + ".settings"))
    {
        const auto prefix = config_prefix + ".settings";

        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(prefix, config_keys);

        SettingsChanges changes;

        for (const std::string & key : config_keys)
        {
            const auto value = config.getString(prefix + "." + key);
            changes.emplace_back(key, value);
        }

        local_context.applySettingsChanges(changes);
    }
    return local_context;
}

}
