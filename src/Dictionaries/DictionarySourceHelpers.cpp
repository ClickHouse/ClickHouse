#include "DictionarySourceHelpers.h"
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
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
/// For simple key
void formatIDs(BlockOutputStreamPtr & out, const std::vector<UInt64> & ids)
{
    auto column = ColumnUInt64::create(ids.size());
    memcpy(column->getData().data(), ids.data(), ids.size() * sizeof(ids.front()));

    Block block{{std::move(column), std::make_shared<DataTypeUInt64>(), "id"}};

    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
}

/// For composite key
void formatKeys(
    const DictionaryStructure & dict_struct,
    BlockOutputStreamPtr & out,
    const Columns & key_columns,
    const std::vector<size_t> & requested_rows)
{
    Block block;
    for (size_t i = 0, size = key_columns.size(); i < size; ++i)
    {
        const ColumnPtr & source_column = key_columns[i];
        auto filtered_column = source_column->cloneEmpty();
        filtered_column->reserve(requested_rows.size());

        for (size_t idx : requested_rows)
            filtered_column->insertFrom(*source_column, idx);

        block.insert({std::move(filtered_column), (*dict_struct.key)[i].type, toString(i)});
    }

    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
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
