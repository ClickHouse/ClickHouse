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

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

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

ContextMutablePtr copyContextAndApplySettings(
    const std::string & config_prefix,
    ContextPtr context,
    const Poco::Util::AbstractConfiguration & config)
{
    auto local_context = Context::createCopy(context);
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

        local_context->applySettingsChanges(changes);
    }
    return local_context;
}


BlockInputStreamWithAdditionalColumns::BlockInputStreamWithAdditionalColumns(
    Block block_to_add_, std::unique_ptr<IBlockInputStream> && stream_)
    : block_to_add(std::move(block_to_add_))
    , stream(std::move(stream_))
{
}

Block BlockInputStreamWithAdditionalColumns::getHeader() const
{
    auto header = stream->getHeader();

    if (header)
    {
        for (Int64 i = static_cast<Int64>(block_to_add.columns() - 1); i >= 0; --i)
            header.insert(0, block_to_add.getByPosition(i).cloneEmpty());
    }

    return header;
}

Block BlockInputStreamWithAdditionalColumns::readImpl()
{
    auto block = stream->read();

    if (block)
    {
        auto block_rows = block.rows();

        auto cut_block = block_to_add.cloneWithCutColumns(current_range_index, block_rows);

        if (cut_block.rows() != block_rows)
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Number of rows in block to add after cut must equal to number of rows in block from inner stream");

        for (Int64 i = static_cast<Int64>(cut_block.columns() - 1); i >= 0; --i)
            block.insert(0, cut_block.getByPosition(i));

        current_range_index += block_rows;
    }

    return block;
}

void BlockInputStreamWithAdditionalColumns::readPrefix()
{
    stream->readPrefix();
}

void BlockInputStreamWithAdditionalColumns::readSuffix()
{
    stream->readSuffix();
}

String BlockInputStreamWithAdditionalColumns::getName() const
{
    return "BlockInputStreamWithAdditionalColumns";
}
}
