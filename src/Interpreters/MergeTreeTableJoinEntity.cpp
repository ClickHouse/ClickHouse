#include <Interpreters/MergeTreeTableJoinEntity.h>

#include <Columns/ColumnsNumber.h>
#include <Common/ArenaAllocator.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{

using KeyToRowsMap = MergeTreeTableJoinEntity::KeyToRowsMap;

KeyToRowsMap buildKeyToRowsMap(const Block & data_block, const std::vector<size_t> & key_positions, size_t rows_count, Arena & pool)
{
    ColumnRawPtrs key_columns;
    key_columns.reserve(key_positions.size());
    for (auto key_position : key_positions)
        key_columns.push_back(data_block.getByPosition(key_position).column.get());

    using Method = ColumnsHashing::HashMethodHashed<typename KeyToRowsMap::value_type, typename KeyToRowsMap::mapped_type, false>;

    KeyToRowsMap result;
    Method method(key_columns, {}, nullptr);
    for (size_t row = 0; row < rows_count; ++row)
    {
        auto emplace_result = method.emplaceKey(result, row, pool);
        emplace_result.getMapped().push_back(row, &pool);
    }

    return result;
}

}

MergeTreeTableJoinEntity::MergeTreeTableJoinEntity(Names primary_key_, Block data_block_)
    : primary_key(std::move(primary_key_))
    , sample_block(data_block_.cloneEmpty())
    , rows_count(data_block_.rows())
{
    Columns columns_with_default;
    columns_with_default.reserve(data_block_.columns());
    for (const auto & column : data_block_.getColumns())
        columns_with_default.push_back(column->cloneResized(rows_count + 1));

    data_block_with_default = data_block_.cloneWithColumns(columns_with_default);

    for (size_t position = 0; position < sample_block.columns(); ++position)
        column_positions.emplace(sample_block.getByPosition(position).name, position);

    key_positions.reserve(primary_key.size());
    for (const auto & key_name : primary_key)
    {
        auto it = column_positions.find(key_name);
        if (it == column_positions.end())
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Cannot find key column '{}' in table lookup cache", key_name);

        key_positions.push_back(it->second);
    }

    key_to_rows = buildKeyToRowsMap(data_block_with_default, key_positions, rows_count, key_to_rows_pool);
}

Names MergeTreeTableJoinEntity::getPrimaryKey() const
{
    return primary_key;
}

Block MergeTreeTableJoinEntity::getSampleBlock(const Names & required_columns) const
{
    if (required_columns.empty())
        return sample_block;

    Block result;
    for (const auto & column_name : required_columns)
    {
        auto it = column_positions.find(column_name);
        if (it == column_positions.end())
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Cannot find column '{}' in table lookup cache", column_name);

        result.insert(sample_block.getByPosition(it->second));
    }

    return result;
}

Chunk MergeTreeTableJoinEntity::getByKeys(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets) const
{
    return getByKeysImpl(keys, required_columns, out_null_map, out_offsets, KeyValueLookupMode::AllMatches);
}

Chunk MergeTreeTableJoinEntity::getByKeysWithMode(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets,
    KeyValueLookupMode mode) const
{
    return getByKeysImpl(keys, required_columns, out_null_map, out_offsets, mode);
}

Chunk MergeTreeTableJoinEntity::getByKeysImpl(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets,
    KeyValueLookupMode mode) const
{
    if (keys.size() != primary_key.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Key column number mismatch, expected {}, got {}",
            primary_key.size(),
            keys.size());

    auto result_block = getSampleBlock(required_columns);
    if (keys.empty() || keys[0].column->empty())
        return Chunk(result_block.cloneEmptyColumns(), 0);

    ColumnRawPtrs key_columns;
    key_columns.reserve(keys.size());
    for (const auto & key : keys)
        key_columns.push_back(key.column.get());

    using Method = ColumnsHashing::HashMethodHashed<typename KeyToRowsMap::value_type, typename KeyToRowsMap::mapped_type, false>;

    Arena probe_pool;
    Method key_getter(key_columns, {}, nullptr);

    out_offsets.clear();
    out_offsets.reserve(keys[0].column->size());

    out_null_map.clear();
    out_null_map.reserve(keys[0].column->size());

    auto selector = ColumnUInt64::create();
    auto & selector_data = selector->getData();
    selector_data.reserve(keys[0].column->size());

    const bool first_match_only = mode == KeyValueLookupMode::FirstMatch;

    for (size_t row = 0; row < keys[0].column->size(); ++row)
    {
        auto find_result = key_getter.findKey(key_to_rows, row, probe_pool);
        if (find_result.isFound())
        {
            const auto & matching_rows = find_result.getMapped();
            if (first_match_only)
            {
                selector_data.push_back(matching_rows[0]);
                out_null_map.push_back(true);
            }
            else
            {
                for (size_t matching_row : matching_rows)
                {
                    selector_data.push_back(matching_row);
                    out_null_map.push_back(true);
                }
            }
        }
        else
        {
            selector_data.push_back(rows_count);
            out_null_map.push_back(false);
        }

        if (!first_match_only)
            out_offsets.push_back(selector_data.size());
    }

    MutableColumns result_columns;
    result_columns.reserve(result_block.columns());
    for (const auto & result_column : result_block)
    {
        const auto & source_column = data_block_with_default.getByPosition(column_positions.at(result_column.name)).column;
        result_columns.push_back(IColumn::mutate(source_column->index(*selector, 0)));
    }

    return Chunk(std::move(result_columns), selector_data.size());
}

}
