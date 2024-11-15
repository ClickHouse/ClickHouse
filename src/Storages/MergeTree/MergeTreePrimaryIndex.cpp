#include <Storages/MergeTree/MergeTreePrimaryIndex.h>
#include <Storages/MergeTree/MergeTreePrimaryIndexColumn.h>
#include <Storages/MergeTree/MergeTreePrimaryIndexNumeric.h>
#include <Core/Field.h>

namespace DB
{

PrimaryIndex::PrimaryIndex(Columns raw_columns, const Settings & settings)
    : num_rows(raw_columns.empty() ? 0 : raw_columns[0]->size())
    , block_sizes(raw_columns.size(), settings.min_block_size)
{
    init(std::move(raw_columns), settings);
}

PrimaryIndex::PrimaryIndex(Columns raw_columns, std::vector<size_t> num_equal_ranges, const Settings & settings)
    : num_rows(raw_columns.empty() ? 0 : raw_columns[0]->size())
{
    if (num_equal_ranges.empty())
    {
        block_sizes.assign(raw_columns.size(), settings.min_block_size);
    }
    else
    {
        chassert(raw_columns.size() == num_equal_ranges.size());
        block_sizes.resize(raw_columns.size());

        for (size_t i = 0; i < block_sizes.size(); ++i)
        {
            if (num_equal_ranges[i] == 1)
                block_sizes[i] = num_rows;
            else
                block_sizes[i] = std::max(settings.min_block_size, num_rows / (num_equal_ranges[i] + 1));
        }
    }

    init(std::move(raw_columns), settings);
}

void PrimaryIndex::init(Columns raw_columns, const Settings & settings)
{
    for (size_t i = 0; i < raw_columns.size(); ++i)
    {
        if (!settings.compress || num_rows == 0)
        {
            columns.push_back(createIndexColumnRaw(std::move(raw_columns[i])));
        }
        else if (auto column_number = createIndexColumnNumeric(raw_columns[i], block_sizes[i], settings.max_ratio_to_compress))
        {
            columns.push_back(std::move(column_number));
        }
        else if (auto column_lc = createIndexColumnLowCardinality(raw_columns[i], block_sizes[i], settings.max_ratio_to_compress))
        {
            columns.push_back(std::move(column_lc));
        }
        else
        {
            columns.push_back(createIndexColumnRaw(std::move(raw_columns[i])));
        }
    }
}

size_t PrimaryIndex::bytes() const
{
    size_t result = 0;
    for (const auto & column : columns)
        result += column->bytes();
    return result;
}

size_t PrimaryIndex::allocatedBytes() const
{
    size_t result = 0;
    for (const auto & column : columns)
        result += column->allocatedBytes();
    return result;
}

bool PrimaryIndex::isCompressed() const
{
    return std::ranges::any_of(columns, [](const auto & column) { return column->isCompressed(); });
}

Columns PrimaryIndex::getRawColumns() const
{
    Columns result;
    result.reserve(columns.size());
    for (const auto & column : columns)
        result.push_back(column->getRawColumn());
    return result;
}

Field PrimaryIndex::get(size_t col_idx, size_t row_idx) const
{
    Field res;
    get(col_idx, row_idx, res);
    return res;
}

void PrimaryIndex::get(size_t col_idx, size_t row_idx, Field & field) const
{
    columns.at(col_idx)->get(row_idx, field);
}

}
