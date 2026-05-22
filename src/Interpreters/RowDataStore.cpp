#include <Columns/ColumnNullable.h>
#include <Interpreters/RowDataStore.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <base/types.h>
#include <Common/Exception.h>

#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

RowDataStore::RowLayout RowDataStore::initLayout(const Columns & columns)
{
    RowLayout layout;
    layout.reserve(columns.size());

    size_t offset = 0;
    for (const auto & column : columns)
    {
        ColumnPtr sample_col = column->cloneEmpty();

        bool is_nullable = false;
        const IColumn * check_col = sample_col.get();
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(check_col))
        {
            check_col = nullable->getNestedColumnPtr().get();
            is_nullable = true;
        }

        if (!check_col->isFixedAndContiguous())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RowDataStore can only store fixed-size and contiguous columns, but got {}.", sample_col->getFamilyName());

        size_t field_size = sample_col->sizeOfValueIfFixed();
        layout.push_back(FieldLayout{sample_col, offset, field_size, is_nullable});
        offset += field_size;
    }
    return layout;
}

RowDataStore::RowDataStore(const RowLayout & layout_)
    : layout(layout_)
    , row_length(layout.empty() ? 0 : layout.back().offset + layout.back().size)
{
}

RowDataStore::RowDataStore(RowLayout && layout_)
    : layout(std::move(layout_))
    , row_length(layout.empty() ? 0 : layout.back().offset + layout.back().size)
{
}

std::shared_ptr<RowDataStore> RowDataStore::create(const Columns & columns)
{
    /// Columns are materialized to make sure all blocks have
    /// the same split of columnar and row store columns.
    Columns materialized_columns;
    materialized_columns.reserve(columns.size());
    for (const auto & col : columns)
        materialized_columns.push_back(col->convertToFullIfNeeded());

    RowLayout layout = initLayout(materialized_columns);
    auto row_store = std::shared_ptr<RowDataStore>(new RowDataStore(std::move(layout)));
    if (!materialized_columns.empty() && !materialized_columns[0]->empty())
        row_store->gatherRows(materialized_columns, 0, materialized_columns[0]->size());
    return row_store;
}

void RowDataStore::gatherRows(const Columns & columns, size_t start, size_t length)
{
    if (columns.size() != layout.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Number of appended columns ({}) does not match the number of columns in the layout ({}).",
            columns.size(),
            layout.size());

    if (length == 0)
        return;

    size_t data_size = chars.size();
    chars.resize(data_size + length * row_length);
    char * dst = chars.data() + data_size;

    for (size_t i = 0; i < layout.size(); ++i)
    {
        const auto & field_layout = layout[i];
        if (field_layout.is_nullable)
        {
            const auto * nullable_column = assert_cast<const ColumnNullable *>(columns[i].get());
            const char * null_src = nullable_column->getNullMapColumn().getDataAt(start).data();
            const char * data_src = nullable_column->getNestedColumn().getDataAt(start).data();
            const size_t value_size = field_layout.size - 1;

            for (size_t row = 0; row < length; ++row)
            {
                char * row_dst = dst + row * row_length + field_layout.offset;
                row_dst[0] = null_src[row];
                memcpy(row_dst + 1, data_src + row * value_size, value_size);
            }
        }
        else
        {
            const char * src = columns[i]->getDataAt(start).data();
            for (size_t row = 0; row < length; ++row)
                memcpy(dst + row * row_length + field_layout.offset, src + row * field_layout.size, field_layout.size);
        }
    }
}

void RowDataStore::gatherRow(const Columns & columns, size_t row_num)
{
    gatherRows(columns, row_num, 1);
}

void RowDataStore::scatterRows(std::vector<IColumn *> & columns, size_t start, size_t length) const
{
    if (columns.size() != layout.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Number of destination columns ({}) does not match the number of columns in the layout ({}).",
            columns.size(),
            layout.size());

    const char * base = getRowAt(start);
    for (size_t i = 0; i < layout.size(); ++i)
    {
        const auto & field_layout = layout[i];
        if (field_layout.is_nullable)
        {
            auto * nullable_column = assert_cast<ColumnNullable *>(columns[i]);
            auto & null_map = nullable_column->getNullMapData();
            IColumn & nested_column = nullable_column->getNestedColumn();
            const size_t value_size = field_layout.size - 1;

            for (size_t row = 0; row < length; ++row)
            {
                const char * row_data = base + row * row_length + field_layout.offset;
                null_map.push_back(*reinterpret_cast<const UInt8 *>(row_data));
                nested_column.insertData(row_data + 1, value_size);
            }
        }
        else
        {
            for (size_t row = 0; row < length; ++row)
                columns[i]->insertData(base + row * row_length + field_layout.offset, field_layout.size);
        }
    }
}

void RowDataStore::scatterRows(std::vector<IColumn *> & columns, const PaddedPODArray<UInt64> & row_nums) const
{
    if (columns.size() != layout.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Number of destination columns ({}) does not match the number of columns in the layout ({}).",
            columns.size(),
            layout.size());

    size_t length = row_nums.size();
    if (length == 0)
        return;

    for (size_t i = 0; i < layout.size(); ++i)
    {
        const auto & field_layout = layout[i];
        if (field_layout.is_nullable)
        {
            auto * nullable_column = assert_cast<ColumnNullable *>(columns[i]);
            auto & null_map = nullable_column->getNullMapData();
            IColumn & nested_column = nullable_column->getNestedColumn();
            const size_t value_size = field_layout.size - 1;

            null_map.reserve(null_map.size() + length);
            nested_column.reserve(nested_column.size() + length);
            for (size_t j = 0; j < length; ++j)
            {
                const char * row_data = getRowAt(row_nums[j]) + field_layout.offset;
                null_map.push_back(*reinterpret_cast<const UInt8 *>(row_data));
                nested_column.insertData(row_data + 1, value_size);
            }
        }
        else
        {
            columns[i]->reserve(columns[i]->size() + length);
            for (size_t j = 0; j < length; ++j)
                columns[i]->insertData(getRowAt(row_nums[j]) + field_layout.offset, field_layout.size);
        }
    }
}

void RowDataStore::scatterRow(std::vector<IColumn *> & columns, size_t row_num) const
{
    scatterRows(columns, row_num, 1);
}

RowDataStore::FieldLayout RowDataStore::getFieldLayout(size_t input_col_index) const
{
    return layout[input_col_index];
}

MutableColumns RowDataStore::buildEmptyColumns() const
{
    MutableColumns columns(layout.size());
    for (size_t i = 0; i < layout.size(); ++i)
        columns[i] = layout[i].sample_column->cloneEmpty();
    return columns;
}

bool isRowStorageUseful(const ColumnPtr & column)
{
    const IColumn * check_col = column.get();
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
        check_col = column_nullable->getNestedColumnPtr().get();

    return check_col->isFixedAndContiguous() && column->sizeOfValueIfFixed() <= 64;
}
}
