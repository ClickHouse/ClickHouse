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

RowDataStore::RowDataStore(RowLayout && layout_)
    : layout(std::move(layout_))
    , row_length(layout.empty() ? 0 : layout.back().offset + layout.back().size)
{
}

std::shared_ptr<RowDataStore> RowDataStore::create()
{
    return std::shared_ptr<RowDataStore>(new RowDataStore(RowLayout{}));
}

std::shared_ptr<RowDataStore> RowDataStore::create(const Columns & columns)
{
    auto row_store = create();
    row_store->init(columns);
    return row_store;
}

void RowDataStore::init(const Columns & columns)
{
    if (init_flag)
        return;
    init_flag = true;

    layout = initLayout(columns);
    row_length = layout.empty() ? 0 : layout.back().offset + layout.back().size;

    if (!columns.empty() && !columns[0]->empty())
        gatherRows(columns, 0, columns[0]->size());
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

RowDataStore::FieldLayout RowDataStore::getFieldLayout(size_t input_col_index) const
{
    return layout[input_col_index];
}

bool isRowStorageUseful(const ColumnPtr & column)
{
    const IColumn * check_col = column.get();
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
        check_col = column_nullable->getNestedColumnPtr().get();

    return check_col->isFixedAndContiguous() && column->sizeOfValueIfFixed() <= 64;
}
}
