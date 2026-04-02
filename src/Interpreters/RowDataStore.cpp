#include <Interpreters/RowDataStore.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
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
        if (!sample_col->isFixedAndContiguous())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnFixedRowTuple should only be used to wrap fixed and continous columns but was used for {}.", sample_col->getFamilyName());

        size_t field_size = sample_col->sizeOfValueIfFixed();
        layout.push_back(FieldLayout{sample_col, field_size, offset});
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

RowDataStore RowDataStore::create(const Columns & columns)
{
    RowLayout layout = initLayout(columns);
    RowDataStore row_store(std::move(layout));
    if (!columns.empty() && columns[0]->size() > 0)
        row_store.gatherRows(columns, 0, columns[0]->size());
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
    chars.resize_fill(data_size + length * row_length);
    char * dst = chars.data() + data_size;

    for (size_t i = 0; i < layout.size(); ++i)
    {
        auto field_layout = layout[i];
        const char * src = columns[i]->getDataAt(start).data();
        for (size_t row = 0; row < length; ++row)
            memcpy(dst + row * row_length + field_layout.offset, src + row * field_layout.size, field_layout.size);
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

    const char * row_data = getRowAt(start);
    for (size_t row = 0; row < length; ++row)
    {
        for (size_t i = 0; i < layout.size(); ++i)
            columns[i]->insertData(row_data + layout[i].offset, layout[i].size);
        row_data += row_length;
    }
}

void RowDataStore::scatterRow(std::vector<IColumn *> & columns, size_t row_num) const
{
    scatterRows(columns, row_num, 1);
}

std::pair<size_t, size_t> RowDataStore::getFieldOffsetAndSize(size_t input_col_index) const
{
    return {layout[input_col_index].offset, layout[input_col_index].size};
}

MutableColumns RowDataStore::buildEmptyColumns() const
{
    MutableColumns columns(layout.size());
    for (size_t i = 0; i < layout.size(); ++i)
        columns[i] = layout[i].sample_column->cloneEmpty();
    return columns;
}

MutableColumns RowDataStore::buildColumns() const
{
    auto columns = buildEmptyColumns();
    std::vector<IColumn *> ptrs;
    ptrs.reserve(columns.size());
    for (auto & col : columns)
        ptrs.push_back(col.get());
    scatterRows(ptrs, 0, size());
    return columns;
}

bool isRowStorageUseful(const ColumnPtr & column)
{
    const IColumn * col = column.get();
    if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(col))
        col = column_replicated->getNestedColumn().get();

    /// Avoid copying long fixed string values.
    return col->isFixedAndContiguous() && col->sizeOfValueIfFixed() < 64;
}

}
