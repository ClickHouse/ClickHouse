#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray.h>

#include <vector>


namespace DB
{

/// Stores multiple fixed and contiguous columns in row-major format.
/// Each row is a tuple of values from several source columns.
/// Each source column must have isFixedAndContiguous true.
///
/// Row Layout:
///   [fixed_field_0 | fixed_field_1 | ... | fixed_field_n ]
///
/// The order of field matches the order of source columns.
class RowDataStore
{
private:
    using Chars = PaddedPODArray<char>;

    struct FieldLayout
    {
        /// Used for mapping back to source columns.
        ColumnPtr sample_column;
        size_t size;
        size_t offset;
    };

    using RowLayout = std::vector<FieldLayout>;

    /// Contiguous buffer of rows.
    Chars chars;
    RowLayout layout;
    size_t row_length;

    explicit RowDataStore(const RowLayout & layout_);
    explicit RowDataStore(RowLayout && layout_);

public:
    static RowDataStore create(const Columns & columns);

    void gatherRows(const Columns & columns, size_t start, size_t length);
    void gatherRow(const Columns & columns, size_t row_num);

    void scatterRows(std::vector<IColumn *> & columns, size_t start, size_t length) const;
    void scatterRow(std::vector<IColumn *> & columns, size_t row_num) const;

    std::pair<size_t, size_t> getFieldOffsetAndSize(size_t input_col_index) const;

    const char * getRowAt(size_t index) const { return chars.data() + index * row_length; }
    size_t size() const { return chars.size() / row_length; }
    size_t byteSize() const { return chars.size(); }
    size_t byteSizeAt(size_t /*n*/) const { return row_length; }
    size_t allocatedBytes() const { return chars.allocated_bytes(); }

    MutableColumns buildEmptyColumns() const;
    MutableColumns buildColumns() const;

private:
    static RowLayout initLayout(const Columns & columns);
};

bool isRowStorageUseful(const ColumnPtr & column);

}
