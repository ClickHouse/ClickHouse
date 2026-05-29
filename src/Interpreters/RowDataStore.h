#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray.h>

#include <vector>


namespace DB
{

/// Stores multiple columns in row-major format.
/// Each row is a tuple of values from several source columns.
/// Both plain fixed-width columns and `Nullable(T)` wrappers over such columns are supported.
///
/// Row Layout:
///   [field_0 | field_1 | ... | field_n ]
///
/// A nullable field is laid out as [null_byte | value_bytes]
/// where `null_byte` is 1 if the value is NULL and 0 otherwise.
///
/// The order of fields matches the order of source columns.
class RowDataStore;
using RowDataStorePtr = std::shared_ptr<RowDataStore>;

class RowDataStore
{
public:
    struct FieldLayout
    {
        /// Used for mapping back to source columns.
        ColumnPtr sample_column;
        size_t offset;
        size_t size;
        bool is_nullable;
    };

    using RowLayout = std::vector<FieldLayout>;

    static std::shared_ptr<RowDataStore> create();
    static std::shared_ptr<RowDataStore> create(const Columns & columns);

    /// Initialize the row store from a set of columns.
    void init(const Columns & columns);

    /// Read `length` consecutive rows from `columns` starting at `start` and pack them into the row-major buffer.
    /// For nullable fields the null flag is written at the field's first byte followed by the value.
    void gatherRows(const Columns & columns, size_t start, size_t length);

    FieldLayout getFieldLayout(size_t input_col_index) const;

    const char * getRowAt(size_t index) const { return chars.data() + index * row_length; }
    size_t size() const { return chars.size() / row_length; }
    size_t byteSizeAt(size_t /*n*/) const { return row_length; }
    size_t allocatedBytes() const { return chars.allocated_bytes(); }

private:
    using Chars = PaddedPODArray<char>;

    /// Contiguous buffer of rows.
    Chars chars;
    RowLayout layout;
    size_t row_length;
    bool init_flag = false;

    explicit RowDataStore(RowLayout && layout_);

    static RowLayout initLayout(const Columns & columns);
};

bool isRowStorageUseful(const ColumnPtr & column);

}
