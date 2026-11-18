#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType_fwd.h>
#include <Common/PODArray.h>

#include <optional>
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
        DataTypePtr type;
        size_t offset;
        size_t size;
        bool is_nullable;
    };

    using RowLayout = std::vector<FieldLayout>;
    using RowLayoutPtr = std::shared_ptr<const RowLayout>;

    explicit RowDataStore(RowLayoutPtr layout_);

    /// Compute the row-major layout for `columns` in input order.
    static RowLayoutPtr computeLayout(const Columns & columns, const DataTypes & types);

    /// Create the row-major buffer and fills it with rows from `columns` in input order.
    static std::shared_ptr<RowDataStore> create(const RowLayoutPtr & layout, const Columns & columns);

    /// Scatter rows from the row-major buffer into columns in layout order.
    MutableColumns scatterRows(size_t start, size_t length) const;
    MutableColumns scatterRows(const PaddedPODArray<UInt64> & row_nums) const;

    const FieldLayout & getFieldLayout(size_t input_col_index) const;

    /// Derives optimal batch size for reading and writing into the row store based on L2 cache size.
    std::optional<size_t> getBatchSize() const;

    const char * getRowAt(size_t index) const { return chars.data() + index * row_length; }
    size_t size() const { return row_length != 0 ? chars.size() / row_length : 0; }
    size_t byteSizeAt(size_t /*n*/) const { return row_length; }
    size_t allocatedBytes() const { return chars.empty() ? 0 : chars.allocated_bytes(); }

private:
    using Chars = PaddedPODArray<char>;

    /// Contiguous buffer of rows.
    Chars chars;
    RowLayoutPtr layout;
    size_t row_length;

    /// Read `length` consecutive rows from `columns` starting at `start` and pack them into the row-major buffer.
    /// For nullable fields the null flag is written at the field's first byte followed by the value.
    void gatherRows(const Columns & columns, size_t start, size_t length);
};

bool isRowStorageUseful(const ColumnPtr & column);

}
