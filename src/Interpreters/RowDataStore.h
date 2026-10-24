#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Names.h>
#include <DataTypes/IDataType_fwd.h>
#include <Common/PODArray.h>
#include <base/types.h>

#include <optional>
#include <vector>


namespace DB
{

/// Stores multiple columns in row-major format.
/// Each row is a tuple of values from several source columns.
/// Plain fixed-width columns, `String` columns, `Tuple` columns of fixed-width elements and `Nullable(T)`
/// wrappers over such columns are supported.
///
/// Row Layout:
///   [field_0 | field_1 | ... | field_n ]
///
/// A nullable field is laid out as [null_byte | value_bytes]
/// where `null_byte` is 1 if the value is NULL and 0 otherwise.
/// A string field is a 16-byte `UnhashedPackedStringRef`: a value of up to 15 bytes is stored inline,
/// a longer one points to the overflow buffer.
/// A tuple field is its elements laid out one after another, each one a field of its own whose offset
/// is counted from the start of the row.
///
/// The order of fields matches the order of source columns.
class RowDataStore;
class Block;
class TableJoin;
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
        bool is_string = false;
        std::vector<FieldLayout> elements;
    };

    struct FieldPlanes
    {
        enum class Shape : UInt8
        {
            Fixed,
            String,
            Tuple,
        };

        Shape shape = Shape::Fixed;
        std::optional<const char *> null_map;
        const char * data = nullptr;
        size_t stride = 0;
        std::vector<const FieldLayout *> children;
    };

    using RowLayout = std::vector<FieldLayout>;
    using RowLayoutPtr = std::shared_ptr<const RowLayout>;

    explicit RowDataStore(RowLayoutPtr layout_);

    /// Compute the row-major layout for `columns` in input order.
    static RowLayoutPtr computeLayout(const Columns & columns, const DataTypes & types);

    /// The row length every store built from `layout` has.
    static size_t rowLengthOf(const RowLayout & layout) { return layout.empty() ? 0 : layout.back().offset + layout.back().size; }

    /// Cache misses that reading one row from a store built from `layout` saves over reading it column by column.
    static size_t cacheMissReduction(const RowLayout & layout);

    /// Create the row-major buffer and fills it with rows from `columns` in input order.
    static std::shared_ptr<RowDataStore> create(const RowLayoutPtr & layout, const Columns & columns);

    /// Scatter rows from the row-major buffer into columns in layout order.
    MutableColumns scatterRows(size_t start, size_t length) const;
    MutableColumns scatterRows(const PaddedPODArray<UInt64> & row_nums) const;

    const FieldLayout & getFieldLayout(size_t input_col_index) const;
    FieldPlanes getPlanes(const FieldLayout & field) const;

    const char * getRowAt(size_t index) const { return chars.data() + index * row_length; }
    size_t size() const { return row_length != 0 ? chars.size() / row_length : 0; }
    size_t allocatedBytes() const;
    size_t byteSizeAt(size_t index) const;

private:
    using Chars = PaddedPODArray<char>;

    /// Contiguous buffer of rows.
    Chars chars;
    RowLayoutPtr layout;
    size_t row_length;
    Chars overflow;

    /// Read `length` consecutive rows from `columns` starting at `start` and pack them into the row-major buffer.
    /// For nullable fields the null flag is written at the field's first byte followed by the value.
    void gatherRows(const Columns & columns, size_t start, size_t length);
};

/// Right-side columns that qualify for the row store.
NameSet computeRowStoreCandidateColumns(
    const TableJoin & table_join, const Block & right_sample_block, UInt64 right_rows, UInt64 min_string_column_bytes);

/// Derives optimal batch size for reading and writing into the row store based on L2 cache size.
size_t rowStoreBatchSize(size_t row_length);

}
