#pragma once

#include <Columns/ColumnString.h>
#include <Columns/IColumnUnique.h>
#include <Core/Field.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

/// String compressed dictionary.
/// Compression is done with Front Coding Block Difference to First algorithm.
/// The strings are sorted and divided into blocks.
/// The first string in a block is stored explicitly, other strings are stored as pairs (common_prefix_length, remaining_suffix)
/// with respect to the first string in their block.
/// This column is not used on its own but only as implementation detail of ColumnLowCardinality.
class ColumnUniqueFCBlockDF final : public COWHelper<IColumnUnique, ColumnUniqueFCBlockDF>
{
    friend class COWHelper<IColumnUnique, ColumnUniqueFCBlockDF>;

private:
    ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size, bool is_nullable);
    ColumnUniqueFCBlockDF(const ColumnUniqueFCBlockDF & other);

public:
    std::string getName() const override { return "UniqueFCBlockDF"; }

    MutableColumnPtr cloneEmpty() const override;

    /// Nested column is compressed
    ColumnPtr getNestedColumn() const override;
    ColumnPtr getNestedNotNullableColumn() const override { return getDecompressedAll(); }
    bool nestedColumnIsNullable() const override { return is_nullable; }
    
    /// Afterwards users must reindex
    void nestedToNullable() override;

    /// Afterwards users must reindex
    void nestedRemoveNullable() override;
    
    bool nestedCanBeInsideNullable() const override { return true; }

    size_t size() const override { return data_column->size(); }

    bool isCollationSupported() const override { return true; }

    bool haveIndexesChanged() const override;
    MutableColumnPtr detachChangedIndexes() override;

    size_t uniqueInsert(const Field & x) override;
    bool tryUniqueInsert(const Field & x, size_t & index) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    MutableColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    IColumnUnique::IndexesWithOverflow
    uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length, size_t max_dictionary_size) override;
    size_t uniqueInsertData(const char * pos, size_t length) override;
    size_t uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) override;

    size_t getDefaultValueIndex() const override { return 0; }
    size_t getNullValueIndex() const override;
    size_t getNestedTypeDefaultValueIndex() const override { return is_nullable ? 1 : 0; }
    bool canContainNulls() const override { return is_nullable; }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    std::pair<String, DataTypePtr> getValueNameAndType(size_t n) const override;

    bool isDefaultAt(size_t n) const override { return n == getDefaultValueIndex(); }
    bool isNullAt(size_t n) const override { return is_nullable && n == getNullValueIndex(); }

    /// This methos is not implemented as there is no continuous memory chunk containing the value
    StringRef getDataAt(size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getDataAt' not implemented for ColumnUniqueFCBlockDF");
    }

    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;

    const char * skipSerializedInArena(const char *) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'skipSerializedInArena' is not implemented for ColumnUniqueFCBlockDF");
    }

    void updateHashWithValue(size_t n, SipHash & hash_func) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    void getExtremes(Field & min, Field & max) const override;

    bool valuesHaveFixedSize() const override { return false; }
    bool isFixedAndContiguous() const override { return false; }
    bool isNumeric() const override { return false; }

    size_t sizeOfValueIfFixed() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method 'sizeOfValueIfFixed' not implemented for ColumnUniqueFCBlockDF");
    }

    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    void protect() override;
    size_t allocatedBytes() const override;

    void forEachSubcolumn(IColumn::ColumnCallback callback) const override
    { 
        callback(data_column); 
        callback(common_prefix_lengths);
    }

    void forEachMutableSubcolumn(IColumn::MutableColumnCallback callback) override 
    { 
        callback(data_column);
        callback(common_prefix_lengths);
    }

    void forEachSubcolumnRecursively(IColumn::RecursiveColumnCallback callback) const override
    { 
        callback(*data_column);
        callback(*common_prefix_lengths); 
    }

    void forEachMutableSubcolumnRecursively(IColumn::RecursiveMutableColumnCallback callback) override
    {
        callback(*data_column);
        callback(*common_prefix_lengths);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnUniqueFCBlockDF *>(&rhs))
        {
            return data_column->structureEquals(*rhs_concrete->data_column) &&
                   common_prefix_lengths->structureEquals(*rhs_concrete->common_prefix_lengths);
        }
        return false;
    }

    double getRatioOfDefaultRows(double) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getRatioOfDefaultRows' not implemented for ColumnUniqueFCBlockDF");
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getNumberOfDefaultRows' not implemented for ColumnUniqueFCBlockDF");
    }

    void getIndicesOfNonDefaultRows(IColumn::Offsets &, size_t, size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getIndicesOfNonDefaultRows' not implemented for ColumnUniqueFCBlockDF");
    }

    const UInt64 * tryGetSavedHash() const override { return nullptr; }

    UInt128 getHash() const override;

    std::optional<UInt64> getOrFindValueIndex(StringRef value) const override;

private:
    /// Default value and null value for nullable and only default value otherwise
    size_t specialValuesCount() const;

    String getDecompressedAt(size_t pos) const;

    struct DecompressedValue
    {
        StringRef prefix;
        StringRef suffix;

        size_t size() const { return prefix.size + suffix.size; }
    };

    DecompressedValue getDecompressedRefsAt(size_t pos) const;

    /// Calculates size without decompressing, excluding null terminator
    size_t getSizeAt(size_t pos) const;

    /// The header at this pos is always less or equal to the value
    size_t getPosOfClosestHeader(StringRef value) const;

    /// Value will be at this pos if inserted alone
    size_t getPosToInsert(StringRef value) const;

    /// Returns a string column containing the decompressed values
    MutableColumnPtr getDecompressedValues(size_t start, size_t length) const;

    /// Returns a string column containing all the decompressed values
    MutableColumnPtr getDecompressedAll() const;

    /// Inserts data from `to_insert` into `column_to_modify`. Stores sorted version of `column_to_modify` in `sorted_column` and returns 
    /// a column containing the permutation of data initialy in `column_to_mofiy`. This data is `old_indexes_mapping` itself.
    /// This method provides an optimized way to calculate `old_indexes_mapping` during inserts
    MutableColumnPtr prepareForInsert(const MutableColumnPtr & column_to_modify, const ColumnPtr & to_insert, ColumnPtr & sorted_column);

    /// Compresses `string_column` and stores this compression instead of current
    /// Assumes that string_column is sorted
    void calculateCompression(const ColumnPtr & string_column);

    /// Returns pointer to the end of serialization (first byte past the data)
    /// `pos` is the index at which the value resides in the column
    char * serializeIntoMemory(size_t pos, DecompressedValue value, char * memory) const;

    IColumn::WrappedPtr data_column;
    IColumn::WrappedPtr common_prefix_lengths;
    size_t block_size;

    ColumnPtr old_indexes_mapping;

    bool is_nullable;
    mutable IColumnUnique::IncrementalHash hash;
};

}
