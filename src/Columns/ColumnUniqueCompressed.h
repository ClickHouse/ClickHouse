#pragma once

#include <Columns/ColumnString.h>
#include <Columns/IColumnUnique.h>
#include <Core/Field.h>
#include <Common/PODArray_fwd.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
}

/// String compressed dictionary.
/// Compression is done with Front Coding Block Difference to First algorithm.
/// The data is treated as strings. It is sorted and divided into blocks.
/// The first string in a block is stored explicitly, other strings are stored as pairs (common_prefix_length, remaining_suffix)
/// with respect to the first string in their block.
/// This column is not used on its own but only as implementation detail of ColumnLowCardinality.
class ColumnUniqueFCBlockDF final : public COWHelper<IColumnUnique, ColumnUniqueFCBlockDF>
{
    friend class COWHelper<IColumnUnique, ColumnUniqueFCBlockDF>;

private:
    ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size);
    explicit ColumnUniqueFCBlockDF(const IDataType & data_type);
    ColumnUniqueFCBlockDF(const ColumnUniqueFCBlockDF & other);

public:
    using Length = UInt64;
    using Lengths = PaddedPODArray<Length>;

    std::string getName() const override { return "UniqueFCBlockDF"; }

    MutableColumnPtr cloneEmpty() const override;

    /// Nested column is compressed
    const ColumnPtr & getNestedColumn() const override { return data_column; }
    const ColumnPtr & getNestedNotNullableColumn() const override { return data_column; }
    bool nestedColumnIsNullable() const override { return false; }
    void nestedToNullable() override { }
    void nestedRemoveNullable() override { }

    size_t uniqueInsert(const Field & x) override;
    bool tryUniqueInsert(const Field & x, size_t & index) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    MutableColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    IColumnUnique::IndexesWithOverflow
    uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length, size_t max_dictionary_size) override;
    size_t uniqueInsertData(const char * pos, size_t length) override;
    size_t uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) override;

    size_t getDefaultValueIndex() const override;
    size_t getNullValueIndex() const override;
    size_t getNestedTypeDefaultValueIndex() const override;
    bool canContainNulls() const override;

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    std::pair<String, DataTypePtr> getValueNameAndType(size_t n) const override;

    bool isDefaultAt(size_t n) const override
    {
        (void)n;
        return false;
    }

    /// This methos is not implemented as there is no contigous memory chunk containing the value
    StringRef getDataAt(size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getDataAt' not implemented for ColumnUniqueFCBlockDF");
    }

    bool isNullAt(size_t n) const override;
    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash_func) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    void getExtremes(Field & min, Field & max) const override;
    bool valuesHaveFixedSize() const override;
    bool isFixedAndContiguous() const override;
    size_t sizeOfValueIfFixed() const override;
    bool isNumeric() const override;

    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    void protect() override;
    size_t allocatedBytes() const override;

    void forEachSubcolumn(IColumn::ColumnCallback callback) const override { callback(data_column); }

    void forEachMutableSubcolumn(IColumn::MutableColumnCallback callback) override;

    void forEachSubcolumnRecursively(IColumn::RecursiveColumnCallback callback) const override
    {
        callback(*data_column);
        data_column->forEachSubcolumnRecursively(callback);
    }

    void forEachMutableSubcolumnRecursively(IColumn::RecursiveMutableColumnCallback callback) override;

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnUniqueFCBlockDF *>(&rhs))
            return data_column->structureEquals(*rhs_concrete->data_column);
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

    const UInt64 * tryGetSavedHash() const override;

    UInt128 getHash() const override;

    std::optional<UInt64> getOrFindValueIndex(StringRef value) const override;

private:
    String getDecompressedAt(size_t pos) const;

    /// The header at this pos is always less or equal to the value
    size_t getPosOfClosestHeader(StringRef value) const;

    /// Value will be at this pos if inserted alone
    size_t getPosToInsert(StringRef value) const;

    /// Returns a string column containing all the decompressed values
    MutableColumnPtr getDecompressedColumn() const;

    IColumn::WrappedPtr data_column;
    Lengths common_prefix_lengths;

    size_t block_size;
};

}
