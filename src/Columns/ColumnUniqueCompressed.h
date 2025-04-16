#pragma once

#include <Columns/IColumnUnique.h>
#include <Columns/ColumnString.h>
#include <Common/PODArray_fwd.h>
#include <Core/Field.h>

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
template <typename ColumnType>
class ColumnUniqueFCBlockDF final : public COWHelper<IColumnUnique, ColumnUniqueFCBlockDF<ColumnType>>
{
    friend class COWHelper<IColumnUnique, ColumnUniqueFCBlockDF<ColumnType>>;

private:
    ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size);
    explicit ColumnUniqueFCBlockDF(const IDataType & data_type);
    ColumnUniqueFCBlockDF(const ColumnUniqueFCBlockDF & other) = default;

public:
    using Length = UInt64;
    using Lengths = PaddedPODArray<Length>;

    std::string getName() const override { return "UniqueFCBlockDF"; }

    MutableColumnPtr cloneEmpty() const override;

    /// Nested column is compressed
    const ColumnPtr & getNestedColumn() const override { return data_column; }
    const ColumnPtr & getNestedNotNullableColumn() const override { return data_column; }
    bool nestedColumnIsNullable() const override { return false; }
    void nestedToNullable() override {}
    void nestedRemoveNullable() override {}

    size_t uniqueInsert(const Field & x) override;
    bool tryUniqueInsert(const Field & x, size_t & index) override;
    size_t uniqueInsertFrom(const IColumn & src, size_t n) override;
    MutableColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    IColumnUnique::IndexesWithOverflow uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length,
                                                                     size_t max_dictionary_size) override;
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

    /* This methos is not implemented as there is no contigous memory chunk containing the value */
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

    void forEachSubcolumn(IColumn::ColumnCallback callback) const override
    {
        callback(data_column);
    }

    void forEachMutableSubcolumn(IColumn::MutableColumnCallback callback) override;

    void forEachSubcolumnRecursively(IColumn::RecursiveColumnCallback callback) const override
    {
        callback(*data_column);
        data_column->forEachSubcolumnRecursively(callback);
    }

    void forEachMutableSubcolumnRecursively(IColumn::RecursiveMutableColumnCallback callback) override;

    bool structureEquals(const IColumn & rhs) const override
    {
        if (auto rhs_concrete = typeid_cast<const ColumnUniqueFCBlockDF *>(&rhs))
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
    ColumnPtr getDecompressedColumn() const;

    IColumn::WrappedPtr data_column;
    Lengths common_prefix_lengths;

    size_t block_size;
};

template <typename ColumnType>
String ColumnUniqueFCBlockDF<ColumnType>::getDecompressedAt(size_t pos) const
{
    chassert(pos < data_column->size());

    const size_t pos_in_block = pos % block_size;
    if (pos_in_block == 0) {
        return data_column->getDataAt(pos).toString();
    }

    const size_t header_pos = pos - pos_in_block;
    const StringRef header = data_column->getDataAt(header_pos);
    const StringRef suffix = data_column->getDataAt(pos);
    const size_t prefix_length = common_prefix_lengths[pos];
    String output;
    output.resize(prefix_length + suffix.size);
    memcpy(output.data(), header.data, prefix_length);
    memcpy(output.data() + prefix_length, suffix.data, suffix.size);
    return output;
}

template <typename ColumnType>
ColumnPtr ColumnUniqueFCBlockDF<ColumnType>::getDecompressedColumn() const
{
    auto output_column = ColumnString::create();
    output_column->reserve(data_column->size());
    for (size_t i = 0; i < data_column->size(); ++i)
    {
        String decompressed_value = getDecompressedAt(i);
        output_column->insert(std::move(decompressed_value));
    }
    return output_column;
}

template <typename ColumnType>
ColumnUniqueFCBlockDF<ColumnType>::ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size_)
    : data_column(ColumnString::create()),
      block_size(block_size_)
{
    if (!typeid_cast<const ColumnString *>(&string_column))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnUniqueFCBlockDF expected ColumnString, but got {}", string_column->getName());
    }

    IColumn::Permutation sorted_permutation;
    string_column->getPermutation(IColumn::PermutationSortDirection::Ascending,
                                 IColumn::PermutationSortStability::Unstable,
                                     0, /* limit */
                        1, /* nan_direction_hint */
                                      sorted_permutation);
    auto sorted_column = string_column->permute(sorted_permutation, 0);

    StringRef current_header = "";
    StringRef prev_data = ""; // to skip duplicates
    size_t pos_in_block = 0;
    for (size_t i = 0; i < sorted_column->size(); ++i) 
    {
        const StringRef data = sorted_column->getDataAt(i);
        if (prev_data == data) 
        {
            continue;
        }
        if (pos_in_block == 0) 
        {
            current_header = data;
            data_column->insertData(current_header.data, current_header.size);
            common_prefix_lengths.push_back(current_header.size);
        } 
        else 
        {
            size_t same_prefix_length = 0;
            while (same_prefix_length < current_header.size && same_prefix_length < data.size
                   && current_header.data[same_prefix_length] == data.data[same_prefix_length])
            {
                ++same_prefix_length;
            }
            data_column->insertData(data.data + same_prefix_length, data.size - same_prefix_length);
            common_prefix_lengths.push_back(same_prefix_length);
        }
        prev_data = data;
        ++pos_in_block;
        if (pos_in_block == block_size) 
        {
            pos_in_block = 0;
        }
    }
}

template <typename ColumnType>
size_t ColumnUniqueFCBlockDF<ColumnType>::getPosOfClosestHeader(StringRef value) const
{
    /// it's a binsearch over "header" positions
    size_t left = 0;
    size_t right = (data_column->size() - 1) / block_size;
    size_t output = 0;
    while (left <= right) 
    {
        size_t mid = (left + right) / 2;
        size_t header_index = mid * block_size;

        const StringRef header = data_column->getDataAt(header_index);
        if (header < value || header == value) 
        {
            output = header_index;
            left = mid + 1;
        }
        else 
        {
            if (mid == 0) 
            {
                break;
            }
            right = mid - 1;
        }
    }
    return output;
}

template <typename ColumnType>
size_t ColumnUniqueFCBlockDF<ColumnType>::getPosToInsert(StringRef value) const
{
    size_t pos = getPosOfClosestHeader(value);
    StringRef data = getDecompressedAt(pos);
    while (data < value && pos < data_column->size())
    {
        ++pos;
        data = getDecompressedAt(pos);
    }
    return pos;
}

template <typename ColumnType>
std::optional<UInt64> ColumnUniqueFCBlockDF<ColumnType>::getOrFindValueIndex(StringRef value) const
{
    const size_t expected_pos = getPosToInsert(value);
    if (expected_pos == data_column->size() || data_column->getDataAt(expected_pos) != value)
    {
        return {};
    }
    return expected_pos;
}

template <typename ColumnType>
MutableColumnPtr ColumnUniqueFCBlockDF<ColumnType>::cloneEmpty() const
{
    return ColumnUniqueFCBlockDF<ColumnType>::create(ColumnString::create(), block_size);
}

template <typename ColumnType>
size_t ColumnUniqueFCBlockDF<ColumnType>::uniqueInsert(const Field & x)
{
    const size_t output = getPosToInsert(x.safeGet<String>());

    auto single_value_column = ColumnString::create();
    single_value_column->insert(x);

    auto temp_column = getDecompressedColumn();
    temp_column->insertRangeFrom(single_value_column, 0, single_value_column->size());

    *this = ColumnUniqueFCBlockDF(temp_column, block_size);
    return output;
}

template <typename ColumnType>
MutableColumnPtr ColumnUniqueFCBlockDF<ColumnType>::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    auto temp_column = getDecompressedColumn();
    temp_column->insertRangeFrom(src, start, length);

    return ColumnUniqueFCBlockDF::create(temp_column, block_size);
}

}
