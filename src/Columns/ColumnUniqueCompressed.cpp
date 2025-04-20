#include <Columns/ColumnUniqueCompressed.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>

namespace DB
{

String ColumnUniqueFCBlockDF::getDecompressedAt(size_t pos) const
{
    chassert(pos < data_column->size());

    /// Default / Null value
    if (!pos)
    {
        return "";
    }

    const size_t pos_in_block = (pos - 1) % block_size;
    if (pos_in_block == 0)
    {
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

ColumnUniqueFCBlockDF::DecompressedValue ColumnUniqueFCBlockDF::getDecompressedRefsAt(size_t pos) const
{
    chassert(pos < data_column->size());

    /// Default / Null value
    if (!pos)
    {
        return {{nullptr, 0}, {nullptr, 0}};
    }

    const size_t pos_in_block = (pos - 1) % block_size;
    if (pos_in_block == 0)
    {
        const StringRef prefix = data_column->getDataAt(pos);
        const StringRef suffix = {nullptr, 0};
        return {prefix, suffix};
    }

    const size_t header_pos = pos - pos_in_block;
    const StringRef prefix = {data_column->getDataAt(header_pos).data, common_prefix_lengths[pos]};
    const StringRef suffix = data_column->getDataAt(pos);
    return {prefix, suffix};
}

size_t ColumnUniqueFCBlockDF::getSizeAt(size_t pos) const
{
    return data_column->getDataAt(pos).size + common_prefix_lengths[pos];
}

MutableColumnPtr ColumnUniqueFCBlockDF::getDecompressedColumn() const
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

ColumnUniqueFCBlockDF::ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size_, bool is_nullable_)
    : data_column(ColumnString::create())
    , block_size(block_size_)
    , is_nullable(is_nullable_)
{
    if (!typeid_cast<const ColumnString *>(string_column.get()))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnUniqueFCBlockDF expected ColumnString, but got {}", string_column->getName());
    }

    IColumn::Permutation sorted_permutation;
    string_column->getPermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Unstable,
        0, /* limit */
        1, /* nan_direction_hint */
        sorted_permutation);
    auto sorted_column = string_column->permute(sorted_permutation, 0);

    /// Default / Null value
    data_column->insert("");
    common_prefix_lengths.push_back(0);

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

ColumnUniqueFCBlockDF::ColumnUniqueFCBlockDF(const ColumnUniqueFCBlockDF & other)
    : data_column(other.data_column)
    , common_prefix_lengths(other.common_prefix_lengths.begin(), other.common_prefix_lengths.end())
{
}

size_t ColumnUniqueFCBlockDF::getPosOfClosestHeader(StringRef value) const
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

size_t ColumnUniqueFCBlockDF::getPosToInsert(StringRef value) const
{
    size_t pos = getPosOfClosestHeader(value);
    StringRef data = getDecompressedAt(pos);
    /// it's guranteed that this takes no more than block_size iterations
    while (data < value && pos < data_column->size())
    {
        ++pos;
        data = getDecompressedAt(pos);
    }
    return pos;
}

void ColumnUniqueFCBlockDF::recalculateForNewData(const ColumnPtr & string_column)
{
    ColumnUniqueFCBlockDF temp_column(string_column, block_size, is_nullable);
    data_column = temp_column.data_column;
    common_prefix_lengths = std::move(temp_column.common_prefix_lengths);
}

std::optional<UInt64> ColumnUniqueFCBlockDF::getOrFindValueIndex(StringRef value) const
{
    const size_t expected_pos = getPosToInsert(value);
    if (expected_pos == data_column->size() || data_column->getDataAt(expected_pos) != value)
    {
        return {};
    }
    return expected_pos;
}

MutableColumnPtr ColumnUniqueFCBlockDF::cloneEmpty() const
{
    return ColumnUniqueFCBlockDF::create(ColumnString::create(), block_size, is_nullable);
}

size_t ColumnUniqueFCBlockDF::uniqueInsert(const Field & x)
{
    const size_t output = getPosToInsert(x.safeGet<String>());

    auto single_value_column = ColumnString::create();
    single_value_column->insert(x);

    auto temp_strings_column = getDecompressedColumn();
    temp_strings_column->insertRangeFrom(*single_value_column, 0, single_value_column->size());

    recalculateForNewData(std::move(temp_strings_column));

    return output;
}

bool ColumnUniqueFCBlockDF::tryUniqueInsert(const Field & x, size_t & index)
{
    String result;
    if (!x.tryGet<String>(result))
    {
        return false;
    }

    index = uniqueInsertData(result.data(), result.size());
    return true;
}

MutableColumnPtr ColumnUniqueFCBlockDF::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    auto temp_column = getDecompressedColumn();
    temp_column->insertRangeFrom(src, start, length);

    recalculateForNewData(std::move(temp_column));

    auto positions = ColumnVector<UInt64>::create();
    for (size_t i = start; i < start + length; ++i)
    {
        const StringRef data = src.getDataAt(i);
        const UInt64 pos = getPosToInsert(data);
        positions->insert(pos);
    }
    return positions;
}

size_t ColumnUniqueFCBlockDF::uniqueInsertData(const char * pos, size_t length)
{
    const size_t output = getPosToInsert(StringRef{pos, length});
    auto single_value_column = ColumnString::create();
    single_value_column->insertData(pos, length);
    recalculateForNewData(std::move(single_value_column));
    return output;
}

size_t ColumnUniqueFCBlockDF::uniqueInsertFrom(const IColumn & src, size_t n)
{
    const StringRef data = src.getDataAt(n);
    const size_t output = uniqueInsertData(data.data, data.size);
    return output;
}

IColumnUnique::IndexesWithOverflow
ColumnUniqueFCBlockDF::uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length, size_t max_dictionary_size)
{
    const size_t limit = max_dictionary_size >= data_column->size() ? max_dictionary_size - data_column->size() : 0;

    auto extracted_values_column = ColumnString::create();
    extracted_values_column->insertRangeFrom(src, start, length);

    IColumn::Permutation sorted_permutation;
    extracted_values_column->getPermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Unstable,
        0, /* limit */
        1, /* nan_direction_hint */
        sorted_permutation);
    auto sorted_column = extracted_values_column->permute(sorted_permutation, 0);

    auto indexes = uniqueInsertRangeFrom(*sorted_column, 0, limit);

    auto overflow = ColumnVector<UInt64>::create();
    for (size_t i = limit; i < sorted_permutation.size(); ++i)
    {
        overflow->insert(sorted_permutation[i]);
    }

    return IndexesWithOverflow{std::move(indexes), std::move(overflow)};
}

size_t ColumnUniqueFCBlockDF::getNullValueIndex() const
{
    if (!is_nullable)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnUniqueFCBlockDF is not nullable");
    }
    return 0;
}

Field ColumnUniqueFCBlockDF::operator[](size_t n) const
{
    return getDecompressedAt(n);
}

void ColumnUniqueFCBlockDF::get(size_t n, Field & res) const
{
    res = getDecompressedAt(n);
}

std::pair<String, DataTypePtr> ColumnUniqueFCBlockDF::getValueNameAndType(size_t n) const
{
    return data_column->getValueNameAndType(n); /// it's always String
}

void ColumnUniqueFCBlockDF::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const
{
    if (empty())
    {
        return;
    }

    const size_t rows_count = size();
    if (sizes.empty())
    {
        sizes.resize_fill(rows_count);
    }
    else if (sizes.size() != rows_count)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of sizes: {} doesn't match rows_num: {}. It is a bug", sizes.size(), rows_count);
    }

    if (is_null)
    {
        for (size_t i = 0; i < rows_count; ++i)
        {
            if (is_null[i])
            {
                ++sizes[i];
            }
            else
            {
                const size_t string_size = getSizeAt(i);
                sizes[i] += sizeof(string_size) + string_size + 2 /* null byte and null terminator */;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_count; ++i)
        {
            size_t string_size = getSizeAt(i);
            sizes[i] += sizeof(string_size) + string_size + 1 /* null terminator */;
        }
    }
}

char * ColumnUniqueFCBlockDF::serializeIntoMemory(size_t pos, DecompressedValue value, char * memory) const
{
    const size_t value_size = value.size() + 1; /* Null terminator */

    if (is_nullable)
    {
        UInt8 flag = (pos == getNullValueIndex() ? 1 : 0);
        unalignedStore<UInt8>(memory, flag);
        ++memory;
    }

    memcpy(memory, &value_size, sizeof(value_size));
    memory += sizeof(value_size);
    memcpy(memory, value.prefix.data, value.prefix.size);
    memory += value.prefix.size;
    memcpy(memory, value.suffix.data, value.suffix.size);
    memory += value.suffix.size;
    *memory = '\0';
    ++memory;

    return memory;
}

StringRef ColumnUniqueFCBlockDF::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    const DecompressedValue value = getDecompressedRefsAt(n);

    StringRef res;
    const size_t value_size = value.size() + 1; /* Null terminator */
    res.size = sizeof(value_size) + value_size + is_nullable;
    char * pos = arena.allocContinue(res.size, begin);
    res.data = pos;

    serializeIntoMemory(n, value, pos);

    return res;
}

char * ColumnUniqueFCBlockDF::serializeValueIntoMemory(size_t n, char * memory) const
{
    const DecompressedValue value = getDecompressedRefsAt(n);
    return serializeIntoMemory(n, value, memory);
}

void ColumnUniqueFCBlockDF::updateHashWithValue(size_t n, SipHash & hash_func) const
{
    data_column->updateHashWithValue(n, hash_func);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnUniqueFCBlockDF::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnUniqueFCBlockDF::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    if (is_nullable)
    {
        /// See ColumnNullable::compareAt
        bool lval_is_null = n == getNullValueIndex();
        bool rval_is_null = m == getNullValueIndex();

        if (unlikely(lval_is_null || rval_is_null))
        {
            if (lval_is_null && rval_is_null)
            {
                return 0;
            }
            return lval_is_null ? nan_direction_hint : -nan_direction_hint;
        }
    }

    /// TODO: it's inefficient, it's possible to do better
    const String lhs_value = getDecompressedAt(n);
    const String rhs_value = assert_cast<const ColumnUniqueFCBlockDF &>(rhs).getDecompressedAt(m);
    const StringRef lhsref = lhs_value;
    const StringRef rhsref = rhs_value;
    if (lhsref == rhsref)
    {
        return 0;
    }
    if (lhsref < rhsref)
    {
        return -1;
    }
    return 1;
}

void ColumnUniqueFCBlockDF::getExtremes(Field & min, Field & max) const
{
    /// Only default / null value
    if (size() == 1)
    {
        min = "";
        max = "";
        return;
    }

    /// As values are sorted
    get(1, min);
    get(size() - 1, max);
}

size_t ColumnUniqueFCBlockDF::byteSize() const
{
    return data_column->byteSize() + common_prefix_lengths.size() * sizeof(common_prefix_lengths[0]);
}

size_t ColumnUniqueFCBlockDF::byteSizeAt(size_t n) const
{
    return data_column->byteSizeAt(n) + sizeof(common_prefix_lengths[n]);
}

void ColumnUniqueFCBlockDF::protect()
{
    data_column->protect();
    common_prefix_lengths.protect();
}

size_t ColumnUniqueFCBlockDF::allocatedBytes() const
{
    return data_column->allocatedBytes() + common_prefix_lengths.allocated_bytes();
}

UInt128 ColumnUniqueFCBlockDF::getHash() const
{
    return hash.getHash(data_column);
}

}
