#include <Columns/ColumnUniqueCompressed.h>

namespace DB
{

String ColumnUniqueFCBlockDF::getDecompressedAt(size_t pos) const
{
    chassert(pos < data_column->size());

    const size_t pos_in_block = pos % block_size;
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

ColumnUniqueFCBlockDF::ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size_)
    : data_column(ColumnString::create())
    , block_size(block_size_)
{
    if (!typeid_cast<const ColumnString *>(&string_column))
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
    : data_column(other.data_column),
      common_prefix_lengths(other.common_prefix_lengths.begin(), other.common_prefix_lengths.end())
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
    /// it's guranteed that this takes up to block_size iterations
    while (data < value && pos < data_column->size())
    {
        ++pos;
        data = getDecompressedAt(pos);
    }
    return pos;
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
    return ColumnUniqueFCBlockDF::create(ColumnString::create(), block_size);
}

size_t ColumnUniqueFCBlockDF::uniqueInsert(const Field & x)
{
    const size_t output = getPosToInsert(x.safeGet<String>());

    auto single_value_column = ColumnString::create();
    single_value_column->insert(x);

    auto temp_strings_column = getDecompressedColumn();
    temp_strings_column->insertRangeFrom(*single_value_column, 0, single_value_column->size());

    ColumnUniqueFCBlockDF temp_column(std::move(temp_strings_column), block_size);

    data_column = temp_column.data_column;
    common_prefix_lengths = std::move(temp_column.common_prefix_lengths);

    return output;
}

MutableColumnPtr ColumnUniqueFCBlockDF::uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    auto temp_column = getDecompressedColumn();
    temp_column->insertRangeFrom(src, start, length);

    return ColumnUniqueFCBlockDF::create(temp_column, block_size);
}

}
