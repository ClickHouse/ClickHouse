#include <Columns/ColumnUniqueCompressed.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

namespace
{

const ColumnString * getAndCheckColumnString(const IColumn * column)
{
    const ColumnString * ptr = nullptr;
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column))
    {
        ptr = typeid_cast<const ColumnString *>(&nullable_column->getNestedColumn());
    }
    else
    {
        ptr = typeid_cast<const ColumnString *>(column);
    }

    if (!ptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnString, got {}", column->getName());
    }

    return ptr;
}

template <typename T>
MutableColumnPtr getLengthColumn(const PaddedPODArray<UInt64> & lengths)
{
    MutableColumnPtr column = ColumnVector<T>::create(lengths.size());
    auto & data = static_cast<ColumnVector<T> *>(column.get())->getData();
    for (size_t i = 0; i < lengths.size(); ++i)
    {
        data[i] = static_cast<T>(lengths[i]);
    }
    return column;
}

}

String ColumnUniqueFCBlockDF::getDecompressedAt(size_t pos) const
{
    chassert(pos < data_column->size());

    /// Default and Null value
    if (pos < specialValuesCount())
    {
        return "";
    }

    const size_t pos_in_block = (pos - specialValuesCount()) % block_size;
    if (pos_in_block == 0)
    {
        return String(data_column->getDataAt(pos));
    }

    const size_t header_pos = pos - pos_in_block;
    const std::string_view header = data_column->getDataAt(header_pos);
    const std::string_view suffix = data_column->getDataAt(pos);
    const size_t prefix_length = common_prefix_lengths->get64(pos);
    String output;
    output.resize(prefix_length + suffix.size());
    memcpy(output.data(), header.data(), prefix_length);
    memcpy(output.data() + prefix_length, suffix.data(), suffix.size());
    return output;
}

ColumnUniqueFCBlockDF::DecompressedValue ColumnUniqueFCBlockDF::getDecompressedRefsAt(size_t pos) const
{
    chassert(pos < data_column->size());

    /// Default and Null value
    if (pos < specialValuesCount())
    {
        return {};
    }

    const size_t pos_in_block = (pos - specialValuesCount()) % block_size;
    if (pos_in_block == 0)
    {
        const std::string_view prefix = data_column->getDataAt(pos);
        const std::string_view suffix;
        return {prefix, suffix};
    }

    const size_t header_pos = pos - pos_in_block;
    const std::string_view prefix = data_column->getDataAt(header_pos).substr(0, common_prefix_lengths->get64(pos));
    const std::string_view suffix = data_column->getDataAt(pos);
    return {prefix, suffix};
}

size_t ColumnUniqueFCBlockDF::getSizeAt(size_t pos) const
{
    return data_column->getDataAt(pos).size() + common_prefix_lengths->get64(pos);
}

MutableColumnPtr ColumnUniqueFCBlockDF::getDecompressedValues(size_t start, size_t length) const
{
    chassert(start + length <= size());
    auto output_column = ColumnString::create();
    output_column->reserve(length);
    for (size_t i = start; i < start + length; ++i)
    {
        String decompressed_value = getDecompressedAt(i);
        output_column->insert(std::move(decompressed_value));
    }
    return output_column;
}

MutableColumnPtr ColumnUniqueFCBlockDF::getDecompressedAll() const
{
    return getDecompressedValues(0, size());
}

ColumnPtr ColumnUniqueFCBlockDF::getNestedColumn() const
{
    auto decompressed = getDecompressedAll();

    if (!is_nullable)
    {
        return decompressed;
    }

    ColumnUInt8::MutablePtr null_mask = ColumnUInt8::create(size(), UInt8(0));
    null_mask->getData()[getNullValueIndex()] = 1;
    return ColumnNullable::create(std::move(decompressed), std::move(null_mask));
}

ColumnUniqueFCBlockDF::ColumnUniqueFCBlockDF(const ColumnPtr & string_column, size_t block_size_, bool is_nullable_)
    : data_column(ColumnString::create())
    , block_size(block_size_)
    , is_nullable(is_nullable_)
{
    /// sanity check
    if (block_size < 2)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnUniqueFCBlockDF is senseless for block_size < 2, got block_size={}", block_size);
    }

    if (!typeid_cast<const ColumnString *>(string_column.get()))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnUniqueFCBlockDF expected ColumnString, but got {}", string_column->getName());
    }

    IColumn::Permutation sorted_permutation;
    string_column->getPermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Unstable,
        0, /* limit */
        -1, /* nan_direction_hint */
        sorted_permutation);
    auto sorted_column = string_column->permute(sorted_permutation, 0);
    calculateCompression(sorted_column);
}

ColumnUniqueFCBlockDF::ColumnUniqueFCBlockDF(const ColumnUniqueFCBlockDF & other)
    : data_column(other.data_column)
    , common_prefix_lengths(other.common_prefix_lengths),
      block_size(other.block_size),
      old_indexes_mapping(other.old_indexes_mapping),
      is_nullable(other.is_nullable)
{
}

size_t ColumnUniqueFCBlockDF::getPosOfClosestHeader(std::string_view value) const
{
    /// Default value case
    if (value.empty())
    {
        return getNestedTypeDefaultValueIndex();
    }

    /// it's a binsearch over "header" positions
    const size_t special_values_count = specialValuesCount();
    size_t left = 0;
    size_t right = (data_column->size() - special_values_count + block_size - 1) / block_size;
    size_t output = special_values_count;
    while (left < right)
    {
        const size_t mid = (left + right) / 2;
        const size_t header_index = mid * block_size + special_values_count;

        const std::string_view header = data_column->getDataAt(header_index);
        if (header < value || header == value)
        {
            output = header_index;
            left = mid + 1;
        }
        else
        {
            right = mid;
        }
    }
    return output;
}

size_t ColumnUniqueFCBlockDF::getPosToInsert(std::string_view value) const
{
    size_t pos = getPosOfClosestHeader(value);
    /// it's guaranteed that this takes no more than block_size iterations
    while (pos < data_column->size())
    {
        const String data = getDecompressedAt(pos);
        if (data < value)
        {
            ++pos;
        }
        else
        {
            break;
        }
    }
    return pos;
}

void ColumnUniqueFCBlockDF::calculateCompression(const ColumnPtr & string_column)
{
    data_column = data_column->cloneEmpty();

    PaddedPODArray<UInt64> lengths;
    size_t max_length = 0;

    const auto insert_length = [&](size_t length)
    {
        lengths.push_back(length);
        max_length = std::max(max_length, length);
    };

    /// Default and Null value
    for (size_t i = 0; i < specialValuesCount(); ++i)
    {
        data_column->insertDefault();
        insert_length(0);
    }

    std::string_view current_header;
    std::string_view prev_data; // to skip duplicates
    size_t pos_in_block = 0;
    for (size_t i = 0; i < string_column->size(); ++i)
    {
        const std::string_view data = string_column->getDataAt(i);
        if (prev_data == data)
        {
            continue;
        }
        if (pos_in_block == 0)
        {
            current_header = data;
            data_column->insertData(current_header.data(), current_header.size());
            insert_length(current_header.size());
        }
        else
        {
            size_t same_prefix_length = 0;
            while (same_prefix_length < current_header.size() && same_prefix_length < data.size()
                   && current_header[same_prefix_length] == data[same_prefix_length])
            {
                ++same_prefix_length;
            }
            data_column->insertData(data.data() + same_prefix_length, data.size() - same_prefix_length);
            insert_length(same_prefix_length);
        }
        prev_data = data;
        ++pos_in_block;
        if (pos_in_block == block_size)
        {
            pos_in_block = 0;
        }
    }

    if (max_length <= std::numeric_limits<UInt8>::max())
    {
        common_prefix_lengths = getLengthColumn<UInt8>(lengths);
    }
    else if (max_length <= std::numeric_limits<UInt16>::max())
    {
        common_prefix_lengths = getLengthColumn<UInt16>(lengths);
    }
    else if (max_length <= std::numeric_limits<UInt32>::max())
    {
        common_prefix_lengths = getLengthColumn<UInt32>(lengths);
    }
    else
    {
        common_prefix_lengths = getLengthColumn<UInt64>(lengths);
    }
}

std::optional<UInt64> ColumnUniqueFCBlockDF::getOrFindValueIndex(std::string_view value) const
{
    const size_t expected_pos = getPosToInsert(value);
    if (expected_pos == data_column->size() || getDecompressedAt(expected_pos) != value)
    {
        return {};
    }
    return expected_pos;
}

MutableColumnPtr ColumnUniqueFCBlockDF::cloneEmpty() const
{
    return ColumnUniqueFCBlockDF::create(ColumnString::create(), block_size, is_nullable);
}

MutableColumnPtr ColumnUniqueFCBlockDF::cloneEmptyNullable() const
{
    return ColumnUniqueFCBlockDF::create(ColumnString::create(), block_size, true);
}

size_t ColumnUniqueFCBlockDF::uniqueInsert(const Field & x)
{
    if (x.isNull())
    {
        return getNullValueIndex();
    }

    const String & str = x.safeGet<String>();
    const size_t output = getPosToInsert(str);

    uniqueInsertData(str.data(), str.size());

    return output;
}

bool ColumnUniqueFCBlockDF::tryUniqueInsert(const Field & x, size_t & index)
{
    if (x.isNull())
    {
        if (is_nullable)
        {
            index = getNullValueIndex();
            return true;
        }
        return false;
    }

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
    const ColumnString * src_column = getAndCheckColumnString(&src);

    ColumnPtr sorted_column;
    old_indexes_mapping = prepareForInsert(getDecompressedAll(), src_column->cut(start, length), sorted_column);
    calculateCompression(sorted_column);

    auto positions = ColumnVector<UInt64>::create();
    if (is_nullable) /// keeping it outside of loop for performance
    {
        for (size_t i = start; i < start + length; ++i)
        {
            if (src.isNullAt(i))
            {
                positions->insert(getNullValueIndex());
            }
            else
            {
                const std::string_view data = src_column->getDataAt(i);
                const UInt64 pos = getPosToInsert(data);
                positions->insert(pos);
            }
        }
    }
    else
    {
        for (size_t i = start; i < start + length; ++i)
        {
            const std::string_view data = src_column->getDataAt(i);
            const UInt64 pos = getPosToInsert(data);
            positions->insert(pos);
        }
    }

    return positions;
}

size_t ColumnUniqueFCBlockDF::uniqueInsertData(const char * pos, size_t length)
{
    const size_t output = getPosToInsert(std::string_view{pos, length});
    auto single_value_column = ColumnString::create();
    single_value_column->insertData(pos, length);

    ColumnPtr sorted_column;
    old_indexes_mapping = prepareForInsert(getDecompressedAll(), std::move(single_value_column), sorted_column);
    calculateCompression(sorted_column);

    return output;
}

size_t ColumnUniqueFCBlockDF::uniqueDeserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings)
{
    if (is_nullable)
    {
        UInt8 val = 0;
        readBinaryLittleEndian<UInt8>(val, in);
        if (val)
            return getNullValueIndex();
    }

    const bool serialize_string_with_zero_byte = settings && settings->serialize_string_with_zero_byte;
    size_t string_size = 0;
    readBinaryLittleEndian<size_t>(string_size, in);
    if (in.available() < string_size)
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Not enough data to deserialize string value in ColumnUniqueFCBlockDF.");

    const size_t ret = uniqueInsertData(in.position(), string_size - serialize_string_with_zero_byte);
    in.ignore(string_size);
    return ret;
}

size_t ColumnUniqueFCBlockDF::uniqueDeserializeAndInsertAggregationStateValueFromArena(ReadBuffer & in)
{
    const auto settings = IColumn::SerializationSettings::createForAggregationState();
    return uniqueDeserializeAndInsertFromArena(in, &settings);
}

size_t ColumnUniqueFCBlockDF::uniqueInsertFrom(const IColumn & src, size_t n)
{
    if (is_nullable && src.isNullAt(n))
    {
        return getNullValueIndex();
    }
    const ColumnString * src_column = getAndCheckColumnString(&src);
    const std::string_view data = src_column->getDataAt(n);
    const size_t output = uniqueInsertData(data.data(), data.size());
    return output;
}

IColumnUnique::IndexesWithOverflow
ColumnUniqueFCBlockDF::uniqueInsertRangeWithOverflow(const IColumn & src, size_t start, size_t length, size_t max_dictionary_size)
{
    HashSet<std::string_view> to_add_strings;
    const auto is_already_present = [&to_add_strings, this](std::string_view value)
    {
        const auto index = getOrFindValueIndex(value);
        return index.has_value() || to_add_strings.contains(value);
    };

    size_t first_overflowed = start + length;
    for (size_t i = start; i < start + length; ++i)
    {
        if (src.isNullAt(i))
        {
            continue;
        }
        const std::string_view data = src.getDataAt(i);
        if (size() + to_add_strings.size() >= max_dictionary_size)
        {
            first_overflowed = i;
            break;
        }
        if (!is_already_present(data))
        {
            to_add_strings.insert(data);
        }
    }

    auto values = getDecompressedAll();

    const ColumnString * src_column = getAndCheckColumnString(&src);

    auto to_add = src_column->cut(start, first_overflowed);

    ColumnPtr sorted_column;
    old_indexes_mapping = prepareForInsert(values, to_add, sorted_column);
    calculateCompression(sorted_column);

    HashMap<std::string_view, size_t> overflow_map;
    MutableColumnPtr indexes = ColumnVector<UInt64>::create(length);
    MutableColumnPtr overflow = src.cloneEmpty();
    auto & indexes_data = static_cast<ColumnVector<UInt64> *>(indexes.get())->getData();
    if (is_nullable) /// keeping it outside of loop for performance
    {
        for (size_t i = start; i < start + length; ++i)
        {
            if (src.isNullAt(i))
            {
                indexes_data[i] = getNullValueIndex();
            }
            else
            {
                const std::string_view data = src.getDataAt(i);
                const auto pos = getOrFindValueIndex(data);
                if (pos.has_value())
                {
                    indexes_data[i] = pos.value();
                }
                else
                {
                    overflow_map.insert({data, overflow_map.size()});
                    const auto overflow_pos = overflow_map.at(data);
                    indexes_data[i] = overflow_pos + size();
                    if (overflow->size() == overflow_pos)
                    {
                        overflow->insertFrom(src, i);
                    }
                }
            }
        }
    }
    else
    {
        for (size_t i = start; i < start + length; ++i)
        {
            const std::string_view data = src.getDataAt(i);
            const auto pos = getOrFindValueIndex(data);
            if (pos.has_value())
            {
                indexes_data[i] = pos.value();
            }
            else
            {
                overflow_map.insert({data, overflow_map.size()});
                const auto overflow_pos = overflow_map.at(data);
                indexes_data[i] = overflow_pos + size();
                if (overflow->size() == overflow_pos)
                {
                    overflow->insertFrom(src, i);
                }
            }
        }
    }

    return {std::move(indexes), IColumn::mutate(std::move(overflow))};
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
    if (is_nullable && n == getNullValueIndex())
    {
        return {};
    }
    return getDecompressedAt(n);
}

void ColumnUniqueFCBlockDF::get(size_t n, Field & res) const
{
    if (is_nullable && n == getNullValueIndex())
    {
        res = Field{};
        return;
    }
    res = getDecompressedAt(n);
}

void ColumnUniqueFCBlockDF::getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const IColumn::Options & options) const
{
    if (is_nullable && n == getNullValueIndex())
    {
        if (options.notFull(name_buf))
            name_buf << "NULL";
        return;
    }
    auto value_column = ColumnString::create();
    value_column->insert(getDecompressedAt(n));
    value_column->getValueNameImpl(name_buf, 0, options);
}

void ColumnUniqueFCBlockDF::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null, const IColumn::SerializationSettings * settings) const
{
    /// nullable is handled internally
    chassert(!is_null);

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

    const bool serialize_string_with_zero_byte = settings && settings->serialize_string_with_zero_byte;
    if (is_nullable)
    {
        ++sizes[0]; /* instead of checking i == getNullValueIndex() in the loop for performance, assumes getNullValueIndex is 0 */
        for (size_t i = 1; i < rows_count; ++i)
        {
            const size_t string_size = getSizeAt(i) + serialize_string_with_zero_byte;
            sizes[i] += 1 /* null byte */ + sizeof(string_size) + string_size;
        }
    }
    else
    {
        for (size_t i = 0; i < rows_count; ++i)
        {
            const size_t string_size = getSizeAt(i) + serialize_string_with_zero_byte;
            sizes[i] += sizeof(string_size) + string_size;
        }
    }
}

std::optional<size_t> ColumnUniqueFCBlockDF::getSerializedValueSize(size_t n, const IColumn::SerializationSettings * settings) const
{
    const bool serialize_string_with_zero_byte = settings && settings->serialize_string_with_zero_byte;
    if (is_nullable)
    {
        if (n == getNullValueIndex())
            return 1;
        return 1 + sizeof(size_t) + getSizeAt(n) + serialize_string_with_zero_byte;
    }
    return sizeof(size_t) + getSizeAt(n) + serialize_string_with_zero_byte;
}

char * ColumnUniqueFCBlockDF::serializeIntoMemory(size_t pos, DecompressedValue value, char * memory, bool serialize_string_with_zero_byte) const
{
    if (is_nullable)
    {
        const UInt8 flag = (pos == getNullValueIndex() ? 1 : 0);
        unalignedStore<UInt8>(memory, flag);
        ++memory;
        if (flag)
        {
            return memory;
        }
    }

    const size_t value_size = value.size() + serialize_string_with_zero_byte;

    memcpy(memory, &value_size, sizeof(value_size));
    memory += sizeof(value_size);

    if (value.prefix.data()) /// clang tidy is mad at nullptrs in memcpy even if count is zero
    {
        memcpy(memory, value.prefix.data(), value.prefix.size());
    }
    memory += value.prefix.size();

    if (value.suffix.data())
    {
        memcpy(memory, value.suffix.data(), value.suffix.size());
    }
    memory += value.suffix.size();
    if (serialize_string_with_zero_byte)
    {
        *memory = '\0';
        ++memory;
    }

    return memory;
}

std::string_view ColumnUniqueFCBlockDF::serializeValueIntoArena(
    size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const
{
    const bool serialize_string_with_zero_byte = settings && settings->serialize_string_with_zero_byte;
    DecompressedValue value;
    size_t serialization_size = 0;

    if (is_nullable && n == getNullValueIndex())
    {
        serialization_size = 1;
    }
    else
    {
        value = getDecompressedRefsAt(n);
        serialization_size = (is_nullable ? 1 : 0) + sizeof(size_t) + value.size() + serialize_string_with_zero_byte;
    }

    char * pos = arena.allocContinue(serialization_size, begin);
    serializeIntoMemory(n, value, pos, serialize_string_with_zero_byte);
    return {pos, serialization_size};
}

char * ColumnUniqueFCBlockDF::serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const
{
    const bool serialize_string_with_zero_byte = settings && settings->serialize_string_with_zero_byte;
    const DecompressedValue value = getDecompressedRefsAt(n);
    return serializeIntoMemory(n, value, memory, serialize_string_with_zero_byte);
}

void ColumnUniqueFCBlockDF::updateHashWithValue(size_t n, SipHash & hash_func) const
{
    if (is_nullable)
    {
        const UInt8 null_flag = (n == getNullValueIndex());
        hash_func.update(reinterpret_cast<const char *>(&null_flag), sizeof(null_flag));
        if (null_flag)
        {
            return;
        }
    }
    /// Same as ColumnString::updateHashWithValue
    const DecompressedValue value = getDecompressedRefsAt(n);
    const size_t size_used_in_hash = value.size() + 1;
    hash_func.update(reinterpret_cast<const char *>(&size_used_in_hash), sizeof(size_used_in_hash));
    hash_func.update(value.prefix.data(), value.prefix.size());
    hash_func.update(value.suffix.data(), value.suffix.size());
    hash_func.update(UInt8(0));
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
    const std::string_view lhsref = lhs_value;
    const std::string_view rhsref = rhs_value;
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

void ColumnUniqueFCBlockDF::getExtremes(Field & min, Field & max, size_t start, size_t end) const
{
    /// Skip default and null value
    start = std::max(start, specialValuesCount());
    if (start >= end)
    {
        min = "";
        max = "";
        return;
    }

    /// As values are sorted
    get(start, min);
    get(end - 1, max);
}

size_t ColumnUniqueFCBlockDF::byteSize() const
{
    return data_column->byteSize() + common_prefix_lengths->byteSize();
}

size_t ColumnUniqueFCBlockDF::byteSizeAt(size_t n) const
{
    return data_column->byteSizeAt(n) + common_prefix_lengths->sizeOfValueIfFixed();
}

void ColumnUniqueFCBlockDF::protect()
{
    data_column->protect();
    common_prefix_lengths->protect();
}

size_t ColumnUniqueFCBlockDF::allocatedBytes() const
{
    return data_column->allocatedBytes() + common_prefix_lengths->allocatedBytes();
}

UInt128 ColumnUniqueFCBlockDF::getHash() const
{
    return hash.getHash(getNestedColumn());
}

bool ColumnUniqueFCBlockDF::haveIndexesChanged() const
{
    return old_indexes_mapping != nullptr;
}

MutableColumnPtr ColumnUniqueFCBlockDF::detachChangedIndexes()
{
    MutableColumnPtr temp = IColumn::mutate(std::move(old_indexes_mapping));
    old_indexes_mapping = nullptr;
    return temp;
}

MutableColumnPtr ColumnUniqueFCBlockDF::prepareForInsert(const MutableColumnPtr & column_to_modify, const ColumnPtr & to_insert, ColumnPtr & sorted_column)
{
    const size_t initial_size = column_to_modify->size();

    IColumn::Permutation sorted_permutation;
    column_to_modify->insertRangeFrom(*to_insert, 0, to_insert->size());
    column_to_modify->getPermutation(
        IColumn::PermutationSortDirection::Ascending,
        IColumn::PermutationSortStability::Unstable,
        0, /* limit */
        -1, /* nan_direction_hint */
        sorted_permutation);
    sorted_column = column_to_modify->permute(sorted_permutation, 0);

    HashMap<std::string_view, size_t> map;
    for (size_t i = 0; i < sorted_column->size(); ++i)
    {
        map.insert({sorted_column->getDataAt(i), map.size()});
    }

    const size_t special_values = specialValuesCount();
    const auto get_column_of_type = [&](auto type)
    {
        using IntType = decltype(type);
        auto column = ColumnVector<IntType>::create(initial_size);
        ColumnVector<IntType> * column_ptr = static_cast<ColumnVector<IntType> *>(column.get());

        /// we must take into account that in nullable case there are 2 empty strings
        for (size_t i = 0; i < special_values; ++i)
        {
            column_ptr->getData()[i] = static_cast<IntType>(i);
        }
        for (size_t i = special_values; i < initial_size; ++i)
        {
            column_ptr->getData()[i] = static_cast<IntType>(map.at(column_to_modify->getDataAt(i)) + special_values - 1);
        }
        return column;
    };

    const UInt64 max_index = map.size() + special_values - 2;
    if (max_index <= std::numeric_limits<UInt8>::max())
    {
        return get_column_of_type(UInt8());
    }
    else if (max_index <= std::numeric_limits<UInt16>::max())
    {
        return get_column_of_type(UInt16());
    }
    else if (max_index <= std::numeric_limits<UInt32>::max())
    {
        return get_column_of_type(UInt32());
    }
    return get_column_of_type(UInt64());
}

size_t ColumnUniqueFCBlockDF::specialValuesCount() const
{
    return is_nullable ? 2 : 1;
}

void ColumnUniqueFCBlockDF::nestedToNullable()
{
    chassert(!is_nullable);

    is_nullable = true;

    auto temp = ColumnString::create();
    temp->insertDefault();
    temp->insertRangeFrom(*data_column, 0, size());
    data_column = std::move(temp);

    auto temp_lengths = common_prefix_lengths->cloneEmpty();
    temp_lengths->reserve(size() + 1);
    temp_lengths->insertDefault();
    temp_lengths->insertRangeFrom(*common_prefix_lengths, 0, common_prefix_lengths->size());
    common_prefix_lengths = std::move(temp_lengths);

    auto mapping = ColumnUInt64::create(size() - 1);
    for (size_t i = 0; i < size() - 1; ++i)
    {
        mapping->getData()[i] = i + 1;
    }
    mapping->getData()[0] = 0; /// default value becomes NULL
    old_indexes_mapping = std::move(mapping);
}

void ColumnUniqueFCBlockDF::nestedRemoveNullable()
{
    chassert(is_nullable);

    is_nullable = false;

    data_column = data_column->cut(1, size() - 1);

    auto temp_lengths = common_prefix_lengths->cloneEmpty();
    temp_lengths->insertRangeFrom(*common_prefix_lengths, 1, common_prefix_lengths->size() - 1);
    common_prefix_lengths = std::move(temp_lengths);

    auto mapping = ColumnUInt64::create(size() + 1);
    for (size_t i = 0; i < size(); ++i)
    {
        mapping->getData()[i + 1] = i;
    }
    mapping->getData()[0] = 0; /// null values map into default
    old_indexes_mapping = std::move(mapping);
}

}
