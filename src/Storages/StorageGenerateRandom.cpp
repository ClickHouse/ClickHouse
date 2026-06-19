#include <Storages/IStorage.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageGenerateRandom.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/SelectQueryInfo.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/ASTLiteral.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Common/DateLUTImpl.h>
#include <Common/SipHash.h>
#include <Common/intExp10.h>
#include <Common/randomSeed.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionGenerateRandomStructure.h>

#include <pcg_random.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 generate_random_max_json_dynamic_keys;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_LARGE_STRING_SIZE;
}


namespace
{

struct GenerateRandomState
{
    std::atomic<UInt64> add_total_rows = 0;
};
using GenerateRandomStatePtr = std::shared_ptr<GenerateRandomState>;

void fillBufferWithRandomBytes(char * __restrict data, size_t size, pcg64 & rng)
{
    char * __restrict end = data + size;
    while (data < end)
    {
        /// The loop can be further optimized.
        UInt64 number = rng();
        if constexpr (std::endian::native == std::endian::big)
            unalignedStoreLittleEndian<UInt64>(data, number);
        else
            unalignedStore<UInt64>(data, number);
        data += sizeof(UInt64); /// We assume that data has at least 7-byte padding (see PaddedPODArray)
    }
}

void fillBufferWithRandomPrintableASCIIBytes(char * __restrict data, size_t size, pcg64 & rng)
{
    size_t pos = 0;
    for (; pos + 4 <= size; pos += 4)
    {
        UInt64 rand = rng();

        UInt16 rand1 = static_cast<UInt16>(rand);
        UInt16 rand2 = static_cast<UInt16>(rand >> 16);
        UInt16 rand3 = static_cast<UInt16>(rand >> 32);
        UInt16 rand4 = static_cast<UInt16>(rand >> 48);

        /// Printable characters are from range [32; 126].
        /// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/

        data[pos + 0] = static_cast<char>(32 + ((rand1 * 95) >> 16));
        data[pos + 1] = static_cast<char>(32 + ((rand2 * 95) >> 16));
        data[pos + 2] = static_cast<char>(32 + ((rand3 * 95) >> 16));
        data[pos + 3] = static_cast<char>(32 + ((rand4 * 95) >> 16));

        /// NOTE gcc failed to vectorize this code (aliasing of char?)
    }

    if (pos < size)
    {
        UInt64 rand = rng();
        for (; pos < size; ++pos)
        {
            data[pos] = static_cast<char>(32 + ((static_cast<UInt16>(rand) * 95) >> 16));
            rand >>= 16;
        }
    }
}

template <typename T>
T randomInteger(pcg64 & rng)
{
    if constexpr (sizeof(T) <= 8)
        return T(rng());
    else if constexpr (sizeof(T) == 16)
        return T({rng(), rng()});
    else if constexpr (sizeof(T) == 32)
        return T({rng(), rng(), rng(), rng()});
    else
        static_assert(false, "unexpected sizeof(T)");
}

/// Pick a random bit width and randomize only so many lowest bits, so that small numbers are more likely.
/// Otherwise near-zero values for 64-bit types would effectively never be generated, and many code paths wouldn't be hit.
/// Set the remaining upper bits to either all 0 or all 1 (i.e. negative number close to 0).
///
/// Implementation works on a UInt64 representation to avoid UB (signed overflow, shift-into-sign-bit).
/// For wide integers, we apply the same logic to the lowest 64-bit word and extend the sign to upper words.
template <typename T>
T fuzzyRandomInteger(pcg64 & rng)
{
    T number = randomInteger<T>(rng);

    size_t num_bits = rng() % (sizeof(T) * 8);

    if constexpr (sizeof(T) <= sizeof(UInt64))
    {
        UInt64 low_mask = num_bits == 64 ? ~UInt64(0) : (UInt64(1) << num_bits) - 1;
        UInt64 u_number = static_cast<UInt64>(static_cast<std::make_unsigned_t<T>>(number));
        UInt64 sign = -(u_number >> (sizeof(T) * 8 - 1));
        return static_cast<T>((u_number & low_mask) | (sign & ~low_mask));
    }
    else
    {
        /// For wide integers, build the mask and sign-extension word by word.
        /// Each word is 64 bits. We process from the least significant word upward.
        constexpr size_t num_words = sizeof(T) / sizeof(UInt64);
        UInt64 words[num_words];
        memcpy(words, &number, sizeof(T));

        UInt64 sign_word = (words[num_words - 1] >> 63) ? ~UInt64(0) : UInt64(0);

        for (size_t w = 0; w < num_words; ++w)
        {
            size_t word_lo = w * 64;
            size_t word_hi = word_lo + 64;

            if (num_bits >= word_hi)
            {
                /// This entire word is within the random part — keep as-is.
            }
            else if (num_bits <= word_lo)
            {
                /// This entire word is above the random part — fill with sign.
                words[w] = sign_word;
            }
            else
            {
                /// The boundary falls within this word.
                size_t bits_in_word = num_bits - word_lo;
                UInt64 low_mask = (UInt64(1) << bits_in_word) - 1;
                words[w] = (words[w] & low_mask) | (sign_word & ~low_mask);
            }
        }

        T result{};
        memcpy(&result, words, sizeof(T));
        return result;
    }
}

/// Note: only sizeof(T) matters, it's ok to e.g. use T = UInt64 for Int64 column to reduce the number of template instantiations.
template <typename T>
void fillBufferWithRandomNumbers(char * __restrict data, size_t count, pcg64 & rng, bool fuzzy)
{
    T * values = reinterpret_cast<T*>(data);
    if (fuzzy)
    {
        for (size_t i = 0; i < count; ++i)
            values[i] = fuzzyRandomInteger<T>(rng);
    }
    else
    {
        fillBufferWithRandomBytes(data, count * sizeof(T), rng);

        /// Byteswap each value so that the generated *number* is the same on big-endian and little-endian machines (for the same seed).
        /// (Why not store rng outputs in native endianness in the first place?
        ///  For non-64 bit values it would produce results different from previous versions of clickhouse.
        ///  Maybe that would be ok, we'd just have to update all the tests that rely on it.)
        if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1)
        {
            for (size_t i = 0; i < count; ++i)
                values[i] = unalignedLoadLittleEndian<T>(&values[i]);
        }
    }
}

template <typename T>
void fillRandomDecimals(char * __restrict data, size_t count, T range, pcg64 & rng, bool fuzzy)
{
    T * values = reinterpret_cast<T*>(data);
    if (fuzzy)
    {
        /// Ignore range because out-of-range values can appear in practice, e.g. `toDecimal32(2000000000, 0)`.
        for (size_t i = 0; i < count; ++i)
            values[i] = fuzzyRandomInteger<T>(rng);
    }
    else
    {
        for (size_t i = 0; i != count; ++i)
            /// Note: '%' preserves sign, so we get value in [-range + 1, range - 1].
            values[i] = randomInteger<T>(rng) % range;
    }
}

void appendFuzzyRandomString(ColumnString::Chars & out, size_t max_length, pcg64 & rng)
{
    const size_t initial_size = out.size();
    /// With probability 1/3 generate some special value like date or decimal.
    UInt64 which = rng() % 18;
    switch (which)
    {
        case 0: // Date
        case 1: // DateTime64
        {
            /// Each component possibly slightly out of valid range.
            UInt64 year = rng() % (2302 - 1898 + 1) + 1898;
            UInt64 month = rng() % 14;
            UInt64 day = rng() % 33;
            String s = fmt::format("{}-{}-{}", year, month, day);
            if (which == 1)
            {
                UInt64 hour = rng() % 25;
                UInt64 minute = rng() % 61;
                UInt64 second = rng() % 61;
                UInt64 scale = rng() % 11;
                s += fmt::format(" {}:{}:{}", hour, minute, second);
                if (scale > 0)
                    s += fmt::format(".{:0{}}", rng() % intExp10(static_cast<int>(scale)), static_cast<int>(scale));
            }
            out.insert(out.end(), s.begin(), s.end());
            break;
        }
        case 2: // UUID
        {
            auto s = formatUUID(UUID(fuzzyRandomInteger<UInt128>(rng)));
            out.insert(out.end(), s.begin(), s.end());
            break;
        }
        case 3: // IPv4
        {
            WriteBufferFromVector<ColumnString::Chars> buf(out, AppendModeTag{});
            writeIPv4Text(IPv4(fuzzyRandomInteger<UInt32>(rng)), buf);
            buf.finalize();
            break;
        }
        case 4: // IPv6
        {
            WriteBufferFromVector<ColumnString::Chars> buf(out, AppendModeTag{});
            writeIPv6Text(IPv6(fuzzyRandomInteger<UInt128>(rng)), buf);
            buf.finalize();
            break;
        }
        case 5: // type name
        {
            String s = FunctionGenerateRandomStructure::generateRandomDataType(rng, /*allow_suspicious_lc_types=*/ true, /*allow_complex_types=*/ rng() % 2);
            out.insert(out.end(), s.begin(), s.end());
            break;
        }
        default: break;
    }

    size_t size = out.size() - initial_size;
    if (size > 0)
    {
        if (size > max_length)
        {
            size = max_length;
            out.resize(initial_size + size);
        }
    }
    else
    {
        /// Just generate some characters at random.

        size = rng() % (max_length + 1);
        /// Generate short size more often.
        UInt64 shift = rng() % (65 - getLeadingZeroBits(max_length));
        size &= (shift < 64 ? (1ull << shift) : 0) - 1;

        out.resize(initial_size + size);
        if (size == 0)
            return;
        /// Pick from a few alphabets.
        switch (rng() % 3)
        {
            case 0: // arbitrary bytes
                fillBufferWithRandomBytes(reinterpret_cast<char *>(out.data() + initial_size), size, rng);
                break;
            case 1: // printable ASCII
                fillBufferWithRandomPrintableASCIIBytes(reinterpret_cast<char *>(out.data() + initial_size), size, rng);
                break;
            case 2: // digits, maybe with a leading '-' and/or a '.' somewhere
            {
                size_t i = 0;
                UInt64 r = rng();

                /// Maybe prepend '-'.
                if (r % 4 == 0)
                {
                    out[initial_size + i] = '-';
                    ++i;
                }
                r >>= 2;

                /// Fill the digits.
                for (; i < size; i += 8)
                {
                    UInt64 t = rng();
                    for (size_t j = 0; j < 8; ++j)
                    {
                        /// Relying on padding, suppress bounds check.
                        out.data()[initial_size + i + j] = char((t & 0xff) % 10 + '0');
                        t >>= 8;
                    }
                }

                /// Maybe insert a '.'.
                if (r % 4 == 0)
                    out[initial_size + (r >> 2) % size] = '.';

                break;
            }
            default: chassert(false);
        }
    }

    /// Randomly add, remove, or overwrite characters sometimes.
    while (rng() % 10 == 0)
    {
        switch (rng() % 3)
        {
            case 0:
                if (size < max_length)
                {
                    ++size;
                    size_t idx = rng() % size;
                    UInt8 ch[1] = {UInt8(rng() & 0xff)};
                    out.insert(out.begin() + initial_size + idx, &ch[0], &ch[0] + 1);
                }
                break;
            case 1:
                if (size > 0)
                {
                    size_t idx = rng() % size;
                    out.erase(out.begin() + initial_size + idx);
                    --size;
                }
                break;
            case 2:
                if (size > 0)
                {
                    size_t idx = rng() % size;
                    out[initial_size + idx] = char(rng() & 0xff);
                }
                break;
            default: chassert(false);
        }
    }
}


size_t estimateValueSize(
    const DataTypePtr type,
    UInt64 max_array_length,
    UInt64 max_string_length,
    UInt64 max_json_dynamic_keys = 5)
{
    if (type->haveMaximumSizeOfValue())
        return type->getMaximumSizeOfValueInMemory();

    TypeIndex idx = type->getTypeId();

    switch (idx)
    {
        case TypeIndex::String:
        {
            return max_string_length + sizeof(UInt64);
        }

        /// The logic in this function should reflect the logic of fillColumnWithRandomData.
        case TypeIndex::Array:
        {
            auto nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType();
            return sizeof(size_t) + estimateValueSize(nested_type, max_array_length / 2, max_string_length, max_json_dynamic_keys);
        }

        case TypeIndex::Map:
        {
            const DataTypePtr & nested_type = typeid_cast<const DataTypeMap &>(*type).getNestedType();
            return sizeof(size_t) + estimateValueSize(nested_type, max_array_length / 2, max_string_length, max_json_dynamic_keys);
        }

        case TypeIndex::Tuple:
        {
            auto elements = typeid_cast<const DataTypeTuple *>(type.get())->getElements();
            const size_t tuple_size = elements.size();
            size_t res = 0;

            for (size_t i = 0; i < tuple_size; ++i)
                res += estimateValueSize(elements[i], max_array_length, max_string_length, max_json_dynamic_keys);

            return res;
        }

        case TypeIndex::Nullable:
        {
            auto nested_type = typeid_cast<const DataTypeNullable &>(*type).getNestedType();
            return 1 + estimateValueSize(nested_type, max_array_length, max_string_length, max_json_dynamic_keys);
        }

        case TypeIndex::LowCardinality:
        {
            auto nested_type = typeid_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            return sizeof(size_t) + estimateValueSize(nested_type, max_array_length, max_string_length, max_json_dynamic_keys);
        }

        case TypeIndex::Dynamic:
        {
            return max_string_length + sizeof(UInt64);
        }

        case TypeIndex::Variant:
        {
            const auto & variants = typeid_cast<const DataTypeVariant &>(*type).getVariants();
            size_t res = 1; // discriminator byte
            for (const auto & v : variants)
                res = std::max(res, estimateValueSize(v, max_array_length, max_string_length, max_json_dynamic_keys));
            return res;
        }

        case TypeIndex::Object:
        {
            const auto & object_type = typeid_cast<const DataTypeObject &>(*type);
            size_t res = 0;
            for (const auto & [path, path_type] : object_type.getTypedPaths())
                res += estimateValueSize(path_type, max_array_length / 2, max_string_length, max_json_dynamic_keys);
            res += max_json_dynamic_keys * (max_string_length + sizeof(UInt64));
            return res;
        }

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'GenerateRandom' is not implemented for type {}", type->getName());
    }
}

}

ColumnPtr fillColumnWithRandomData(
    DataTypePtr type,
    UInt64 limit,
    UInt64 max_array_length,
    UInt64 max_string_length,
    pcg64 & rng,
    bool fuzzy,
    UInt64 max_json_dynamic_keys)
{
    TypeIndex idx = type->getTypeId();

    switch (idx)
    {
        case TypeIndex::String:
        {
            /// Mostly the same as the implementation of randomPrintableASCII function.

            auto column = ColumnString::create();
            ColumnString::Chars & data_to = column->getChars();
            ColumnString::Offsets & offsets_to = column->getOffsets();
            offsets_to.resize(limit);

            IColumn::Offset offset = 0;
            for (size_t row_num = 0; row_num < limit; ++row_num)
            {
                if (fuzzy)
                {
                    appendFuzzyRandomString(data_to, max_string_length, rng);
                    offset = data_to.size();
                }
                else
                {
                    size_t length = rng() % (max_string_length + 1);    /// Slow

                    IColumn::Offset next_offset = offset + length;
                    data_to.resize(next_offset);
                    fillBufferWithRandomPrintableASCIIBytes(reinterpret_cast<char *>(data_to.data() + offset), length, rng);
                    offset = next_offset;
                }
                offsets_to[row_num] = offset;
            }

            return column;
        }

        case TypeIndex::Enum8:
        {
            auto column = ColumnVector<Int8>::create();
            auto values = typeid_cast<const DataTypeEnum<Int8> *>(type.get())->getValues();
            auto & data = column->getData();
            data.resize(limit);

            size_t enum_size = values.size();
            for (UInt64 i = 0; i < limit; ++i)
            {
                size_t off = rng() % enum_size;
                data[i] = values[off].second;
            }

            return column;
        }

        case TypeIndex::Enum16:
        {
            auto column = ColumnVector<Int16>::create();
            auto values = typeid_cast<const DataTypeEnum<Int16> *>(type.get())->getValues();
            auto & data = column->getData();
            data.resize(limit);

            size_t enum_size = values.size();
            for (UInt64 i = 0; i < limit; ++i)
            {
                size_t off = rng() % enum_size;
                data[i] = values[off].second;
            }

            return column;
        }

        case TypeIndex::Array:
        {
            auto nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType();

            auto offsets_column = ColumnVector<ColumnArray::Offset>::create();
            auto & offsets = offsets_column->getData();

            UInt64 offset = 0;
            offsets.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                offset += static_cast<UInt64>(rng()) % (max_array_length + 1);
                offsets[i] = offset;
            }

            /// This division by two makes the size growth subexponential on depth.
            auto data_column = fillColumnWithRandomData(nested_type, offset, max_array_length / 2, max_string_length, rng, fuzzy, max_json_dynamic_keys);

            return ColumnArray::create(data_column, std::move(offsets_column));
        }

        case TypeIndex::Map:
        {
            const DataTypePtr & nested_type = typeid_cast<const DataTypeMap &>(*type).getNestedType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, max_array_length / 2, max_string_length, rng, fuzzy, max_json_dynamic_keys);
            return ColumnMap::create(nested_column);
        }

        case TypeIndex::Tuple:
        {
            auto elements = typeid_cast<const DataTypeTuple *>(type.get())->getElements();
            if (elements.empty())
                return ColumnTuple::create(limit);

            const size_t tuple_size = elements.size();
            Columns tuple_columns(tuple_size);

            for (size_t i = 0; i < tuple_size; ++i)
                tuple_columns[i] = fillColumnWithRandomData(elements[i], limit, max_array_length, max_string_length, rng, fuzzy, max_json_dynamic_keys);

            return ColumnTuple::create(std::move(tuple_columns));
        }

        case TypeIndex::Nullable:
        {
            auto nested_type = typeid_cast<const DataTypeNullable &>(*type).getNestedType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, max_array_length, max_string_length, rng, fuzzy, max_json_dynamic_keys);

            auto null_map_column = ColumnUInt8::create();
            auto & null_map = null_map_column->getData();
            null_map.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
                null_map[i] = rng() % 16 == 0; /// No real motivation for this.

            return ColumnNullable::create(nested_column, std::move(null_map_column));
        }

        case TypeIndex::UInt8:
        {
            auto column = ColumnUInt8::create();
            auto & data = column->getData();
            data.resize(limit);
            if (isBool(type))
            {
                for (size_t i = 0; i < limit; ++i)
                    data[i] = rng() % 2;
            }
            else
            {
                fillBufferWithRandomNumbers<UInt8>(reinterpret_cast<char *>(data.data()), limit, rng, fuzzy);
            }
            return column;
        }
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Date:
        {
            auto column = ColumnUInt16::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt16>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Date32:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);

            if (fuzzy)
                /// Ignore range because out-of-range Date32 values can appear in practice, e.g. `toDate(0) + 2000000000`.
                fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            else
                for (size_t i = 0; i < limit; ++i)
                    column->getData()[i] = (rng() % static_cast<UInt64>(DATE_LUT_SIZE)) - DAYNUM_OFFSET_EPOCH;

            return column;
        }
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::DateTime:
        {
            auto column = ColumnUInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UInt64:
        {
            auto column = ColumnUInt64::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UInt128:
        {
            auto column = ColumnUInt128::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt128>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UInt256:
        {
            auto column = ColumnUInt256::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt256>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UUID:
        {
            auto column = ColumnUUID::create();
            column->getData().resize(limit);
            /// NOTE This is slightly incorrect as random UUIDs should have fixed version 4.
            fillBufferWithRandomNumbers<UInt128>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int8:
        {
            auto column = ColumnInt8::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt8>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int16:
        {
            auto column = ColumnInt16::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt16>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int32:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int64:
        {
            auto column = ColumnInt64::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int128:
        {
            auto column = ColumnInt128::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt128>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int256:
        {
            auto column = ColumnInt256::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt256>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Float32:
        {
            auto column = ColumnFloat32::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Float64:
        {
            auto column = ColumnFloat64::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }

        case TypeIndex::Decimal32:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal32> &>(*type);
            auto column = decimal_type.createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal32> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int32 range = common::exp10_i32(decimal_type.getPrecision());
            fillRandomDecimals<Int32>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::Decimal64:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal64> &>(*type);
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal64> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int64 range = common::exp10_i64(decimal_type.getPrecision());
            fillRandomDecimals<Int64>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::Decimal128:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal128> &>(*type);
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal128> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int128 range = common::exp10_i128(decimal_type.getPrecision());
            fillRandomDecimals<Int128>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::Decimal256:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal256> &>(*type);
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal256> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int256 range = common::exp10_i256(decimal_type.getPrecision());
            fillRandomDecimals<Int256>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::FixedString:
        {
            size_t n = typeid_cast<const DataTypeFixedString &>(*type).getN();
            auto column = ColumnFixedString::create(n);
            column->getChars().resize_fill(limit * n);
            if (fuzzy)
            {
                ColumnString::Chars temp;
                for (size_t row_num = 0; row_num < limit; ++row_num)
                {
                    temp.clear();
                    appendFuzzyRandomString(temp, n, rng);
                    chassert(temp.size() <= n);
                    memcpy(column->getChars().data() + row_num * n, temp.data(), temp.size());
                }
            }
            else
            {
                fillBufferWithRandomBytes(reinterpret_cast<char *>(column->getChars().data()), limit * n, rng);
            }
            return column;
        }
        case TypeIndex::DateTime64:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<DateTime64> &>(*column);
            column_concrete.getData().resize(limit);
            UInt64 range = (1ULL << 32) * intExp10(typeid_cast<const DataTypeDateTime64 &>(*type).getScale());
            if (fuzzy)
                fillRandomDecimals<Int64>(reinterpret_cast<char *>(column_concrete.getData().data()), limit, static_cast<Int64>(range), rng, fuzzy);
            else
            {
                /// Keep the old behavior for non-fuzzy mode: use UInt64 arithmetic
                for (size_t i = 0; i < limit; ++i)
                    column_concrete.getData()[i] = rng() % range;
            }
            return column;
        }
        case TypeIndex::LowCardinality:
        {
            /// We are generating the values using the same random distribution as for full columns
            /// so it's not in fact "low cardinality",
            /// but it's ok for testing purposes, because the LowCardinality data type supports high cardinality data as well.

            auto nested_type = typeid_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, max_array_length, max_string_length, rng, fuzzy, max_json_dynamic_keys);

            auto column = type->createColumn();
            typeid_cast<ColumnLowCardinality &>(*column).insertRangeFromFullColumn(*nested_column, 0, limit);

            return column;
        }
        case TypeIndex::IPv4:
        {
            auto column = ColumnIPv4::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::IPv6:
        {
            auto column = ColumnIPv6::create();
            column->getData().resize(limit);
            /// IPv6 is always stored as big-endian in memory, so we don't use fillBufferWithRandomNumbers here.
            fillBufferWithRandomBytes(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(IPv6), rng);
            return column;
        }

        case TypeIndex::BFloat16:
        {
            auto column = ColumnBFloat16::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt16>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }

        case TypeIndex::Time:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);
            /// Time is stored as seconds in [-999:59:59, 999:59:59] = [-3599999, 3599999].
            /// Filling raw Int32 bits would produce mostly out-of-range values that saturate
            /// to the boundary on text output. Clamp to the valid range instead.
            static constexpr Int32 time_range = 3'599'999;
            for (size_t i = 0; i < limit; ++i)
                column->getData()[i] = static_cast<Int32>(rng() % (2 * time_range + 1)) - time_range;
            return column;
        }

        case TypeIndex::Time64:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Time64> &>(*column);
            column_concrete.getData().resize(limit);
            /// Time64 is stored as Int64 ticks; max value in seconds is 3599999, scaled by 10^scale.
            UInt64 range = static_cast<UInt64>(3'599'999) * intExp10(typeid_cast<const DataTypeTime64 &>(*type).getScale());
            for (size_t i = 0; i < limit; ++i)
                column_concrete.getData()[i] = static_cast<Int64>(rng() % (2 * range + 1)) - static_cast<Int64>(range);
            return column;
        }

        case TypeIndex::Variant:
        {
            const auto & variant_type = typeid_cast<const DataTypeVariant &>(*type);
            const auto & variants = variant_type.getVariants();
            if (variants.empty())
            {
                auto column = type->createColumn();
                column->insertManyDefaults(limit);
                return column;
            }

            auto column = type->createColumn();
            auto & column_variant = typeid_cast<ColumnVariant &>(*column);
            /// One extra outcome produces an untyped NULL (NULL_DISCRIMINATOR) so that the
            /// null state of a bare Variant column is also exercised.
            const size_t num_outcomes = variants.size() + 1;
            for (UInt64 i = 0; i < limit; ++i)
            {
                size_t outcome = rng() % num_outcomes;
                if (outcome == variants.size())
                {
                    column_variant.insertDefault(); // inserts NULL_DISCRIMINATOR
                }
                else
                {
                    auto chosen = static_cast<ColumnVariant::Discriminator>(outcome);
                    auto single = fillColumnWithRandomData(variants[chosen], 1, max_array_length, max_string_length, rng, fuzzy, max_json_dynamic_keys);
                    column_variant.insertIntoVariantFrom(chosen, *single, 0);
                }
            }
            return column;
        }

        case TypeIndex::Dynamic:
        {
            const auto & dynamic_type = typeid_cast<const DataTypeDynamic &>(*type);
            auto column = ColumnDynamic::create(dynamic_type.getMaxDynamicTypes());

            /// Pool of basic scalar types used for random value generation.
            static const DataTypePtr scalar_pool[] = {
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeInt64>(),
                std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeString>(),
            };
            static constexpr size_t pool_size = std::size(scalar_pool);

            for (UInt64 i = 0; i < limit; ++i)
            {
                if (rng() % 16 == 0)
                {
                    column->insertDefault();
                }
                else
                {
                    size_t type_idx = rng() % pool_size;
                    auto single = fillColumnWithRandomData(scalar_pool[type_idx], 1, max_array_length, max_string_length, rng, fuzzy, max_json_dynamic_keys);
                    Field f;
                    single->get(0, f);
                    column->insert(f);
                }
            }
            return column;
        }

        case TypeIndex::Object:
        {
            const auto & object_type = typeid_cast<const DataTypeObject &>(*type);

            /// Fill typed paths by recursing into their declared types.
            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_path_columns;
            for (const auto & [path, path_type] : object_type.getTypedPaths())
            {
                typed_path_columns[path] = fillColumnWithRandomData(
                    path_type, limit, max_array_length / 2, max_string_length, rng, fuzzy, max_json_dynamic_keys)->assumeMutable();
            }

            /// Generate a small number of random dynamic paths, controlled by max_json_dynamic_keys.
            const size_t max_dyn = std::min<size_t>(max_json_dynamic_keys, object_type.getMaxDynamicPaths());
            const size_t num_dyn = max_dyn == 0 ? 0 : rng() % (max_dyn + 1);
            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> dynamic_path_columns;
            const auto & typed_paths = object_type.getTypedPaths();
            auto dyn_type = std::make_shared<DataTypeDynamic>(object_type.getMaxDynamicTypes());
            for (size_t i = 0; i < num_dyn; ++i)
            {
                /// Use a prefix unlikely to collide with user-declared typed paths (e.g. JSON(k0 UInt32)).
                String key = "_rnd_" + std::to_string(i);
                if (typed_paths.contains(key))
                    continue;
                dynamic_path_columns[key] = fillColumnWithRandomData(
                    dyn_type, limit, max_array_length / 2, max_string_length, rng, fuzzy, max_json_dynamic_keys)->assumeMutable();
            }

            /// Shared data: empty Array(Tuple(String, String)) with one empty array per row.
            auto shared_data_col = DataTypeObject::getTypeOfSharedData()->createColumn();
            shared_data_col->insertManyDefaults(limit);

            return ColumnObject::create(
                std::move(typed_path_columns),
                std::move(dynamic_path_columns),
                std::move(shared_data_col),
                object_type.getMaxDynamicPaths(),
                object_type.getMaxDynamicPaths(),
                object_type.getMaxDynamicTypes());
        }

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'GenerateRandom' is not implemented for type {}", type->getName());
    }
}

namespace
{

class GenerateSource final : public ISource
{
public:
    GenerateSource(
        UInt64 block_size_,
        UInt64 max_array_length_,
        UInt64 max_string_length_,
        UInt64 random_seed_,
        Block block_header_,
        ContextPtr context_,
        GenerateRandomStatePtr state_)
        : ISource(std::make_shared<const Block>(Nested::flattenNested(prepareBlockToFill(block_header_))))
        , block_size(block_size_)
        , max_array_length(max_array_length_)
        , max_string_length(max_string_length_)
        , max_json_dynamic_keys(context_->getSettingsRef()[Setting::generate_random_max_json_dynamic_keys])
        , block_to_fill(std::move(block_header_))
        , rng(random_seed_)
        , context(context_)
        , shared_state(state_)
    {
    }

    String getName() const override { return "GenerateRandom"; }

protected:
    Chunk generate() override
    {
        Columns columns;
        columns.reserve(block_to_fill.columns());

        for (const auto & elem : block_to_fill)
            columns.emplace_back(fillColumnWithRandomData(elem.type, block_size, max_array_length, max_string_length, rng, /*fuzzy=*/ false, max_json_dynamic_keys));

        columns = Nested::flattenNested(block_to_fill.cloneWithColumns(columns)).getColumns();

        UInt64 total_rows = shared_state->add_total_rows.fetch_and(0);
        if (total_rows)
            addTotalRowsApprox(total_rows);

        auto chunk = Chunk{std::move(columns), block_size};
        progress(chunk.getNumRows(), chunk.bytes());

        return chunk;
    }

private:
    UInt64 block_size;
    UInt64 max_array_length;
    UInt64 max_string_length;
    UInt64 max_json_dynamic_keys;
    Block block_to_fill;

    pcg64 rng;

    ContextPtr context;
    GenerateRandomStatePtr shared_state;

    static Block & prepareBlockToFill(Block & block)
    {
        /// To support Nested types, we will collect them to single Array of Tuple.
        auto names_and_types = Nested::collect(block.getNamesAndTypesList());
        block.clear();

        for (auto & column : names_and_types)
            block.insert(ColumnWithTypeAndName(column.type, column.name));

        return block;
    }
};

}


StorageGenerateRandom::StorageGenerateRandom(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    UInt64 max_array_length_,
    UInt64 max_string_length_,
    const std::optional<UInt64> & random_seed_)
    : StorageWithCommonVirtualColumns(table_id_), max_array_length(max_array_length_), max_string_length(max_string_length_)
{
    static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;
    static constexpr size_t MAX_STRING_SIZE = 1 << 30;

    if (max_array_length > MAX_ARRAY_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in GenerateRandom: {}, maximum: {}",
                        max_array_length, MAX_ARRAY_SIZE);
    if (max_string_length > MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size in GenerateRandom: {}, maximum: {}",
                        max_string_length, MAX_STRING_SIZE);

    random_seed = random_seed_ ? sipHash64(*random_seed_) : randomSeed();
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageGenerateRandom::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


void registerStorageGenerateRandom(StorageFactory & factory);
void registerStorageGenerateRandom(StorageFactory & factory)
{
    factory.registerStorage("GenerateRandom", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage GenerateRandom requires at most three arguments: "
                            "random_seed, max_string_length, max_array_length.");

        std::optional<UInt64> random_seed;
        UInt64 max_string_length = 10;
        UInt64 max_array_length = 10;

        if (!engine_args.empty())
        {
            engine_args[0] = evaluateConstantExpressionAsLiteral(engine_args[0], args.getLocalContext());
            random_seed = checkAndGetLiteralArgument<UInt64>(engine_args[0], "random_seed");
        }

        if (engine_args.size() >= 2)
        {
            engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.getLocalContext());
            max_string_length = checkAndGetLiteralArgument<UInt64>(engine_args[1], "max_string_length");
        }

        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionAsLiteral(engine_args[2], args.getLocalContext());
            max_array_length = checkAndGetLiteralArgument<UInt64>(engine_args[2], "max_array_length");
        }

        return std::make_shared<StorageGenerateRandom>(args.table_id, args.columns, args.comment, max_array_length, max_string_length, random_seed);
    },
    {},
    Documentation{
        .description = R"DOCS_MD(
The GenerateRandom table engine produces random data for given table schema.

Usage examples:

- Use in test to populate reproducible large table.
- Generate random input for fuzzing tests.

## Usage in ClickHouse Server {#usage-in-clickhouse-server}

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

The `max_array_length` and `max_string_length` parameters specify maximum length of all
array or map columns and strings correspondingly in generated data.

Generate table engine supports only `SELECT` queries.

It supports all [DataTypes](../../../sql-reference/data-types/index.md) that can be stored in a table except `AggregateFunction`.

## Example {#example}

**1.** Set up the `generate_engine_table` table:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Query the data:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Details of Implementation {#details-of-implementation}

- Not supported:
  - `ALTER`
  - `SELECT ... SAMPLE`
  - `INSERT`
  - Indices
  - Replication
)DOCS_MD",
        .syntax = "ENGINE = GenerateRandom([random_seed[, max_string_length[, max_array_length]]])",
        .related = {"FuzzJSON", "FuzzQuery"}});
}

Pipe StorageGenerateRandom::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    const ColumnsDescription & our_columns = storage_snapshot->metadata->getColumns();
    Block block_header;
    for (const auto & name : column_names)
    {
        const auto & name_type = our_columns.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    /// Correction of block size for wide tables.
    size_t preferred_block_size_bytes = context->getSettingsRef()[Setting::preferred_block_size_bytes];
    if (preferred_block_size_bytes)
    {
        UInt64 max_json_dynamic_keys_for_estimate = context->getSettingsRef()[Setting::generate_random_max_json_dynamic_keys];
        size_t estimated_row_size_bytes = estimateValueSize(std::make_shared<DataTypeTuple>(block_header.getDataTypes()), max_array_length, max_string_length, max_json_dynamic_keys_for_estimate);

        size_t estimated_block_size_bytes = 0;
        if (common::mulOverflow(max_block_size, estimated_row_size_bytes, estimated_block_size_bytes))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large estimated block size in GenerateRandom table: its estimation leads to 64bit overflow");

        if (estimated_block_size_bytes > preferred_block_size_bytes)
        {
            max_block_size = static_cast<size_t>(static_cast<double>(max_block_size) * (static_cast<double>(preferred_block_size_bytes) / static_cast<double>(estimated_block_size_bytes)));
            if (max_block_size == 0)
                max_block_size = 1;
        }
    }

    UInt64 query_limit = query_info.trivial_limit;
    if (query_limit && num_streams * max_block_size > query_limit)
    {
        /// We want to avoid spawning more streams than necessary
        num_streams = std::min(num_streams, static_cast<size_t>(((query_limit + max_block_size - 1) / max_block_size)));
    }
    Pipes pipes;
    pipes.reserve(num_streams);

    /// Will create more seed values for each source from initial seed.
    pcg64 generate(random_seed);

    auto shared_state = std::make_shared<GenerateRandomState>(query_info.trivial_limit);

    for (UInt64 i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<GenerateSource>(
            max_block_size, max_array_length, max_string_length, generate(), block_header, context, shared_state);
        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}

}
