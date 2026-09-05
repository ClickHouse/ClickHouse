#include <Processors/Formats/Impl/NetCDFOutputFormat.h>

#include "config.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Common/intExp.h>
#include <Common/transformEndianness.h>

#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <unordered_set>
#include <vector>

#if USE_ICU
#include <unicode/unorm2.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>
#endif

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_NORMALIZE_STRING;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

constexpr Int32 NC_DIMENSION = 10;
constexpr Int32 NC_VARIABLE = 11;
constexpr Int32 NC_ATTRIBUTE = 12;

/// The name of the only dimension that the data of a table has.
constexpr std::string_view ROW_DIMENSION_NAME = "row";
/// The suffix of the name of the dimension that holds the length of the strings of a column.
constexpr std::string_view STRING_DIMENSION_SUFFIX = "_strlen";

/// Classic NetCDF requires UTF-8 names to be NFC-normalized, so canonically equivalent names
/// have a unique byte representation in the header.
void checkNameIsNFC(const String & name)
{
    if (std::all_of(name.begin(), name.end(), [](unsigned char c) { return c < 0x80; }))
        return;

#if USE_ICU
    UErrorCode error = U_ZERO_ERROR;
    const UNormalizer2 * normalizer = unorm2_getNFCInstance(&error);
    if (U_FAILURE(error))
        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Cannot get the NFC normalizer: {}", u_errorName(error));

    std::vector<UChar> utf16(name.size());
    int32_t utf16_size = 0;
    u_strFromUTF8(
        utf16.data(),
        static_cast<int32_t>(utf16.size()),
        &utf16_size,
        name.data(),
        static_cast<int32_t>(name.size()),
        &error);
    if (U_FAILURE(error))
        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Cannot convert a NetCDF name to UTF-16: {}", u_errorName(error));

    if (!unorm2_isNormalized(normalizer, utf16.data(), utf16_size, &error))
    {
        if (U_FAILURE(error))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Cannot normalize a NetCDF name: {}", u_errorName(error));

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The NetCDF format cannot store the name {} because it is not NFC-normalized", name);
    }
#else
    /// Without ICU we cannot distinguish an NFC name from an equivalent non-normalized spelling.
    /// Reject non-ASCII names rather than write a header that violates the classic NetCDF format.
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "The NetCDF format cannot store the non-ASCII name {} because this build cannot validate NFC normalization", name);
#endif
}

UInt64 alignUpTo4(UInt64 size)
{
    return (size + 3) / 4 * 4;
}

template <typename T>
void writeBigEndian(WriteBuffer & out, T value)
{
    transformEndianness<std::endian::big, std::endian::native>(value);
    out.write(reinterpret_cast<const char *>(&value), sizeof(T));
}

/// Every `NON_NEG` field of the header is a signed 32-bit value in CDF-1 and CDF-2, so a file that
/// has to put a larger number anywhere in its header can only be written as CDF-5.
constexpr UInt64 MAX_CDF2_SIZE = std::numeric_limits<Int32>::max();

/// `NON_NEG` in the specification: a 32-bit value in CDF-1 and CDF-2, and a 64-bit value in CDF-5.
void writeSize(WriteBuffer & out, UInt64 value, UInt8 version)
{
    if (version == 5)
        writeBigEndian<Int64>(out, static_cast<Int64>(value));
    else
        writeBigEndian<Int32>(out, static_cast<Int32>(value));
}

void writePadding(WriteBuffer & out, UInt64 size)
{
    static constexpr char zeros[4] = {};
    out.write(zeros, alignUpTo4(size) - size);
}

void writeNetCDFName(WriteBuffer & out, const String & name, UInt8 version)
{
    writeSize(out, name.size(), version);
    out.write(name.data(), name.size());
    writePadding(out, name.size());
}

void writeAttribute(WriteBuffer & out, std::string_view name, NetCDFType type, UInt64 num_elements, const String & data, UInt8 version)
{
    writeNetCDFName(out, String(name), version);
    writeBigEndian<Int32>(out, static_cast<Int32>(type));
    writeSize(out, num_elements, version);
    out.write(data.data(), data.size());
    writePadding(out, data.size());
}

/// A name that the format cannot store would produce a file that no reader can open. The rules are
/// the ones the reader enforces, plus the NFC normalization, which the reader cannot require of the
/// files of other writers but which this writer can guarantee for the files it produces.
void checkName(const String & name)
{
    String reason = checkNetCDFName(name);
    if (!reason.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The NetCDF format cannot store the name {} because {}", name, reason);

    checkNameIsNFC(name);
}

/// The value that the NetCDF library writes for the data that is not there. It is used here to
/// write the NULLs, together with the `_FillValue` attribute that gives it that meaning.
template <typename T>
String makeFillValue(T value)
{
    transformEndianness<std::endian::big, std::endian::native>(value);
    return String(reinterpret_cast<const char *>(&value), sizeof(T));
}

/// The value that marks a NULL has to be a value that the data itself does not contain, or reading
/// the file back would turn that value into a NULL. The default of the netCDF library is only the
/// first choice, and the search goes over the bit patterns of the type, because the comparison of a
/// reader is on the bytes of a value as they are stored in the file.
///
/// A column of `size` values takes at most `size` distinct values, so one of any `size + 1`
/// candidates is free, and a single pass over the data that marks the candidates it takes in a
/// bitmap of that many bits is enough. The search is therefore linear in the size of the column,
/// and its memory is an eighth of the number of its values, which is a fraction of the column
/// itself, already buffered in whole.
template <typename T>
String chooseFillValue(const T * data, size_t size, const UInt8 * null_map, T preferred)
{
    using Bits = std::conditional_t<sizeof(T) == 1, UInt8,
        std::conditional_t<sizeof(T) == 2, UInt16, std::conditional_t<sizeof(T) == 4, UInt32, UInt64>>>;

    /// Zero means that the domain of the type does not fit in the counter, and then it is larger
    /// than the number of values that any column can hold, so it never runs out.
    static constexpr UInt64 domain_size = UInt64(std::numeric_limits<Bits>::max()) + 1;

    /// The candidates are the bit patterns that follow the preferred one, going down. There is no
    /// point in looking further than the domain of the type.
    UInt64 num_candidates = UInt64(size) + 1;
    if (domain_size != 0)
        num_candidates = std::min(num_candidates, domain_size);

    static constexpr UInt64 bits_in_word = 64;
    const auto first_candidate = std::bit_cast<Bits>(preferred);
    std::vector<UInt64> taken((num_candidates + bits_in_word - 1) / bits_in_word, 0);

    for (size_t i = 0; i < size; ++i)
    {
        if (null_map && null_map[i])
            continue;

        UInt64 offset = static_cast<Bits>(first_candidate - std::bit_cast<Bits>(data[i]));
        if (offset < num_candidates)
            taken[offset / bits_in_word] |= UInt64(1) << (offset % bits_in_word);
    }

    for (size_t word = 0; word < taken.size(); ++word)
    {
        if (taken[word] == std::numeric_limits<UInt64>::max())
            continue;

        /// The bits above the last candidate are zero, so the first free bit of the last word can
        /// be past the end, and then every candidate is taken.
        UInt64 offset = word * bits_in_word + std::countr_one(taken[word]);
        if (offset < num_candidates)
            return makeFillValue<T>(std::bit_cast<T>(static_cast<Bits>(first_candidate - offset)));
    }

    /// The data takes every value of the type, which is only possible for the small types.
    return {};
}

/// The CF conventions name only the units of the scales 0, 3, 6 and 9, and a made-up string in the
/// `units` attribute would not be decoded back into timestamps, so a `DateTime64` of another scale
/// is written in the next finer named unit, with the values multiplied accordingly.
UInt32 getCanonicalTimeScale(UInt32 scale)
{
    return (scale + 2) / 3 * 3;
}

/// The `units` attribute of the CF conventions, which is what tells a reader that the numbers of a
/// variable are dates or times rather than plain numbers.
String getUnits(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Date:
        case TypeIndex::Date32:
            return "days since 1970-01-01";
        case TypeIndex::DateTime:
            return "seconds since 1970-01-01 00:00:00";
        case TypeIndex::DateTime64:
        {
            UInt32 scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
            switch (getCanonicalTimeScale(scale))
            {
                case 0: return "seconds since 1970-01-01 00:00:00";
                case 3: return "milliseconds since 1970-01-01 00:00:00";
                case 6: return "microseconds since 1970-01-01 00:00:00";
                default: return "nanoseconds since 1970-01-01 00:00:00";
            }
        }
        default:
            return {};
    }
}

NetCDFType getNetCDFType(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Enum8:
            return NetCDFType::Byte;
        case TypeIndex::UInt8:
            return NetCDFType::UByte;
        case TypeIndex::Int16:
        case TypeIndex::Enum16:
            return NetCDFType::Short;
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            return NetCDFType::UShort;
        case TypeIndex::Int32:
        case TypeIndex::Date32:
            return NetCDFType::Int;
        case TypeIndex::UInt32:
        case TypeIndex::DateTime:
            return NetCDFType::UInt;
        case TypeIndex::Int64:
        case TypeIndex::DateTime64:
            return NetCDFType::Int64;
        case TypeIndex::UInt64:
            return NetCDFType::UInt64;
        case TypeIndex::Float32:
            return NetCDFType::Float;
        case TypeIndex::Float64:
            return NetCDFType::Double;
        case TypeIndex::String:
        case TypeIndex::FixedString:
            return NetCDFType::Char;
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The type {} cannot be written in the NetCDF format", type->getName());
    }
}

template <typename T>
void writeVectorColumn(WriteBuffer & out, const IColumn & column, const UInt8 * null_map, const String & fill_value)
{
    const auto & data = assert_cast<const ColumnVector<T> &>(column).getData();

    for (size_t i = 0; i < data.size(); ++i)
    {
        if (null_map && null_map[i])
            out.write(fill_value.data(), fill_value.size());
        else
            writeBigEndian<T>(out, data[i]);
    }
}

void writeDateTime64Column(WriteBuffer & out, const IColumn & column, const UInt8 * null_map, const String & fill_value)
{
    const auto & data = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData();

    for (size_t i = 0; i < data.size(); ++i)
    {
        if (null_map && null_map[i])
            out.write(fill_value.data(), fill_value.size());
        else
            writeBigEndian<Int64>(out, data[i].value);
    }
}

/// Picks the value to write the NULLs of a column as, looking at the data that the column holds.
String chooseFillValue(const IColumn & column, const UInt8 * null_map, const String & name)
{
    size_t size = column.size();

    switch (column.getDataType())
    {
        case TypeIndex::Int8:
            return chooseFillValue<Int8>(assert_cast<const ColumnInt8 &>(column).getData().data(), size, null_map, -127);
        case TypeIndex::UInt8:
            return chooseFillValue<UInt8>(assert_cast<const ColumnUInt8 &>(column).getData().data(), size, null_map, 255);
        case TypeIndex::Int16:
            return chooseFillValue<Int16>(assert_cast<const ColumnInt16 &>(column).getData().data(), size, null_map, -32767);
        case TypeIndex::UInt16:
            return chooseFillValue<UInt16>(assert_cast<const ColumnUInt16 &>(column).getData().data(), size, null_map, 65535);
        case TypeIndex::Int32:
            return chooseFillValue<Int32>(assert_cast<const ColumnInt32 &>(column).getData().data(), size, null_map, -2147483647);
        case TypeIndex::UInt32:
            return chooseFillValue<UInt32>(assert_cast<const ColumnUInt32 &>(column).getData().data(), size, null_map, 4294967295U);
        case TypeIndex::Int64:
            return chooseFillValue<Int64>(
                assert_cast<const ColumnInt64 &>(column).getData().data(), size, null_map, -9223372036854775806LL);
        case TypeIndex::UInt64:
            return chooseFillValue<UInt64>(
                assert_cast<const ColumnUInt64 &>(column).getData().data(), size, null_map, 18446744073709551614ULL);
        case TypeIndex::Float32:
            return chooseFillValue<Float32>(
                assert_cast<const ColumnFloat32 &>(column).getData().data(), size, null_map, 9.9692099683868690e+36f);
        case TypeIndex::Float64:
            return chooseFillValue<Float64>(
                assert_cast<const ColumnFloat64 &>(column).getData().data(), size, null_map, 9.9692099683868690e+36);
        case TypeIndex::DateTime64:
        {
            /// A `DateTime64` is a 64-bit integer with a scale, and it is written as one.
            const auto & data = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData();
            return chooseFillValue<Int64>(
                reinterpret_cast<const Int64 *>(data.data()), size, null_map, -9223372036854775806LL);
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Unexpected column for the variable {} of the NetCDF format", name);
    }
}

void writeStringColumn(WriteBuffer & out, const IColumn & column, const UInt8 * null_map, UInt64 string_length)
{
    static constexpr char zeros[64] = {};

    for (size_t i = 0; i < column.size(); ++i)
    {
        /// A NULL is written as an empty string. The column under a `ColumnNullable` is allowed to
        /// hold arbitrary garbage in the rows that are NULL, so it must not be looked at.
        std::string_view value;
        if (!null_map || !null_map[i])
            value = column.getDataAt(i);
        UInt64 to_write = std::min<UInt64>(value.size(), string_length);
        writeString(value.substr(0, to_write), out);

        /// A string shorter than the dimension of the variable is padded with zero bytes.
        for (UInt64 rest = string_length - to_write; rest != 0;)
        {
            UInt64 zeros_to_write = std::min<UInt64>(rest, sizeof(zeros));
            out.write(zeros, zeros_to_write);
            rest -= zeros_to_write;
        }
    }
}

}

NetCDFOutputFormat::NetCDFOutputFormat(WriteBuffer & out_, SharedHeader header_)
    : IOutputFormat(header_, out_)
{
    std::unordered_set<String> used_dimension_names;
    dimension_names.emplace_back(ROW_DIMENSION_NAME);
    used_dimension_names.insert(dimension_names.back());

    /// A dimension that has a variable of its own is a dimension of the row space rather than the
    /// length of a string, so the name of the dimension of a string column has to differ from the
    /// name of every column, or the reader would not read that column back as a string.
    for (const auto & column : *header_)
        used_dimension_names.insert(column.name);

    /// A block can carry duplicate column names (`SELECT x, x FROM t`), but a file with two
    /// variables of the same name would not be read back: the reader rejects it as malformed.
    std::unordered_set<String> variable_names;

    for (const auto & column : *header_)
    {
        checkName(column.name);

        if (!variable_names.insert(column.name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The NetCDF format cannot store more than one column named {}", column.name);

        DataTypePtr type = removeLowCardinality(column.type);
        bool is_nullable = type->isNullable();
        type = removeNullable(type);

        Variable variable;
        variable.name = column.name;
        variable.type = getNetCDFType(type);
        variable.is_string = variable.type == NetCDFType::Char;
        variable.units = getUnits(type);
        variable.data = type->createColumn();

        if (type->getTypeId() == TypeIndex::DateTime64)
        {
            UInt32 scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
            variable.time_multiplier = common::exp10_i64(static_cast<int>(getCanonicalTimeScale(scale) - scale));
        }

        /// There is nothing in the format to mark a string as missing: an empty string is the
        /// closest thing, and it is what a NULL is written as. For the other types the value that
        /// the NULLs are written as is chosen in `finalizeImpl`, when the data is all there.
        if (is_nullable)
            variable.null_map = ColumnUInt8::create();

        if (variable.is_string)
        {
            String dimension_name = column.name + String(STRING_DIMENSION_SUFFIX);
            for (size_t attempt = 1; !used_dimension_names.insert(dimension_name).second; ++attempt)
                dimension_name = column.name + String(STRING_DIMENSION_SUFFIX) + "_" + toString(attempt);

            /// The suffix may push the name of a valid column over the length bound of the reader.
            checkName(dimension_name);

            variable.string_dimension_id = dimension_names.size();
            dimension_names.push_back(dimension_name);
        }

        variables.push_back(std::move(variable));
    }
}

void NetCDFOutputFormat::consume(Chunk chunk)
{
    size_t chunk_rows = chunk.getNumRows();
    if (chunk_rows == 0)
        return;

    num_rows += chunk_rows;

    for (size_t i = 0; i < variables.size(); ++i)
    {
        auto & variable = variables[i];

        ColumnPtr column = chunk.getColumns()[i]->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

        if (const auto * nullable = typeid_cast<const ColumnNullable *>(column.get()))
        {
            if (variable.null_map)
                variable.null_map->insertRangeFrom(nullable->getNullMapColumn(), 0, chunk_rows);
            column = nullable->getNestedColumnPtr();
        }
        else if (variable.null_map)
        {
            variable.null_map->insertManyDefaults(chunk_rows);
        }

        variable.data->insertRangeFrom(*column, 0, chunk_rows);
    }
}

void NetCDFOutputFormat::finalizeImpl()
{
    bool needs_64_bit_data = false;

    for (auto & variable : variables)
    {
        if (variable.is_string)
        {
            const auto * fixed_string = typeid_cast<const ColumnFixedString *>(variable.data.get());
            if (fixed_string)
                variable.string_length = fixed_string->getN();

            /// The dimension of a variable of strings is the length of the longest string in the
            /// column. The rows that are NULL are written as empty strings, and the data under
            /// them is arbitrary, so they are not taken into account.
            const UInt8 * null_map = variable.null_map
                ? assert_cast<const ColumnUInt8 &>(*variable.null_map).getData().data()
                : nullptr;
            for (size_t i = 0; i < variable.data->size(); ++i)
            {
                if (null_map && null_map[i])
                    continue;

                std::string_view value = variable.data->getDataAt(i);

                /// A string shorter than the dimension of the variable is padded with zero bytes,
                /// so a value that itself ends in a zero byte cannot be read back intact: every
                /// reader of the format treats its last byte as padding. Refuse to corrupt it.
                if (value.ends_with('\0'))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The NetCDF format cannot store the value of the column {} at the row {} "
                        "because it ends in a zero byte, which is indistinguishable from the padding of shorter strings",
                        variable.name, i);

                if (!fixed_string)
                    variable.string_length = std::max<UInt64>(variable.string_length, value.size());
            }

            /// A dimension of a length of zero is only allowed for the unlimited dimension.
            variable.string_length = std::max<UInt64>(variable.string_length, 1);
            variable.element_size = variable.string_length;
        }
        else
        {
            variable.element_size = netCDFTypeSize(variable.type);

            /// A `DateTime64` of a scale whose unit has no name in the CF conventions is written
            /// in the next finer named unit, so the values are multiplied. The rescale happens
            /// before the value of the NULLs is chosen, because that value has to be absent from
            /// the data as it is stored in the file. The rows that are NULL are written as that
            /// value, and the data under them is arbitrary, so they are not rescaled.
            if (variable.time_multiplier != 1)
            {
                auto & data = assert_cast<ColumnDecimal<DateTime64> &>(*variable.data).getData();
                const UInt8 * null_map = variable.null_map
                    ? assert_cast<const ColumnUInt8 &>(*variable.null_map).getData().data()
                    : nullptr;
                for (size_t i = 0; i < data.size(); ++i)
                {
                    if (null_map && null_map[i])
                        continue;

                    if (common::mulOverflow(data[i].value, variable.time_multiplier, data[i].value))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The NetCDF format cannot store the value of the column {} at the row {}: "
                            "the CF conventions have no name for the unit of its scale, and the value "
                            "does not fit in 64 bits in the next finer named unit", variable.name, i);
                }
            }

            /// The value that the NULLs are written as has to be absent from the data of the
            /// column, so it can only be chosen once the whole column is there.
            if (variable.null_map)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*variable.null_map).getData();
                const bool has_nulls = std::find(null_map.begin(), null_map.end(), 1) != null_map.end();

                /// A column that has no NULLs at all needs no value to write them as: the attribute
                /// is not written, and the column is read back as not Nullable.
                if (has_nulls)
                {
                    variable.fill_value = chooseFillValue(*variable.data, null_map.data(), variable.name);

                    /// The data can only take every value of the type when the type is small, and
                    /// then there is nothing to write a NULL as.
                    if (variable.fill_value.empty())
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The column {} is Nullable and its values take every value of its type, so the NetCDF "
                            "format has no value left to write the NULLs as", variable.name);
                }
            }
        }

        if (common::mulOverflow(num_rows, variable.element_size, variable.size))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                "The size of the variable {} does not fit in 64 bits", variable.name);

        /// The declared size of a variable is written to the header rounded up to four bytes, and
        /// the length of the dimension of a string column is written to the header as well, so the
        /// file has to be CDF-5 as soon as any of them is too large for a 32-bit field. Otherwise
        /// the header would be truncated and the file would not be readable back.
        UInt64 declared_size = num_rows == 0 ? variable.element_size : variable.size;
        needs_64_bit_data |= netCDFTypeRequiresCDF5(variable.type)
            || variable.string_length > MAX_CDF2_SIZE
            || declared_size > MAX_CDF2_SIZE
            || alignUpTo4(declared_size) > MAX_CDF2_SIZE;
    }

    /// The length of the dimension of the rows is a header field of the same kind.
    needs_64_bit_data |= num_rows > MAX_CDF2_SIZE;

    version = needs_64_bit_data ? 5 : 2;

    dimension_lengths.assign(dimension_names.size(), 0);
    dimension_lengths[0] = num_rows;
    for (const auto & variable : variables)
        if (variable.is_string)
            dimension_lengths[variable.string_dimension_id] = variable.string_length;

    /// The offsets of the data are a part of the header, and the size of the header does not depend
    /// on them, so it is measured by writing the header once with the offsets left at zero.
    String measured_header;
    {
        WriteBufferFromString measure_buffer(measured_header);
        writeHeader(measure_buffer);
        measure_buffer.finalize();
    }

    /// A table with no rows is written as a file with the unlimited dimension and no records in it,
    /// because a dimension of a length of zero is only allowed for the unlimited dimension. Then
    /// every variable is a record variable, and the offsets reserve the space of one record even
    /// though nothing is written after the header.
    UInt64 offset = measured_header.size();
    for (auto & variable : variables)
    {
        variable.begin = offset;

        if (common::addOverflow(offset, alignUpTo4(num_rows == 0 ? variable.element_size : variable.size), offset))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "The size of the NetCDF file does not fit in 64 bits");
    }

    writeHeader(out);

    for (const auto & variable : variables)
        writeVariableData(variable);
}

void NetCDFOutputFormat::writeHeader(WriteBuffer & buffer) const
{
    buffer.write("CDF", 3);
    buffer.write(static_cast<char>(version));

    /// The data is written as fixed-size variables over a dimension of a known length, so the file
    /// has no records. A table with no rows is the exception: a dimension of a length of zero is
    /// only allowed for the unlimited dimension, so an empty file has one and no records in it.
    writeSize(buffer, 0, version);

    writeBigEndian<Int32>(buffer, NC_DIMENSION);
    writeSize(buffer, dimension_names.size(), version);
    for (size_t i = 0; i < dimension_names.size(); ++i)
    {
        writeNetCDFName(buffer, dimension_names[i], version);
        writeSize(buffer, dimension_lengths[i], version);
    }

    /// The file has no global attributes.
    writeBigEndian<Int32>(buffer, 0);
    writeSize(buffer, 0, version);

    writeBigEndian<Int32>(buffer, NC_VARIABLE);
    writeSize(buffer, variables.size(), version);

    for (const auto & variable : variables)
    {
        writeNetCDFName(buffer, variable.name, version);

        writeSize(buffer, variable.is_string ? 2 : 1, version);
        writeSize(buffer, 0, version);
        if (variable.is_string)
            writeSize(buffer, variable.string_dimension_id, version);

        size_t num_attributes = (variable.units.empty() ? 0 : 1) + (variable.fill_value.empty() ? 0 : 1);
        writeBigEndian<Int32>(buffer, num_attributes == 0 ? 0 : NC_ATTRIBUTE);
        writeSize(buffer, num_attributes, version);

        if (!variable.fill_value.empty())
            writeAttribute(buffer, "_FillValue", variable.type, 1, variable.fill_value, version);
        if (!variable.units.empty())
            writeAttribute(buffer, "units", NetCDFType::Char, variable.units.size(), variable.units, version);

        writeBigEndian<Int32>(buffer, static_cast<Int32>(variable.type));
        writeSize(buffer, alignUpTo4(num_rows == 0 ? variable.element_size : variable.size), version);
        writeBigEndian<Int64>(buffer, static_cast<Int64>(variable.begin));
    }
}

void NetCDFOutputFormat::writeVariableData(const Variable & variable) const
{
    /// A string column has no `_FillValue`: the NULLs are written as empty strings, so the null map
    /// is needed for it as well.
    const UInt8 * null_map = nullptr;
    if (variable.null_map && (variable.is_string || !variable.fill_value.empty()))
        null_map = assert_cast<const ColumnUInt8 &>(*variable.null_map).getData().data();

    if (variable.is_string)
    {
        writeStringColumn(out, *variable.data, null_map, variable.string_length);
    }
    else
    {
        switch (variable.data->getDataType())
        {
            case TypeIndex::Int8:
                writeVectorColumn<Int8>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::UInt8:
                writeVectorColumn<UInt8>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::Int16:
                writeVectorColumn<Int16>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::UInt16:
                writeVectorColumn<UInt16>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::Int32:
                writeVectorColumn<Int32>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::UInt32:
                writeVectorColumn<UInt32>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::Int64:
                writeVectorColumn<Int64>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::UInt64:
                writeVectorColumn<UInt64>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::Float32:
                writeVectorColumn<Float32>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::Float64:
                writeVectorColumn<Float64>(out, *variable.data, null_map, variable.fill_value);
                break;
            case TypeIndex::DateTime64:
                writeDateTime64Column(out, *variable.data, null_map, variable.fill_value);
                break;
            default:
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Unexpected column for the variable {} of the NetCDF format", variable.name);
        }
    }

    writePadding(out, variable.size);
}


void registerOutputFormatNetCDF(FormatFactory & factory);
void registerOutputFormatNetCDF(FormatFactory & factory)
{
    factory.registerOutputFormat("NetCDF", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings &,
        FormatFilterInfoPtr)
    {
        return std::make_shared<NetCDFOutputFormat>(buf, std::make_shared<const Block>(sample));
    });

    factory.markFormatHasNoAppendSupport("NetCDF");
    factory.markOutputFormatNotTTYFriendly("NetCDF");
    factory.setContentType("NetCDF", "application/x-netcdf");
}

#pragma clang diagnostic pop

}
