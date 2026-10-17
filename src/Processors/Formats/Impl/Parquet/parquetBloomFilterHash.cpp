#include <Processors/Formats/Impl/Parquet/parquetBloomFilterHash.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#if USE_PARQUET

#include <xxhash.h>

namespace DB
{

using Parquet::parq::SchemaElement;
using Parquet::parq::ConvertedType;

namespace
{

uint64_t hashBytes(const void * data, size_t size)
{
/// With XXH_INLINE_ALL (from contrib/xxHash) every XXH function is marked as unused,
/// so any actual use triggers this warning.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
    return XXH_INLINE_XXH64(data, size, /*seed*/ 0);
#pragma clang diagnostic pop
}

template <typename T>
uint64_t hashValue(T value)
{
    return hashBytes(&value, sizeof(T));
}

}

/// A column without a logical or converted type is eligible for both string and integer hashing.
/// A logical type, if present, takes precedence over the legacy converted type.
static bool isParquetStringTypeSupportedForBloomFilters(const SchemaElement & schema_element)
{
    if (schema_element.__isset.logicalType)
    {
        const auto & logical_type = schema_element.logicalType;
        return logical_type.__isset.STRING || logical_type.__isset.JSON || logical_type.__isset.BSON;
    }

    if (schema_element.__isset.converted_type)
    {
        const auto converted_type = schema_element.converted_type;
        return converted_type == ConvertedType::UTF8 || converted_type == ConvertedType::JSON || converted_type == ConvertedType::BSON;
    }

    return true;
}

static bool isParquetIntegerTypeSupportedForBloomFilters(const SchemaElement & schema_element)
{
    if (schema_element.__isset.logicalType)
        return schema_element.logicalType.__isset.INTEGER;

    if (schema_element.__isset.converted_type)
    {
        switch (schema_element.converted_type)
        {
            case ConvertedType::INT_8:
            case ConvertedType::INT_16:
            case ConvertedType::INT_32:
            case ConvertedType::INT_64:
            case ConvertedType::UINT_8:
            case ConvertedType::UINT_16:
            case ConvertedType::UINT_32:
            case ConvertedType::UINT_64:
                return true;
            default:
                return false;
        }
    }

    return true;
}

template <typename T>
uint64_t hashSpecialFLBATypes(const Field & field)
{
    const T & value = field.safeGet<T>();
    return hashBytes(&value, sizeof(T));
};

static std::optional<uint64_t> tryHashStringWithoutCompatibilityCheck(const Field & field)
{
    const auto field_type = field.getType();

    if (field_type != Field::Types::Which::String)
    {
        return std::nullopt;
    }

    const auto & value = field.safeGet<std::string>();
    return hashBytes(value.data(), value.size());
}

static std::optional<uint64_t> tryHashString(const Field & field, const SchemaElement & schema_element)
{
    if (!isParquetStringTypeSupportedForBloomFilters(schema_element))
    {
        return std::nullopt;
    }

    return tryHashStringWithoutCompatibilityCheck(field);
}

static std::optional<uint64_t> tryHashFLBA(const Field & field, const SchemaElement & schema_element)
{
    /// `DECIMAL`-annotated fixed arrays are deliberately ineligible. `parquetTryHashColumn` sees
    /// ClickHouse's native little-endian wide integers, while the Parquet bloom filter hashes the
    /// canonical big-endian decimal bytes; hashing either representation here would mismatch the
    /// other side of the predicate.
    if (!isParquetStringTypeSupportedForBloomFilters(schema_element))
    {
        return std::nullopt;
    }

    const auto field_type = field.getType();

    if (field_type == Field::Types::Which::IPv6 && schema_element.type_length == sizeof(IPv6))
    {
        return hashSpecialFLBATypes<IPv6>(field);
    }

    return tryHashStringWithoutCompatibilityCheck(field);
}

template <typename ParquetPhysicalType>
std::optional<uint64_t> tryHashInt(const Field & field, const SchemaElement & schema_element)
{
    if (!isParquetIntegerTypeSupportedForBloomFilters(schema_element))
    {
        return std::nullopt;
    }

    if (field.getType() == Field::Types::Which::Int64)
    {
        return hashValue(static_cast<ParquetPhysicalType>(field.safeGet<int64_t>()));
    }
    else if (field.getType() == Field::Types::Which::UInt64)
    {
        return hashValue(static_cast<ParquetPhysicalType>(field.safeGet<uint64_t>()));
    }
    else if (field.getType() == Field::Types::IPv4)
    {
        /*
         * In theory, we could accept IPv4 over 64 bits variables. It would only be a problem in case it was hashed using the byte array api
         * with a zero-ed buffer that had a 32 bits variable copied into it.
         *
         * To be on the safe side, accept only in case physical type is 32 bits.
         * */
        if constexpr (std::is_same_v<int32_t, ParquetPhysicalType>)
        {
            return hashValue(static_cast<ParquetPhysicalType>(field.safeGet<IPv4>()));
        }
    }

    return std::nullopt;
}

template <typename ParquetPhysicalType, typename T>
static bool tryHashIntegerColumnTyped(const IColumn & column, std::vector<uint64_t> & hashes)
{
    const auto * typed = checkAndGetColumn<ColumnVector<T>>(&column);
    if (!typed)
        return false;
    for (T value : typed->getData())
        hashes.emplace_back(hashValue(static_cast<ParquetPhysicalType>(value)));
    return true;
}

/// Hash a whole integer column the way `tryHashInt` hashes one query constant. Going through the
/// native data array must produce the same digests as going through `Field`: the `Field` path
/// widens the value to `Int64`/`UInt64` (sign- or zero-extending by the value's own signedness)
/// and then narrows to the physical type, which keeps exactly the low bits - the same result as
/// `static_cast`ing the native value to the physical type directly.
template <typename ParquetPhysicalType>
static bool tryHashIntegerColumn(const IColumn & column, std::vector<uint64_t> & hashes)
{
    return tryHashIntegerColumnTyped<ParquetPhysicalType, Int8>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, Int16>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, Int32>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, Int64>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, UInt8>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, UInt16>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, UInt32>(column, hashes)
        || tryHashIntegerColumnTyped<ParquetPhysicalType, UInt64>(column, hashes);
}

std::optional<uint64_t> parquetTryHashField(const Field & field, const SchemaElement & schema_element)
{
    switch (schema_element.type)
    {
        case Parquet::parq::Type::INT32:
            return tryHashInt<int32_t>(field, schema_element);
        case Parquet::parq::Type::INT64:
            return tryHashInt<int64_t>(field, schema_element);
        case Parquet::parq::Type::BYTE_ARRAY:
            return tryHashString(field, schema_element);
        case Parquet::parq::Type::FIXED_LEN_BYTE_ARRAY:
            return tryHashFLBA(field, schema_element);
        default:
            return std::nullopt;
    }
}

std::optional<std::vector<uint64_t>> parquetTryHashColumn(const IColumn * data_column, const SchemaElement & schema_element)
{
    const IColumn * column = data_column;
    if (const auto & nullable_column = checkAndGetColumn<ColumnNullable>(column))
        column = nullable_column->getNestedColumnPtr().get();

    std::vector<uint64_t> hashes;
    /// Allocate the exact capacity up front rather than growing geometrically via `emplace_back`.
    /// The dictionary-filter pruning path budgets this vector as exactly `size() * sizeof(UInt64)`
    /// against `input_format_parquet_memory_high_watermark` (see `hashDictionaryValues`); a geometric
    /// growth would transiently allocate up to twice that and overshoot the reservation.
    hashes.reserve(column->size());

    /// Hash string columns directly from their underlying buffers. The generic `Field` path below
    /// copies every value into the `std::string` inside `Field` - a heap scratch allocation of up to
    /// the longest value, which the dictionary-filter pruning path does not budget (its reservation in
    /// `hashDictionaryValues` covers only the materialized column, this `hashes` vector, and the
    /// value-set `HashSet`). All the other hashable types (integers, IPv4/IPv6) are stored inline in
    /// `Field` and allocate nothing. The digest must stay identical to what `parquetTryHashField`
    /// produces for query constants: `tryHashString`/`tryHashFLBA` hash the raw value bytes after the
    /// same string-type check, so equal values keep hashing equal on both sides.
    const auto physical_type = schema_element.type;
    if ((physical_type == Parquet::parq::Type::BYTE_ARRAY || physical_type == Parquet::parq::Type::FIXED_LEN_BYTE_ARRAY)
        && (checkAndGetColumn<ColumnString>(column) || checkAndGetColumn<ColumnFixedString>(column)))
    {
        if (!isParquetStringTypeSupportedForBloomFilters(schema_element))
        {
            return std::nullopt;
        }

        for (size_t i = 0u; i < column->size(); i++)
        {
            std::string_view value = column->getDataAt(i);
            hashes.emplace_back(hashBytes(value.data(), value.size()));
        }

        return hashes;
    }

    /// Hash integer columns directly from their data arrays. The generic `Field` path below spends
    /// most of its time constructing a `Field` and re-dispatching on the type for every value; for
    /// dictionaries of hundreds of thousands of entries per row group that dominates the whole
    /// pruning stage. The type check is the same one `tryHashInt` applies per value, hoisted out of
    /// the loop; a column the dispatch below does not recognize (e.g. `IPv4`) falls through to the
    /// generic path unchanged.
    if (physical_type == Parquet::parq::Type::INT32 || physical_type == Parquet::parq::Type::INT64)
    {
        if (isParquetIntegerTypeSupportedForBloomFilters(schema_element))
        {
            bool done = physical_type == Parquet::parq::Type::INT32
                ? tryHashIntegerColumn<int32_t>(*column, hashes)
                : tryHashIntegerColumn<int64_t>(*column, hashes);
            if (done)
                return hashes;
        }
    }

    for (size_t i = 0u; i < column->size(); i++)
    {
        Field f;
        column->get(i, f);

        auto hashed_value = parquetTryHashField(f, schema_element);

        if (!hashed_value)
        {
            return std::nullopt;
        }

        hashes.emplace_back(*hashed_value);
    }

    return hashes;
}

}

#endif
