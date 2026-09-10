#include <Processors/Formats/Impl/Parquet/parquetBloomFilterHash.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#if USE_PARQUET

#include <parquet/metadata.h>
#include <parquet/xxhasher.h>

namespace DB
{

static bool isParquetStringTypeSupportedForBloomFilters(
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type)
{
    if (logical_type &&
        !logical_type->is_none()
        && !(logical_type->is_string() || logical_type->is_BSON() || logical_type->is_JSON()))
    {
        return false;
    }

    if (parquet::ConvertedType::type::NONE != converted_type &&
        !(converted_type == parquet::ConvertedType::JSON || converted_type == parquet::ConvertedType::UTF8
          || converted_type == parquet::ConvertedType::BSON))
    {
        return false;
    }

    return true;
}

static bool isParquetIntegerTypeSupportedForBloomFilters(const std::shared_ptr<const parquet::LogicalType> & logical_type, parquet::ConvertedType::type converted_type)
{
    if (logical_type && !logical_type->is_none() && !logical_type->is_int())
    {
        return false;
    }

    if (parquet::ConvertedType::type::NONE != converted_type && !(converted_type == parquet::ConvertedType::INT_8 || converted_type == parquet::ConvertedType::INT_16
                                                                  || converted_type == parquet::ConvertedType::INT_32 || converted_type == parquet::ConvertedType::INT_64
                                                                  || converted_type == parquet::ConvertedType::UINT_8 || converted_type == parquet::ConvertedType::UINT_16
                                                                  || converted_type == parquet::ConvertedType::UINT_32 || converted_type == parquet::ConvertedType::UINT_64))
    {
        return false;
    }

    return true;
}

template <typename T>
uint64_t hashSpecialFLBATypes(const Field & field)
{
    const T & value = field.safeGet<T>();

    parquet::FLBA flba(reinterpret_cast<const uint8_t*>(&value));

    parquet::XxHasher hasher;

    return hasher.Hash(&flba, sizeof(T));
};

static std::optional<uint64_t> tryHashStringWithoutCompatibilityCheck(const Field & field)
{
    const auto field_type = field.getType();

    if (field_type != Field::Types::Which::String)
    {
        return std::nullopt;
    }

    parquet::XxHasher hasher;
    parquet::ByteArray ba { field.safeGet<std::string>() };

    return hasher.Hash(&ba);
}

static std::optional<uint64_t> tryHashString(
    const Field & field,
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type)
{
    if (!isParquetStringTypeSupportedForBloomFilters(logical_type, converted_type))
    {
        return std::nullopt;
    }

    return tryHashStringWithoutCompatibilityCheck(field);
}

static std::optional<uint64_t> tryHashFLBA(
    const Field & field,
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type,
    std::size_t parquet_column_length)
{
    /// `DECIMAL`-annotated fixed arrays are deliberately ineligible. `parquetTryHashColumn` sees
    /// ClickHouse's native little-endian wide integers, while the Parquet bloom filter hashes the
    /// canonical big-endian decimal bytes; hashing either representation here would mismatch the
    /// other side of the predicate.
    if (!isParquetStringTypeSupportedForBloomFilters(logical_type, converted_type))
    {
        return std::nullopt;
    }

    const auto field_type = field.getType();

    if (field_type == Field::Types::Which::IPv6 && parquet_column_length == sizeof(IPv6))
    {
        return hashSpecialFLBATypes<IPv6>(field);
    }

    return tryHashStringWithoutCompatibilityCheck(field);
}

template <typename ParquetPhysicalType>
std::optional<uint64_t> tryHashInt(const Field & field, const std::shared_ptr<const parquet::LogicalType> & logical_type, parquet::ConvertedType::type converted_type)
{
    if (!isParquetIntegerTypeSupportedForBloomFilters(logical_type, converted_type))
    {
        return std::nullopt;
    }

    parquet::XxHasher hasher;

    if (field.getType() == Field::Types::Which::Int64)
    {
        return hasher.Hash(static_cast<ParquetPhysicalType>(field.safeGet<int64_t>()));
    }
    else if (field.getType() == Field::Types::Which::UInt64)
    {
        return hasher.Hash(static_cast<ParquetPhysicalType>(field.safeGet<uint64_t>()));
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
            return hasher.Hash(static_cast<ParquetPhysicalType>(field.safeGet<IPv4>()));
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
    parquet::XxHasher hasher;
    for (T value : typed->getData())
        hashes.emplace_back(hasher.Hash(static_cast<ParquetPhysicalType>(value)));
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

std::optional<uint64_t> parquetTryHashField(const Field & field, const parquet::ColumnDescriptor * parquet_column_descriptor)
{
    const auto physical_type = parquet_column_descriptor->physical_type();
    const auto & logical_type = parquet_column_descriptor->logical_type();
    const auto converted_type = parquet_column_descriptor->converted_type();

    switch (physical_type)
    {
        case parquet::Type::type::INT32:
            return tryHashInt<int32_t>(field, logical_type, converted_type);
        case parquet::Type::type::INT64:
            return tryHashInt<int64_t>(field, logical_type, converted_type);
        case parquet::Type::type::BYTE_ARRAY:
            return tryHashString(field, logical_type, converted_type);
        case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
            return tryHashFLBA(field, logical_type, converted_type, parquet_column_descriptor->type_length());
        default:
            return std::nullopt;
    }
}

std::optional<std::vector<uint64_t>> parquetTryHashColumn(const IColumn * data_column, const parquet::ColumnDescriptor * parquet_column_descriptor)
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
    const auto physical_type = parquet_column_descriptor->physical_type();
    if ((physical_type == parquet::Type::type::BYTE_ARRAY || physical_type == parquet::Type::type::FIXED_LEN_BYTE_ARRAY)
        && (checkAndGetColumn<ColumnString>(column) || checkAndGetColumn<ColumnFixedString>(column)))
    {
        if (!isParquetStringTypeSupportedForBloomFilters(
                parquet_column_descriptor->logical_type(), parquet_column_descriptor->converted_type()))
        {
            return std::nullopt;
        }

        parquet::XxHasher hasher;
        for (size_t i = 0u; i < column->size(); i++)
        {
            std::string_view value = column->getDataAt(i);
            parquet::ByteArray ba{value};
            hashes.emplace_back(hasher.Hash(&ba));
        }

        return hashes;
    }

    /// Hash integer columns directly from their data arrays. The generic `Field` path below spends
    /// most of its time constructing a `Field` and re-dispatching on the type for every value; for
    /// dictionaries of hundreds of thousands of entries per row group that dominates the whole
    /// pruning stage. The type check is the same one `tryHashInt` applies per value, hoisted out of
    /// the loop; a column the dispatch below does not recognize (e.g. `IPv4`) falls through to the
    /// generic path unchanged.
    if (physical_type == parquet::Type::type::INT32 || physical_type == parquet::Type::type::INT64)
    {
        if (isParquetIntegerTypeSupportedForBloomFilters(
                parquet_column_descriptor->logical_type(), parquet_column_descriptor->converted_type()))
        {
            bool done = physical_type == parquet::Type::type::INT32
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

        auto hashed_value = parquetTryHashField(f, parquet_column_descriptor);

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
