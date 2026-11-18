#include <Processors/Formats/Impl/Parquet/parquetBloomFilterHash.h>

#include <Columns/ColumnNullable.h>

#if USE_PARQUET

#include <parquet/metadata.h>
#include <parquet/xxhasher.h>

namespace DB
{

bool isParquetStringTypeSupportedForBloomFilters(
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

bool isParquetIntegerTypeSupportedForBloomFilters(const std::shared_ptr<const parquet::LogicalType> & logical_type, parquet::ConvertedType::type converted_type)
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

std::optional<uint64_t> tryHashStringWithoutCompatibilityCheck(const Field & field)
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

std::optional<uint64_t> tryHashString(
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

std::optional<uint64_t> tryHashFLBA(
    const Field & field,
    const std::shared_ptr<const parquet::LogicalType> & logical_type,
    parquet::ConvertedType::type converted_type,
    std::size_t parquet_column_length)
{
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
