#include <Processors/Formats/Impl/Parquet/keyConditionRPNToParquetRPN.h>

#if USE_PARQUET

#include <parquet/metadata.h>
#include <parquet/xxhasher.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

const parquet::ColumnDescriptor * getColumnDescriptorIfBloomFilterIsPresent(
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    std::size_t clickhouse_column_index)
{
    if (clickhouse_column_index_to_parquet_index.size() <= clickhouse_column_index)
    {
        return nullptr;
    }

    const auto & parquet_indexes = clickhouse_column_index_to_parquet_index[clickhouse_column_index].parquet_indexes;

    // complex types like structs, tuples and maps will have more than one index.
    // we don't support those for now
    if (parquet_indexes.size() > 1)
    {
        return nullptr;
    }

    if (parquet_indexes.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Something bad happened, raise an issue and try the query with `input_format_parquet_bloom_filter_push_down=false`");
    }

    auto parquet_column_index = parquet_indexes[0];

    const auto * parquet_column_descriptor = parquet_rg_metadata->schema()->Column(parquet_column_index);

    bool column_has_bloom_filter = parquet_rg_metadata->ColumnChunk(parquet_column_index)->bloom_filter_offset().has_value();
    if (!column_has_bloom_filter)
    {
        return nullptr;
    }

    return parquet_column_descriptor;
}


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

std::optional<uint64_t> tryHash(const Field & field, const parquet::ColumnDescriptor * parquet_column_descriptor)
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

std::optional<std::vector<uint64_t>> hash(const IColumn * data_column, const parquet::ColumnDescriptor * parquet_column_descriptor)
{
    std::vector<uint64_t> hashes;

    for (size_t i = 0u; i < data_column->size(); i++)
    {
        Field f;
        data_column->get(i, f);

        auto hashed_value = tryHash(f, parquet_column_descriptor);

        if (!hashed_value)
        {
            return std::nullopt;
        }

        hashes.emplace_back(*hashed_value);
    }

    return hashes;
}

KeyCondition::RPN keyConditionRPNToParquetRPN(const std::vector<KeyCondition::RPNElement> & rpn,
                                              const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
                                              const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata)
{
    std::vector<KeyCondition::RPNElement> condition_elements;

    using RPNElement = KeyCondition::RPNElement;

    for (const auto & rpn_element : rpn)
    {
        condition_elements.emplace_back(rpn_element);
        // this would be a problem for `where negate(x) = -58`.
        // It would perform a bf search on `-58`, and possibly miss row groups containing this data.
        if (!rpn_element.monotonic_functions_chain.empty())
        {
            continue;
        }

        KeyCondition::BloomFilterData::HashesForColumns hashes;

        if (rpn_element.function == RPNElement::FUNCTION_IN_RANGE
            || rpn_element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            // Only FUNCTION_EQUALS is supported and for that extremes need to be the same
            if (rpn_element.range.left != rpn_element.range.right)
            {
                continue;
            }

            const auto * parquet_column_descriptor =
                getColumnDescriptorIfBloomFilterIsPresent(parquet_rg_metadata, clickhouse_column_index_to_parquet_index, rpn_element.key_column);

            if (!parquet_column_descriptor)
            {
                continue;
            }

            auto hashed_value = tryHash(rpn_element.range.left, parquet_column_descriptor);

            if (!hashed_value)
            {
                continue;
            }

            std::vector<uint64_t> hashes_for_column;
            hashes_for_column.emplace_back(*hashed_value);

            hashes.emplace_back(std::move(hashes_for_column));

            std::vector<std::size_t> key_columns;
            key_columns.emplace_back(rpn_element.key_column);

            condition_elements.back().bloom_filter_data = KeyCondition::BloomFilterData {std::move(hashes), std::move(key_columns)};
        }
        else if (rpn_element.function == RPNElement::FUNCTION_IN_SET
                 || rpn_element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            const auto & set_index = rpn_element.set_index;
            const auto & ordered_set = set_index->getOrderedSet();
            const auto & indexes_mapping = set_index->getIndexesMapping();
            bool found_empty_column = false;

            std::vector<std::size_t> key_columns;

            for (auto i = 0u; i < ordered_set.size(); i++)
            {
                const auto & set_column = ordered_set[i];

                const auto * parquet_column_descriptor = getColumnDescriptorIfBloomFilterIsPresent(
                    parquet_rg_metadata,
                    clickhouse_column_index_to_parquet_index,
                    indexes_mapping[i].key_index);

                if (!parquet_column_descriptor)
                {
                    continue;
                }

                auto column = set_column;

                if (column->empty())
                {
                    found_empty_column = true;
                    break;
                }

                if (const auto & nullable_column = checkAndGetColumn<ColumnNullable>(set_column.get()))
                {
                    column = nullable_column->getNestedColumnPtr();
                }

                auto hashes_for_column_opt = hash(column.get(), parquet_column_descriptor);

                if (!hashes_for_column_opt)
                {
                    continue;
                }

                auto & hashes_for_column = *hashes_for_column_opt;

                if (hashes_for_column.empty())
                {
                    continue;
                }

                hashes.emplace_back(hashes_for_column);

                key_columns.push_back(indexes_mapping[i].key_index);
            }

            if (found_empty_column)
            {
                // todo arthur
                continue;
            }

            if (hashes.empty())
            {
                continue;
            }

            condition_elements.back().bloom_filter_data = {std::move(hashes), std::move(key_columns)};
        }
    }

    return condition_elements;
}

}

#endif
