#include <Processors/Formats/Impl/Parquet/ParquetBloomFilterCondition.h>
#include <iostream>

#if USE_PARQUET

#include <parquet/bloom_filter.h>
#include <parquet/xxhasher.h>
#include <Interpreters/convertFieldToType.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
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

bool maybeTrueOnBloomFilter(const std::vector<uint64_t> & hashes, const std::unique_ptr<parquet::BloomFilter> & bloom_filter)
{
    for (const auto hash : hashes)
    {
        if (bloom_filter->FindHash(hash))
        {
            return true;
        }
    }

    return false;
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

}

ParquetBloomFilterCondition::ParquetBloomFilterCondition(const std::vector<ConditionElement> & condition_, const Block & header_)
    : condition(condition_), header(header_)
{
}

bool ParquetBloomFilterCondition::mayBeTrueOnRowGroup(const ColumnIndexToBF & column_index_to_column_bf) const
{
    using Function = ConditionElement::Function;
    std::vector<BoolMask> rpn_stack;

    for (const auto & element : condition)
    {
        if (element.function == Function::FUNCTION_IN
            || element.function == Function::FUNCTION_NOT_IN)
        {
            bool maybe_true = true;
            for (auto column_index = 0u; column_index < element.hashes_per_column.size(); column_index++)
            {
                // in case bloom filter is not present for this row group
                // https://github.com/ClickHouse/ClickHouse/pull/62966#discussion_r1722361237
                if (!column_index_to_column_bf.contains(element.key_columns[column_index]))
                {
                    rpn_stack.emplace_back(true, true);
                    continue;
                }

                bool column_maybe_contains = maybeTrueOnBloomFilter(
                    element.hashes_per_column[column_index],
                    column_index_to_column_bf.at(element.key_columns[column_index]));

                if (!column_maybe_contains)
                {
                    maybe_true = false;
                    break;
                }
            }

            rpn_stack.emplace_back(maybe_true, true);
            if (element.function == Function::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == Function::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == Function::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == Function::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == Function::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else if (element.function == Function::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else
        {
            rpn_stack.emplace_back(true, true);
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::mayBeTrueOnRowGroup");

    return rpn_stack[0].can_be_true;
}

std::unordered_set<std::size_t> ParquetBloomFilterCondition::getFilteringColumnKeys() const
{
    std::unordered_set<std::size_t> column_keys;

    for (const auto & element : condition)
    {
        for (const auto index : element.key_columns)
        {
            column_keys.insert(index);
        }
    }

    return column_keys;
}

/*
 * `KeyCondition::rpn` is overly complex for bloom filters, some operations are not even supported. Not only that, but to avoid hashing each time
 * we loop over a rpn element, we need to store hashes instead of where predicate values. To address this, we loop over `KeyCondition::rpn`
 * and build a simplified RPN that holds hashes instead of values.
 *
 * `KeyCondition::RPNElement::FUNCTION_IN_RANGE` becomes:
 *      `FUNCTION_IN`
 *      `FUNCTION_UNKNOWN` when range limits are different
 * `KeyCondition::RPNElement::FUNCTION_IN_SET` becomes
 *      `FUNCTION_IN`
 *
 * Complex types and structs are not supported.
 * There are two sources of data types being analyzed, and they need to be compatible: DB::Field type and parquet type.
 * This is determined by the `isColumnSupported` method.
 *
 * Some interesting examples:
 * 1. file(..., 'str_column UInt64') where str_column = 50; Field.type == UInt64. Parquet type string. Not supported.
 * 2. file(...) where str_column = 50; Field.type == String (conversion already taken care by `KeyCondition`). Parquet type string.
 * 3. file(...) where uint32_column = toIPv4(5). Field.type == IPv4. Incompatible column types, resolved by `KeyCondition` itself.
 * 4. file(...) where toIPv4(uint32_column) = toIPv4(5). Field.type == IPv4. We know it is safe to hash it using an int32 API.
 * */
std::vector<ParquetBloomFilterCondition::ConditionElement> keyConditionRPNToParquetBloomFilterCondition(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata)
{
    std::vector<ParquetBloomFilterCondition::ConditionElement> condition_elements;

    using RPNElement = KeyCondition::RPNElement;
    using Function = ParquetBloomFilterCondition::ConditionElement::Function;

    for (const auto & rpn_element : rpn)
    {
        // this would be a problem for `where negate(x) = -58`.
        // It would perform a bf search on `-58`, and possibly miss row groups containing this data.
        if (!rpn_element.monotonic_functions_chain.empty())
        {
            condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
            continue;
        }

        ParquetBloomFilterCondition::ConditionElement::HashesForColumns hashes;

        if (rpn_element.function == RPNElement::FUNCTION_IN_RANGE
            || rpn_element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            // Only FUNCTION_EQUALS is supported and for that extremes need to be the same
            if (rpn_element.range.left != rpn_element.range.right)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            const auto * parquet_column_descriptor =
                getColumnDescriptorIfBloomFilterIsPresent(parquet_rg_metadata, clickhouse_column_index_to_parquet_index, rpn_element.key_column);

            if (!parquet_column_descriptor)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            auto hashed_value = tryHash(rpn_element.range.left, parquet_column_descriptor);

            if (!hashed_value)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            std::vector<uint64_t> hashes_for_column;
            hashes_for_column.emplace_back(*hashed_value);

            hashes.emplace_back(std::move(hashes_for_column));

            auto function = rpn_element.function == RPNElement::FUNCTION_IN_RANGE
                ? ParquetBloomFilterCondition::ConditionElement::Function::FUNCTION_IN
                : ParquetBloomFilterCondition::ConditionElement::Function::FUNCTION_NOT_IN;

            std::vector<std::size_t> key_columns;
            key_columns.emplace_back(rpn_element.key_column);

            condition_elements.emplace_back(function, std::move(hashes), std::move(key_columns));
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
                condition_elements.emplace_back(Function::ALWAYS_FALSE);
                continue;
            }

            if (hashes.empty())
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            auto function = RPNElement::FUNCTION_IN_SET == rpn_element.function ? Function::FUNCTION_IN : Function::FUNCTION_NOT_IN;

            condition_elements.emplace_back(function, hashes, key_columns);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_NOT)
        {
            condition_elements.emplace_back(Function::FUNCTION_NOT);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_OR)
        {
            condition_elements.emplace_back(Function::FUNCTION_OR);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_AND)
        {
            condition_elements.emplace_back(Function::FUNCTION_AND);
        }
        else
        {
            condition_elements.emplace_back(Function::ALWAYS_TRUE);
        }
    }

    return condition_elements;
}

}

#endif
