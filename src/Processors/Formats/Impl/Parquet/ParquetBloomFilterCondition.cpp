#include <Processors/Formats/Impl/Parquet/ParquetBloomFilterCondition.h>
#include <iostream>

#if USE_PARQUET

#include <parquet/bloom_filter.h>
#include <parquet/xxhasher.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/misc.h>
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
    template <typename HashingType, typename FieldType>
    uint64_t hashInt(Field & field)
    {
        parquet::XxHasher hasher;
        return hasher.Hash(static_cast<HashingType>(field.safeGet<FieldType>()));
    }

    uint64_t hashString(Field & field)
    {
        parquet::XxHasher hasher;
        parquet::ByteArray ba { field.safeGet<std::string>() };
        return hasher.Hash(&ba);
    }

    std::optional<uint64_t> hash(Field & field, const DataTypePtr & clickhouse_type)
    {
        switch (clickhouse_type->getTypeId())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
                return hashInt<int32_t, uint32_t>(field);
            case TypeIndex::UInt64:
                return hashInt<int64_t, uint64_t>(field);
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
                return hashInt<int32_t, int32_t>(field);
            case TypeIndex::Int64:
                return hashInt<int64_t, int64_t>(field);
            case TypeIndex::String:
            case TypeIndex::FixedString:
                return hashString(field);
            default:
                return std::nullopt;
        }
    }

    std::vector<uint64_t> hash(const IColumn * data_column, const DataTypePtr & clickhouse_type)
    {
        std::vector<uint64_t> hashes;

        for (size_t i = 0u; i < data_column->size(); i++)
        {
            Field f;
            data_column->get(i, f);

            Field converted_field = tryConvertFieldToType(f, *clickhouse_type);

            if (converted_field.isNull())
            {
                return {};
            }

            if (auto hashed_value = hash(converted_field, clickhouse_type))
            {
                hashes.emplace_back(*hashed_value);
            }
        }

        return hashes;
    }

    bool maybeTrueOnBloomFilter(const std::vector<uint64_t> & hashes, const std::unique_ptr<parquet::BloomFilter> & bloom_filter, bool match_all)
    {
        for (const auto hash : hashes)
        {
            bool found = bloom_filter->FindHash(hash);

            if (match_all && !found)
                return false;
            if (!match_all && found)
                return true;
        }

        return match_all;
    }

    bool isClickHouseTypeCompatibleWithParquetIntegerType(const DataTypePtr clickhouse_type)
    {
        return isInteger(clickhouse_type) || isIPv4(clickhouse_type);
    }

    bool isClickHouseTypeCompatibleWithParquetByteType(const DataTypePtr clickhouse_type)
    {
        return isStringOrFixedString(clickhouse_type) || isIPv6(clickhouse_type)
            || isUInt128(clickhouse_type) || isUInt256(clickhouse_type);
    }

    bool isColumnSupported(const DataTypePtr clickhouse_type, const parquet::ColumnDescriptor * column_descriptor)
    {
        if (column_descriptor->converted_type() == parquet::ConvertedType::NONE && column_descriptor->logical_type() != nullptr)
        {
            return true;
        }

        const auto physical_type = column_descriptor->physical_type();
        const auto & logical_type = column_descriptor->logical_type();
        const auto converted_type = column_descriptor->converted_type();

        // there is no logical type over boolean
        if (physical_type == parquet::Type::type::BOOLEAN)
        {
            return true;
        }

        if (physical_type == parquet::Type::type::INT32 || physical_type == parquet::Type::type::INT64)
        {
            if (!isClickHouseTypeCompatibleWithParquetIntegerType(clickhouse_type))
            {
                return false;
            }

            if (!logical_type && parquet::ConvertedType::type::NONE == converted_type)
            {
                return true;
            }

            if (logical_type && logical_type->is_int())
            {
                return true;
            }

            if (converted_type == parquet::ConvertedType::INT_8 || converted_type == parquet::ConvertedType::INT_16
                    || converted_type == parquet::ConvertedType::INT_32 || converted_type == parquet::ConvertedType::INT_64
                    || converted_type == parquet::ConvertedType::UINT_8 || converted_type == parquet::ConvertedType::UINT_16
                    || converted_type == parquet::ConvertedType::UINT_32 || converted_type == parquet::ConvertedType::UINT_64)
            {
                return true;
            }
        }
        else if (physical_type == parquet::Type::type::BYTE_ARRAY || physical_type == parquet::Type::type::FIXED_LEN_BYTE_ARRAY)
        {
            // branching with false and true is weird
            if (!isClickHouseTypeCompatibleWithParquetByteType(clickhouse_type))
            {
                return false;
            }

            if (!logical_type && parquet::ConvertedType::type::NONE == converted_type)
            {
                return true;
            }

            if (logical_type && (logical_type->is_string() || logical_type->is_BSON() || logical_type->is_JSON()))
            {
                return true;
            }

            if (converted_type == parquet::ConvertedType::JSON || converted_type == parquet::ConvertedType::UTF8 || converted_type == parquet::ConvertedType::BSON)
            {
                return true;
            }
        }

        return false;
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
        if (element.function == Function::FUNCTION_EQUALS
                 || element.function == Function::FUNCTION_NOT_EQUALS)
        {
            // in case bloom filter is not present for this row group
            // https://github.com/ClickHouse/ClickHouse/pull/62966#discussion_r1722361237
            if (!column_index_to_column_bf.contains(element.key_columns[0]))
            {
                rpn_stack.emplace_back(true, true);
                continue;
            }

            const auto & bloom_filter = column_index_to_column_bf.at(element.key_columns[0]);

            bool maybe_true = maybeTrueOnBloomFilter(element.hashes_per_column[0], bloom_filter, false);

            rpn_stack.emplace_back(maybe_true, true);

            if (element.function == Function::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == Function::FUNCTION_IN
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
                    column_index_to_column_bf.at(element.key_columns[column_index]),
                    false);

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

std::vector<ParquetBloomFilterCondition::ConditionElement> keyConditionRPNToParquetBloomFilterCondition(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const Block & header,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata)
{
    std::vector<ParquetBloomFilterCondition::ConditionElement> condition_elements;
    const auto & data_types = header.getDataTypes();

    using RPNElement = KeyCondition::RPNElement;
    using Function = ParquetBloomFilterCondition::ConditionElement::Function;

    for (const auto & rpn_element : rpn)
    {
        ParquetBloomFilterCondition::ConditionElement::HashesForColumns hashes;

        if (rpn_element.function == RPNElement::FUNCTION_IN_RANGE
            || rpn_element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            if (rpn_element.range.left != rpn_element.range.right)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            const auto & parquet_indexes = clickhouse_column_index_to_parquet_index[rpn_element.key_column].parquet_indexes;

            // complex types like structs, tuples and maps will have more than one index.
            // we don't support those for now
            if (parquet_indexes.size() > 1)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            if (parquet_indexes.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Something bad happened, raise an issue and try the query with `input_format_parquet_bloom_filter_push_down=false`");
            }

            auto parquet_column_index = parquet_indexes[0];

            bool column_has_bloom_filter = parquet_rg_metadata->ColumnChunk(parquet_column_index)->bloom_filter_offset().has_value();
            if (!column_has_bloom_filter)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            if (rpn_element.key_column >= data_types.size())
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            const DataTypePtr actual_type = removeNullable(data_types[rpn_element.key_column]);

            if (!isColumnSupported(actual_type, parquet_rg_metadata->schema()->Column(parquet_column_index)))
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            auto field = tryConvertFieldToType(rpn_element.range.left, *actual_type);

            if (field.isNull())
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            auto hashed_value = hash(field, actual_type);

            if (!hashed_value)
            {
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN);
                continue;
            }

            std::vector<uint64_t> hashes_for_column;
            hashes_for_column.emplace_back(*hashed_value);

            hashes.emplace_back(std::move(hashes_for_column));

            auto function = rpn_element.function == RPNElement::FUNCTION_IN_RANGE
                ? ParquetBloomFilterCondition::ConditionElement::Function::FUNCTION_EQUALS
                : ParquetBloomFilterCondition::ConditionElement::Function::FUNCTION_NOT_EQUALS;

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

            std::vector<std::size_t> key_columns;

            for (auto i = 0u; i < ordered_set.size(); i++)
            {
                const auto & set_column = ordered_set[i];

                const auto & parquet_indexes = clickhouse_column_index_to_parquet_index[indexes_mapping[i].key_index].parquet_indexes;

                // complex types like structs, tuples and maps will have more than one index.
                // we don't support those for now
                if (parquet_indexes.size() > 1)
                {
                    continue;
                }

                if (parquet_indexes.empty())
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Something bad happened, raise an issue and try the query with `input_format_parquet_bloom_filter_push_down=false`");
                }

                auto parquet_column_index = parquet_indexes[0];

                const DataTypePtr actual_type = removeNullable(data_types[clickhouse_column_index_to_parquet_index[indexes_mapping[i].key_index].clickhouse_index]);

                if (!isColumnSupported(actual_type, parquet_rg_metadata->schema()->Column(parquet_column_index)))
                {
                    continue;
                }

                bool column_has_bloom_filter = parquet_rg_metadata->ColumnChunk(parquet_column_index)->bloom_filter_offset().has_value();
                if (!column_has_bloom_filter)
                {
                    continue;
                }

                auto column = set_column;

                if (const auto & nullable_column = checkAndGetColumn<ColumnNullable>(set_column.get()))
                {
                    column = nullable_column->getNestedColumnPtr();
                }

                auto hashes_for_column = hash(column.get(), actual_type);

                if (hashes_for_column.empty())
                {
                    continue;
                }

                hashes.emplace_back(hashes_for_column);

                key_columns.push_back(indexes_mapping[i].key_index);
            }

            if (hashes.empty())
            {
                // todo maybe it is not necessary to copy push the hashes
                condition_elements.emplace_back(Function::FUNCTION_UNKNOWN, hashes);
                continue;
            }

            auto function = RPNElement::FUNCTION_IN_SET == rpn_element.function ? Function::FUNCTION_IN : Function::FUNCTION_NOT_IN;

            condition_elements.emplace_back(function, hashes, key_columns);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_NOT)
        {
            condition_elements.emplace_back(Function::FUNCTION_NOT, hashes);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_OR)
        {
            condition_elements.emplace_back(Function::FUNCTION_OR, hashes);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_AND)
        {
            condition_elements.emplace_back(Function::FUNCTION_AND, hashes);
        }
        else
        {
            condition_elements.emplace_back(Function::ALWAYS_TRUE, hashes);
        }
    }

    return condition_elements;
}

}

#endif
