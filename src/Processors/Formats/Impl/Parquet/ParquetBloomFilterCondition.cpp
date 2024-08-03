#include <Processors/Formats/Impl/Parquet/ParquetBloomFilterCondition.h>

#if USE_PARQUET

#include <parquet/bloom_filter.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/misc.h>
#include <Interpreters/convertFieldToType.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    parquet::ByteArray createByteArray(std::string_view view, TypeIndex type, uint8_t * buffer, uint32_t buffer_size)
    {
        if (isStringOrFixedString(type))
        {
            return view;
        }
        else
        {
            auto size = static_cast<uint32_t>(std::max(view.size(), sizeof(uint32_t)));
            chassert(size <= buffer_size);
            std::copy(view.begin(), view.end(), buffer);
            return parquet::ByteArray(size, buffer);
        }
    }

    ColumnPtr hash(const IColumn * data_column, const std::unique_ptr<parquet::BloomFilter> & bloom_filter)
    {
        static constexpr uint32_t buffer_size = 32;
        uint8_t buffer[buffer_size] = {0};

        auto column_size = data_column->size();

        auto hashes_column = ColumnUInt64::create(column_size);
        ColumnUInt64::Container & hashes_internal_data = hashes_column->getData();

        for (size_t i = 0; i < column_size; ++i)
        {
            const auto data_view = data_column->getDataAt(i).toView();

            const auto ba = createByteArray(data_view, data_column->getDataType(), buffer, buffer_size);

            const auto hash = bloom_filter->Hash(&ba);

            hashes_internal_data[i] = hash;
        }

        return hashes_column;
    }

    bool maybeTrueOnBloomFilter(ColumnPtr hash_column, const std::unique_ptr<parquet::BloomFilter> & bloom_filter, bool match_all)
    {
        const auto * uint64_column = typeid_cast<const ColumnUInt64 *>(hash_column.get());

        if (!uint64_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash column must be UInt64.");

        const ColumnUInt64::Container & hashes = uint64_column->getData();

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

    DataTypePtr getPrimitiveType(const DataTypePtr & data_type)
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
        {
            if (!typeid_cast<const DataTypeArray *>(array_type->getNestedType().get()))
                return getPrimitiveType(array_type->getNestedType());
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of bloom filter index.", data_type->getName());
        }

        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(data_type.get()))
            return getPrimitiveType(nullable_type->getNestedType());

        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_type.get()))
            return getPrimitiveType(low_cardinality_type->getDictionaryType());

        return data_type;
    }
}

ParquetBloomFilterCondition::ParquetBloomFilterCondition(const std::vector<ConditionElement> & condition_)
: condition(condition_)
{
}

bool ParquetBloomFilterCondition::mayBeTrueOnRowGroup(const IndexColumnToColumnBF & column_index_to_column_bf) const
{
    using Function = ConditionElement::Function;
    std::vector<BoolMask> rpn_stack;

    for (const auto & element : condition)
    {
        if (element.function == Function::FUNCTION_EQUALS
                 || element.function == Function::FUNCTION_NOT_EQUALS)
        {
            bool maybe_true = maybeTrueOnBloomFilter(element.columns[0], column_index_to_column_bf.at(element.key_columns[0]), false);

            rpn_stack.emplace_back(maybe_true, true);

            if (element.function == Function::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();

        }
        else if (element.function == Function::FUNCTION_IN
                 || element.function == Function::FUNCTION_NOT_IN)
        {
            bool maybe_true = true;
            for (auto column_index = 0u; column_index < element.columns.size(); column_index++)
            {
                bool column_maybe_contains = maybeTrueOnBloomFilter(
                    element.columns[column_index],
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::mayBeTrueInRange");

    return rpn_stack[0].can_be_true;
}

std::vector<ParquetBloomFilterCondition::ConditionElement> keyConditionRPNToParquetBloomFilterCondition(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const std::vector<DataTypePtr> & data_types,
    const ParquetBloomFilterCondition::IndexColumnToColumnBF & column_index_to_column_bf)
{
    std::vector<ParquetBloomFilterCondition::ConditionElement> condition_elements;

    using RPNElement = KeyCondition::RPNElement;
    using Function = ParquetBloomFilterCondition::ConditionElement::Function;

    for (const auto & rpn_element : rpn)
    {
        Columns columns;

        if (rpn_element.function == RPNElement::FUNCTION_IN_RANGE
            || rpn_element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            if (rpn_element.range.left != rpn_element.range.right)
            {
                condition_elements.emplace_back(Function::ALWAYS_TRUE);
                continue;
            }

            if (!column_index_to_column_bf.contains(rpn_element.key_column))
            {
                condition_elements.emplace_back(Function::ALWAYS_TRUE);
                continue;
            }

            if (rpn_element.key_column >= data_types.size())
            {
                condition_elements.emplace_back(Function::ALWAYS_TRUE);
                continue;
            }

            const DataTypePtr actual_type = getPrimitiveType(data_types[rpn_element.key_column]);

            auto column = actual_type->createColumn();
            column->insert(rpn_element.range.left);

            auto hashed = hash(column.get(), column_index_to_column_bf.at(rpn_element.key_column));
            columns.emplace_back(std::move(hashed));

            auto function = rpn_element.function == RPNElement::FUNCTION_IN_RANGE
                ? ParquetBloomFilterCondition::ConditionElement::Function::FUNCTION_EQUALS
                : ParquetBloomFilterCondition::ConditionElement::Function::FUNCTION_NOT_EQUALS;

            std::vector<std::size_t> key_columns;
            key_columns.emplace_back(rpn_element.key_column);

            condition_elements.emplace_back(function, std::move(columns), key_columns);
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
                columns.emplace_back(hash(set_column.get(), column_index_to_column_bf.at(indexes_mapping[i].key_index)));
                key_columns.push_back(indexes_mapping[i].key_index);
            }

            auto function = RPNElement::FUNCTION_IN_SET == rpn_element.function ? Function::FUNCTION_IN : Function::FUNCTION_NOT_IN;

            condition_elements.emplace_back(function, columns, key_columns);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_NOT)
        {
            condition_elements.emplace_back(Function::FUNCTION_NOT, columns);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_OR)
        {
            condition_elements.emplace_back(Function::FUNCTION_OR, columns);
        }
        else if (rpn_element.function == RPNElement::FUNCTION_AND)
        {
            condition_elements.emplace_back(Function::FUNCTION_AND, columns);
        }
        else
        {
            condition_elements.emplace_back(Function::ALWAYS_TRUE, columns);
        }
    }

    return condition_elements;
}

}

#endif
