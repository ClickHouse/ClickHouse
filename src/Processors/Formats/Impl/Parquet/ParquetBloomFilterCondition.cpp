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

ParquetBloomFilterCondition::ParquetBloomFilterCondition(const std::vector<RPNElement> & rpn_, const std::vector<DataTypePtr> & data_types_)
: rpn(rpn_), data_types(data_types_)
{
}

bool ParquetBloomFilterCondition::mayBeTrueOnRowGroup(const IndexColumnToColumnBF & column_index_to_column_bf) const
{
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_RANGE
                 || element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            if (element.range.left != element.range.right)
            {
                rpn_stack.emplace_back(true, true);
                continue;
            }

            if (!column_index_to_column_bf.contains(element.key_column))
            {
                rpn_stack.emplace_back(true, true);
                continue;
            }

            if (element.key_column >= data_types.size())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TODO ARTHUR");
            }

            const DataTypePtr actual_type = getPrimitiveType(data_types[element.key_column]);

            auto column = actual_type->createColumn();
            column->insert(element.range.left);

            auto hashed = hash(column.get(), column_index_to_column_bf.at(element.key_column));

            bool maybe_true = maybeTrueOnBloomFilter(hashed, column_index_to_column_bf.at(element.key_column), false);

            rpn_stack.emplace_back(maybe_true, true);

            if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE || element.function == RPNElement::FUNCTION_NOT_IN_SET)
                rpn_stack.back() = !rpn_stack.back();

        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_SET
                 || element.function == RPNElement::FUNCTION_NOT_IN_SET
                 || element.function == RPNElement::ALWAYS_FALSE)
        {
            const auto & set_index = element.set_index;
            const auto & ordered_set = set_index->getOrderedSet();
            const auto & set_column = ordered_set[0];
            const auto hashed = hash(set_column.get(), column_index_to_column_bf.at(element.key_column));

            bool maybe_true = maybeTrueOnBloomFilter(hashed, column_index_to_column_bf.at(element.key_column), false);

            rpn_stack.emplace_back(maybe_true, true);
            if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
                rpn_stack.back() = !rpn_stack.back();
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::mayBeTrueInRange");

    return rpn_stack[0].can_be_true;
}

}

#endif
