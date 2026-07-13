#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Common/FieldVisitorToString.h>

/// Include this last — see the reason inside
#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVectorData.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
}


template <typename Derived>
class FunctionNumericIndexedVectorHelper
{
public:
    ColumnPtr executeHelper(
        const DataTypePtr index_type,
        const DataTypePtr value_type,
        const Array & parameters,
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const
    {
        return executeResolveIndexType(index_type, value_type, parameters, arguments, result_type, input_rows_count);
    }

    ColumnPtr executeResolveIndexType(
        const DataTypePtr index_type,
        const DataTypePtr value_type,
        const Array & parameters,
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const
    {
        WhichDataType which(index_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeResolveValueType<TYPE>(value_type, parameters, arguments, result_type, input_rows_count);
        FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected index type {}.", index_type->getName());
    }

    template <typename IndexType>
    ColumnPtr executeResolveValueType(
        const DataTypePtr value_type,
        const Array & parameters,
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const
    {
        WhichDataType which(value_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return executeDispatcher<IndexType, TYPE>(parameters, arguments, result_type, input_rows_count);
        FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected value type {}", value_type->getName());
    }

    template <typename IndexType, typename ValueType>
    ColumnPtr executeDispatcher(
        const Array & parameters, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        if (parameters.empty() || applyVisitor(FieldVisitorToString(), parameters[0]) != "BSI")
        {
            return static_cast<const Derived *>(this)->template executeBSI<BSINumericIndexedVector<IndexType, ValueType>>(
                arguments, result_type, input_rows_count);
        }
        else
        {
            /// This is a placeholder for implementing NumericIndexedVector with various storage structures.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported parameters");
        }
    }
};

}
