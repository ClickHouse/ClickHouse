// #include <optional>

// #include <Core/Field.h>

// #include <Columns/ColumnsNumber.h>
// #include <Columns/ColumnTuple.h>

// #include <Common/typeid_cast.h>

// #include <DataTypes/DataTypeTuple.h>
// #include <DataTypes/DataTypeNullable.h>

// #include <Parsers/ASTExpressionList.h>
// #include <Parsers/ASTFunction.h>
// #include <Parsers/ASTLiteral.h>

#include <Interpreters/ProbSet.h>
// #include <Interpreters/convertFieldToType.h>
// #include <Interpreters/evaluateConstantExpression.h>
// #include <Interpreters/NullableUtils.h>
// #include <Interpreters/sortBlock.h>
// #include <Interpreters/castColumn.h>
// #include <Interpreters/Context.h>

// #include <Processors/Chunk.h>

// #include <Storages/MergeTree/KeyCondition.h>

// #include <base/range.h>
// #include <base/sort.h>
// #include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

template <typename Method>
void NO_INLINE ProbSet::insertFromBlockImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map,
    ColumnUInt8::Container * out_filter)
{
    if (null_map)
    {
        if (out_filter)
            insertFromBlockImplCase<Method, true, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, true, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
    else
    {
        if (out_filter)
            insertFromBlockImplCase<Method, false, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, false, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
}


template <typename Method, bool has_null_map, bool build_filter>
void NO_INLINE ProbSet::insertFromBlockImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    [[maybe_unused]] ConstNullMapPtr null_map,
    [[maybe_unused]] ColumnUInt8::Container * out_filter)
{
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (build_filter)
                {
                    (*out_filter)[i] = false;
                }
                continue;
            }
        }

        [[maybe_unused]] auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        if constexpr (build_filter)
            (*out_filter)[i] = emplace_result.isInserted();
    }
}

template <typename Method>
void NO_INLINE ProbSet::executeImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    if (null_map)
        executeImplCase<Method, true>(method, key_columns, vec_res, negative, rows, null_map);
    else
        executeImplCase<Method, false>(method, key_columns, vec_res, negative, rows, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE ProbSet::executeImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    Arena pool;
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// NOTE Optimization is not used for consecutive identical strings.

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            vec_res[i] = negative;
        }
        else
        {
            auto find_result = state.findKey(method.data, i, pool);
            vec_res[i] = negative ^ find_result.isFound();
        }
    }
}

void ProbSet::initialize_data(ColumnRawPtrs key_columns) {
    data.initProb(data.chooseMethodProb(key_columns, key_sizes, name_of_filter), size_of_filter, precision);
}

}

