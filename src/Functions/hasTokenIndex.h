#pragma once

#include <Columns/ColumnVector.h>
#include <Common/StringSearcher.h>
#include "base/defines.h"
#include <Core/ColumnNumbers.h>

#include <Functions/IFunction.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>

#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Interpreters/Context.h>

#include <Storages/MergeTree/MergeTreeIndexGin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_INITIALIZED;
}

template <typename T>
concept FunctionIndexConcept = requires(T t)
{
    { T::name } -> std::convertible_to<std::string_view>;
    typename T::ResultType;
    //{ t.executeImplIndex() } -> std::same_as<typename T::ResultType>;

} && std::is_arithmetic_v<typename T::ResultType>;



template <FunctionIndexConcept Impl>
class FunctionIndex : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    using ResultType = typename Impl::ResultType;
    ContextPtr context;

    explicit FunctionIndex(ContextPtr _context)
        : context(_context)
    {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionIndex<Impl>>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeNumber<ResultType>>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} expects at least 2 arguments", getName());

        const ColumnWithTypeAndName & index_argument = arguments[0];
        const ColumnWithTypeAndName & token_argument = arguments[1];

        chassert(index_argument.column->size() == input_rows_count);
        chassert(token_argument.column->size() == input_rows_count);

        printf("input_rows_count = %zu index->size() = %zu token->size() = %zu context = %p, index_argument %s, token_argument %s\n",
            input_rows_count,
            index_argument.column->size(),
            token_argument.column->size(),
            static_cast<const void *>(context.get()),
            index_argument.type->getName().c_str(),
            token_argument.type->getName().c_str()
        );

        auto col_res = ColumnVector<ResultType>::create();
        PaddedPODArray<ResultType> &vec_res = col_res->getData();
        vec_res.resize(index_argument.column->size());
        std::ranges::fill(vec_res, 0);

        /// This is a not totally save method to ensure that this works.
        /// Apparently when input_rows_count == 0 the indexes are not constructed yet.
        /// This seems to happen when calling interpreter_with_analyzer->GetQueryPlan();
        if (input_rows_count == 0)
            return col_res;

        auto const [skip_indexes, skip_indexes_mutex] = context->getStorageSnapshot();
        if (skip_indexes == nullptr)
            throw Exception(ErrorCodes::NOT_INITIALIZED, "Index function: {} cannot access skip_indexes.", getName());

        // RangesInDataParts parts_with_ranges = indexInfo.parts;
        // UsefulSkipIndexes & skip_indexes = indexInfo.indexes->skip_indexes

        // const auto & index_and_condition = skip_indexes.useful_indices[idx];
        // auto & ranges = parts_with_ranges[part_index];

        // index_helper = index_and_condition.index;
        // part = ranges.data_part
        // skip_indexes = indexes->skip_indexes

        // for idx in skip_indexes

        return col_res;
    }
};


struct HasTokenIndexImpl
{
    static constexpr auto name = "hasTokenIndex";
    using ResultType = UInt8;

};


using FunctionHasTokenIndex = FunctionIndex<HasTokenIndexImpl>;



}
