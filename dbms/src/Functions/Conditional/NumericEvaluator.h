#pragma once

#include <Functions/Conditional/CondException.h>
#include <Functions/Conditional/common.h>
#include <Functions/Conditional/NullMapBuilder.h>
#include <Functions/Conditional/CondSource.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}

namespace Conditional
{

namespace
{

/// This class provides type-independent access to the values of a numeric branch
/// (then, else) column. Returned values have the type TResult.
template <typename TResult>
class NumericSource
{
public:
    virtual TResult get(size_t i) const = 0;
    virtual ~NumericSource() = default;
};

template <typename TResult>
using NumericSourcePtr = std::unique_ptr<NumericSource<TResult>>;

template <typename TResult>
using NumericSources = std::vector<NumericSourcePtr<TResult>>;

/// Column type-specific implementation of NumericSource.
template <typename TResult, typename TType, bool IsConst>
class NumericSourceImpl;

template <typename TResult, typename TType>
class NumericSourceImpl<TResult, TType, true> final : public NumericSource<TResult>
{
public:
    NumericSourceImpl(const Block & block, const ColumnNumbers & args, const Branch & br)
    {
        size_t index = br.index;

        const ColumnPtr & col = block.getByPosition(args[index]).column;
        auto const_col = checkAndGetColumnConst<ColumnVector<TType>>(&*col);
        if (!const_col)
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
        data = const_col->template getValue<TType>();
    }

    TResult get(size_t i) const override
    {
        return static_cast<TResult>(data);
    };

private:
    TType data;
};

template <typename TResult, typename TType>
class NumericSourceImpl<TResult, TType, false> final : public NumericSource<TResult>
{
public:
    NumericSourceImpl(const Block & block, const ColumnNumbers & args, const Branch & br)
        : data_array{initDataArray(block, args, br)}
    {
    }

    TResult get(size_t i) const override
    {
        return static_cast<TResult>(data_array[i]);
    };

private:
    static const PaddedPODArray<TType> & initDataArray(const Block & block,
        const ColumnNumbers & args, const Branch & br)
    {
        size_t index = br.index;
        const ColumnPtr & col = block.getByPosition(args[index]).column;
        const auto * vec_col = typeid_cast<const ColumnVector<TType> *>(&*col);
        if (!vec_col)
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
        return vec_col->getData();
    }

private:
    const PaddedPODArray<TType> & data_array;
};

/// Create a numeric column accessor if TType is the type registered
/// in the specified branch info.
template <typename TResult, typename TType>
class NumericSourceCreator final
{
public:
    static bool execute(NumericSourcePtr<TResult> & source, const Block & block,
        const ColumnNumbers & args, const Branch & br)
    {
        auto type_name = br.type->getName();
        if (TypeName<TType>::get() == type_name)
        {
            if (br.is_const)
                source = std::make_unique<NumericSourceImpl<TResult, TType, true>>(block, args, br);
            else
                source = std::make_unique<NumericSourceImpl<TResult, TType, false>>(block, args, br);
            return true;
        }
        else
            return false;
    }
};

}

/// Processing of multiIf in the case of scalar numeric types.
template <typename TResult>
class NumericEvaluator final
{
public:
    static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
    {
        const CondSources conds = createConds(block, args);
        const NumericSources<TResult> sources = createNumericSources(block, args, branches);
        size_t row_count = conds[0].getSize();
        PaddedPODArray<TResult> & res = createSink(block, result, row_count);

        if (builder)
            builder.init(args);

        for (size_t cur_row = 0; cur_row < row_count; ++cur_row)
        {
            bool has_triggered_cond = false;

            size_t cur_source = 0;
            for (const auto & cond : conds)
            {
                if (cond.get(cur_row))
                {
                    res[cur_row] = sources[cur_source]->get(cur_row);
                    if (builder)
                        builder.update(args[branches[cur_source].index], cur_row);
                    has_triggered_cond = true;
                    break;
                }
                ++cur_source;
            }

            if (!has_triggered_cond)
            {
                res[cur_row] = sources.back()->get(cur_row);
                if (builder)
                    builder.update(args[branches.back().index], cur_row);
            }
        }
    }

private:
    /// Create the result column.
    static PaddedPODArray<TResult> & createSink(Block & block, size_t result, size_t size)
    {
        std::shared_ptr<ColumnVector<TResult>> col_res = std::make_shared<ColumnVector<TResult>>();
        block.getByPosition(result).column = col_res;

        typename ColumnVector<TResult>::Container_t & vec_res = col_res->getData();
        vec_res.resize(size);

        return vec_res;
    }

    /// Create accessors for condition values.
    static CondSources createConds(const Block & block, const ColumnNumbers & args)
    {
        CondSources conds;
        conds.reserve(getCondCount(args));

        for (size_t i = firstCond(); i < elseArg(args); i = nextCond(i))
            conds.emplace_back(block, args, i);
        return conds;
    }

    /// Create accessors for branch values.
    static NumericSources<TResult> createNumericSources(const Block & block,
        const ColumnNumbers & args, const Branches & branches)
    {
        NumericSources<TResult> sources;
        sources.reserve(branches.size());

        for (const auto & br : branches)
        {
            NumericSourcePtr<TResult> source;

            if (! (NumericSourceCreator<TResult, UInt8>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, UInt16>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, UInt32>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, UInt64>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Int8>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Int16>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Int32>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Int64>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Float32>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Float64>::execute(source, block, args, br)
                || NumericSourceCreator<TResult, Null>::execute(source, block, args, br)))
                throw CondException{CondErrorCodes::NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT, toString(br.index)};

            sources.push_back(std::move(source));
        }

        return sources;
    }
};

/// Processing of multiIf in the case of an invalid return type.
template <>
class NumericEvaluator<NumberTraits::Error>
{
public:
    /// For the meaning of the builder parameter, see the FunctionMultiIf::perform() declaration.
    static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
    {
        throw Exception{"Unexpected type in multiIf function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

}

}
