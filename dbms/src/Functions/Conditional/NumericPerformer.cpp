#include <Functions/Conditional/NumericPerformer.h>
#include <Functions/Conditional/NullMapBuilder.h>
#include <Functions/Conditional/CondException.h>
#include <Functions/Conditional/ArgsInfo.h>
#include <Functions/Conditional/NumericEvaluator.h>
#include <Functions/Conditional/ArrayEvaluator.h>
#include <Functions/NumberTraits.h>
#include <Functions/DataTypeTraits.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}

namespace Conditional
{

namespace
{

struct Category
{
    static const auto NONE = 0;
    static const auto NUMERIC = UINT8_C(1) << 0;
    static const auto NUMERIC_ARRAY = UINT8_C(1) << 1;
    static const auto NULL_VALUE = UINT8_C(1) << 2;
};

/// This class provides a means to collect type information on a branch
/// (then or else) of a multiIf function.
template <typename TType>
struct PredicateBase
{
protected:
    static bool appendBranchInfo(size_t index, const Block & block,
        const ColumnNumbers & args, Branches & branches)
    {
        const IColumn * col = block.safeGetByPosition(args[index]).column.get();

        const ColumnVector<TType> * vec_col = nullptr;
        const ColumnConst<TType> * const_col = nullptr;

        const ColumnArray * arr_col = nullptr;
        const ColumnVector<TType> * arr_vec_col = nullptr;
        const ColumnConstArray * arr_const_col = nullptr;

        Branch branch;

        vec_col = typeid_cast<const ColumnVector<TType> *>(col);
        if (vec_col != nullptr)
            branch.is_const = false;
        else
        {
            const_col = typeid_cast<const ColumnConst<TType> *>(col);
            if (const_col != nullptr)
                branch.is_const = true;
            else
            {
                arr_col = typeid_cast<const ColumnArray *>(col);
                if (arr_col != nullptr)
                {
                    arr_vec_col = typeid_cast<const ColumnVector<TType> *>(&arr_col->getData());
                    if (arr_vec_col != nullptr)
                        branch.is_const = false;
                    else
                        return false;
                }
                else
                {
                    arr_const_col = typeid_cast<const ColumnConstArray *>(col);
                    if (arr_const_col != nullptr)
                    {
                        const IDataType * data = arr_const_col->getDataType().get();
                        const DataTypeArray * arr = typeid_cast<const DataTypeArray *>(data);
                        if (arr == nullptr)
                            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

                        const IDataType * nested_type = arr->getNestedType().get();

                        using ElementType = DataTypeNumber<TType>;

                        if (typeid_cast<const ElementType *>(nested_type) == nullptr)
                            return false;

                        branch.is_const = true;
                    }
                    else
                        return false;
                }
            }
        }

        branch.index = index;
        branch.type = DataTypeTraits::DataTypeFromFieldTypeOrError<TType>::getDataType();

        if ((vec_col != nullptr) || (const_col != nullptr))
            branch.category = Category::NUMERIC;
        else if ((arr_vec_col != nullptr) || (arr_const_col != nullptr))
            branch.category = Category::NUMERIC_ARRAY;
        else
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

        branches.push_back(branch);

        return true;
    }
};

template <>
struct PredicateBase<Null>
{
protected:
    static bool appendBranchInfo(size_t index, const Block & block,
        const ColumnNumbers & args, Branches & branches)
    {
        const IColumn * col = block.safeGetByPosition(args[index]).column.get();
        const ColumnNull * const_col = typeid_cast<const ColumnNull *>(col);

        if (const_col == nullptr)
            return false;

        Branch branch;
        branch.is_const = true;
        branch.index = index;
        branch.type = DataTypeTraits::DataTypeFromFieldTypeOrError<Null>::getDataType();
        branch.category = Category::NULL_VALUE;

        branches.push_back(branch);

        return true;
    }
};

template <>
struct PredicateBase<NumberTraits::Error>
{
};

/// Collect type information on the unique else branch of a multiIf function.
/// Determine the returned value type. Perform the multiIf.
template <typename TResult, typename TType>
struct ElsePredicate final : public PredicateBase<TType>
{
    using Base = PredicateBase<TType>;

    using TCombined = typename NumberTraits::TypeProduct<
        TResult,
        typename NumberTraits::EmbedType<TType>::Type
    >::Type;

    using TFinal = typename NumberTraits::ToOrdinaryType<TCombined>::Type;
    using TFinal2 = typename RemoveNullable<TFinal>::Type;

    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        if (!Base::appendBranchInfo(index, block, args, branches))
            return false;

        /// We have collected all the information we need.
        /// Now perform the multiIf.

        UInt8 category = Category::NONE;
        for (const auto & branch : branches)
            category |= branch.category;

        if (category & Category::NUMERIC)
        {
            if (category & Category::NUMERIC_ARRAY)
                throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
            NumericEvaluator<TFinal2>::perform(branches, block, args, result, builder);
        }
        else if (category & Category::NUMERIC_ARRAY)
            ArrayEvaluator<TFinal2>::perform(branches, block, args, result, builder);
        else
            throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

        return true;
    }
};

/// We cannot have only null branches.
template <typename Nullity>
struct ElsePredicate<NumberTraits::Enriched::Void<Nullity>, Null> final : public PredicateBase<Null>
{
    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

/// Specialization for incorrect type information.
template <typename TResult>
struct ElsePredicate<TResult, NumberTraits::Error> : public PredicateBase<NumberTraits::Error>
{
    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

/// Specialization for incorrect type information.
template <typename TType>
struct ElsePredicate<NumberTraits::Error, TType>
{
    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

/// Collect type information on a then branch of a multiIf function.
/// Update the returned value type information. Go to the next branch.
template <typename TResult, typename TType>
struct ThenPredicate final : public PredicateBase<TType>
{
    using Base = PredicateBase<TType>;

    using TCombined = typename NumberTraits::TypeProduct<
        TResult,
        typename NumberTraits::EmbedType<TType>::Type
    >::Type;

    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        if (!Base::appendBranchInfo(index, block, args, branches))
            return false;

        /// Guess what comes after Then.
        size_t index2 = index + 1;

        if (index2 != elseArg(args))
        {
            /// We have a pair Cond-Then. Process the next Then.
            if (! (ThenPredicate<TCombined, UInt8>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, UInt16>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, UInt32>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, UInt64>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Int8>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Int16>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Int32>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Int64>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Float32>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Float64>::execute(index2 + 1, block, args, result, builder, branches)
                || ThenPredicate<TCombined, Null>::execute(index2 + 1, block, args, result, builder, branches)))
                return false;
        }
        else
        {
            /// We have an Else which ends the multiIf. Process it.
            if (! (ElsePredicate<TCombined, UInt8>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, UInt16>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, UInt32>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, UInt64>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Int8>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Int16>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Int32>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Int64>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Float32>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Float64>::execute(index2, block, args, result, builder, branches)
                || ElsePredicate<TCombined, Null>::execute(index2, block, args, result, builder, branches)))
                return false;
        }

        return true;
    }
};

/// Specialization for incorrect type information.
template <typename TResult>
struct ThenPredicate<TResult, NumberTraits::Error>
{
    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

/// Specialization for incorrect type information.
template <typename TType>
struct ThenPredicate<NumberTraits::Error, TType>
{
    static bool execute(size_t index, Block & block, const ColumnNumbers & args,
        size_t result, NullMapBuilder & builder, Branches & branches)
    {
        throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

/// First Then
struct FirstThenPredicate final
{
    static bool execute(Block & block, const ColumnNumbers & args, size_t result, NullMapBuilder & builder)
    {
        using Void = NumberTraits::Enriched::Void<NumberTraits::HasNoNull>;
        Branches branches;

        return ThenPredicate<Void, UInt8>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, UInt16>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, UInt32>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, UInt64>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Int8>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Int16>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Int32>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Int64>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Float32>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Float64>::execute(firstThen(), block, args, result, builder, branches)
            || ThenPredicate<Void, Null>::execute(firstThen(), block, args, result, builder, branches);
    }
};

}

bool NumericPerformer::perform(Block & block, const ColumnNumbers & args,
    size_t result, NullMapBuilder & builder)
{
    return FirstThenPredicate::execute(block, args, result, builder);
}

}

}
