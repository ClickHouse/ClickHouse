#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/FunctionIfBase.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/castColumn.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

using namespace GatherUtils;

/** Selection function by condition: if(cond, then, else).
  * cond - UInt8
  * then, else - numeric types for which there is a general type, or dates, datetimes, or strings, or arrays of these types.
  */

template <typename A, typename B, typename ResultType>
struct NumIfImpl
{
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayB = typename ColumnVector<B>::Container;
    using ColVecResult = ColumnVector<ResultType>;

    static ColumnPtr vectorVector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        return col_res;
    }

    static ColumnPtr vectorConstant(const ArrayCond & cond, const ArrayA & a, B b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
        return col_res;
    }

    static ColumnPtr constantVector(const ArrayCond & cond, A a, const ArrayB & b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
        return col_res;
    }

    static ColumnPtr constantConstant(const ArrayCond & cond, A a, B b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
        return col_res;
    }
};

template <typename A, typename B, typename R>
struct NumIfImpl<Decimal<A>, Decimal<B>, Decimal<R>>
{
    using ResultType = Decimal<R>;
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = typename ColumnDecimal<Decimal<A>>::Container;
    using ArrayB = typename ColumnDecimal<Decimal<B>>::Container;
    using ColVecResult = ColumnDecimal<ResultType>;
    using Block = ColumnsWithTypeAndName;

    static ColumnPtr vectorVector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        return col_res;
    }

    static ColumnPtr vectorConstant(const ArrayCond & cond, const ArrayA & a, B b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
        return col_res;
    }

    static ColumnPtr constantVector(const ArrayCond & cond, A a, const ArrayB & b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
        return col_res;
    }

    static ColumnPtr constantConstant(const ArrayCond & cond, A a, B b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
        return col_res;
    }
};

template <typename A, typename B>
struct NumIfImpl<A, B, NumberTraits::Error>
{
private:
    [[noreturn]] static void throwError()
    {
        throw Exception("Incompatible types of arguments corresponding to two conditional branches", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
public:
    template <typename... Args> static ColumnPtr vectorVector(Args &&...) { throwError(); }
    template <typename... Args> static ColumnPtr vectorConstant(Args &&...) { throwError(); }
    template <typename... Args> static ColumnPtr constantVector(Args &&...) { throwError(); }
    template <typename... Args> static ColumnPtr constantConstant(Args &&...) { throwError(); }
};


class FunctionIf : public FunctionIfBase</*null_is_false=*/false>
{
public:
    static constexpr auto name = "if";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIf>(); }

private:
    template <typename T0, typename T1>
    static UInt32 decimalScale(const ColumnsWithTypeAndName & arguments [[maybe_unused]])
    {
        if constexpr (IsDecimalNumber<T0> && IsDecimalNumber<T1>)
        {
            UInt32 left_scale = getDecimalScale(*arguments[1].type);
            UInt32 right_scale = getDecimalScale(*arguments[2].type);
            if (left_scale != right_scale)
                throw Exception("Conditional functions with different Decimal scales", ErrorCodes::NOT_IMPLEMENTED);
            return left_scale;
        }
        else
            return std::numeric_limits<UInt32>::max();
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeRightType(
            const ColumnUInt8 * cond_col,
            const ColumnsWithTypeAndName & arguments,
            const ColVecT0 * col_left) const
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        const IColumn * col_right_untyped = arguments[2].column.get();
        UInt32 scale = decimalScale<T0, T1>(arguments);

        if (const auto * col_right_vec = checkAndGetColumn<ColVecT1>(col_right_untyped))
        {
            return NumIfImpl<T0, T1, ResultType>::vectorVector(
                cond_col->getData(), col_left->getData(), col_right_vec->getData(), scale);
        }
        else if (const auto * col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_untyped))
        {
            return NumIfImpl<T0, T1, ResultType>::vectorConstant(
                cond_col->getData(), col_left->getData(), col_right_const->template getValue<T1>(), scale);
        }

        return nullptr;
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeConstRightType(
            const ColumnUInt8 * cond_col,
            const ColumnsWithTypeAndName & arguments,
            const ColumnConst * col_left) const
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        const IColumn * col_right_untyped = arguments[2].column.get();
        UInt32 scale = decimalScale<T0, T1>(arguments);

        if (const auto * col_right_vec = checkAndGetColumn<ColVecT1>(col_right_untyped))
        {
            return NumIfImpl<T0, T1, ResultType>::constantVector(
                cond_col->getData(), col_left->template getValue<T0>(), col_right_vec->getData(), scale);
        }
        else if (const auto * col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_untyped))
        {
            return NumIfImpl<T0, T1, ResultType>::constantConstant(
                cond_col->getData(), col_left->template getValue<T0>(), col_right_const->template getValue<T1>(), scale);
        }

        return nullptr;
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeRightTypeArray(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr result_type,
        [[maybe_unused]] const ColumnArray * col_left_array,
        [[maybe_unused]] size_t input_rows_count) const
    {
        if constexpr (std::is_same_v<NumberTraits::Error, typename NumberTraits::ResultOfIf<T0, T1>::Type>)
            return nullptr;
        else
        {
            using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

            const IColumn * col_right_untyped = arguments[2].column.get();

            if (const auto * col_right_array = checkAndGetColumn<ColumnArray>(col_right_untyped))
            {
                const ColVecT1 * col_right_vec = checkAndGetColumn<ColVecT1>(&col_right_array->getData());
                if (!col_right_vec)
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    NumericArraySource<T0>(*col_left_array),
                    NumericArraySource<T1>(*col_right_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }
            else if (const auto * col_right_const_array = checkAndGetColumnConst<ColumnArray>(col_right_untyped))
            {
                const ColumnArray * col_right_const_array_data = checkAndGetColumn<ColumnArray>(&col_right_const_array->getDataColumn());
                if (!checkColumn<ColVecT1>(&col_right_const_array_data->getData()))
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    NumericArraySource<T0>(*col_left_array),
                    ConstSource<NumericArraySource<T1>>(*col_right_const_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }
        }

        return nullptr;
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeConstRightTypeArray(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] const ColumnConst * col_left_const_array,
        [[maybe_unused]] size_t input_rows_count) const
    {
        if constexpr (std::is_same_v<NumberTraits::Error, typename NumberTraits::ResultOfIf<T0, T1>::Type>)
            return nullptr;
        else
        {
            using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

            const IColumn * col_right_untyped = arguments[2].column.get();

            if (const auto * col_right_array = checkAndGetColumn<ColumnArray>(col_right_untyped))
            {
                const ColVecT1 * col_right_vec = checkAndGetColumn<ColVecT1>(&col_right_array->getData());

                if (!col_right_vec)
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    ConstSource<NumericArraySource<T0>>(*col_left_const_array),
                    NumericArraySource<T1>(*col_right_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }
            else if (const auto * col_right_const_array = checkAndGetColumnConst<ColumnArray>(col_right_untyped))
            {
                const ColumnArray * col_right_const_array_data = checkAndGetColumn<ColumnArray>(&col_right_const_array->getDataColumn());
                if (!checkColumn<ColVecT1>(&col_right_const_array_data->getData()))
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    ConstSource<NumericArraySource<T0>>(*col_left_const_array),
                    ConstSource<NumericArraySource<T1>>(*col_right_const_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }
        }

        return nullptr;
    }

    template <typename T0, typename T1>
    ColumnPtr executeTyped(const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        using ColVecT0 = std::conditional_t<IsDecimalNumber<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
        using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;

        const IColumn * col_left_untyped = arguments[1].column.get();

        bool left_ok = false;
        ColumnPtr right_column = nullptr;

        if (const auto * col_left = checkAndGetColumn<ColVecT0>(col_left_untyped))
        {
            left_ok = true;
            right_column = executeRightType<T0, T1, ColVecT0, ColVecT1>(cond_col, arguments, col_left);
        }
        else if (const auto * col_const_left = checkAndGetColumnConst<ColVecT0>(col_left_untyped))
        {
            left_ok = true;
            right_column = executeConstRightType<T0, T1, ColVecT0, ColVecT1>(cond_col, arguments, col_const_left);
        }
        else if (const auto * col_arr_left = checkAndGetColumn<ColumnArray>(col_left_untyped))
        {
            if (auto col_arr_left_elems = checkAndGetColumn<ColVecT0>(&col_arr_left->getData()))
            {
                left_ok = true;
                right_column = executeRightTypeArray<T0, T1, ColVecT0, ColVecT1>(
                    cond_col, arguments, result_type, col_arr_left, input_rows_count);
            }
        }
        else if (const auto * col_const_arr_left = checkAndGetColumnConst<ColumnArray>(col_left_untyped))
        {
            if (checkColumn<ColVecT0>(&assert_cast<const ColumnArray &>(col_const_arr_left->getDataColumn()).getData()))
            {
                left_ok = true;
                right_column = executeConstRightTypeArray<T0, T1, ColVecT0, ColVecT1>(
                    cond_col, arguments, result_type, col_const_arr_left, input_rows_count);
            }
        }

        if (!left_ok)
            return nullptr;

        const ColumnWithTypeAndName & right_column_typed = arguments[2];
        if (!right_column)
            throw Exception("Illegal column " + right_column_typed.column->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        return right_column;
    }

    static ColumnPtr executeString(const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const IColumn * col_then_untyped = arguments[1].column.get();
        const IColumn * col_else_untyped = arguments[2].column.get();

        const ColumnString * col_then = checkAndGetColumn<ColumnString>(col_then_untyped);
        const ColumnString * col_else = checkAndGetColumn<ColumnString>(col_else_untyped);
        const ColumnFixedString * col_then_fixed = checkAndGetColumn<ColumnFixedString>(col_then_untyped);
        const ColumnFixedString * col_else_fixed = checkAndGetColumn<ColumnFixedString>(col_else_untyped);
        const ColumnConst * col_then_const = checkAndGetColumnConst<ColumnString>(col_then_untyped);
        const ColumnConst * col_else_const = checkAndGetColumnConst<ColumnString>(col_else_untyped);
        const ColumnConst * col_then_const_fixed = checkAndGetColumnConst<ColumnFixedString>(col_then_untyped);
        const ColumnConst * col_else_const_fixed = checkAndGetColumnConst<ColumnFixedString>(col_else_untyped);

        const PaddedPODArray<UInt8> & cond_data = cond_col->getData();
        size_t rows = cond_data.size();

        if (isFixedString(result_type))
        {
            /// The result is FixedString.

            auto col_res_untyped = result_type->createColumn();
            ColumnFixedString * col_res = assert_cast<ColumnFixedString *>(col_res_untyped.get());
            auto sink = FixedStringSink(*col_res, rows);

            if (col_then_fixed && col_else_fixed)
                conditional(FixedStringSource(*col_then_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const_fixed)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed),
                            ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else
                return nullptr;

            return col_res_untyped;
        }

        if (isString(result_type))
        {
            /// The result is String.

            auto col_res = ColumnString::create();
            auto sink = StringSink(*col_res, rows);

            if (col_then && col_else)
                conditional(StringSource(*col_then), StringSource(*col_else), sink, cond_data);
            else if (col_then && col_else_const)
                conditional(StringSource(*col_then), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then_const && col_else)
                conditional(ConstSource<StringSource>(*col_then_const), StringSource(*col_else), sink, cond_data);
            else if (col_then_const && col_else_const)
                conditional(ConstSource<StringSource>(*col_then_const), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then && col_else_fixed)
                conditional(StringSource(*col_then), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else)
                conditional(FixedStringSource(*col_then_fixed), StringSource(*col_else), sink, cond_data);
            else if (col_then_const && col_else_fixed)
                conditional(ConstSource<StringSource>(*col_then_const), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then && col_else_const_fixed)
                conditional(StringSource(*col_then), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), StringSource(*col_else), sink, cond_data);
            else if (col_then_const && col_else_const_fixed)
                conditional(ConstSource<StringSource>(*col_then_const), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then_fixed && col_else_fixed)
                conditional(FixedStringSource(*col_then_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const_fixed)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed),
                            ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else
                return nullptr;

            return col_res;
        }

        return nullptr;
    }

    static ColumnPtr executeGenericArray(const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        /// For generic implementation, arrays must be of same type.
        if (!arguments[1].type->equals(*arguments[2].type))
            return nullptr;

        const IColumn * col_then_untyped = arguments[1].column.get();
        const IColumn * col_else_untyped = arguments[2].column.get();

        const ColumnArray * col_arr_then = checkAndGetColumn<ColumnArray>(col_then_untyped);
        const ColumnArray * col_arr_else = checkAndGetColumn<ColumnArray>(col_else_untyped);
        const ColumnConst * col_arr_then_const = checkAndGetColumnConst<ColumnArray>(col_then_untyped);
        const ColumnConst * col_arr_else_const = checkAndGetColumnConst<ColumnArray>(col_else_untyped);

        const PaddedPODArray<UInt8> & cond_data = cond_col->getData();
        size_t rows = cond_data.size();

        if ((col_arr_then || col_arr_then_const)
            && (col_arr_else || col_arr_else_const))
        {
            auto res = result_type->createColumn();
            auto * col_res = assert_cast<ColumnArray *>(res.get());

            if (col_arr_then && col_arr_else)
                conditional(GenericArraySource(*col_arr_then), GenericArraySource(*col_arr_else), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else if (col_arr_then && col_arr_else_const)
                conditional(GenericArraySource(*col_arr_then), ConstSource<GenericArraySource>(*col_arr_else_const), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else if (col_arr_then_const && col_arr_else)
                conditional(ConstSource<GenericArraySource>(*col_arr_then_const), GenericArraySource(*col_arr_else), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else if (col_arr_then_const && col_arr_else_const)
                conditional(ConstSource<GenericArraySource>(*col_arr_then_const), ConstSource<GenericArraySource>(*col_arr_else_const), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else
                return nullptr;

            return res;
        }

        return nullptr;
    }

    ColumnPtr executeTuple(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        /// Calculate function for each corresponding elements of tuples.

        const ColumnWithTypeAndName & arg1 = arguments[1];
        const ColumnWithTypeAndName & arg2 = arguments[2];

        Columns col1_contents;
        Columns col2_contents;

        if (const ColumnTuple * tuple1 = typeid_cast<const ColumnTuple *>(arg1.column.get()))
            col1_contents = tuple1->getColumnsCopy();
        else if (const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(arg1.column.get()))
            col1_contents = convertConstTupleToConstantElements(*const_tuple);
        else
            return nullptr;

        if (const ColumnTuple * tuple2 = typeid_cast<const ColumnTuple *>(arg2.column.get()))
            col2_contents = tuple2->getColumnsCopy();
        else if (const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(arg2.column.get()))
            col2_contents = convertConstTupleToConstantElements(*const_tuple);
        else
            return nullptr;

        const DataTypeTuple & type1 = static_cast<const DataTypeTuple &>(*arg1.type);
        const DataTypeTuple & type2 = static_cast<const DataTypeTuple &>(*arg2.type);
        const DataTypeTuple & tuple_result = static_cast<const DataTypeTuple &>(*result_type);

        ColumnsWithTypeAndName temporary_columns(3);
        temporary_columns[0] = arguments[0];

        size_t tuple_size = type1.getElements().size();
        Columns tuple_columns(tuple_size);

        for (size_t i = 0; i < tuple_size; ++i)
        {
            temporary_columns[1] = {col1_contents[i], type1.getElements()[i], {}};
            temporary_columns[2] = {col2_contents[i], type2.getElements()[i], {}};

            tuple_columns[i] = executeImpl(temporary_columns, tuple_result.getElements()[i], input_rows_count);
        }

        return ColumnTuple::create(tuple_columns);
    }

    static ColumnPtr executeGeneric(
        const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        /// Convert both columns to the common type (if needed).
        const ColumnWithTypeAndName & arg1 = arguments[1];
        const ColumnWithTypeAndName & arg2 = arguments[2];

        DataTypePtr common_type = getLeastSupertype({arg1.type, arg2.type});

        ColumnPtr col_then = castColumn(arg1, common_type);
        ColumnPtr col_else = castColumn(arg2, common_type);

        MutableColumnPtr result_column = common_type->createColumn();
        result_column->reserve(input_rows_count);

        bool then_is_const = isColumnConst(*col_then);
        bool else_is_const = isColumnConst(*col_else);

        const auto & cond_array = cond_col->getData();

        if (then_is_const && else_is_const)
        {
            const IColumn & then_nested_column = assert_cast<const ColumnConst &>(*col_then).getDataColumn();
            const IColumn & else_nested_column = assert_cast<const ColumnConst &>(*col_else).getDataColumn();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(then_nested_column, 0);
                else
                    result_column->insertFrom(else_nested_column, 0);
            }
        }
        else if (then_is_const)
        {
            const IColumn & then_nested_column = assert_cast<const ColumnConst &>(*col_then).getDataColumn();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(then_nested_column, 0);
                else
                    result_column->insertFrom(*col_else, i);
            }
        }
        else if (else_is_const)
        {
            const IColumn & else_nested_column = assert_cast<const ColumnConst &>(*col_else).getDataColumn();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(*col_then, i);
                else
                    result_column->insertFrom(else_nested_column, 0);
            }
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                result_column->insertFrom(cond_array[i] ? *col_then : *col_else, i);
        }

        return result_column;
    }

    ColumnPtr executeForConstAndNullableCondition(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
    {
        const ColumnWithTypeAndName & arg_cond = arguments[0];
        bool cond_is_null = arg_cond.column->onlyNull();

        ColumnPtr not_const_condition = arg_cond.column;
        bool cond_is_const = false;
        bool cond_is_true = false;
        bool cond_is_false = false;
        if (const auto * const_arg = checkAndGetColumn<ColumnConst>(*arg_cond.column))
        {
            cond_is_const = true;
            not_const_condition = const_arg->getDataColumnPtr();
            ColumnPtr data_column = const_arg->getDataColumnPtr();
            if (const auto * const_nullable_arg = checkAndGetColumn<ColumnNullable>(*data_column))
            {
                data_column = const_nullable_arg->getNestedColumnPtr();
                if (!data_column->empty())
                    cond_is_null = const_nullable_arg->getNullMapData()[0];
            }

            if (!data_column->empty())
            {
                cond_is_true = !cond_is_null && checkAndGetColumn<ColumnUInt8>(*data_column)->getBool(0);
                cond_is_false = !cond_is_null && !cond_is_true;
            }
        }

        const auto & column1 = arguments[1];
        const auto & column2 = arguments[2];

        if (cond_is_true)
            return castColumn(column1, result_type);
        else if (cond_is_false || cond_is_null)
            return castColumn(column2, result_type);

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*not_const_condition))
        {
            ColumnPtr new_cond_column = nullable->getNestedColumnPtr();
            size_t column_size = arg_cond.column->size();

            if (checkAndGetColumn<ColumnUInt8>(*new_cond_column))
            {
                auto nested_column_copy = new_cond_column->cloneResized(new_cond_column->size());
                typeid_cast<ColumnUInt8 *>(nested_column_copy.get())->applyZeroMap(nullable->getNullMapData());
                new_cond_column = std::move(nested_column_copy);

                if (cond_is_const)
                    new_cond_column = ColumnConst::create(new_cond_column, column_size);
            }
            else
                throw Exception("Illegal column " + arg_cond.column->getName() + " of " + getName() + " condition",
                                ErrorCodes::ILLEGAL_COLUMN);

            ColumnsWithTypeAndName temporary_columns
            {
                { new_cond_column, removeNullable(arg_cond.type), arg_cond.name },
                column1,
                column2,
            };

            return executeImpl(temporary_columns, result_type, new_cond_column->size());
        }

        return nullptr;
    }

    static ColumnPtr materializeColumnIfConst(const ColumnPtr & column)
    {
        return column->convertToFullColumnIfConst();
    }

    static ColumnPtr makeNullableColumnIfNot(const ColumnPtr & column)
    {
        auto materialized = materializeColumnIfConst(column);

        if (isColumnNullable(*materialized))
            return materialized;

        return ColumnNullable::create(materialized, ColumnUInt8::create(column->size(), 0));
    }

    /// Return nested column recursively removing Nullable, examples:
    /// Nullable(size = 1, Int32(size = 1), UInt8(size = 1)) -> Int32(size = 1)
    /// Const(size = 0, Nullable(size = 1, Int32(size = 1), UInt8(size = 1))) ->
    /// Const(size = 0, Int32(size = 1))
    static ColumnPtr recursiveGetNestedColumnWithoutNullable(const ColumnPtr & column)
    {
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*column))
        {
            /// Nullable cannot contain Nullable
            return nullable->getNestedColumnPtr();
        }
        else if (const auto * column_const = checkAndGetColumn<ColumnConst>(*column))
        {
            /// Save Constant, but remove Nullable
            return ColumnConst::create(recursiveGetNestedColumnWithoutNullable(column_const->getDataColumnPtr()), column->size());
        }

        return column;
    }

    ColumnPtr executeForNullableThenElse(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const ColumnWithTypeAndName & arg_cond = arguments[0];
        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];

        const auto * then_is_nullable = checkAndGetColumn<ColumnNullable>(*arg_then.column);
        const auto * else_is_nullable = checkAndGetColumn<ColumnNullable>(*arg_else.column);

        if (!then_is_nullable && !else_is_nullable)
            return nullptr;

        /** Calculate null mask of result and nested column separately.
          */
        ColumnPtr result_null_mask;

        {
            ColumnsWithTypeAndName temporary_columns(
            {
                arg_cond,
                {
                    then_is_nullable
                        ? then_is_nullable->getNullMapColumnPtr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                },
                {
                    else_is_nullable
                        ? else_is_nullable->getNullMapColumnPtr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                }
            });

            result_null_mask = executeImpl(temporary_columns, std::make_shared<DataTypeUInt8>(), input_rows_count);
        }

        ColumnPtr result_nested_column;

        {
            ColumnsWithTypeAndName temporary_columns(
            {
                arg_cond,
                {
                    recursiveGetNestedColumnWithoutNullable(arg_then.column),
                    removeNullable(arg_then.type),
                    ""
                },
                {
                    recursiveGetNestedColumnWithoutNullable(arg_else.column),
                    removeNullable(arg_else.type),
                    ""
                }
            });

            result_nested_column = executeImpl(temporary_columns, removeNullable(result_type), temporary_columns.front().column->size());
        }

        return ColumnNullable::create(
            materializeColumnIfConst(result_nested_column), materializeColumnIfConst(result_null_mask));
    }

    ColumnPtr executeForNullThenElse(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const ColumnWithTypeAndName & arg_cond = arguments[0];
        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];

        bool then_is_null = arg_then.column->onlyNull();
        bool else_is_null = arg_else.column->onlyNull();

        if (!then_is_null && !else_is_null)
            return nullptr;

        if (then_is_null && else_is_null)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());

        /// If then is NULL, we create Nullable column with null mask OR-ed with condition.
        if (then_is_null)
        {
            if (cond_col)
            {
                if (isColumnNullable(*arg_else.column))
                {
                    auto arg_else_column = arg_else.column;
                    auto result_column = IColumn::mutate(std::move(arg_else_column));
                    assert_cast<ColumnNullable &>(*result_column).applyNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column));
                    return result_column;
                }
                else
                    return ColumnNullable::create(materializeColumnIfConst(arg_else.column), arg_cond.column);
            }
            else if (cond_const_col)
            {
                if (cond_const_col->getValue<UInt8>())
                    return result_type->createColumn()->cloneResized(input_rows_count);
                else
                    return makeNullableColumnIfNot(arg_else.column);
            }
            else
                throw Exception("Illegal column " + arg_cond.column->getName() + " of first argument of function " + getName()
                    + ". Must be ColumnUInt8 or ColumnConstUInt8.",
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        /// If else is NULL, we create Nullable column with null mask OR-ed with negated condition.
        if (else_is_null)
        {
            if (cond_col)
            {
                size_t size = input_rows_count;
                const auto & null_map_data = cond_col->getData();

                auto negated_null_map = ColumnUInt8::create();
                auto & negated_null_map_data = negated_null_map->getData();
                negated_null_map_data.resize(size);

                for (size_t i = 0; i < size; ++i)
                    negated_null_map_data[i] = !null_map_data[i];

                if (isColumnNullable(*arg_then.column))
                {
                    auto arg_then_column = arg_then.column;
                    auto result_column = IColumn::mutate(std::move(arg_then_column));
                    assert_cast<ColumnNullable &>(*result_column).applyNegatedNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column));
                    return result_column;
                }
                else
                    return ColumnNullable::create(materializeColumnIfConst(arg_then.column), std::move(negated_null_map));
            }
            else if (cond_const_col)
            {
                if (cond_const_col->getValue<UInt8>())
                    return makeNullableColumnIfNot(arg_then.column);
                else
                    return result_type->createColumn()->cloneResized(input_rows_count);
            }
            else
                throw Exception("Illegal column " + arg_cond.column->getName() + " of first argument of function " + getName()
                    + ". Must be ColumnUInt8 or ColumnConstUInt8.",
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return nullptr;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[2];

        if (arguments[0]->isNullable())
            return getReturnTypeImpl({
                removeNullable(arguments[0]), arguments[1], arguments[2]});

        if (!WhichDataType(arguments[0]).isUInt8())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument (condition) of function if. Must be UInt8.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return getLeastSupertype({arguments[1], arguments[2]});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr res;
        if (   (res = executeForConstAndNullableCondition(arguments, result_type, input_rows_count))
            || (res = executeForNullThenElse(arguments, result_type, input_rows_count))
            || (res = executeForNullableThenElse(arguments, result_type, input_rows_count)))
            return res;

        const ColumnWithTypeAndName & arg_cond = arguments[0];
        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];

        /// A case for identical then and else (pointers are the same).
        if (arg_then.column.get() == arg_else.column.get())
        {
            /// Just point result to them.
            return arg_then.column;
        }

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());
        ColumnPtr materialized_cond_col;

        if (cond_const_col)
        {
            if (arg_then.type->equals(*arg_else.type))
            {
                return cond_const_col->getValue<UInt8>()
                    ? arg_then.column
                    : arg_else.column;
            }
            else
            {
                materialized_cond_col = cond_const_col->convertToFullColumn();
                cond_col = typeid_cast<const ColumnUInt8 *>(&*materialized_cond_col);
            }
        }

        if (!cond_col)
            throw Exception("Illegal column " + arg_cond.column->getName() + " of first argument of function " + getName()
                + ". Must be ColumnUInt8 or ColumnConstUInt8.",
                ErrorCodes::ILLEGAL_COLUMN);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using T0 = typename Types::LeftType;
            using T1 = typename Types::RightType;

            return (res = executeTyped<T0, T1>(cond_col, arguments, result_type, input_rows_count)) != nullptr;
        };

        TypeIndex left_id = arg_then.type->getTypeId();
        TypeIndex right_id = arg_else.type->getTypeId();

        if (const auto * left_array = checkAndGetDataType<DataTypeArray>(arg_then.type.get()))
            left_id = left_array->getNestedType()->getTypeId();

        if (const auto * right_array = checkAndGetDataType<DataTypeArray>(arg_else.type.get()))
            right_id = right_array->getNestedType()->getTypeId();

        if (!(callOnBasicTypes<true, true, true, false>(left_id, right_id, call)
            || (res = executeTyped<UInt128, UInt128>(cond_col, arguments, result_type, input_rows_count))
            || (res = executeString(cond_col, arguments, result_type))
            || (res = executeGenericArray(cond_col, arguments, result_type))
            || (res = executeTuple(arguments, result_type, input_rows_count))))
        {
            return executeGeneric(cond_col, arguments, input_rows_count);
        }

        return res;
    }
};

}

void registerFunctionIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIf>(FunctionFactory::CaseInsensitive);
}

}
