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
#include <Functions/IFunction.h>
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
    extern const int NOT_IMPLEMENTED;
}


using namespace GatherUtils;

/** Selection function by condition: if(cond, then, else).
  * cond - UInt8
  * then, else - numeric types for which there is a general type, or dates, datetimes, or strings, or arrays of these types.
  */


template <typename A, typename B, typename ResultType>
struct NumIfImpl
{
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = PaddedPODArray<A>;
    using ArrayB = PaddedPODArray<B>;
    using ColVecResult = ColumnVector<ResultType>;

    static void vector_vector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void vector_constant(const ArrayCond & cond, const ArrayA & a, B b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void constant_vector(const ArrayCond & cond, A a, const ArrayB & b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void constant_constant(const ArrayCond & cond, A a, B b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
        block.getByPosition(result).column = std::move(col_res);
    }
};

template <typename A, typename B, typename R>
struct NumIfImpl<Decimal<A>, Decimal<B>, Decimal<R>>
{
    using ResultType = Decimal<R>;
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = DecimalPaddedPODArray<Decimal<A>>;
    using ArrayB = DecimalPaddedPODArray<Decimal<B>>;
    using ColVecResult = ColumnDecimal<ResultType>;

    static void vector_vector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, Block & block, size_t result, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void vector_constant(const ArrayCond & cond, const ArrayA & a, B b, Block & block, size_t result, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void constant_vector(const ArrayCond & cond, A a, const ArrayB & b, Block & block, size_t result, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void constant_constant(const ArrayCond & cond, A a, B b, Block & block, size_t result, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        typename ColVecResult::Container & res = col_res->getData();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
        block.getByPosition(result).column = std::move(col_res);
    }
};

template <typename A, typename B>
struct NumIfImpl<A, B, NumberTraits::Error>
{
private:
    [[noreturn]] static void throw_error()
    {
        throw Exception("Internal logic error: invalid types of arguments 2 and 3 of if", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
public:
    template <typename... Args> static void vector_vector(Args &&...) { throw_error(); }
    template <typename... Args> static void vector_constant(Args &&...) { throw_error(); }
    template <typename... Args> static void constant_vector(Args &&...) { throw_error(); }
    template <typename... Args> static void constant_constant(Args &&...) { throw_error(); }
};


class FunctionIf : public FunctionIfBase</*null_is_false=*/false>
{
public:
    static constexpr auto name = "if";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIf>(context); }
    FunctionIf(const Context & context) : context(context) {}

private:
    template <typename T0, typename T1>
    static constexpr bool allow_arrays =
        !IsDecimalNumber<T0> && !IsDecimalNumber<T1> &&
        !std::is_same_v<T0, UInt128> && !std::is_same_v<T1, UInt128>;

    template <typename T0, typename T1>
    static UInt32 decimalScale(Block & block [[maybe_unused]], const ColumnNumbers & arguments [[maybe_unused]])
    {
        if constexpr (IsDecimalNumber<T0> && IsDecimalNumber<T1>)
        {
            UInt32 left_scale = getDecimalScale(*block.getByPosition(arguments[1]).type);
            UInt32 right_scale = getDecimalScale(*block.getByPosition(arguments[2]).type);
            if (left_scale != right_scale)
                throw Exception("Conditional functions with different Decimal scales", ErrorCodes::NOT_IMPLEMENTED);
            return left_scale;
        }
        else
            return std::numeric_limits<UInt32>::max();
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    bool executeRightType(
        const ColumnUInt8 * cond_col,
        Block & block,
        const ColumnNumbers & arguments,
        size_t result,
        const ColVecT0 * col_left)
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        const IColumn * col_right_untyped = block.getByPosition(arguments[2]).column.get();
        UInt32 scale = decimalScale<T0, T1>(block, arguments);

        if (auto col_right_vec = checkAndGetColumn<ColVecT1>(col_right_untyped))
        {
            NumIfImpl<T0, T1, ResultType>::vector_vector(
                cond_col->getData(), col_left->getData(), col_right_vec->getData(), block, result, scale);
            return true;
        }
        else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_untyped))
        {
            NumIfImpl<T0, T1, ResultType>::vector_constant(
                cond_col->getData(), col_left->getData(), col_right_const->template getValue<T1>(), block, result, scale);
            return true;
        }

        return false;
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    bool executeConstRightType(
        const ColumnUInt8 * cond_col,
        Block & block,
        const ColumnNumbers & arguments,
        size_t result,
        const ColumnConst * col_left)
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        const IColumn * col_right_untyped = block.getByPosition(arguments[2]).column.get();
        UInt32 scale = decimalScale<T0, T1>(block, arguments);

        if (auto col_right_vec = checkAndGetColumn<ColVecT1>(col_right_untyped))
        {
            NumIfImpl<T0, T1, ResultType>::constant_vector(
                cond_col->getData(), col_left->template getValue<T0>(), col_right_vec->getData(), block, result, scale);
            return true;
        }
        else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_untyped))
        {
            NumIfImpl<T0, T1, ResultType>::constant_constant(
                cond_col->getData(), col_left->template getValue<T0>(), col_right_const->template getValue<T1>(), block, result, scale);
            return true;
        }

        return false;
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    bool executeRightTypeArray(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] Block & block,
        [[maybe_unused]] const ColumnNumbers & arguments,
        [[maybe_unused]] size_t result,
        [[maybe_unused]] const ColumnArray * col_left_array,
        [[maybe_unused]] size_t input_rows_count)
    {
        if constexpr (std::is_same_v<NumberTraits::Error, typename NumberTraits::ResultOfIf<T0, T1>::Type>)
            return false;
        else if constexpr (allow_arrays<T0, T1>)
        {
            using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

            const IColumn * col_right_untyped = block.getByPosition(arguments[2]).column.get();

            if (auto col_right_array = checkAndGetColumn<ColumnArray>(col_right_untyped))
            {
                const ColVecT1 * col_right_vec = checkAndGetColumn<ColVecT1>(&col_right_array->getData());
                if (!col_right_vec)
                    return false;

                auto res = block.getByPosition(result).type->createColumn();

                conditional(
                    NumericArraySource<T0>(*col_left_array),
                    NumericArraySource<T1>(*col_right_array),
                    NumericArraySink<ResultType>(static_cast<ColumnArray &>(*res), input_rows_count),
                    cond_col->getData());

                block.getByPosition(result).column = std::move(res);
                return true;
            }
            else if (auto col_right_const_array = checkAndGetColumnConst<ColumnArray>(col_right_untyped))
            {
                const ColumnArray * col_right_const_array_data = checkAndGetColumn<ColumnArray>(&col_right_const_array->getDataColumn());
                if (!checkColumn<ColVecT1>(&col_right_const_array_data->getData()))
                    return false;

                auto res = block.getByPosition(result).type->createColumn();

                conditional(
                    NumericArraySource<T0>(*col_left_array),
                    ConstSource<NumericArraySource<T1>>(*col_right_const_array),
                    NumericArraySink<ResultType>(static_cast<ColumnArray &>(*res), input_rows_count),
                    cond_col->getData());

                block.getByPosition(result).column = std::move(res);
                return true;
            }
        }

        return false;
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    bool executeConstRightTypeArray(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] Block & block,
        [[maybe_unused]] const ColumnNumbers & arguments,
        [[maybe_unused]] size_t result,
        [[maybe_unused]] const ColumnConst * col_left_const_array,
        [[maybe_unused]] size_t input_rows_count)
    {
        if constexpr (std::is_same_v<NumberTraits::Error, typename NumberTraits::ResultOfIf<T0, T1>::Type>)
            return false;
        else if constexpr (allow_arrays<T0, T1>)
        {
            using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

            const IColumn * col_right_untyped = block.getByPosition(arguments[2]).column.get();

            if (auto col_right_array = checkAndGetColumn<ColumnArray>(col_right_untyped))
            {
                const ColVecT1 * col_right_vec = checkAndGetColumn<ColVecT1>(&col_right_array->getData());

                if (!col_right_vec)
                    return false;

                auto res = block.getByPosition(result).type->createColumn();

                conditional(
                    ConstSource<NumericArraySource<T0>>(*col_left_const_array),
                    NumericArraySource<T1>(*col_right_array),
                    NumericArraySink<ResultType>(static_cast<ColumnArray &>(*res), input_rows_count),
                    cond_col->getData());

                block.getByPosition(result).column = std::move(res);
                return true;
            }
            else if (auto col_right_const_array = checkAndGetColumnConst<ColumnArray>(col_right_untyped))
            {
                const ColumnArray * col_right_const_array_data = checkAndGetColumn<ColumnArray>(&col_right_const_array->getDataColumn());
                if (!checkColumn<ColVecT1>(&col_right_const_array_data->getData()))
                    return false;

                auto res = block.getByPosition(result).type->createColumn();

                conditional(
                    ConstSource<NumericArraySource<T0>>(*col_left_const_array),
                    ConstSource<NumericArraySource<T1>>(*col_right_const_array),
                    NumericArraySink<ResultType>(static_cast<ColumnArray &>(*res), input_rows_count),
                    cond_col->getData());

                block.getByPosition(result).column = std::move(res);
                return true;
            }
        }

        return false;
    }

    template <typename T0, typename T1>
    bool executeTyped(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        using ColVecT0 = std::conditional_t<IsDecimalNumber<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
        using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;

        const IColumn * col_left_untyped = block.getByPosition(arguments[1]).column.get();

        bool left_ok = false;
        bool right_ok = false;

        if (auto col_left = checkAndGetColumn<ColVecT0>(col_left_untyped))
        {
            left_ok = true;
            right_ok = executeRightType<T0, T1, ColVecT0, ColVecT1>(cond_col, block, arguments, result, col_left);
        }
        else if (auto col_const_left = checkAndGetColumnConst<ColVecT0>(col_left_untyped))
        {
            left_ok = true;
            right_ok = executeConstRightType<T0, T1, ColVecT0, ColVecT1>(cond_col, block, arguments, result, col_const_left);
        }
        else if (auto col_arr_left = checkAndGetColumn<ColumnArray>(col_left_untyped))
        {
            if (auto col_arr_left_elems = checkAndGetColumn<ColVecT0>(&col_arr_left->getData()))
            {
                left_ok = true;
                right_ok = executeRightTypeArray<T0, T1, ColVecT0, ColVecT1>(
                    cond_col, block, arguments, result, col_arr_left, input_rows_count);
            }
        }
        else if (auto col_const_arr_left = checkAndGetColumnConst<ColumnArray>(col_left_untyped))
        {
            if (checkColumn<ColVecT0>(&static_cast<const ColumnArray &>(col_const_arr_left->getDataColumn()).getData()))
            {
                left_ok = true;
                right_ok = executeConstRightTypeArray<T0, T1, ColVecT0, ColVecT1>(
                    cond_col, block, arguments, result, col_const_arr_left, input_rows_count);
            }
        }

        if (!left_ok)
            return false;

        ColumnWithTypeAndName & right_column_typed = block.getByPosition(arguments[2]);
        if (!right_ok)
            throw Exception("Illegal column " + right_column_typed.column->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        return true;
    }

    bool executeString(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const IColumn * col_then_untyped = block.getByPosition(arguments[1]).column.get();
        const IColumn * col_else_untyped = block.getByPosition(arguments[2]).column.get();

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

        if ((col_then_fixed || col_then_const_fixed)
            && (col_else_fixed || col_else_const_fixed))
        {
            /// The result is FixedString.

            auto col_res_untyped = block.getByPosition(result).type->createColumn();
            ColumnFixedString * col_res = static_cast<ColumnFixedString *>(col_res_untyped.get());
            auto sink = FixedStringSink(*col_res, rows);

            if (col_then_fixed && col_else_fixed)
                conditional(FixedStringSource(*col_then_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const_fixed)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);

            block.getByPosition(result).column = std::move(col_res_untyped);
            return true;
        }

        if ((col_then || col_then_const || col_then_fixed || col_then_const_fixed)
            && (col_else || col_else_const || col_else_fixed || col_else_const_fixed))
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

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

    bool executeGenericArray(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
    {
        /// For generic implementation, arrays must be of same type.
        if (!block.getByPosition(arguments[1]).type->equals(*block.getByPosition(arguments[2]).type))
            return false;

        const IColumn * col_then_untyped = block.getByPosition(arguments[1]).column.get();
        const IColumn * col_else_untyped = block.getByPosition(arguments[2]).column.get();

        const ColumnArray * col_arr_then = checkAndGetColumn<ColumnArray>(col_then_untyped);
        const ColumnArray * col_arr_else = checkAndGetColumn<ColumnArray>(col_else_untyped);
        const ColumnConst * col_arr_then_const = checkAndGetColumnConst<ColumnArray>(col_then_untyped);
        const ColumnConst * col_arr_else_const = checkAndGetColumnConst<ColumnArray>(col_else_untyped);

        const PaddedPODArray<UInt8> & cond_data = cond_col->getData();
        size_t rows = cond_data.size();

        if ((col_arr_then || col_arr_then_const)
            && (col_arr_else || col_arr_else_const))
        {
            auto res = block.getByPosition(result).type->createColumn();
            auto col_res = static_cast<ColumnArray *>(res.get());

            if (col_arr_then && col_arr_else)
                conditional(GenericArraySource(*col_arr_then), GenericArraySource(*col_arr_else), GenericArraySink(*col_res, rows), cond_data);
            else if (col_arr_then && col_arr_else_const)
                conditional(GenericArraySource(*col_arr_then), ConstSource<GenericArraySource>(*col_arr_else_const), GenericArraySink(*col_res, rows), cond_data);
            else if (col_arr_then_const && col_arr_else)
                conditional(ConstSource<GenericArraySource>(*col_arr_then_const), GenericArraySource(*col_arr_else), GenericArraySink(*col_res, rows), cond_data);
            else if (col_arr_then_const && col_arr_else_const)
                conditional(ConstSource<GenericArraySource>(*col_arr_then_const), ConstSource<GenericArraySource>(*col_arr_else_const), GenericArraySink(*col_res, rows), cond_data);
            else
                return false;

            block.getByPosition(result).column = std::move(res);
            return true;
        }

        return false;
    }

    bool executeTuple(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        /// Calculate function for each corresponding elements of tuples.

        const ColumnWithTypeAndName & arg1 = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg2 = block.getByPosition(arguments[2]);

        Columns col1_contents;
        Columns col2_contents;

        if (const ColumnTuple * tuple1 = typeid_cast<const ColumnTuple *>(arg1.column.get()))
            col1_contents = tuple1->getColumnsCopy();
        else if (const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(arg1.column.get()))
            col1_contents = convertConstTupleToConstantElements(*const_tuple);
        else
            return false;

        if (const ColumnTuple * tuple2 = typeid_cast<const ColumnTuple *>(arg2.column.get()))
            col2_contents = tuple2->getColumnsCopy();
        else if (const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(arg2.column.get()))
            col2_contents = convertConstTupleToConstantElements(*const_tuple);
        else
            return false;

        const DataTypeTuple & type1 = static_cast<const DataTypeTuple &>(*arg1.type);
        const DataTypeTuple & type2 = static_cast<const DataTypeTuple &>(*arg2.type);

        Block temporary_block;
        temporary_block.insert(block.getByPosition(arguments[0]));

        size_t tuple_size = type1.getElements().size();
        Columns tuple_columns(tuple_size);

        for (size_t i = 0; i < tuple_size; ++i)
        {
            temporary_block.insert({nullptr,
                getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), type1.getElements()[i], type2.getElements()[i]}),
                {}});

            temporary_block.insert({col1_contents[i], type1.getElements()[i], {}});
            temporary_block.insert({col2_contents[i], type2.getElements()[i], {}});

            /// temporary_block will be: cond, res_0, ..., res_i, then_i, else_i
            executeImpl(temporary_block, {0, i + 2, i + 3}, i + 1, input_rows_count);
            temporary_block.erase(i + 3);
            temporary_block.erase(i + 2);

            tuple_columns[i] = temporary_block.getByPosition(i + 1).column;
        }

        /// temporary_block is: cond, res_0, res_1, res_2...

        block.getByPosition(result).column = ColumnTuple::create(tuple_columns);
        return true;
    }

    void executeGeneric(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        /// Convert both columns to the common type (if needed).

        const ColumnWithTypeAndName & arg1 = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg2 = block.getByPosition(arguments[2]);

        DataTypePtr common_type = getLeastSupertype({arg1.type, arg2.type});

        ColumnPtr col_then = castColumn(arg1, common_type, context);
        ColumnPtr col_else = castColumn(arg2, common_type, context);

        MutableColumnPtr result_column = common_type->createColumn();
        result_column->reserve(input_rows_count);

        bool then_is_const = col_then->isColumnConst();
        bool else_is_const = col_else->isColumnConst();

        const auto & cond_array = cond_col->getData();

        if (then_is_const && else_is_const)
        {
            const IColumn & then_nested_column = static_cast<const ColumnConst &>(*col_then).getDataColumn();
            const IColumn & else_nested_column = static_cast<const ColumnConst &>(*col_else).getDataColumn();

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
            const IColumn & then_nested_column = static_cast<const ColumnConst &>(*col_then).getDataColumn();

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
            const IColumn & else_nested_column = static_cast<const ColumnConst &>(*col_else).getDataColumn();

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

        block.getByPosition(result).column = std::move(result_column);
    }

    bool executeForNullableCondition(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
    {
        const ColumnWithTypeAndName & arg_cond = block.getByPosition(arguments[0]);
        bool cond_is_null = arg_cond.column->onlyNull();
        bool cond_is_nullable = arg_cond.column->isColumnNullable();

        if (cond_is_null)
        {
            block.getByPosition(result).column = std::move(block.getByPosition(arguments[2]).column);
            return true;
        }

        if (cond_is_nullable)
        {
            Block temporary_block
            {
                { static_cast<const ColumnNullable &>(*arg_cond.column).getNestedColumnPtr(), removeNullable(arg_cond.type), arg_cond.name },
                block.getByPosition(arguments[1]),
                block.getByPosition(arguments[2]),
                block.getByPosition(result)
            };

            executeImpl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());

            block.getByPosition(result).column = std::move(temporary_block.getByPosition(3).column);
            return true;
        }

        return false;
    }

    static ColumnPtr materializeColumnIfConst(const ColumnPtr & column)
    {
        return column->convertToFullColumnIfConst();
    }

    static ColumnPtr makeNullableColumnIfNot(const ColumnPtr & column)
    {
        if (column->isColumnNullable())
            return column;

        return ColumnNullable::create(
            materializeColumnIfConst(column), ColumnUInt8::create(column->size(), 0));
    }

    static ColumnPtr getNestedColumn(const ColumnPtr & column)
    {
        if (column->isColumnNullable())
            return static_cast<const ColumnNullable &>(*column).getNestedColumnPtr();

        return column;
    }

    bool executeForNullableThenElse(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        const ColumnWithTypeAndName & arg_cond = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & arg_then = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg_else = block.getByPosition(arguments[2]);

        bool then_is_nullable = typeid_cast<const ColumnNullable *>(arg_then.column.get());
        bool else_is_nullable = typeid_cast<const ColumnNullable *>(arg_else.column.get());

        if (!then_is_nullable && !else_is_nullable)
            return false;

        /** Calculate null mask of result and nested column separately.
          */
        ColumnPtr result_null_mask;

        {
            Block temporary_block(
            {
                arg_cond,
                {
                    then_is_nullable
                        ? static_cast<const ColumnNullable *>(arg_then.column.get())->getNullMapColumnPtr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                },
                {
                    else_is_nullable
                        ? static_cast<const ColumnNullable *>(arg_else.column.get())->getNullMapColumnPtr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                },
                {
                    nullptr,
                    std::make_shared<DataTypeUInt8>(),
                    ""
                }
            });

            executeImpl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());

            result_null_mask = temporary_block.getByPosition(3).column;
        }

        ColumnPtr result_nested_column;

        {
            Block temporary_block(
            {
                arg_cond,
                {
                    getNestedColumn(arg_then.column),
                    removeNullable(arg_then.type),
                    ""
                },
                {
                    getNestedColumn(arg_else.column),
                    removeNullable(arg_else.type),
                    ""
                },
                {
                    nullptr,
                    removeNullable(block.getByPosition(result).type),
                    ""
                }
            });

            executeImpl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());

            result_nested_column = temporary_block.getByPosition(3).column;
        }

        block.getByPosition(result).column = ColumnNullable::create(
            materializeColumnIfConst(result_nested_column), materializeColumnIfConst(result_null_mask));
        return true;
    }

    bool executeForNullThenElse(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        const ColumnWithTypeAndName & arg_cond = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & arg_then = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg_else = block.getByPosition(arguments[2]);

        bool then_is_null = arg_then.column->onlyNull();
        bool else_is_null = arg_else.column->onlyNull();

        if (!then_is_null && !else_is_null)
            return false;

        if (then_is_null && else_is_null)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConstWithDefaultValue(input_rows_count);
            return true;
        }

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());

        /// If then is NULL, we create Nullable column with null mask OR-ed with condition.
        if (then_is_null)
        {
            if (cond_col)
            {
                if (arg_else.column->isColumnNullable())
                {
                    auto arg_else_column = arg_else.column;
                    auto result_column = (*std::move(arg_else_column)).mutate();
                    static_cast<ColumnNullable &>(*result_column).applyNullMap(static_cast<const ColumnUInt8 &>(*arg_cond.column));
                    block.getByPosition(result).column = std::move(result_column);
                }
                else
                {
                    block.getByPosition(result).column = ColumnNullable::create(
                        materializeColumnIfConst(arg_else.column), arg_cond.column);
                }
            }
            else if (cond_const_col)
            {
                if (cond_const_col->getValue<UInt8>())
                    block.getByPosition(result).column = block.getByPosition(result).type->createColumn()->cloneResized(input_rows_count);
                else
                    block.getByPosition(result).column = makeNullableColumnIfNot(arg_else.column);
            }
            else
                throw Exception("Illegal column " + arg_cond.column->getName() + " of first argument of function " + getName()
                    + ". Must be ColumnUInt8 or ColumnConstUInt8.",
                    ErrorCodes::ILLEGAL_COLUMN);
            return true;
        }

        /// If else is NULL, we create Nullable column with null mask OR-ed with negated condition.
        if (else_is_null)
        {
            if (cond_col)
            {
                size_t size = input_rows_count;
                auto & null_map_data = cond_col->getData();

                auto negated_null_map = ColumnUInt8::create();
                auto & negated_null_map_data = negated_null_map->getData();
                negated_null_map_data.resize(size);

                for (size_t i = 0; i < size; ++i)
                    negated_null_map_data[i] = !null_map_data[i];

                if (arg_then.column->isColumnNullable())
                {
                    auto arg_then_column = arg_then.column;
                    auto result_column = (*std::move(arg_then_column)).mutate();
                    static_cast<ColumnNullable &>(*result_column).applyNegatedNullMap(static_cast<const ColumnUInt8 &>(*arg_cond.column));
                    block.getByPosition(result).column = std::move(result_column);
                }
                else
                {
                    block.getByPosition(result).column = ColumnNullable::create(
                        materializeColumnIfConst(arg_then.column), std::move(negated_null_map));
                }
            }
            else if (cond_const_col)
            {
                if (cond_const_col->getValue<UInt8>())
                    block.getByPosition(result).column = makeNullableColumnIfNot(arg_then.column);
                else
                    block.getByPosition(result).column = block.getByPosition(result).type->createColumn()->cloneResized(input_rows_count);
            }
            else
                throw Exception("Illegal column " + arg_cond.column->getName() + " of first argument of function " + getName()
                    + ". Must be ColumnUInt8 or ColumnConstUInt8.",
                    ErrorCodes::ILLEGAL_COLUMN);
            return true;
        }

        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForNulls() const override { return false; }

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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        if (executeForNullableCondition(block, arguments, result, input_rows_count)
            || executeForNullThenElse(block, arguments, result, input_rows_count)
            || executeForNullableThenElse(block, arguments, result, input_rows_count))
            return;

        const ColumnWithTypeAndName & arg_cond = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & arg_then = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg_else = block.getByPosition(arguments[2]);

        /// A case for identical then and else (pointers are the same).
        if (arg_then.column.get() == arg_else.column.get())
        {
            /// Just point result to them.
            block.getByPosition(result).column = arg_then.column;
            return;
        }

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());
        ColumnPtr materialized_cond_col;

        if (cond_const_col)
        {
            if (arg_then.type->equals(*arg_else.type))
            {
                block.getByPosition(result).column = cond_const_col->getValue<UInt8>()
                    ? arg_then.column
                    : arg_else.column;
                return;
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

            if constexpr (IsDecimalNumber<T0> == IsDecimalNumber<T1>)
                return executeTyped<T0, T1>(cond_col, block, arguments, result, input_rows_count);
            else
                throw Exception("Conditional function with Decimal and non Decimal", ErrorCodes::NOT_IMPLEMENTED);
        };

        TypeIndex left_id = arg_then.type->getTypeId();
        TypeIndex right_id = arg_else.type->getTypeId();

        if (auto left_array = checkAndGetDataType<DataTypeArray>(arg_then.type.get()))
            left_id = left_array->getNestedType()->getTypeId();

        if (auto rigth_array = checkAndGetDataType<DataTypeArray>(arg_else.type.get()))
            right_id = rigth_array->getNestedType()->getTypeId();

        if (!(callOnBasicTypes<true, true, true, false>(left_id, right_id, call)
            || executeTyped<UInt128, UInt128>(cond_col, block, arguments, result, input_rows_count)
            || executeString(cond_col, block, arguments, result)
            || executeGenericArray(cond_col, block, arguments, result)
            || executeTuple(block, arguments, result, input_rows_count)))
        {
            executeGeneric(cond_col, block, arguments, result, input_rows_count);
        }
    }

    const Context & context;
};

void registerFunctionIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIf>(FunctionFactory::CaseInsensitive);
}

}
