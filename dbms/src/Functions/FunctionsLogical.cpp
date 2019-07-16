#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>

#include <algorithm>


namespace DB
{

void registerFunctionsLogical(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAnd>();
    factory.registerFunction<FunctionOr>();
    factory.registerFunction<FunctionXor>();
    factory.registerFunction<FunctionNot>();
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
using namespace FunctionsLogicalDetail;

using UInt8Container = ColumnUInt8::Container;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;


MutableColumnPtr convertFromTernaryData(const UInt8Container & ternary_data, const bool make_nullable)
{
    const size_t rows_count = ternary_data.size();

    auto new_column = ColumnUInt8::create(rows_count);
    std::transform(
            ternary_data.cbegin(), ternary_data.cend(), new_column->getData().begin(),
            [](const auto x) { return x == Ternary::True; });

    if (!make_nullable)
        return new_column;

    auto null_column = ColumnUInt8::create(rows_count);
    std::transform(
            ternary_data.cbegin(), ternary_data.cend(), null_column->getData().begin(),
            [](const auto x) { return x == Ternary::Null; });

    return ColumnNullable::create(std::move(new_column), std::move(null_column));
}

template <typename T>
bool tryConvertColumnToUInt8(const IColumn * column, UInt8Container & res)
{
    const auto col = checkAndGetColumn<ColumnVector<T>>(column);
    if (!col)
        return false;

    std::transform(
            col->getData().cbegin(), col->getData().cend(), res.begin(),
            [](const auto x) { return x != 0; });

    return true;
}

void convertColumnToUInt8(const IColumn * column, UInt8Container & res)
{
    if (!tryConvertColumnToUInt8<Int8>(column, res) &&
        !tryConvertColumnToUInt8<Int16>(column, res) &&
        !tryConvertColumnToUInt8<Int32>(column, res) &&
        !tryConvertColumnToUInt8<Int64>(column, res) &&
        !tryConvertColumnToUInt8<UInt16>(column, res) &&
        !tryConvertColumnToUInt8<UInt32>(column, res) &&
        !tryConvertColumnToUInt8<UInt64>(column, res) &&
        !tryConvertColumnToUInt8<Float32>(column, res) &&
        !tryConvertColumnToUInt8<Float64>(column, res))
        throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
}


///  TODO: Add a good comment here about what this is
using ValueGetter = std::function<Ternary::ResultType (size_t)>;

template <typename ... Types>
struct ValueGetterBuilderImpl;

template <typename Type, typename ...Types>
struct ValueGetterBuilderImpl<Type, Types...>
{
    static ValueGetter build(const IColumn * x)
    {
        if (const auto nullable_column = typeid_cast<const ColumnNullable *>(x))
        {
            if (const auto nested_column = typeid_cast<const ColumnVector<Type> *>(nullable_column->getNestedColumnPtr().get()))
            {
                return [&null_data = nullable_column->getNullMapData(), &column_data = nested_column->getData()](size_t i)
                { return Ternary::makeValue(column_data[i], null_data[i]); };
            }
            else
                return ValueGetterBuilderImpl<Types...>::build(x);
        }
        else if (const auto column = typeid_cast<const ColumnVector<Type> *>(x))
            return [&column_data = column->getData()](size_t i) { return Ternary::makeValue(column_data[i]); };
        else
            return ValueGetterBuilderImpl<Types...>::build(x);
    }
};

template <>
struct ValueGetterBuilderImpl<>
{
    static ValueGetter build(const IColumn * x)
    {
        throw Exception(
                std::string("Unknown numeric column of type: ") + demangle(typeid(x).name()),
                ErrorCodes::LOGICAL_ERROR);
    }
};

using ValueGetterBuilder =
        ValueGetterBuilderImpl<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;


template <typename Op, size_t N>
class AssociativeApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    AssociativeApplierImpl(const UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData()), next(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const ResultValueType a, const size_t i) const
    {
        if constexpr (Op::isSaturable())
            return Op::isSaturatedValue(a) ? a : Op::apply(a, next.apply(vec[i], i));
        else
            return Op::apply(a, next.apply(vec[i], i));
    }

private:
    const UInt8Container & vec;
    const AssociativeApplierImpl<Op, N - 1> next;
};

template <typename Op>
class AssociativeApplierImpl<Op, 1>
{
    using ResultValueType = typename Op::ResultType;

public:
    AssociativeApplierImpl(const UInt8ColumnPtrs & in)
        : vec(in[in.size() - 1]->getData()) {}

    inline ResultValueType apply(const ResultValueType a, const size_t i) const
    {
        return Op::apply(a, vec[i]);
    }

private:
    const UInt8Container & vec;
};


template <class Op>
static bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res)
{
    bool has_res = false;
    for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
    {
        if (!in[i]->isColumnConst())
            continue;

        Field value = (*in[i])[0];

        UInt8 x = !value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
        if (has_res)
        {
            res = Op::apply(res, x);
        }
        else
        {
            res = x;
            has_res = true;
        }

        in.erase(in.begin() + i);
    }
    return has_res;
}


template <typename Op, size_t N>
class AssociativeGenericApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : val_getter{ValueGetterBuilder::build(in[in.size() - N])}, next{in} {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const ResultValueType a, const size_t i) const
    {
        if constexpr (Op::isSaturable())
            return Op::isSaturatedValue(a) ? a : Op::apply(a, next.apply(i));
        else
            return Op::apply(a, next.apply(i));

    }
    inline ResultValueType apply(const size_t i) const
    {
        return apply(val_getter(i), i);
    }

private:
    const ValueGetter val_getter;
    const AssociativeGenericApplierImpl<Op, N - 1> next;
};


template <typename Op>
class AssociativeGenericApplierImpl<Op, 2>
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : val_getter_L{ValueGetterBuilder::build(in[in.size() - 2])}
        , val_getter_R{ValueGetterBuilder::build(in[in.size() - 1])} {}

    inline ResultValueType apply(const size_t i) const
    {
        return Op::apply(val_getter_L(i), val_getter_R(i));
    }

private:
    const ValueGetter val_getter_L;
    const ValueGetter val_getter_R;
};

template <typename Op>
class AssociativeGenericApplierImpl<Op, 1>
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : val_getter{ValueGetterBuilder::build(in[in.size() - 1])} {}

    inline ResultValueType apply(const size_t i) const { return val_getter(i); }

private:
    const ValueGetter val_getter;
};


template <class Op>
bool extractConstColumnsTernary(ColumnRawPtrs & in, UInt8 & res_3v)
{
    bool has_res = false;
    for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
    {
        if (!in[i]->isColumnConst())
            continue;

        const auto field_value = (*in[i])[0];

        UInt8 value_3v = field_value.isNull()
                ? Ternary::makeValue(false, true)
                : Ternary::makeValue(applyVisitor(FieldVisitorConvertToNumber<bool>(), field_value));

        if (has_res)
        {
            res_3v = Op::apply(res_3v, value_3v);
        }
        else
        {
            res_3v = value_3v;
            has_res = true;
        }

        in.erase(in.begin() + i);
    }
    return has_res;
}


template <
    template <typename, size_t> typename OperationApplierImpl, typename Op,
    size_t N, bool use_result_as_input>
struct OperationApplier
{
    template <typename Columns, typename Result>
    static void NO_INLINE run(Columns & in, Result & result)
    {
        if (N > in.size())
        {
            OperationApplier<OperationApplierImpl, Op, N - 1, use_result_as_input>::run(in, result);
            return;
        }

        const OperationApplierImpl<Op, N> operationApplierImpl(in);
        size_t i = 0;
        for (auto & res : result)
            if constexpr (use_result_as_input)
                res = operationApplierImpl.apply(res, i++);
            else
                res = operationApplierImpl.apply(i++);

        in.erase(in.end() - N, in.end());
    }
};

template <
    template <typename, size_t> typename OperationApplierImpl, typename Op>
struct OperationApplier<OperationApplierImpl, Op, 0, true>
{
    template <typename Columns, typename Result>
    static void NO_INLINE run(Columns &, Result &)
    {
        throw Exception(
                "AssociativeOperationImpl::execute(...): not enough arguments to run this method",
                ErrorCodes::LOGICAL_ERROR);
    }
};

template <
    template <typename, size_t> typename OperationApplierImpl, typename Op>
struct OperationApplier<OperationApplierImpl, Op, 1, false>
{
    template <typename Columns, typename Result>
    static void NO_INLINE run(Columns &, Result &)
    {
        throw Exception(
                "AssociativeOperationImpl::execute(...): not enough arguments to run this method",
                ErrorCodes::LOGICAL_ERROR);
    }
};


template <class Op>
static void executeForTernaryLogicImpl(ColumnRawPtrs arguments, ColumnWithTypeAndName & result_info, size_t input_rows_count)
{
    /// Combine all constant columns into a single constant value.
    UInt8 const_3v_value = 0;
    const bool has_consts = extractConstColumnsTernary<Op>(arguments, const_3v_value);

    /// If the constant value uniquely determines the result, return it.
    if (has_consts && (arguments.empty() || (Op::isSaturable() && Op::isSaturatedValue(const_3v_value))))
    {
        result_info.column = ColumnConst::create(
            convertFromTernaryData(UInt8Container({const_3v_value}), result_info.type->isNullable()),
            input_rows_count
        );
        return;
    }

    const auto result_column = ColumnUInt8::create(input_rows_count);

    /// Efficiently combine all the columns of the correct type.
    while (arguments.size() > 1)
    {
        /// Combining 10 columns per pass is the fastest for large block sizes.
        /// For small block sizes - more columns is faster.
        OperationApplier<AssociativeGenericApplierImpl, Op, 10, false>::run(arguments, result_column->getData());
        arguments.insert(arguments.cbegin(), result_column.get());
    }

    result_info.column = convertFromTernaryData(result_column->getData(), result_info.type->isNullable());
}


template <class Op>
static void basicExecuteImpl(ColumnRawPtrs arguments, ColumnWithTypeAndName & result_info, size_t input_rows_count)
{
    /// Combine all constant columns into a single constant value.
    UInt8 const_val = 0;
    bool has_consts = extractConstColumns<Op>(arguments, const_val);

    /// If the constant value uniquely determines the result, return it.
    if (has_consts && (arguments.empty() || Op::apply(const_val, 0) == Op::apply(const_val, 1)))
    {
        if (!arguments.empty())
            const_val = Op::apply(const_val, 0);
        result_info.column = DataTypeUInt8().createColumnConst(input_rows_count, toField(const_val));
        return;
    }

    /// If the constant value is a neutral element, let's forget about it.
    if (has_consts && Op::apply(const_val, 0) == 0 && Op::apply(const_val, 1) == 1)
        has_consts = false;

    auto col_res = ColumnUInt8::create();
    UInt8Container & vec_res = col_res->getData();

    if (has_consts)
    {
        vec_res.assign(input_rows_count, const_val);
        arguments.push_back(col_res.get());
    }
    else
    {
        vec_res.resize(input_rows_count);
    }

    /// Convert all columns to UInt8
    UInt8ColumnPtrs uint8_args;
    Columns converted_columns;

    for (const IColumn * column : arguments)
    {
        if (auto uint8_column = checkAndGetColumn<ColumnUInt8>(column))
            uint8_args.push_back(uint8_column);
        else
        {
            auto converted_column = ColumnUInt8::create(input_rows_count);
            convertColumnToUInt8(column, converted_column->getData());
            uint8_args.push_back(converted_column.get());
            converted_columns.emplace_back(std::move(converted_column));
        }
    }

    /// Effeciently combine all the columns of the correct type.
    while (uint8_args.size() > 1)
    {
        /// With a large block size, combining 10 columns per pass is the fastest.
        /// When small - more, is faster.
        OperationApplier<AssociativeApplierImpl, Op, 10, true>::run(uint8_args, vec_res);
        uint8_args.push_back(col_res.get());
    }

    /// This is possible if there is exactly one non-constant among the arguments, and it is of type UInt8.
    if (uint8_args[0] != col_res.get())
        vec_res.assign(uint8_args[0]->getData());

    result_info.column = std::move(col_res);
}

}

template <typename Impl, typename Name>
DataTypePtr FunctionAnyArityLogical<Impl, Name>::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 2)
        throw Exception("Number of arguments for function \"" + getName() + "\" should be at least 2: passed "
            + toString(arguments.size()),
            ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    bool has_nullable_arguments = false;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto & arg_type = arguments[i];

        if (!has_nullable_arguments)
        {
            has_nullable_arguments = arg_type->isNullable();
            if (has_nullable_arguments && !Impl::specialImplementationForNulls())
                throw Exception("Logical error: Unexpected type of argument for function \"" + getName() + "\": "
                    " argument " + toString(i + 1) + " is of type " + arg_type->getName(), ErrorCodes::LOGICAL_ERROR);
        }

        if (!(isNativeNumber(arg_type)
            || (Impl::specialImplementationForNulls() && (arg_type->onlyNull() || isNativeNumber(removeNullable(arg_type))))))
            throw Exception("Illegal type ("
                + arg_type->getName()
                + ") of " + toString(i + 1) + " argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    auto result_type = std::make_shared<DataTypeUInt8>();
    return has_nullable_arguments
            ? makeNullable(result_type)
            : result_type;
}

template <typename Impl, typename Name>
void FunctionAnyArityLogical<Impl, Name>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_index, size_t input_rows_count)
{
    ColumnRawPtrs args_in;
    for (const auto arg_index : arguments)
        args_in.push_back(block.getByPosition(arg_index).column.get());

    auto & result_info = block.getByPosition(result_index);
    if (result_info.type->isNullable())
        executeForTernaryLogicImpl<Impl>(std::move(args_in), result_info, input_rows_count);
    else
        basicExecuteImpl<Impl>(std::move(args_in), result_info, input_rows_count);
}


template <typename A, typename Op>
struct UnaryOperationImpl
{
    using ResultType = typename Op::ResultType;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayC = typename ColumnVector<ResultType>::Container;

    static void NO_INLINE vector(const ArrayA & a, ArrayC & c)
    {
        std::transform(
                a.cbegin(), a.cend(), c.begin(),
                [](const auto x) { return Op::apply(x); });
    }

    static void constant(A a, ResultType & c)
    {
        c = Op::apply(a);
    }
};

template <template <typename> class Impl, typename Name>
DataTypePtr FunctionUnaryLogical<Impl, Name>::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (!isNativeNumber(arguments[0]))
        throw Exception("Illegal type ("
            + arguments[0]->getName()
            + ") of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeUInt8>();
}

template <template <typename> class Impl, typename T>
bool functionUnaryExecuteType(Block & block, const ColumnNumbers & arguments, size_t result)
{
    if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
    {
        auto col_res = ColumnUInt8::create();

        typename ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(col->getData().size());
        UnaryOperationImpl<T, Impl<T>>::vector(col->getData(), vec_res);

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    return false;
}

template <template <typename> class Impl, typename Name>
void FunctionUnaryLogical<Impl, Name>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    if (!(functionUnaryExecuteType<Impl, UInt8>(block, arguments, result)
        || functionUnaryExecuteType<Impl, UInt16>(block, arguments, result)
        || functionUnaryExecuteType<Impl, UInt32>(block, arguments, result)
        || functionUnaryExecuteType<Impl, UInt64>(block, arguments, result)
        || functionUnaryExecuteType<Impl, Int8>(block, arguments, result)
        || functionUnaryExecuteType<Impl, Int16>(block, arguments, result)
        || functionUnaryExecuteType<Impl, Int32>(block, arguments, result)
        || functionUnaryExecuteType<Impl, Int64>(block, arguments, result)
        || functionUnaryExecuteType<Impl, Float32>(block, arguments, result)
        || functionUnaryExecuteType<Impl, Float64>(block, arguments, result)))
       throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
}

}
