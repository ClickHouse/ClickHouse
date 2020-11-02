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
    factory.registerFunction<FunctionNot>(FunctionFactory::CaseInsensitive); /// Operator NOT(x) can be parsed as a function.
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


MutableColumnPtr buildColumnFromTernaryData(const UInt8Container & ternary_data, const bool make_nullable)
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
bool tryConvertColumnToBool(const IColumn * column, UInt8Container & res)
{
    const auto col = checkAndGetColumn<ColumnVector<T>>(column);
    if (!col)
        return false;

    std::transform(
            col->getData().cbegin(), col->getData().cend(), res.begin(),
            [](const auto x) { return !!x; });

    return true;
}

void convertAnyColumnToBool(const IColumn * column, UInt8Container & res)
{
    if (!tryConvertColumnToBool<Int8>(column, res) &&
        !tryConvertColumnToBool<Int16>(column, res) &&
        !tryConvertColumnToBool<Int32>(column, res) &&
        !tryConvertColumnToBool<Int64>(column, res) &&
        !tryConvertColumnToBool<UInt16>(column, res) &&
        !tryConvertColumnToBool<UInt32>(column, res) &&
        !tryConvertColumnToBool<UInt64>(column, res) &&
        !tryConvertColumnToBool<Float32>(column, res) &&
        !tryConvertColumnToBool<Float64>(column, res))
        throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
}


template <class Op, typename Func>
static bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res, Func && func)
{
    bool has_res = false;

    for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
    {
        UInt8 x;

        if (in[i]->onlyNull())
            x = func(Null());
        else if (isColumnConst(*in[i]))
            x = func((*in[i])[0]);
        else
            continue;

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

template <class Op>
inline bool extractConstColumnsAsBool(ColumnRawPtrs & in, UInt8 & res)
{
    return extractConstColumns<Op>(
        in, res,
        [](const Field & value)
        {
            return !value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), value);
        }
    );
}

template <class Op>
inline bool extractConstColumnsAsTernary(ColumnRawPtrs & in, UInt8 & res_3v)
{
    return extractConstColumns<Op>(
        in, res_3v,
        [](const Field & value)
        {
            return value.isNull()
                    ? Ternary::makeValue(false, true)
                    : Ternary::makeValue(applyVisitor(FieldVisitorConvertToNumber<bool>(), value));
        }
    );
}


/// N.B. This class calculates result only for non-nullable types
template <typename Op, size_t N>
class AssociativeApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeApplierImpl(const UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData()), next(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const size_t i) const
    {
        const auto a = !!vec[i];
        if constexpr (Op::isSaturable())
            return Op::isSaturatedValue(a) ? a : Op::apply(a, next.apply(i));
        else
            return Op::apply(a, next.apply(i));
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
    explicit AssociativeApplierImpl(const UInt8ColumnPtrs & in)
        : vec(in[in.size() - 1]->getData()) {}

    inline ResultValueType apply(const size_t i) const { return !!vec[i]; }

private:
    const UInt8Container & vec;
};


/// A helper class used by AssociativeGenericApplierImpl
/// Allows for on-the-fly conversion of any data type into intermediate ternary representation
using TernaryValueGetter = std::function<Ternary::ResultType (size_t)>;

template <typename ... Types>
struct ValueGetterBuilderImpl;

template <typename Type, typename ...Types>
struct ValueGetterBuilderImpl<Type, Types...>
{
    static TernaryValueGetter build(const IColumn * x)
    {
        if (x->onlyNull())
        {
            return [](size_t){ return Ternary::Null; };
        }
        else if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(x))
        {
            if (const auto * nested_column = typeid_cast<const ColumnVector<Type> *>(nullable_column->getNestedColumnPtr().get()))
            {
                return [
                    &null_data = nullable_column->getNullMapData(),
                    &column_data = nested_column->getData()](size_t i)
                {
                    return Ternary::makeValue(column_data[i], null_data[i]);
                };
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
    static TernaryValueGetter build(const IColumn * x)
    {
        throw Exception(
                std::string("Unknown numeric column of type: ") + demangle(typeid(*x).name()),
                ErrorCodes::LOGICAL_ERROR);
    }
};

using ValueGetterBuilder =
        ValueGetterBuilderImpl<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;

/// This class together with helper class ValueGetterBuilder can be used with columns of arbitrary data type
/// Allows for on-the-fly conversion of any type of data into intermediate ternary representation
/// and eliminates the need to materialize data columns in intermediate representation
template <typename Op, size_t N>
class AssociativeGenericApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : val_getter{ValueGetterBuilder::build(in[in.size() - N])}, next{in} {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const size_t i) const
    {
        const auto a = val_getter(i);
        if constexpr (Op::isSaturable())
            return Op::isSaturatedValueTernary(a) ? a : Op::apply(a, next.apply(i));
        else
            return Op::apply(a, next.apply(i));
    }

private:
    const TernaryValueGetter val_getter;
    const AssociativeGenericApplierImpl<Op, N - 1> next;
};


template <typename Op>
class AssociativeGenericApplierImpl<Op, 1>
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : val_getter{ValueGetterBuilder::build(in[in.size() - 1])} {}

    inline ResultValueType apply(const size_t i) const { return val_getter(i); }

private:
    const TernaryValueGetter val_getter;
};


/// Apply target function by feeding it "batches" of N columns
/// Combining 8 columns per pass is the fastest method, because it's the maximum when clang vectorizes a loop.
template <
    typename Op, template <typename, size_t> typename OperationApplierImpl, size_t N = 8>
struct OperationApplier
{
    template <typename Columns, typename ResultData>
    static void apply(Columns & in, ResultData & result_data, bool use_result_data_as_input = false)
    {
        if (!use_result_data_as_input)
            doBatchedApply<false>(in, result_data.data(), result_data.size());
        while (!in.empty())
            doBatchedApply<true>(in, result_data.data(), result_data.size());
    }

    template <bool CarryResult, typename Columns, typename Result>
    static void NO_INLINE doBatchedApply(Columns & in, Result * __restrict result_data, size_t size)
    {
        if (N > in.size())
        {
            OperationApplier<Op, OperationApplierImpl, N - 1>
                ::template doBatchedApply<CarryResult>(in, result_data, size);
            return;
        }

        const OperationApplierImpl<Op, N> operation_applier_impl(in);
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (CarryResult)
                result_data[i] = Op::apply(result_data[i], operation_applier_impl.apply(i));
            else
                result_data[i] = operation_applier_impl.apply(i);
        }

        in.erase(in.end() - N, in.end());
    }
};

template <
    typename Op, template <typename, size_t> typename OperationApplierImpl>
struct OperationApplier<Op, OperationApplierImpl, 0>
{
    template <bool, typename Columns, typename Result>
    static void NO_INLINE doBatchedApply(Columns &, Result &, size_t)
    {
        throw Exception(
                "OperationApplier<...>::apply(...): not enough arguments to run this method",
                ErrorCodes::LOGICAL_ERROR);
    }
};


template <class Op>
static ColumnPtr executeForTernaryLogicImpl(ColumnRawPtrs arguments, const DataTypePtr & result_type, size_t input_rows_count)
{
    /// Combine all constant columns into a single constant value.
    UInt8 const_3v_value = 0;
    const bool has_consts = extractConstColumnsAsTernary<Op>(arguments, const_3v_value);

    /// If the constant value uniquely determines the result, return it.
    if (has_consts && (arguments.empty() || Op::isSaturatedValueTernary(const_3v_value)))
    {
        return ColumnConst::create(
            buildColumnFromTernaryData(UInt8Container({const_3v_value}), result_type->isNullable()),
            input_rows_count
        );
    }

    const auto result_column = has_consts ?
            ColumnUInt8::create(input_rows_count, const_3v_value) : ColumnUInt8::create(input_rows_count);

    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, result_column->getData(), has_consts);

    return buildColumnFromTernaryData(result_column->getData(), result_type->isNullable());
}


template <typename Op, typename ... Types>
struct TypedExecutorInvoker;

template <typename Op>
using FastApplierImpl =
        TypedExecutorInvoker<Op, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;

template <typename Op, typename Type, typename ... Types>
struct TypedExecutorInvoker<Op, Type, Types ...>
{
    template <typename T, typename Result>
    static void apply(const ColumnVector<T> & x, const IColumn & y, Result & result)
    {
        if (const auto column = typeid_cast<const ColumnVector<Type> *>(&y))
            std::transform(
                    x.getData().cbegin(), x.getData().cend(),
                    column->getData().cbegin(), result.begin(),
                    [](const auto a, const auto b) { return Op::apply(!!a, !!b); });
        else
            TypedExecutorInvoker<Op, Types ...>::template apply<T>(x, y, result);
    }

    template <typename Result>
    static void apply(const IColumn & x, const IColumn & y, Result & result)
    {
        if (const auto column = typeid_cast<const ColumnVector<Type> *>(&x))
            FastApplierImpl<Op>::template apply<Type>(*column, y, result);
        else
            TypedExecutorInvoker<Op, Types ...>::apply(x, y, result);
    }
};

template <typename Op>
struct TypedExecutorInvoker<Op>
{
    template <typename T, typename Result>
    static void apply(const ColumnVector<T> &, const IColumn & y, Result &)
    {
        throw Exception(std::string("Unknown numeric column y of type: ") + demangle(typeid(y).name()), ErrorCodes::LOGICAL_ERROR);
    }

    template <typename Result>
    static void apply(const IColumn & x, const IColumn &, Result &)
    {
        throw Exception(std::string("Unknown numeric column x of type: ") + demangle(typeid(x).name()), ErrorCodes::LOGICAL_ERROR);
    }
};


/// Types of all of the arguments are guaranteed to be non-nullable here
template <class Op>
static ColumnPtr basicExecuteImpl(ColumnRawPtrs arguments, size_t input_rows_count)
{
    /// Combine all constant columns into a single constant value.
    UInt8 const_val = 0;
    bool has_consts = extractConstColumnsAsBool<Op>(arguments, const_val);

    /// If the constant value uniquely determines the result, return it.
    if (has_consts && (arguments.empty() || Op::apply(const_val, 0) == Op::apply(const_val, 1)))
    {
        if (!arguments.empty())
            const_val = Op::apply(const_val, 0);
        return DataTypeUInt8().createColumnConst(input_rows_count, toField(const_val));
    }

    /// If the constant value is a neutral element, let's forget about it.
    if (has_consts && Op::apply(const_val, 0) == 0 && Op::apply(const_val, 1) == 1)
        has_consts = false;

    auto col_res = has_consts ?
            ColumnUInt8::create(input_rows_count, const_val) : ColumnUInt8::create(input_rows_count);

    /// FastPath detection goes in here
    if (arguments.size() == (has_consts ? 1 : 2))
    {
        if (has_consts)
            FastApplierImpl<Op>::apply(*arguments[0], *col_res, col_res->getData());
        else
            FastApplierImpl<Op>::apply(*arguments[0], *arguments[1], col_res->getData());

        return col_res;
    }

    /// Convert all columns to UInt8
    UInt8ColumnPtrs uint8_args;
    Columns converted_columns_holder;
    for (const IColumn * column : arguments)
    {
        if (const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(column))
            uint8_args.push_back(uint8_column);
        else
        {
            auto converted_column = ColumnUInt8::create(input_rows_count);
            convertAnyColumnToBool(column, converted_column->getData());
            uint8_args.push_back(converted_column.get());
            converted_columns_holder.emplace_back(std::move(converted_column));
        }
    }

    OperationApplier<Op, AssociativeApplierImpl>::apply(uint8_args, col_res->getData(), has_consts);

    return col_res;
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
ColumnPtr FunctionAnyArityLogical<Impl, Name>::executeImpl(
    ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    ColumnRawPtrs args_in;
    for (const auto & arg_index : arguments)
        args_in.push_back(arg_index.column.get());

    if (result_type->isNullable())
        return executeForTernaryLogicImpl<Impl>(std::move(args_in), result_type, input_rows_count);
    else
        return basicExecuteImpl<Impl>(std::move(args_in), input_rows_count);
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
ColumnPtr functionUnaryExecuteType(ColumnsWithTypeAndName & arguments)
{
    if (auto col = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get()))
    {
        auto col_res = ColumnUInt8::create();

        typename ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(col->getData().size());
        UnaryOperationImpl<T, Impl<T>>::vector(col->getData(), vec_res);

        return col_res;
    }

    return nullptr;
}

template <template <typename> class Impl, typename Name>
ColumnPtr FunctionUnaryLogical<Impl, Name>::executeImpl(ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const
{
    ColumnPtr res;
    if (!((res = functionUnaryExecuteType<Impl, UInt8>(arguments))
        || (res = functionUnaryExecuteType<Impl, UInt16>(arguments))
        || (res = functionUnaryExecuteType<Impl, UInt32>(arguments))
        || (res = functionUnaryExecuteType<Impl, UInt64>(arguments))
        || (res = functionUnaryExecuteType<Impl, Int8>(arguments))
        || (res = functionUnaryExecuteType<Impl, Int16>(arguments))
        || (res = functionUnaryExecuteType<Impl, Int32>(arguments))
        || (res = functionUnaryExecuteType<Impl, Int64>(arguments))
        || (res = functionUnaryExecuteType<Impl, Float32>(arguments))
        || (res = functionUnaryExecuteType<Impl, Float64>(arguments))))
       throw Exception("Illegal column " + arguments[0].column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    return res;
}

}
