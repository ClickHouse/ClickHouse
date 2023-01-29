#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Columns/MaskOperations.h>
#include <Common/typeid_cast.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitors.h>

#include <cstring>
#include <algorithm>


namespace DB
{

REGISTER_FUNCTION(Logical)
{
    factory.registerFunction<FunctionAnd>();
    factory.registerFunction<FunctionOr>();
    factory.registerFunction<FunctionXor>();
    factory.registerFunction<FunctionNot>({}, FunctionFactory::CaseInsensitive); /// Operator NOT(x) can be parsed as a function.
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
    for (size_t i = 0; i < rows_count; ++i)
        new_column->getData()[i] = (ternary_data[i] == Ternary::True);

    if (!make_nullable)
        return new_column;

    auto null_column = ColumnUInt8::create(rows_count);
    for (size_t i = 0; i < rows_count; ++i)
        null_column->getData()[i] = (ternary_data[i] == Ternary::Null);

    return ColumnNullable::create(std::move(new_column), std::move(null_column));
}

template <typename T>
bool tryConvertColumnToBool(const IColumn * column, UInt8Container & res)
{
    const auto column_typed = checkAndGetColumn<ColumnVector<T>>(column);
    if (!column_typed)
        return false;

    auto & data = column_typed->getData();
    size_t data_size = data.size();
    for (size_t i = 0; i < data_size; ++i)
        res[i] = static_cast<bool>(data[i]);

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
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of column: {}", column->getName());
}


template <class Op, bool IsTernary, typename Func>
bool extractConstColumns(ColumnRawPtrs & in, UInt8 & res, Func && func)
{
    bool has_res = false;

    for (Int64 i = static_cast<Int64>(in.size()) - 1; i >= 0; --i)
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
            if constexpr (IsTernary)
                res = Op::ternaryApply(res, x);
            else
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
    return extractConstColumns<Op, false>(
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
    return extractConstColumns<Op, true>(
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


template <typename ... Types>
struct TernaryValueBuilderImpl;

template <typename Type, typename ...Types>
struct TernaryValueBuilderImpl<Type, Types...>
{
    static void build(const IColumn * x, UInt8* __restrict ternary_column_data)
    {
        size_t size = x->size();
        if (x->onlyNull())
        {
            memset(ternary_column_data, Ternary::Null, size);
        }
        else if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(x))
        {
            if (const auto * nested_column = typeid_cast<const ColumnVector<Type> *>(nullable_column->getNestedColumnPtr().get()))
            {
                const auto& null_data = nullable_column->getNullMapData();
                const auto& column_data = nested_column->getData();

                if constexpr (sizeof(Type) == 1)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        auto has_value = static_cast<UInt8>(column_data[i] != 0);
                        auto is_null = !!null_data[i];

                        ternary_column_data[i] = ((has_value << 1) | is_null) & (1 << !is_null);
                    }
                }
                else
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        auto has_value = static_cast<UInt8>(column_data[i] != 0);
                        ternary_column_data[i] = has_value;
                    }

                    for (size_t i = 0; i < size; ++i)
                    {
                        auto has_value = ternary_column_data[i];
                        auto is_null = !!null_data[i];

                        ternary_column_data[i] = ((has_value << 1) | is_null) & (1 << !is_null);
                    }
                }
            }
            else
                TernaryValueBuilderImpl<Types...>::build(x, ternary_column_data);
        }
        else if (const auto column = typeid_cast<const ColumnVector<Type> *>(x))
        {
            auto &column_data = column->getData();

            for (size_t i = 0; i < size; ++i)
            {
                ternary_column_data[i] = (column_data[i] != 0) << 1;
            }
        }
        else
            TernaryValueBuilderImpl<Types...>::build(x, ternary_column_data);
    }
};

template <>
struct TernaryValueBuilderImpl<>
{
    [[noreturn]] static void build(const IColumn * x, UInt8 * /* nullable_ternary_column_data */)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unknown numeric column of type: {}", demangle(typeid(*x).name()));
    }
};

using TernaryValueBuilder =
        TernaryValueBuilderImpl<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;

/// This class together with helper class TernaryValueBuilder can be used with columns of arbitrary data type
/// Converts column of any data type into an intermediate UInt8Column of ternary representation for the
/// vectorized ternary logic evaluation.
template <typename Op, size_t N>
class AssociativeGenericApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : vec(in[in.size() - N]->size()), next{in}
        {
            TernaryValueBuilder::build(in[in.size() - N], vec.data());
        }

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const size_t i) const
    {
        return Op::ternaryApply(vec[i], next.apply(i));
    }

private:
    UInt8Container vec;
    const AssociativeGenericApplierImpl<Op, N - 1> next;
};


template <typename Op>
class AssociativeGenericApplierImpl<Op, 1>
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
        : vec(UInt8Container(in[in.size() - 1]->size()))
        {
            TernaryValueBuilder::build(in[in.size() - 1], vec.data());
        }

    inline ResultValueType apply(const size_t i) const { return vec[i]; }

private:
    UInt8Container vec;
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
            {
                if constexpr (std::is_same_v<OperationApplierImpl<Op, N>, AssociativeApplierImpl<Op, N>>)
                    result_data[i] = Op::apply(result_data[i], operation_applier_impl.apply(i));
                else
                    result_data[i] = Op::ternaryApply(result_data[i], operation_applier_impl.apply(i));
            }
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "OperationApplier<...>::apply(...): not enough arguments to run this method");
    }
};


template <class Op>
ColumnPtr executeForTernaryLogicImpl(ColumnRawPtrs arguments, const DataTypePtr & result_type, size_t input_rows_count)
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
                    [](const auto a, const auto b) { return Op::apply(static_cast<bool>(a), static_cast<bool>(b)); });
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown numeric column y of type: {}", demangle(typeid(y).name()));
    }

    template <typename Result>
    static void apply(const IColumn & x, const IColumn &, Result &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown numeric column x of type: {}", demangle(typeid(x).name()));
    }
};


/// Types of all of the arguments are guaranteed to be non-nullable here
template <class Op>
ColumnPtr basicExecuteImpl(ColumnRawPtrs arguments, size_t input_rows_count)
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
        {
            uint8_args.push_back(uint8_column);
        }
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
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                    "Number of arguments for function \"{}\" should be at least 2: passed {}",
                    getName(), arguments.size());

    bool has_nullable_arguments = false;
    bool has_bool_arguments = false;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto & arg_type = arguments[i];

        if (isBool(arg_type))
            has_bool_arguments = true;

        if (!has_nullable_arguments)
        {
            has_nullable_arguments = arg_type->isNullable();
            if (has_nullable_arguments && !Impl::specialImplementationForNulls())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: Unexpected type of argument for function \"{}\": "
                    " argument {} is of type {}", getName(), i + 1, arg_type->getName());
        }

        if (!(isNativeNumber(arg_type)
            || (Impl::specialImplementationForNulls() && (arg_type->onlyNull() || isNativeNumber(removeNullable(arg_type))))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type ({}) of {} argument of function {}",
                arg_type->getName(), i + 1, getName());
    }

    auto result_type = has_bool_arguments ? DataTypeFactory::instance().get("Bool") : std::make_shared<DataTypeUInt8>();
    return has_nullable_arguments
            ? makeNullable(result_type)
            : result_type;
}

template <bool inverted>
static void applyTernaryLogicImpl(const IColumn::Filter & mask, IColumn::Filter & null_bytemap)
{
    for (size_t i = 0; i != mask.size(); ++i)
    {
        UInt8 value = mask[i];
        if constexpr (inverted)
            value = !value;

        if (null_bytemap[i] && value)
            null_bytemap[i] = 0;
    }
}

template <typename Name>
static void applyTernaryLogic(const IColumn::Filter & mask, IColumn::Filter & null_bytemap)
{
    if (Name::name == NameAnd::name)
        applyTernaryLogicImpl<true>(mask, null_bytemap);
    else if (Name::name == NameOr::name)
        applyTernaryLogicImpl<false>(mask, null_bytemap);
}

template <typename Impl, typename Name>
ColumnPtr FunctionAnyArityLogical<Impl, Name>::executeShortCircuit(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
{
    if (Name::name != NameAnd::name && Name::name != NameOr::name)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} doesn't support short circuit execution", getName());

    executeColumnIfNeeded(arguments[0]);

    /// Let's denote x_i' = maskedExecute(x_i, mask).
    /// 1) AND(x_0, x_1, x_2, ..., x_n)
    /// We will support mask_i = x_0 & x_1 & ... & x_i.
    /// Base:
    /// mask_0 is 1 everywhere, x_0' = x_0.
    /// Iteration:
    /// mask_i = extractMask(mask_{i - 1}, x_{i - 1}')
    /// x_i' = maskedExecute(x_i, mask)
    /// Also we will treat NULL as 1 if x_i' is Nullable
    /// to support ternary logic.
    /// The result is mask_n.
    ///
    /// 1) OR(x_0, x_1, x_2, ..., x_n)
    /// We will support mask_i = !x_0 & !x_1 & ... & !x_i.
    /// mask_0 is 1 everywhere, x_0' = x_0.
    /// mask = extractMask(mask, !x_{i - 1}')
    /// x_i' = maskedExecute(x_i, mask)
    /// Also we will treat NULL as 0 if x_i' is Nullable
    /// to support ternary logic.
    /// The result is !mask_n.

    bool inverted = Name::name != NameAnd::name;
    UInt8 null_value = static_cast<UInt8>(Name::name == NameAnd::name);
    IColumn::Filter mask(arguments[0].column->size(), 1);

    /// If result is nullable, we need to create null bytemap of the resulting column.
    /// We will fill it while extracting mask from arguments.
    std::unique_ptr<IColumn::Filter> nulls;
    if (result_type->isNullable())
        nulls = std::make_unique<IColumn::Filter>(arguments[0].column->size(), 0);

    MaskInfo mask_info;
    for (size_t i = 1; i <= arguments.size(); ++i)
    {
        if (inverted)
            mask_info = extractInvertedMask(mask, arguments[i - 1].column, nulls.get(), null_value);
        else
            mask_info = extractMask(mask, arguments[i - 1].column, nulls.get(), null_value);

        /// If mask doesn't have ones, we don't need to execute the rest arguments,
        /// because the result won't change.
        if (!mask_info.has_ones || i == arguments.size())
            break;

        maskedExecute(arguments[i], mask, mask_info);
    }
    /// For OR function we need to inverse mask to get the resulting column.
    if (inverted)
        inverseMask(mask, mask_info);

    if (nulls)
        applyTernaryLogic<Name>(mask, *nulls);

    auto res = ColumnUInt8::create();
    res->getData() = std::move(mask);

    if (!nulls)
        return res;

    auto bytemap = ColumnUInt8::create();
    bytemap->getData() = std::move(*nulls);
    return ColumnNullable::create(std::move(res), std::move(bytemap));
}

template <typename Impl, typename Name>
ColumnPtr FunctionAnyArityLogical<Impl, Name>::executeImpl(
    const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const
{
    ColumnsWithTypeAndName arguments = args;

    /// Special implementation for short-circuit arguments.
    if (checkShortCircuitArguments(arguments) != -1)
        return executeShortCircuit(arguments, result_type);

    ColumnRawPtrs args_in;
    for (const auto & arg_index : arguments)
        args_in.push_back(arg_index.column.get());

    if (result_type->isNullable())
        return executeForTernaryLogicImpl<Impl>(std::move(args_in), result_type, input_rows_count);
    else
        return basicExecuteImpl<Impl>(std::move(args_in), input_rows_count);
}

template <typename Impl, typename Name>
ColumnPtr FunctionAnyArityLogical<Impl, Name>::getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
{
    /** Try to perform optimization for saturable functions (AndFunction, OrFunction) in case some arguments are
      * constants.
      * If function is not saturable (XorFunction) we cannot perform such optimization.
      * If function is AndFunction and in arguments there is constant false, result is false.
      * If function is OrFunction and in arguments there is constant true, result is true.
      */
    if constexpr (!Impl::isSaturable())
        return nullptr;

    bool has_true_constant = false;
    bool has_false_constant = false;

    for (const auto & argument : arguments)
    {
        ColumnPtr column = argument.column;

        if (!column || !isColumnConst(*column))
            continue;

        DataTypePtr non_nullable_type = removeNullable(argument.type);
        TypeIndex data_type_index = non_nullable_type->getTypeId();

        if (!isNativeNumber(data_type_index))
            continue;

        const ColumnConst * const_column = static_cast<const ColumnConst *>(column.get());

        Field constant_field_value = const_column->getField();
        if (constant_field_value.isNull())
            continue;

        auto field_type = constant_field_value.getType();

        bool constant_value_bool = false;

        if (field_type == Field::Types::Float64)
            constant_value_bool = static_cast<bool>(constant_field_value.get<Float64>());
        else if (field_type == Field::Types::Int64)
            constant_value_bool = static_cast<bool>(constant_field_value.get<Int64>());
        else if (field_type == Field::Types::UInt64)
            constant_value_bool = static_cast<bool>(constant_field_value.get<UInt64>());

        has_true_constant = has_true_constant || constant_value_bool;
        has_false_constant = has_false_constant || !constant_value_bool;
    }

    ColumnPtr result_column;

    if constexpr (std::is_same_v<Impl, AndImpl>)
    {
        if (has_false_constant)
            result_column = result_type->createColumnConst(0, static_cast<UInt8>(false));
    }
    else if constexpr (std::is_same_v<Impl, OrImpl>)
    {
        if (has_true_constant)
            result_column = result_type->createColumnConst(0, static_cast<UInt8>(true));
    }

    return result_column;
}

template <template <typename> class Impl, typename Name>
DataTypePtr FunctionUnaryLogical<Impl, Name>::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (!isNativeNumber(arguments[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type ({}) of argument of function {}",
            arguments[0]->getName(),
            getName());

    return isBool(arguments[0]) ? DataTypeFactory::instance().get("Bool") : std::make_shared<DataTypeUInt8>();
}

template <template <typename> class Impl, typename T>
ColumnPtr functionUnaryExecuteType(const ColumnsWithTypeAndName & arguments)
{
    if (auto col = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get()))
    {
        auto col_res = ColumnUInt8::create(col->getData().size());
        auto & vec_res = col_res->getData();

        UnaryOperationImpl<T, Impl<T>>::vector(col->getData(), vec_res);

        return col_res;
    }

    return nullptr;
}

template <template <typename> class Impl, typename Name>
ColumnPtr FunctionUnaryLogical<Impl, Name>::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const
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
       throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument of function {}",
            arguments[0].column->getName(),
            getName());

    return res;
}

}
