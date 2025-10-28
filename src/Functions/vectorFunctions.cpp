#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/ITupleFunction.h>
#include <Functions/castTypeToEither.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

/// Checks that passed data types are tuples and have the same size.
/// Returns size of tuples.
size_t checkAndGetTuplesSize(const DataTypePtr & lhs_type, const DataTypePtr & rhs_type, const String & function_name = {})
{
    const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(lhs_type.get());
    const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(rhs_type.get());

    if (!left_tuple)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0{} should be tuple, got {}",
                        function_name.empty() ? "" : fmt::format(" of function {}", function_name), lhs_type->getName());

    if (!right_tuple)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 1{}should be tuple, got {}",
                        function_name.empty() ? "" : fmt::format(" of function {}", function_name), rhs_type->getName());

    const auto & left_types = left_tuple->getElements();
    const auto & right_types = right_tuple->getElements();

    if (left_types.size() != right_types.size())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Expected tuples of the same size as arguments{}, got {} and {}",
                        function_name.empty() ? "" : fmt::format(" of function {}", function_name), lhs_type->getName(), rhs_type->getName());
    return left_types.size();
}

}

struct PlusName { static constexpr auto name = "plus"; };
struct MinusName { static constexpr auto name = "minus"; };
struct MultiplyName { static constexpr auto name = "multiply"; };
struct DivideName { static constexpr auto name = "divide"; };
struct ModuloName { static constexpr auto name = "modulo"; };
struct IntDivName { static constexpr auto name = "intDiv"; };
struct IntDivOrZeroName { static constexpr auto name = "intDivOrZero"; };

struct L1Label { static constexpr auto name = "1"; };
struct L2Label { static constexpr auto name = "2"; };
struct L2SquaredLabel { static constexpr auto name = "2Squared"; };
struct L2TransposedLabel { static constexpr auto name = "2";};
struct LinfLabel { static constexpr auto name = "inf"; };
struct LpLabel { static constexpr auto name = "p"; };

struct L2DistanceTransposedTraits;

constexpr std::string makeFirstLetterUppercase(const std::string & str)
{
    std::string res(str);
    res[0] += 'A' - 'a';
    return res;
}

template <class FuncName>
class FunctionTupleOperator : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = "tuple" + makeFirstLetterUppercase(FuncName::name);

    explicit FunctionTupleOperator(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTupleOperator>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t tuple_size = checkAndGetTuplesSize(arguments[0].type, arguments[1].type, getName());

        const auto & left_types = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get())->getElements();
        const auto & right_types = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get())->getElements();

        Columns left_elements = arguments[0].column ? getTupleElements(*arguments[0].column) : Columns();
        Columns right_elements = arguments[1].column ? getTupleElements(*arguments[1].column) : Columns();

        auto func = FunctionFactory::instance().get(FuncName::name, context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{left_elements.empty() ? nullptr : left_elements[i], left_types[i], {}};
                ColumnWithTypeAndName right{right_elements.empty() ? nullptr : right_elements[i], right_types[i], {}};
                auto elem_func = func->build(ColumnsWithTypeAndName{left, right});
                types[i] = elem_func->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();
        auto left_elements = getTupleElements(*arguments[0].column);
        auto right_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = left_elements.size();
        if (tuple_size == 0)
            return ColumnTuple::create(input_rows_count);

        auto func = FunctionFactory::instance().get(FuncName::name, context);
        Columns columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName left{left_elements[i], left_types[i], {}};
            ColumnWithTypeAndName right{right_elements[i], right_types[i], {}};
            auto elem_func = func->build(ColumnsWithTypeAndName{left, right});
            columns[i] = elem_func->execute({left, right}, elem_func->getResultType(), input_rows_count, /* dry_run = */ false)
                                  ->convertToFullColumnIfConst();
        }

        return ColumnTuple::create(columns);
    }
};

using FunctionTuplePlus = FunctionTupleOperator<PlusName>;
using FunctionTupleMinus = FunctionTupleOperator<MinusName>;
using FunctionTupleMultiply = FunctionTupleOperator<MultiplyName>;
using FunctionTupleDivide = FunctionTupleOperator<DivideName>;
using FunctionTupleModulo = FunctionTupleOperator<ModuloName>;
using FunctionTupleIntDiv = FunctionTupleOperator<IntDivName>;
using FunctionTupleIntDivOrZero = FunctionTupleOperator<IntDivOrZeroName>;

class FunctionTupleNegate : public ITupleFunction
{
public:
    static constexpr auto name = "tupleNegate";

    explicit FunctionTupleNegate(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTupleNegate>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[0].column)
            cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_types.size();
        auto negate = FunctionFactory::instance().get("negate", context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_negate = negate->build(ColumnsWithTypeAndName{cur});
                types[i] = elem_negate->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return std::make_shared<DataTypeTuple>(std::move(types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return ColumnTuple::create(input_rows_count);

        auto negate = FunctionFactory::instance().get("negate", context);
        Columns columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_negate = negate->build(ColumnsWithTypeAndName{cur});
            columns[i] = elem_negate->execute({cur}, elem_negate->getResultType(), input_rows_count, /* dry_run = */ false)
                                    ->convertToFullColumnIfConst();
        }

        return ColumnTuple::create(columns);
    }
};

template <class FuncName>
class FunctionTupleOperatorByNumber : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = "tuple" + makeFirstLetterUppercase(FuncName::name) + "ByNumber";

    explicit FunctionTupleOperatorByNumber(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTupleOperatorByNumber>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements = arguments[0].column ? getTupleElements(*arguments[0].column) : Columns();

        size_t tuple_size = cur_types.size();

        const auto & p_column = arguments[1];
        auto func = FunctionFactory::instance().get(FuncName::name, context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_func = func->build(ColumnsWithTypeAndName{cur, p_column});
                types[i] = elem_func->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return ColumnTuple::create(input_rows_count);

        const auto & p_column = arguments[1];
        auto func = FunctionFactory::instance().get(FuncName::name, context);
        Columns columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_func = func->build(ColumnsWithTypeAndName{cur, p_column});
            columns[i] = elem_func->execute({cur, p_column}, elem_func->getResultType(), input_rows_count, /* dry_run = */ false)
                                  ->convertToFullColumnIfConst();
        }

        return ColumnTuple::create(columns);
    }
};

using FunctionTupleMultiplyByNumber = FunctionTupleOperatorByNumber<MultiplyName>;
using FunctionTupleDivideByNumber = FunctionTupleOperatorByNumber<DivideName>;
using FunctionTupleModuloByNumber = FunctionTupleOperatorByNumber<ModuloName>;
using FunctionTupleIntDivByNumber = FunctionTupleOperatorByNumber<IntDivName>;
using FunctionTupleIntDivOrZeroByNumber = FunctionTupleOperatorByNumber<IntDivOrZeroName>;

class FunctionDotProduct : public ITupleFunction
{
public:
    static constexpr auto name = "dotProduct";

    explicit FunctionDotProduct(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionDotProduct>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());

        if (!left_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        if (!right_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 1 of function {} should be tuple, got {}",
                            getName(), arguments[1].type->getName());

        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();

        Columns left_elements;
        Columns right_elements;
        if (arguments[0].column)
            left_elements = getTupleElements(*arguments[0].column);
        if (arguments[1].column)
            right_elements = getTupleElements(*arguments[1].column);

        if (left_types.size() != right_types.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expected tuples of the same size as arguments of function {}. Got {} and {}",
                            getName(), arguments[0].type->getName(), arguments[1].type->getName());

        size_t tuple_size = left_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{left_elements.empty() ? nullptr : left_elements[i], left_types[i], {}};
                ColumnWithTypeAndName right{right_elements.empty() ? nullptr : right_elements[i], right_types[i], {}};
                auto elem_multiply = multiply->build(ColumnsWithTypeAndName{left, right});

                if (i == 0)
                {
                    res_type = elem_multiply->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_multiply->getResultType(), {}};
                auto plus_elem = plus->build({left_type, right_type});
                res_type = plus_elem->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();
        auto left_elements = getTupleElements(*arguments[0].column);
        auto right_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = left_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName left{left_elements[i], left_types[i], {}};
            ColumnWithTypeAndName right{right_elements[i], right_types[i], {}};
            auto elem_multiply = multiply->build(ColumnsWithTypeAndName{left, right});

            ColumnWithTypeAndName column;
            column.type = elem_multiply->getResultType();
            column.column = elem_multiply->execute({left, right}, column.type, input_rows_count, /* dry_run = */ false);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto plus_elem = plus->build({res, column});
                auto res_type = plus_elem->getResultType();
                res.column = plus_elem->execute({res, column}, res_type, input_rows_count, /* dry_run = */ false);
                res.type = res_type;
            }
        }

        return res.column;
    }
};

template <typename Impl>
class FunctionDateOrDateTimeOperationTupleOfIntervals : public ITupleFunction
{
public:
    static constexpr auto name = Impl::name;

    explicit FunctionDateOrDateTimeOperationTupleOfIntervals(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionDateOrDateTimeOperationTupleOfIntervals>(context_);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}. Should be a date or a date with time",
                    arguments[0].type->getName(), getName()};

        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());

        if (!cur_tuple)
            throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of second argument of function {}. Should be a tuple",
                    arguments[0].type->getName(), getName()};

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[1].column)
            cur_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return arguments[0].type;

        auto plus = FunctionFactory::instance().get(Impl::func_name, context);
        DataTypePtr res_type = arguments[0].type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{res_type, {}};
                ColumnWithTypeAndName right{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto plus_elem = plus->build({left, right});
                res_type = plus_elem->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return arguments[0].column;

        auto plus = FunctionFactory::instance().get(Impl::func_name, context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName column{cur_elements[i], cur_types[i], {}};
            auto elem_plus = plus->build(ColumnsWithTypeAndName{i == 0 ? arguments[0] : res, column});
            auto res_type = elem_plus->getResultType();
            res.column = elem_plus->execute({i == 0 ? arguments[0] : res, column}, res_type, input_rows_count, /* dry_run = */ false);
            res.type = res_type;
        }

        return res.column;
    }
};

struct AddTupleOfIntervalsImpl
{
    static constexpr auto name = "addTupleOfIntervals";
    static constexpr auto func_name = "plus";
};

struct SubtractTupleOfIntervalsImpl
{
    static constexpr auto name = "subtractTupleOfIntervals";
    static constexpr auto func_name = "minus";
};

using FunctionAddTupleOfIntervals = FunctionDateOrDateTimeOperationTupleOfIntervals<AddTupleOfIntervalsImpl>;
using FunctionSubtractTupleOfIntervals = FunctionDateOrDateTimeOperationTupleOfIntervals<SubtractTupleOfIntervalsImpl>;

template <bool is_minus>
struct FunctionTupleOperationInterval : public ITupleFunction
{
public:
    static constexpr auto name = is_minus ? "subtractInterval" : "addInterval";

    explicit FunctionTupleOperationInterval(ContextPtr context_) : ITupleFunction(context_) {}

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionTupleOperationInterval>(context_);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isTuple(arguments[0]) && !isInterval(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, must be Tuple or Interval",
                arguments[0]->getName(), getName());

        if (!isInterval(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, must be Interval",
                arguments[1]->getName(), getName());

        DataTypes types;

        const auto * tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].get());

        if (tuple)
        {
            const auto & cur_types = tuple->getElements();

            for (const auto & type : cur_types)
                if (!isInterval(type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of Tuple element of first argument of function {}, must be Interval",
                        type->getName(), getName());

            types = cur_types;
        }
        else
        {
            types = {arguments[0]};
        }

        if (!types.empty())
        {
            const auto * interval_last = checkAndGetDataType<DataTypeInterval>(types.back().get());
            const auto * interval_new = checkAndGetDataType<DataTypeInterval>(arguments[1].get());

            if (!interval_last->equals(*interval_new))
                types.push_back(arguments[1]);
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!isInterval(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, must be Interval",
                arguments[1].type->getName(), getName());

        Columns tuple_columns;

        const auto * first_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * first_interval = checkAndGetDataType<DataTypeInterval>(arguments[0].type.get());
        const auto * second_interval = checkAndGetDataType<DataTypeInterval>(arguments[1].type.get());

        bool can_be_merged;

        if (first_interval)
        {
            can_be_merged = first_interval->equals(*second_interval);

            if (can_be_merged)
                tuple_columns.resize(1);
            else
                tuple_columns.resize(2);

            tuple_columns[0] = arguments[0].column->convertToFullColumnIfConst();
        }
        else if (first_tuple)
        {
            const auto & cur_types = first_tuple->getElements();

            for (const auto & type : cur_types)
                if (!isInterval(type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of Tuple element of first argument of function {}, must be Interval",
                        type->getName(), getName());

            auto cur_elements = getTupleElements(*arguments[0].column);
            size_t tuple_size = cur_elements.size();

            if (tuple_size == 0)
                return ColumnTuple::create(input_rows_count);

            const auto * tuple_last_interval = checkAndGetDataType<DataTypeInterval>(cur_types.back().get());
            can_be_merged = tuple_last_interval->equals(*second_interval);

            if (can_be_merged)
                tuple_columns.resize(tuple_size);
            else
                tuple_columns.resize(tuple_size + 1);

            for (size_t i = 0; i < tuple_size; ++i)
                tuple_columns[i] = cur_elements[i];
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, must be Tuple or Interval",
                arguments[0].type->getName(), getName());


        ColumnPtr & last_column = tuple_columns.back();

        if (can_be_merged)
        {
            ColumnWithTypeAndName left{last_column, arguments[1].type, {}};

            if constexpr (is_minus)
            {
                auto minus = FunctionFactory::instance().get("minus", context);
                auto elem_minus = minus->build({left, arguments[1]});
                last_column = elem_minus->execute({left, arguments[1]}, arguments[1].type, input_rows_count, /* dry_run = */ false)
                                        ->convertToFullColumnIfConst();
            }
            else
            {
                auto plus = FunctionFactory::instance().get("plus", context);
                auto elem_plus = plus->build({left, arguments[1]});
                last_column = elem_plus->execute({left, arguments[1]}, arguments[1].type, input_rows_count, /* dry_run = */ false)
                                        ->convertToFullColumnIfConst();
            }
        }
        else
        {
            if constexpr (is_minus)
            {
                auto negate = FunctionFactory::instance().get("negate", context);
                auto elem_negate = negate->build({arguments[1]});
                last_column = elem_negate->execute({arguments[1]}, arguments[1].type, input_rows_count, /* dry_run = */ false);
            }
            else
            {
                last_column = arguments[1].column;
            }
        }

        return ColumnTuple::create(tuple_columns);
    }
};

using FunctionTupleAddInterval = FunctionTupleOperationInterval<false>;
using FunctionTupleSubtractInterval = FunctionTupleOperationInterval<true>;


/// this is for convenient usage in LNormalize
template <class FuncLabel>
class FunctionLNorm : public ITupleFunction {};

template <>
class FunctionLNorm<L1Label> : public ITupleFunction
{
public:
    static constexpr auto name = "L1Norm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements = arguments[0].column ? getTupleElements(*arguments[0].column) : Columns();

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto abs = FunctionFactory::instance().get("abs", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

                if (i == 0)
                {
                    res_type = elem_abs->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_abs->getResultType(), {}};
                auto plus_elem = plus->build({left_type, right_type});
                res_type = plus_elem->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto abs = FunctionFactory::instance().get("abs", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

            ColumnWithTypeAndName column;
            column.type = elem_abs->getResultType();
            column.column = elem_abs->execute({cur}, column.type, input_rows_count, /* dry_run = */ false);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto plus_elem = plus->build({res, column});
                auto res_type = plus_elem->getResultType();
                res.column = plus_elem->execute({res, column}, res_type, input_rows_count, /* dry_run = */ false);
                res.type = res_type;
            }
        }

        return res.column;
    }
};
using FunctionL1Norm = FunctionLNorm<L1Label>;

template <>
class FunctionLNorm<L2SquaredLabel> : public ITupleFunction
{
public:
    static constexpr auto name = "L2SquaredNorm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[0].column)
            cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_multiply = multiply->build(ColumnsWithTypeAndName{cur, cur});

                if (i == 0)
                {
                    res_type = elem_multiply->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_multiply->getResultType(), {}};
                auto plus_elem = plus->build({left_type, right_type});
                res_type = plus_elem->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_multiply = multiply->build(ColumnsWithTypeAndName{cur, cur});

            ColumnWithTypeAndName column;
            column.type = elem_multiply->getResultType();
            column.column = elem_multiply->execute({cur, cur}, column.type, input_rows_count, /* dry_run = */ false);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto plus_elem = plus->build({res, column});
                auto res_type = plus_elem->getResultType();
                res.column = plus_elem->execute({res, column}, res_type, input_rows_count, /* dry_run = */ false);
                res.type = res_type;
            }
        }

        return res.column;
    }
};
using FunctionL2SquaredNorm = FunctionLNorm<L2SquaredLabel>;

template <>
class FunctionLNorm<L2Label> : public FunctionL2SquaredNorm
{
private:
    using Base =  FunctionL2SquaredNorm;
public:
    static constexpr auto name = "L2Norm";

    explicit FunctionLNorm(ContextPtr context_) : Base(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();
        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto sqrt = FunctionFactory::instance().get("sqrt", context);
        return sqrt->build({ColumnWithTypeAndName{Base::getReturnTypeImpl(arguments), {}}})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        ColumnWithTypeAndName squared_res;
        squared_res.type = Base::getReturnTypeImpl(arguments);
        squared_res.column = Base::executeImpl(arguments, squared_res.type, input_rows_count);

        auto sqrt = FunctionFactory::instance().get("sqrt", context);
        auto sqrt_elem = sqrt->build({squared_res});
        return sqrt_elem->execute({squared_res}, sqrt_elem->getResultType(), input_rows_count, /* dry_run = */ false);
    }
};
using FunctionL2Norm = FunctionLNorm<L2Label>;

template <>
class FunctionLNorm<LinfLabel> : public ITupleFunction
{
public:
    static constexpr auto name = "LinfNorm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[0].column)
            cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto abs = FunctionFactory::instance().get("abs", context);
        auto max = FunctionFactory::instance().get("max2", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

                if (i == 0)
                {
                    res_type = elem_abs->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_abs->getResultType(), {}};
                auto max_elem = max->build({left_type, right_type});
                res_type = max_elem->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto abs = FunctionFactory::instance().get("abs", context);
        auto max = FunctionFactory::instance().get("max2", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

            ColumnWithTypeAndName column;
            column.type = elem_abs->getResultType();
            column.column = elem_abs->execute({cur}, column.type, input_rows_count, /* dry_run = */ false);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto max_elem = max->build({res, column});
                auto res_type = max_elem->getResultType();
                res.column = max_elem->execute({res, column}, res_type, input_rows_count, /* dry_run = */ false);
                res.type = res_type;
            }
        }

        return res.column;
    }
};
using FunctionLinfNorm = FunctionLNorm<LinfLabel>;

template <>
class FunctionLNorm<LpLabel> : public ITupleFunction
{
public:
    static constexpr auto name = "LpNorm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[0].column)
            cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        const auto & p_column = arguments[1];
        auto abs = FunctionFactory::instance().get("abs", context);
        auto pow = FunctionFactory::instance().get("pow", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});
                cur.type = elem_abs->getResultType();
                cur.column = cur.type->createColumn();

                auto elem_pow = pow->build(ColumnsWithTypeAndName{cur, p_column});

                if (i == 0)
                {
                    res_type = elem_pow->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_pow->getResultType(), {}};
                auto plus_elem = plus->build({left_type, right_type});
                res_type = plus_elem->getResultType();
            }
            catch (Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        ColumnWithTypeAndName inv_p_column{std::make_shared<DataTypeFloat64>(), {}};
        return pow->build({ColumnWithTypeAndName{res_type, {}}, inv_p_column})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        const auto & p_column = arguments[1];

        if (!isColumnConst(*p_column.column) && p_column.column->size() != 1)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be either constant Float64 or constant UInt", getName());

        double p;
        if (isFloat(p_column.column->getDataType()))
            p = p_column.column->getFloat64(0);
        else if (isUInt(p_column.column->getDataType()))
            p = p_column.column->getUInt(0);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be either constant Float64 or constant UInt", getName());

        if (p < 1 || p >= HUGE_VAL)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Second argument for function {} must be not less than one and not be an infinity",
                            getName());

        auto abs = FunctionFactory::instance().get("abs", context);
        auto pow = FunctionFactory::instance().get("pow", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});
            cur.column = elem_abs->execute({cur}, elem_abs->getResultType(), input_rows_count, /* dry_run = */ false);
            cur.type = elem_abs->getResultType();

            auto elem_pow = pow->build(ColumnsWithTypeAndName{cur, p_column});

            ColumnWithTypeAndName column;
            column.type = elem_pow->getResultType();
            column.column = elem_pow->execute({cur, p_column}, column.type, input_rows_count, /* dry_run = */ false);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto plus_elem = plus->build({res, column});
                auto res_type = plus_elem->getResultType();
                res.column = plus_elem->execute({res, column}, res_type, input_rows_count, /* dry_run = */ false);
                res.type = res_type;
            }
        }

        ColumnWithTypeAndName inv_p_column{DataTypeFloat64().createColumnConst(input_rows_count, 1 / p),
                                           std::make_shared<DataTypeFloat64>(), {}};
        auto pow_elem = pow->build({res, inv_p_column});
        return pow_elem->execute({res, inv_p_column}, pow_elem->getResultType(), input_rows_count, /* dry_run = */ false);
    }
};
using FunctionLpNorm = FunctionLNorm<LpLabel>;

template <class FuncLabel>
class FunctionLDistance : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name =  std::string("L") + FuncLabel::name + "Distance" + (std::is_same_v<FuncLabel, L2TransposedLabel> ? "Transposed" : "");

    explicit FunctionLDistance(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLDistance>(context_); }

    String getName() const override { return name; }

    bool isVariadic() const override
    {
        if constexpr (std::is_same_v<FuncLabel, L2TransposedLabel>)
            return true;

        return false;
    }

    size_t getNumberOfArguments() const override
    {
        if constexpr (std::is_same_v<FuncLabel, L2TransposedLabel>)
            return 0;
        else if constexpr (FuncLabel::name[0] == 'p')
            return 3;
        else
            return 2;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return {2};
        else
            return {};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionTupleMinus tuple_minus(context);
        auto type = tuple_minus.getReturnTypeImpl(arguments);

        ColumnWithTypeAndName minus_res{type, {}};

        auto func = FunctionFactory::instance().get(std::string("L") + FuncLabel::name + "Norm", context);
        if constexpr (FuncLabel::name[0] == 'p')
            return func->build({minus_res, arguments[2]})->getResultType();
        else
            return func->build({minus_res})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        FunctionTupleMinus tuple_minus(context);
        auto type = tuple_minus.getReturnTypeImpl(arguments);
        auto column = tuple_minus.executeImpl(arguments, DataTypePtr(), input_rows_count);

        ColumnWithTypeAndName minus_res{column, type, {}};

        auto func = FunctionFactory::instance().get(std::string("L") + FuncLabel::name + "Norm", context);
        if constexpr (FuncLabel::name[0] == 'p')
        {
            auto func_elem = func->build({minus_res, arguments[2]});
            return func_elem->execute({minus_res, arguments[2]}, func_elem->getResultType(), input_rows_count, /* dry_run = */ false);
        }
        else
        {
            auto func_elem = func->build({minus_res});
            return func_elem->execute({minus_res}, func_elem->getResultType(), input_rows_count, /* dry_run = */ false);
        }
    }
};

using FunctionL1Distance = FunctionLDistance<L1Label>;
using FunctionL2Distance = FunctionLDistance<L2Label>;
using FunctionL2SquaredDistance = FunctionLDistance<L2SquaredLabel>;
using FunctionL2DistanceTransposed = FunctionLDistance<L2TransposedLabel>;
using FunctionLinfDistance = FunctionLDistance<LinfLabel>;
using FunctionLpDistance = FunctionLDistance<LpLabel>;

template <class FuncLabel>
class FunctionLNormalize : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = std::string("L") + FuncLabel::name + "Normalize";

    explicit FunctionLNormalize(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNormalize>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return 2;
        else
            return 1;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return {1};
        else
            return {};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionLNorm<FuncLabel> norm(context);
        auto type = norm.getReturnTypeImpl(arguments);

        ColumnWithTypeAndName norm_res{type, {}};

        FunctionTupleDivideByNumber divide(context);
        return divide.getReturnTypeImpl({arguments[0], norm_res});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        FunctionLNorm<FuncLabel> norm(context);
        auto type = norm.getReturnTypeImpl(arguments);
        auto column = norm.executeImpl(arguments, DataTypePtr(), input_rows_count);

        ColumnWithTypeAndName norm_res{column, type, {}};

        FunctionTupleDivideByNumber divide(context);
        return divide.executeImpl({arguments[0], norm_res}, DataTypePtr(), input_rows_count);
    }
};

using FunctionL1Normalize = FunctionLNormalize<L1Label>;
using FunctionL2Normalize = FunctionLNormalize<L2Label>;
using FunctionLinfNormalize = FunctionLNormalize<LinfLabel>;
using FunctionLpNormalize = FunctionLNormalize<LpLabel>;

class FunctionCosineDistance : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = "cosineDistance";

    explicit FunctionCosineDistance(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionCosineDistance>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t tuple_size = checkAndGetTuplesSize(arguments[0].type, arguments[1].type, getName());
        if (tuple_size == 0)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Result of function {} is undefined for empty tuples", getName());

        FunctionDotProduct dot(context);
        ColumnWithTypeAndName dot_result{dot.getReturnTypeImpl(arguments), {}};

        FunctionL2Norm norm(context);
        ColumnWithTypeAndName first_norm{norm.getReturnTypeImpl({arguments[0]}), {}};
        ColumnWithTypeAndName second_norm{norm.getReturnTypeImpl({arguments[1]}), {}};

        auto minus = FunctionFactory::instance().get("minus", context);
        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto divide = FunctionFactory::instance().get("divide", context);

        ColumnWithTypeAndName one{std::make_shared<DataTypeUInt8>(), {}};

        ColumnWithTypeAndName multiply_result{multiply->build({first_norm, second_norm})->getResultType(), {}};
        ColumnWithTypeAndName divide_result{divide->build({dot_result, multiply_result})->getResultType(), {}};
        return minus->build({one, divide_result})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// TODO: cosineDistance does not support nullable arguments
        /// https://github.com/ClickHouse/ClickHouse/pull/27933#issuecomment-916670286
        auto return_type = getReturnTypeImpl(arguments);
        if (return_type->isNullable())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        FunctionDotProduct dot(context);
        ColumnWithTypeAndName dot_result{dot.executeImpl(arguments, DataTypePtr(), input_rows_count),
                                         dot.getReturnTypeImpl(arguments), {}};

        FunctionL2Norm norm(context);
        ColumnWithTypeAndName first_norm{norm.executeImpl({arguments[0]}, DataTypePtr(), input_rows_count),
                                         norm.getReturnTypeImpl({arguments[0]}), {}};
        ColumnWithTypeAndName second_norm{norm.executeImpl({arguments[1]}, DataTypePtr(), input_rows_count),
                                          norm.getReturnTypeImpl({arguments[1]}), {}};

        auto minus = FunctionFactory::instance().get("minus", context);
        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto divide = FunctionFactory::instance().get("divide", context);

        ColumnWithTypeAndName one{DataTypeUInt8().createColumnConst(input_rows_count, 1),
                                  std::make_shared<DataTypeUInt8>(), {}};

        auto multiply_elem = multiply->build({first_norm, second_norm});
        ColumnWithTypeAndName multiply_result;
        multiply_result.type = multiply_elem->getResultType();
        multiply_result.column = multiply_elem->execute({first_norm, second_norm},
                                                        multiply_result.type, input_rows_count, /* dry_run = */ false);

        auto divide_elem = divide->build({dot_result, multiply_result});
        ColumnWithTypeAndName divide_result;
        divide_result.type = divide_elem->getResultType();
        divide_result.column = divide_elem->execute({dot_result, multiply_result},
                                                    divide_result.type, input_rows_count, /* dry_run = */ false);

        auto minus_elem = minus->build({one, divide_result});
        return minus_elem->execute({one, divide_result}, minus_elem->getResultType(), input_rows_count, /* dry_run = */ false);
    }
};


/// An adaptor to call Norm/Distance function for tuple or array depending on the 1st argument type
template <class Traits>
class TupleOrArrayFunction : public IFunction
{
public:
    static constexpr auto name = Traits::name;

    explicit TupleOrArrayFunction(ContextPtr context_)
        : IFunction()
        , tuple_function(Traits::CreateTupleFunction(context_))
        , array_function(Traits::CreateArrayFunction(context_)) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<TupleOrArrayFunction>(context_); }

    String getName() const override { return name; }

    bool isVariadic() const override { return tuple_function->isVariadic(); }

    size_t getNumberOfArguments() const override { return tuple_function->getNumberOfArguments(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// Since L2DistanceTransposed is a variadic function, we check the number of arguments, as we won't do it later like with others
        if (std::is_same_v<Traits, L2DistanceTransposedTraits> && arguments.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Function {} requires at least 2 arguments, got {}", getName(), arguments.size());

        /// DataTypeFixedString is needed for the optimised L2DistanceTransposed(vec.1, ..., vec.p, qbit_size, ref_vec) calculation
        bool is_array_or_qbit = checkDataTypes<DataTypeArray>(arguments[0].type.get())
            || checkDataTypes<DataTypeQBit>(arguments[0].type.get()) || checkDataTypes<DataTypeFixedString>(arguments[0].type.get());

        return (is_array_or_qbit ? array_function : tuple_function)->getReturnTypeImpl(arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_array_or_qbit = checkDataTypes<DataTypeArray>(arguments[0].type.get())
            || checkDataTypes<DataTypeQBit>(arguments[0].type.get()) || checkDataTypes<DataTypeFixedString>(arguments[0].type.get());
        return (is_array_or_qbit ? array_function : tuple_function)->executeImpl(arguments, result_type, input_rows_count);
    }

private:
    FunctionPtr tuple_function;
    FunctionPtr array_function;
};

extern FunctionPtr createFunctionArrayDotProduct(ContextPtr context_);

extern FunctionPtr createFunctionArrayL1Norm(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2Norm(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2SquaredNorm(ContextPtr context_);
extern FunctionPtr createFunctionArrayLpNorm(ContextPtr context_);
extern FunctionPtr createFunctionArrayLinfNorm(ContextPtr context_);

extern FunctionPtr createFunctionArrayL1Distance(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2Distance(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2DistanceTransposed(ContextPtr context_);
extern FunctionPtr createFunctionArrayLpDistance(ContextPtr context_);
extern FunctionPtr createFunctionArrayLinfDistance(ContextPtr context_);
extern FunctionPtr createFunctionArrayCosineDistance(ContextPtr context_);

struct DotProduct
{
    static constexpr auto name = "dotProduct";

    static constexpr auto CreateTupleFunction = FunctionDotProduct::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayDotProduct;
};

struct L1NormTraits
{
    static constexpr auto name = "L1Norm";

    static constexpr auto CreateTupleFunction = FunctionL1Norm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL1Norm;
};

struct L2NormTraits
{
    static constexpr auto name = "L2Norm";

    static constexpr auto CreateTupleFunction = FunctionL2Norm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2Norm;
};

struct L2SquaredNormTraits
{
    static constexpr auto name = "L2SquaredNorm";

    static constexpr auto CreateTupleFunction = FunctionL2SquaredNorm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2SquaredNorm;
};

struct LpNormTraits
{
    static constexpr auto name = "LpNorm";

    static constexpr auto CreateTupleFunction = FunctionLpNorm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLpNorm;
};

struct LinfNormTraits
{
    static constexpr auto name = "LinfNorm";

    static constexpr auto CreateTupleFunction = FunctionLinfNorm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLinfNorm;
};

struct L1DistanceTraits
{
    static constexpr auto name = "L1Distance";

    static constexpr auto CreateTupleFunction = FunctionL1Distance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL1Distance;
};

struct L2DistanceTraits
{
    static constexpr auto name = "L2Distance";

    static constexpr auto CreateTupleFunction = FunctionL2Distance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2Distance;
};

struct L2SquaredDistanceTraits
{
    static constexpr auto name = "L2SquaredDistance";

    static constexpr auto CreateTupleFunction = FunctionL2SquaredDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2SquaredDistance;
};

struct L2DistanceTransposedTraits
{
    static constexpr auto name = "L2DistanceTransposed";

    static constexpr auto CreateTupleFunction = FunctionL2DistanceTransposed::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2DistanceTransposed;
};

struct LpDistanceTraits
{
    static constexpr auto name = "LpDistance";

    static constexpr auto CreateTupleFunction = FunctionLpDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLpDistance;
};

struct LinfDistanceTraits
{
    static constexpr auto name = "LinfDistance";

    static constexpr auto CreateTupleFunction = FunctionLinfDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLinfDistance;
};

struct CosineDistanceTraits
{
    static constexpr auto name = "cosineDistance";

    static constexpr auto CreateTupleFunction = FunctionCosineDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayCosineDistance;
};

using TupleOrArrayFunctionDotProduct = TupleOrArrayFunction<DotProduct>;

using TupleOrArrayFunctionL1Norm = TupleOrArrayFunction<L1NormTraits>;
using TupleOrArrayFunctionL2Norm = TupleOrArrayFunction<L2NormTraits>;
using TupleOrArrayFunctionL2SquaredNorm = TupleOrArrayFunction<L2SquaredNormTraits>;
using TupleOrArrayFunctionLpNorm = TupleOrArrayFunction<LpNormTraits>;
using TupleOrArrayFunctionLinfNorm = TupleOrArrayFunction<LinfNormTraits>;

using TupleOrArrayFunctionL1Distance = TupleOrArrayFunction<L1DistanceTraits>;
using TupleOrArrayFunctionL2Distance = TupleOrArrayFunction<L2DistanceTraits>;
using TupleOrArrayFunctionL2SquaredDistance = TupleOrArrayFunction<L2SquaredDistanceTraits>;
using TupleOrArrayFunctionL2DistanceTransposed = TupleOrArrayFunction<L2DistanceTransposedTraits>;
using TupleOrArrayFunctionLpDistance = TupleOrArrayFunction<LpDistanceTraits>;
using TupleOrArrayFunctionLinfDistance = TupleOrArrayFunction<LinfDistanceTraits>;
using TupleOrArrayFunctionCosineDistance = TupleOrArrayFunction<CosineDistanceTraits>;

REGISTER_FUNCTION(VectorFunctions)
{
    /// tuplePlus documentation
    FunctionDocumentation::Description description_tuplePlus = R"(
Calculates the sum of corresponding elements of two tuples of the same size.
)";
    FunctionDocumentation::Syntax syntax_tuplePlus = "tuplePlus(t1, t2)";
    FunctionDocumentation::Arguments arguments_tuplePlus = {
        {"t1", "First tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"t2", "Second tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tuplePlus = {"Returns a tuple containing the sums of corresponding input tuple arguments.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tuplePlus = {
        {"Basic usage", "SELECT tuplePlus((1, 2), (2, 3))", "(3, 5)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tuplePlus = {21, 11};
    FunctionDocumentation::Category category_tuplePlus = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tuplePlus = {description_tuplePlus, syntax_tuplePlus, arguments_tuplePlus, returned_value_tuplePlus, examples_tuplePlus, introduced_in_tuplePlus, category_tuplePlus};
    factory.registerFunction<FunctionTuplePlus>(documentation_tuplePlus);
    factory.registerAlias("vectorSum", FunctionTuplePlus::name, FunctionFactory::Case::Insensitive);

    /// tupleMinus documentation
    FunctionDocumentation::Description description_tupleMinus = R"(
Calculates the difference between corresponding elements of two tuples of the same size.
)";
    FunctionDocumentation::Syntax syntax_tupleMinus = "tupleMinus(t1, t2)";
    FunctionDocumentation::Arguments arguments_tupleMinus = {
        {"t1", "First tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"t2", "Second tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleMinus = {"Returns a tuple containing the results  of the subtractions.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleMinus = {
        {"Basic usage", "SELECT tupleMinus((1, 2), (2, 3))", "(-1, -1)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleMinus = {21, 11};
    FunctionDocumentation::Category category_tupleMinus = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleMinus = {description_tupleMinus, syntax_tupleMinus, arguments_tupleMinus, returned_value_tupleMinus, examples_tupleMinus, introduced_in_tupleMinus, category_tupleMinus};
    factory.registerFunction<FunctionTupleMinus>(documentation_tupleMinus);
    factory.registerAlias("vectorDifference", FunctionTupleMinus::name, FunctionFactory::Case::Insensitive);

    /// tupleMultiply documentation
    FunctionDocumentation::Description description_tupleMultiply = R"(
Calculates the multiplication of corresponding elements of two tuples of the same size.
)";
    FunctionDocumentation::Syntax syntax_tupleMultiply = "tupleMultiply(t1, t2)";
    FunctionDocumentation::Arguments arguments_tupleMultiply = {
        {"t1", "First tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"t2", "Second tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleMultiply = {"Returns a tuple with the results of the multiplications.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleMultiply = {
        {"Basic usage", "SELECT tupleMultiply((1, 2), (2, 3))", "(2, 6)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleMultiply = {21, 11};
    FunctionDocumentation::Category category_tupleMultiply = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleMultiply = {description_tupleMultiply, syntax_tupleMultiply, arguments_tupleMultiply, returned_value_tupleMultiply, examples_tupleMultiply, introduced_in_tupleMultiply, category_tupleMultiply};
    factory.registerFunction<FunctionTupleMultiply>(documentation_tupleMultiply);

    /// tupleDivide documentation
    FunctionDocumentation::Description description_tupleDivide = R"(
Calculates the division of corresponding elements of two tuples of the same size.

:::note
Division by zero will return `inf`.
:::
)";
    FunctionDocumentation::Syntax syntax_tupleDivide = "tupleDivide(t1, t2)";
    FunctionDocumentation::Arguments arguments_tupleDivide = {
        {"t1", "First tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"t2", "Second tuple.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleDivide = {"Returns tuple with the result of division.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleDivide = {
        {"Basic usage", "SELECT tupleDivide((1, 2), (2, 3))", "(0.5, 0.6666666666666666)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleDivide = {21, 11};
    FunctionDocumentation::Category category_tupleDivide = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleDivide = {description_tupleDivide, syntax_tupleDivide, arguments_tupleDivide, returned_value_tupleDivide, examples_tupleDivide, introduced_in_tupleDivide, category_tupleDivide};
    factory.registerFunction<FunctionTupleDivide>(documentation_tupleDivide);

    /// tupleModulo documentation
    FunctionDocumentation::Description description_tupleModulo = R"(
Returns a tuple of the remainders (moduli) of division operations of two tuples.
)";
    FunctionDocumentation::Syntax syntax_tupleModulo = "tupleModulo(tuple_num, tuple_mod)";
    FunctionDocumentation::Arguments arguments_tupleModulo = {
        {"tuple_num", "Tuple of numerator values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"tuple_mod", "Tuple of modulus values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleModulo = {"Returns tuple of the remainders of division. An error is thrown for division by zero.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleModulo = {
        {"Basic usage", "SELECT tupleModulo((15, 10, 5), (5, 3, 2))", "(0, 1, 1)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleModulo = {23, 8};
    FunctionDocumentation::Category category_tupleModulo = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleModulo = {description_tupleModulo, syntax_tupleModulo, arguments_tupleModulo, returned_value_tupleModulo, examples_tupleModulo, introduced_in_tupleModulo, category_tupleModulo};
    factory.registerFunction<FunctionTupleModulo>(documentation_tupleModulo);

    /// tupleIntDiv documentation
    FunctionDocumentation::Description description_tupleIntDiv = R"(
Performs an integer division with a tuple of numerators and a tuple of denominators. Returns a tuple of quotients.
If either tuple contains non-integer elements then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor.
Division by 0 causes an error to be thrown.
)";
    FunctionDocumentation::Syntax syntax_tupleIntDiv = "tupleIntDiv(tuple_num, tuple_div)";
    FunctionDocumentation::Arguments arguments_tupleIntDiv = {
        {"tuple_num", "Tuple of numerator values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"tuple_div", "Tuple of divisor values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleIntDiv = {"Returns a tuple of the quotients.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleIntDiv = {
        {"Basic usage", "SELECT tupleIntDiv((15, 10, 5), (5, 5, 5))", "(3, 2, 1)"},
        {"With decimals", "SELECT tupleIntDiv((15, 10, 5), (5.5, 5.5, 5.5))", "(2, 1, 0)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleIntDiv = {23, 8};
    FunctionDocumentation::Category category_tupleIntDiv = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleIntDiv = {description_tupleIntDiv, syntax_tupleIntDiv, arguments_tupleIntDiv, returned_value_tupleIntDiv, examples_tupleIntDiv, introduced_in_tupleIntDiv, category_tupleIntDiv};
    factory.registerFunction<FunctionTupleIntDiv>(documentation_tupleIntDiv);

    /// tupleIntDivOrZero documentation
    FunctionDocumentation::Description description_tupleIntDivOrZero = R"(
Like [`tupleIntDiv`](#tupleIntDiv) performs integer division of a tuple of numerators and a tuple of denominators, and returns a tuple of the quotients.
In case of division by 0, returns the quotient as 0 instead of throwing an exception.
If either tuple contains non-integer elements then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor.
)";
    FunctionDocumentation::Syntax syntax_tupleIntDivOrZero = "tupleIntDivOrZero(tuple_num, tuple_div)";
    FunctionDocumentation::Arguments arguments_tupleIntDivOrZero = {
        {"tuple_num", "Tuple of numerator values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"tuple_div", "Tuple of divisor values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleIntDivOrZero = {"Returns tuple of the quotients. Returns 0 for quotients where the divisor is 0.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleIntDivOrZero = {
        {"With zero divisors", "SELECT tupleIntDivOrZero((5, 10, 15), (0, 0, 0))", "(0, 0, 0)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleIntDivOrZero = {23, 8};
    FunctionDocumentation::Category category_tupleIntDivOrZero = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleIntDivOrZero = {description_tupleIntDivOrZero, syntax_tupleIntDivOrZero, arguments_tupleIntDivOrZero, returned_value_tupleIntDivOrZero, examples_tupleIntDivOrZero, introduced_in_tupleIntDivOrZero, category_tupleIntDivOrZero};
    factory.registerFunction<FunctionTupleIntDivOrZero>(documentation_tupleIntDivOrZero);

    /// tupleNegate documentation
    FunctionDocumentation::Description description_tupleNegate = R"(
Calculates the negation of the tuple elements.
)";
    FunctionDocumentation::Syntax syntax_tupleNegate = "tupleNegate(t)";
    FunctionDocumentation::Arguments arguments_tupleNegate = {
        {"t", "Tuple to negate.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleNegate = {"Returns a tuple with the result of negation.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleNegate = {
        {"Basic usage", "SELECT tupleNegate((1, 2))", "(-1, -2)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleNegate = {21, 11};
    FunctionDocumentation::Category category_tupleNegate = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleNegate = {description_tupleNegate, syntax_tupleNegate, arguments_tupleNegate, returned_value_tupleNegate, examples_tupleNegate, introduced_in_tupleNegate, category_tupleNegate};
    factory.registerFunction<FunctionTupleNegate>(documentation_tupleNegate);

    /// addTupleOfIntervals documentation
    FunctionDocumentation::Description description_addTupleOfIntervals = R"(
Consecutively adds a tuple of intervals to a date or a date with time.
    )";
    FunctionDocumentation::Syntax syntax_addTupleOfIntervals = R"(
addTupleOfIntervals(datetime, intervals)
    )";
    FunctionDocumentation::Arguments arguments_addTupleOfIntervals = {
        {"datetime", "Date or date with time to add intervals to.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"intervals", "Tuple of intervals to add to `datetime`.", {"Tuple(Interval)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addTupleOfIntervals = {"Returns `date` with added `intervals`", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_addTupleOfIntervals = {
        {"Add tuple of intervals to date", R"(
WITH toDate('2018-01-01') AS date
SELECT addTupleOfIntervals(date, (INTERVAL 1 DAY, INTERVAL 1 MONTH, INTERVAL 1 YEAR))
        )",
        R"(
┌─addTupleOfIntervals(date, (toIntervalDay(1), toIntervalMonth(1), toIntervalYear(1)))─┐
│                                                                           2019-02-02 │
└──────────────────────────────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addTupleOfIntervals = {22, 11};
    FunctionDocumentation::Category category_addTupleOfIntervals = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addTupleOfIntervals = {description_addTupleOfIntervals, syntax_addTupleOfIntervals, arguments_addTupleOfIntervals, returned_value_addTupleOfIntervals, examples_addTupleOfIntervals, introduced_in_addTupleOfIntervals, category_addTupleOfIntervals};

    factory.registerFunction<FunctionAddTupleOfIntervals>(documentation_addTupleOfIntervals);

    /// subtractTupleOfIntervals documentation
    FunctionDocumentation::Description description_subtractTupleOfIntervals = R"(
Consecutively subtracts a tuple of intervals from a date or a date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractTupleOfIntervals = R"(
subtractTupleOfIntervals(datetime, intervals)
    )";
    FunctionDocumentation::Arguments arguments_subtractTupleOfIntervals = {
        {"datetime", "Date or date with time to subtract intervals from.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"intervals", "Tuple of intervals to subtract from `datetime`.", {"Tuple(Interval)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractTupleOfIntervals = {"Returns `date` with subtracted `intervals`", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_subtractTupleOfIntervals = {
    {
        "Subtract tuple of intervals from date",
        R"(
WITH toDate('2018-01-01') AS date SELECT subtractTupleOfIntervals(date, (INTERVAL 1 DAY, INTERVAL 1 YEAR))
        )",
        R"(
┌─subtractTupl⋯alYear(1)))─┐
│               2016-12-31 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractTupleOfIntervals = {22, 11};
    FunctionDocumentation::Category category_subtractTupleOfIntervals = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractTupleOfIntervals = {description_subtractTupleOfIntervals, syntax_subtractTupleOfIntervals, arguments_subtractTupleOfIntervals, returned_value_subtractTupleOfIntervals, examples_subtractTupleOfIntervals, introduced_in_subtractTupleOfIntervals, category_subtractTupleOfIntervals};

    factory.registerFunction<FunctionSubtractTupleOfIntervals>(documentation_subtractTupleOfIntervals);

    /// addInterval documentation
    FunctionDocumentation::Description description_addInterval = R"(
Adds an interval to another interval or tuple of intervals.

:::note
Intervals of the same type will be combined into a single interval. For instance if `toIntervalDay(1)` and `toIntervalDay(2)` are passed then the result will be `(3)` rather than `(1,1)`.
:::
    )";
    FunctionDocumentation::Syntax syntax_addInterval = R"(
addInterval(interval_1, interval_2)
    )";
    FunctionDocumentation::Arguments arguments_addInterval = {
        {"interval_1", "First interval or tuple of intervals.", {"Interval", "Tuple(Interval)"}},
        {"interval_2", "Second interval to be added.", {"Interval"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addInterval = {"Returns a tuple of intervals", {"Tuple(Interval)"}};
    FunctionDocumentation::Examples examples_addInterval = {
        {"Add intervals", R"(
SELECT addInterval(INTERVAL 1 DAY, INTERVAL 1 MONTH);
SELECT addInterval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH);
SELECT addInterval(INTERVAL 2 DAY, INTERVAL 1 DAY)
        )",
        R"(
┌─addInterval(toIntervalDay(1), toIntervalMonth(1))─┐
│ (1,1)                                             │
└───────────────────────────────────────────────────┘
┌─addInterval((toIntervalDay(1), toIntervalYear(1)), toIntervalMonth(1))─┐
│ (1,1,1)                                                                │
└────────────────────────────────────────────────────────────────────────┘
┌─addInterval(toIntervalDay(2), toIntervalDay(1))─┐
│ (3)                                             │
└─────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addInterval = {22, 11};
    FunctionDocumentation::Category category_addInterval = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addInterval = {description_addInterval, syntax_addInterval, arguments_addInterval, returned_value_addInterval, examples_addInterval, introduced_in_addInterval, category_addInterval};

    factory.registerFunction<FunctionTupleAddInterval>(documentation_addInterval);

    /// subtractInterval documentation
    FunctionDocumentation::Description description_subtractInterval = R"(
Adds a negated interval to another interval or tuple of intervals.

Note: Intervals of the same type will be combined into a single interval. For instance if `toIntervalDay(2)` and `toIntervalDay(1)` are
passed then the result will be `(1)` rather than `(2,1)`.
    )";
    FunctionDocumentation::Syntax syntax_subtractInterval = R"(
subtractInterval(interval_1, interval_2)
    )";
    FunctionDocumentation::Arguments arguments_subtractInterval = {
        {"interval_1", "First interval or interval of tuples.", {"Interval", "Tuple(Interval)"}},
        {"interval_2", "Second interval to be negated.", {"Interval"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractInterval = {"Returns a tuple of intervals", {"Tuple(T)"}};
    FunctionDocumentation::Examples examples_subtractInterval = {
        {"Subtract intervals", R"(
SELECT subtractInterval(INTERVAL 1 DAY, INTERVAL 1 MONTH);
SELECT subtractInterval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH);
SELECT subtractInterval(INTERVAL 2 DAY, INTERVAL 1 DAY);
        )",
        R"(
┌─subtractInterval(toIntervalDay(1), toIntervalMonth(1))─┐
│ (1,-1)                                                 │
└────────────────────────────────────────────────────────┘
┌─subtractInterval((toIntervalDay(1), toIntervalYear(1)), toIntervalMonth(1))─┐
│ (1,1,-1)                                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
┌─subtractInterval(toIntervalDay(2), toIntervalDay(1))─┐
│ (1)                                                  │
└──────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractInterval = {22, 11};
    FunctionDocumentation::Category category_subtractInterval = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractInterval = {description_subtractInterval, syntax_subtractInterval, arguments_subtractInterval, returned_value_subtractInterval, examples_subtractInterval, introduced_in_subtractInterval, category_subtractInterval};

    factory.registerFunction<FunctionTupleSubtractInterval>(documentation_subtractInterval);

    /// tupleMultiplyByNumber documentation
    FunctionDocumentation::Description description_tupleMultiplyByNumber = R"(
Returns a tuple with all elements multiplied by a number.
)";
    FunctionDocumentation::Syntax syntax_tupleMultiplyByNumber = "tupleMultiplyByNumber(tuple, number)";
    FunctionDocumentation::Arguments arguments_tupleMultiplyByNumber = {
        {"tuple", "Tuple to multiply.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"number", "Multiplier.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleMultiplyByNumber = {"Returns a tuple with multiplied elements.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleMultiplyByNumber = {
        {"Basic usage", "SELECT tupleMultiplyByNumber((1, 2), -2.1)", "(-2.1, -4.2)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleMultiplyByNumber = {21, 11};
    FunctionDocumentation::Category category_tupleMultiplyByNumber = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleMultiplyByNumber = {description_tupleMultiplyByNumber, syntax_tupleMultiplyByNumber, arguments_tupleMultiplyByNumber, returned_value_tupleMultiplyByNumber, examples_tupleMultiplyByNumber, introduced_in_tupleMultiplyByNumber, category_tupleMultiplyByNumber};
    factory.registerFunction<FunctionTupleMultiplyByNumber>(documentation_tupleMultiplyByNumber);

    /// tupleDivideByNumber documentation
    FunctionDocumentation::Description description_tupleDivideByNumber = R"(
Returns a tuple with all elements divided by a number.

:::note
Division by zero will return `inf`.
:::
)";
    FunctionDocumentation::Syntax syntax_tupleDivideByNumber = "tupleDivideByNumber(tuple, number)";
    FunctionDocumentation::Arguments arguments_tupleDivideByNumber = {
        {"tuple", "Tuple to divide.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"number", "Divider.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleDivideByNumber = {"Returns a tuple with divided elements.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleDivideByNumber = {
        {"Basic usage", "SELECT tupleDivideByNumber((1, 2), 0.5)", "(2, 4)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleDivideByNumber = {21, 11};
    FunctionDocumentation::Category category_tupleDivideByNumber = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleDivideByNumber = {description_tupleDivideByNumber, syntax_tupleDivideByNumber, arguments_tupleDivideByNumber, returned_value_tupleDivideByNumber, examples_tupleDivideByNumber, introduced_in_tupleDivideByNumber, category_tupleDivideByNumber};
    factory.registerFunction<FunctionTupleDivideByNumber>(documentation_tupleDivideByNumber);

    /// tupleModuloByNumber documentation
    FunctionDocumentation::Description description_tupleModuloByNumber = R"(
Returns a tuple of the moduli (remainders) of division operations of a tuple and a given divisor.
)";
    FunctionDocumentation::Syntax syntax_tupleModuloByNumber = "tupleModuloByNumber(tuple_num, div)";
    FunctionDocumentation::Arguments arguments_tupleModuloByNumber = {
        {"tuple_num", "Tuple of numerator elements.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"div", "The divisor value.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleModuloByNumber = {"Returns tuple of the remainders of division. An error is thrown for division by zero.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleModuloByNumber = {
        {"Basic usage", "SELECT tupleModuloByNumber((15, 10, 5), 2)", "(1, 0, 1)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleModuloByNumber = {23, 8};
    FunctionDocumentation::Category category_tupleModuloByNumber = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleModuloByNumber = {description_tupleModuloByNumber, syntax_tupleModuloByNumber, arguments_tupleModuloByNumber, returned_value_tupleModuloByNumber, examples_tupleModuloByNumber, introduced_in_tupleModuloByNumber, category_tupleModuloByNumber};
    factory.registerFunction<FunctionTupleModuloByNumber>(documentation_tupleModuloByNumber);

    /// tupleIntDivByNumber documentation
    FunctionDocumentation::Description description_tupleIntDivByNumber = R"(
Performs integer division of a tuple of numerators by a given denominator, and returns a tuple of the quotients.
If either of the input parameters contain non-integer elements then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor.
An error will be thrown for division by 0.
)";
    FunctionDocumentation::Syntax syntax_tupleIntDivByNumber = "tupleIntDivByNumber(tuple_num, div)";
    FunctionDocumentation::Arguments arguments_tupleIntDivByNumber = {
        {"tuple_num", "Tuple of numerator values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"div", "The divisor value.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleIntDivByNumber = {"Returns a tuple of the quotients.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleIntDivByNumber = {
        {"Basic usage", "SELECT tupleIntDivByNumber((15, 10, 5), 5)", "(3, 2, 1)"},
        {"With decimals", "SELECT tupleIntDivByNumber((15.2, 10.7, 5.5), 5.8)", "(2, 1, 0)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleIntDivByNumber = {23, 8};
    FunctionDocumentation::Category category_tupleIntDivByNumber = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleIntDivByNumber = {description_tupleIntDivByNumber, syntax_tupleIntDivByNumber, arguments_tupleIntDivByNumber, returned_value_tupleIntDivByNumber, examples_tupleIntDivByNumber, introduced_in_tupleIntDivByNumber, category_tupleIntDivByNumber};
    factory.registerFunction<FunctionTupleIntDivByNumber>(documentation_tupleIntDivByNumber);

    /// tupleIntDivOrZeroByNumber documentation
    FunctionDocumentation::Description description_tupleIntDivOrZeroByNumber = R"(
Like [`tupleIntDivByNumber`](#tupleIntDivByNumber) it does integer division of a tuple of numerators by a given denominator, and returns a tuple of the quotients.
It does not throw an error for zero divisors, but rather returns the quotient as zero.
If either the tuple or div contain non-integer elements then the result is calculated by rounding to the nearest integer for each non-integer numerator or divisor.
)";
    FunctionDocumentation::Syntax syntax_tupleIntDivOrZeroByNumber = "tupleIntDivOrZeroByNumber(tuple_num, div)";
    FunctionDocumentation::Arguments arguments_tupleIntDivOrZeroByNumber = {
        {"tuple_num", "Tuple of numerator values.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}},
        {"div", "The divisor value.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_tupleIntDivOrZeroByNumber = {"Returns a tuple of the quotients with `0` for quotients where the divisor is `0`.", {"Tuple((U)Int*)", "Tuple(Float*)", "Tuple(Decimal)"}};
    FunctionDocumentation::Examples examples_tupleIntDivOrZeroByNumber = {
        {"Basic usage", "SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 5)", "(3, 2, 1)"},
        {"With zero divisor", "SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 0)", "(0, 0, 0)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_tupleIntDivOrZeroByNumber = {23, 8};
    FunctionDocumentation::Category category_tupleIntDivOrZeroByNumber = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation_tupleIntDivOrZeroByNumber = {description_tupleIntDivOrZeroByNumber, syntax_tupleIntDivOrZeroByNumber, arguments_tupleIntDivOrZeroByNumber, returned_value_tupleIntDivOrZeroByNumber, examples_tupleIntDivOrZeroByNumber, introduced_in_tupleIntDivOrZeroByNumber, category_tupleIntDivOrZeroByNumber};
    factory.registerFunction<FunctionTupleIntDivOrZeroByNumber>(documentation_tupleIntDivOrZeroByNumber);

    factory.registerFunction<TupleOrArrayFunctionDotProduct>();
    factory.registerAlias("scalarProduct", TupleOrArrayFunctionDotProduct::name, FunctionFactory::Case::Insensitive);

    /// L1Norm documentation
    FunctionDocumentation::Description description_l1_norm = R"(
Calculates the sum of absolute elements of a vector.
    )";
    FunctionDocumentation::Syntax syntax_l1_norm = "L1Norm(vector)";
    FunctionDocumentation::Arguments arguments_l1_norm = {
        {"vector", "Vector or tuple of numeric values.", {"Array(T)", "Tuple(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l1_norm = {"Returns the L1-norm or [taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry) distance.", {"UInt*", "Float*", "Decimal"}};
    FunctionDocumentation::Examples examples_l1_norm = {
        {
            "Basic usage",
            R"(
SELECT L1Norm((1, 2))
            )",
            R"(
┌─L1Norm((1, 2))─┐
│              3 │
└────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l1_norm = {21, 11};
    FunctionDocumentation::Category category_l1_norm = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l1_norm = {description_l1_norm, syntax_l1_norm, arguments_l1_norm, returned_value_l1_norm, examples_l1_norm, introduced_in_l1_norm, category_l1_norm};

    factory.registerFunction<TupleOrArrayFunctionL1Norm>(documentation_l1_norm);

    /// L2Norm documentation
    FunctionDocumentation::Description description_l2_norm = R"(
Calculates the square root of the sum of the squares of the vector elements.
    )";
    FunctionDocumentation::Syntax syntax_l2_norm = "L2Norm(vector)";
    FunctionDocumentation::Arguments arguments_l2_norm = {
        {"vector", "Vector or tuple of numeric values.", {"Tuple(T)", "Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l2_norm = {"Returns the L2-norm or [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance).", {"UInt*", "Float*"}};
    FunctionDocumentation::Examples examples_l2_norm = {
        {
            "Basic usage",
            R"(
SELECT L2Norm((1, 2))
            )",
            R"(
┌───L2Norm((1, 2))─┐
│ 2.23606797749979 │
└──────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l2_norm = {21, 11};
    FunctionDocumentation::Category category_l2_norm = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l2_norm = {description_l2_norm, syntax_l2_norm, arguments_l2_norm, returned_value_l2_norm, examples_l2_norm, introduced_in_l2_norm, category_l2_norm};

    factory.registerFunction<TupleOrArrayFunctionL2Norm>(documentation_l2_norm);

    /// L2SquaredNorm documentation
    FunctionDocumentation::Description description_l2_squared_norm = R"(
Calculates the square root of the sum of the squares of the vector elements (the [`L2Norm`](#L2Norm)) squared.
    )";
    FunctionDocumentation::Syntax syntax_l2_squared_norm = "L2SquaredNorm(vector)";
    FunctionDocumentation::Arguments arguments_l2_squared_norm = {
        {"vector", "Vector or tuple of numeric values.", {"Array(T)", "Tuple(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l2_squared_norm = {"Returns the L2-norm squared.", {"UInt*", "Float*", "Decimal"}};
    FunctionDocumentation::Examples examples_l2_squared_norm = {
        {
            "Basic usage",
            R"(
SELECT L2SquaredNorm((1, 2))
            )",
            R"(
┌─L2SquaredNorm((1, 2))─┐
│                     5 │
└───────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l2_squared_norm = {22, 7};
    FunctionDocumentation::Category category_l2_squared_norm = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l2_squared_norm = {description_l2_squared_norm, syntax_l2_squared_norm, arguments_l2_squared_norm, returned_value_l2_squared_norm, examples_l2_squared_norm, introduced_in_l2_squared_norm, category_l2_squared_norm};

    factory.registerFunction<TupleOrArrayFunctionL2SquaredNorm>(documentation_l2_squared_norm);

    /// LinfNorm documentation
    FunctionDocumentation::Description description_linf_norm = R"(
Calculates the maximum of absolute elements of a vector.
    )";
    FunctionDocumentation::Syntax syntax_linf_norm = "LinfNorm(vector)";
    FunctionDocumentation::Arguments arguments_linf_norm = {
        {"vector", "Vector or tuple of numeric values.", {"Array(T)", "Tuple(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_linf_norm = {"Returns the Linf-norm or the maximum absolute value.", {"Float64"}};
    FunctionDocumentation::Examples examples_linf_norm = {
        {
            "Basic usage",
            R"(
SELECT LinfNorm((1, -2))
            )",
            R"(
┌─LinfNorm((1, -2))─┐
│                 2 │
└───────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_linf_norm = {21, 11};
    FunctionDocumentation::Category category_linf_norm = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_linf_norm = {description_linf_norm, syntax_linf_norm, arguments_linf_norm, returned_value_linf_norm, examples_linf_norm, introduced_in_linf_norm, category_linf_norm};

    factory.registerFunction<TupleOrArrayFunctionLinfNorm>(documentation_linf_norm);

    /// LpNorm documentation
    FunctionDocumentation::Description description_lp_norm = R"(
Calculates the p-norm of a vector, which is the p-th root of the sum of the p-th powers of the absolute elements of its elements.

Special cases:
- When p=1, it's equivalent to L1Norm (Manhattan distance).
- When p=2, it's equivalent to L2Norm (Euclidean distance).
- When p=∞, it's equivalent to LinfNorm (maximum norm).
    )";
    FunctionDocumentation::Syntax syntax_lp_norm = "LpNorm(vector, p)";
    FunctionDocumentation::Arguments arguments_lp_norm = {
        {"vector", "Vector or tuple of numeric values.", {"Tuple(T)", "Array(T)"}},
        {"p", "The power. Possible values are real numbers in the range `[1; inf)`.", {"UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_lp_norm = {"Returns the [Lp-norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm).",{"Float64"}};
    FunctionDocumentation::Examples examples_lp_norm = {
        {
            "Basic usage",
            R"(
SELECT LpNorm((1, -2), 2)
            )",
            R"(
┌─LpNorm((1, -2), 2)─┐
│   2.23606797749979 │
└────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_lp_norm = {21, 11};
    FunctionDocumentation::Category category_lp_norm = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_lp_norm = {description_lp_norm, syntax_lp_norm, arguments_lp_norm, returned_value_lp_norm, examples_lp_norm, introduced_in_lp_norm, category_lp_norm};

    factory.registerFunction<TupleOrArrayFunctionLpNorm>(documentation_lp_norm);

    // Register aliases for norm functions
    factory.registerAlias("normL1", TupleOrArrayFunctionL1Norm::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("normL2", TupleOrArrayFunctionL2Norm::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("normL2Squared", TupleOrArrayFunctionL2SquaredNorm::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("normLinf", TupleOrArrayFunctionLinfNorm::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("normLp", FunctionLpNorm::name, FunctionFactory::Case::Insensitive);

    /// L1Distance documentation
    FunctionDocumentation::Description description_l1_distance = R"(
Calculates the distance between two points (the elements of the vectors are the coordinates) in `L1` space (1-norm ([taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry) distance)).
    )";
    FunctionDocumentation::Syntax syntax_l1_distance = "L1Distance(vector1, vector2)";
    FunctionDocumentation::Arguments arguments_l1_distance = {
        {"vector1", "First vector.", {"Tuple(T)", "Array(T)"}},
        {"vector2", "Second vector.", {"Tuple(T)", "Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l1_distance = {"Returns the 1-norm distance.", {"UInt32", "Float64"}};
    FunctionDocumentation::Examples examples_l1_distance = {
        {
            "Basic usage",
            R"(
SELECT L1Distance((1, 2), (2, 3))
            )",
            R"(
┌─L1Distance((1, 2), (2, 3))─┐
│                          2 │
└────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l1_distance = {21, 11};
    FunctionDocumentation::Category category_l1_distance = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l1_distance = {description_l1_distance, syntax_l1_distance, arguments_l1_distance, returned_value_l1_distance, examples_l1_distance, introduced_in_l1_distance, category_l1_distance};

    factory.registerFunction<TupleOrArrayFunctionL1Distance>(documentation_l1_distance);

    /// L2Distance documentation
    FunctionDocumentation::Description description_l2_distance = R"(
Calculates the distance between two points (the elements of the vectors are the coordinates) in Euclidean space ([Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).
    )";
    FunctionDocumentation::Syntax syntax_l2_distance = "L2Distance(vector1, vector2)";
    FunctionDocumentation::Arguments arguments_l2_distance = {
        {"vector1", "First vector.", {"Tuple(T)", "Array(T)"}},
        {"vector2", "Second vector.", {"Tuple(T)", "Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l2_distance = {"Returns the 2-norm distance.", {"Float64"}};
    FunctionDocumentation::Examples examples_l2_distance = {
        {
            "Basic usage",
            R"(
SELECT L2Distance((1, 2), (2, 3))
            )",
            R"(
┌─L2Distance((1, 2), (2, 3))─┐
│         1.4142135623730951 │
└────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l2_distance = {21, 11};
    FunctionDocumentation::Category category_l2_distance = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l2_distance = {description_l2_distance, syntax_l2_distance, arguments_l2_distance, returned_value_l2_distance, examples_l2_distance, introduced_in_l2_distance, category_l2_distance};

    factory.registerFunction<TupleOrArrayFunctionL2Distance>(documentation_l2_distance);

    /// L2DistanceTransposed documentation
    FunctionDocumentation::Description description_l2_distance_transposed = R"(
Calculates the approximate distance between two points (the values of the vectors are the coordinates) in Euclidean space ([Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).    )";
    FunctionDocumentation::Syntax syntax_l2_distance_transposed = "L2DistanceTransposed(vector1, vector2, p)";
    FunctionDocumentation::Arguments arguments_l2_distance_transposed
        = {{"vectors", "Vectors.", {"QBit(T, UInt64)"}}, {"reference", "Reference vector.", {"Array(T)"}}, {"p", "Number of bits from each vector element to use in the distance calculation (1 to element bit-width). The quantization level controls the precision-speed trade-off. Using fewer bits results in faster I/O and calculations with reduced accuracy, while using more bits increases accuracy at the cost of performance.", {"UInt"}}};
    FunctionDocumentation::ReturnedValue returned_value_l2_distance_transposed = {"Returns the approximate 2-norm distance.", {"Float64"}};
    FunctionDocumentation::Examples examples_l2_distance_transposed
        = {{"Basic usage",
            R"(
CREATE TABLE qbit (id UInt32, vec QBit(Float64, 2)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [0, 1]);
SELECT L2DistanceTransposed(vec, array(1.0, 2.0), 16) FROM qbit;"
)",
            R"(
┌─L2DistanceTransposed([0, 1], [1.0, 2.0], 16)─┐
│                           1.4142135623730951 │
└──────────────────────────────────────────────┘
            )"}};
    FunctionDocumentation::IntroducedIn introduced_in_l2_distance_transposed = {25, 10};
    FunctionDocumentation::Category category_l2_distance_transposed = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l2_distance_transposed
        = {description_l2_distance_transposed,
           syntax_l2_distance_transposed,
           arguments_l2_distance_transposed,
           returned_value_l2_distance_transposed,
           examples_l2_distance_transposed,
           introduced_in_l2_distance_transposed,
           category_l2_distance_transposed};

    factory.registerFunction<TupleOrArrayFunctionL2DistanceTransposed>(documentation_l2_distance_transposed);

    /// L2SquaredDistance documentation
    FunctionDocumentation::Description description_l2_squared_distance = R"(
Calculates the sum of the squares of the difference between the corresponding elements of two vectors.
    )";
    FunctionDocumentation::Syntax syntax_l2_squared_distance = "L2SquaredDistance(vector1, vector2)";
    FunctionDocumentation::Arguments arguments_l2_squared_distance = {
        {"vector1", "First vector.", {"Tuple(T)", "Array(T)"}},
        {"vector2", "Second vector.", {"Tuple(T)", "Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l2_squared_distance = {"Returns the sum of the squares of the difference between the corresponding elements of two vectors.", {"Float64"}};
    FunctionDocumentation::Examples examples_l2_squared_distance = {
        {
            "Basic usage",
            R"(
SELECT L2SquaredDistance([1, 2, 3], [0, 0, 0])
            )",
            R"(
┌─L2SquaredDis⋯ [0, 0, 0])─┐
│                       14 │
└──────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l2_squared_distance = {22, 7};
    FunctionDocumentation::Category category_l2_squared_distance = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l2_squared_distance = {description_l2_squared_distance, syntax_l2_squared_distance, arguments_l2_squared_distance, returned_value_l2_squared_distance, examples_l2_squared_distance, introduced_in_l2_squared_distance, category_l2_squared_distance};

    factory.registerFunction<TupleOrArrayFunctionL2SquaredDistance>(documentation_l2_squared_distance);

    /// LinfDistance documentation
    FunctionDocumentation::Description description_linf_distance = R"(
Calculates the distance between two points (the elements of the vectors are the coordinates) in `L_{inf}` space ([maximum norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm))).
    )";
    FunctionDocumentation::Syntax syntax_linf_distance = "LinfDistance(vector1, vector2)";
    FunctionDocumentation::Arguments arguments_linf_distance = {
        {"vector1", "First vector.", {"Tuple(T)", "Array(T)"}},
        {"vector2", "Second vector.", {"Tuple(T)", "Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_linf_distance = {"Returns the Infinity-norm distance.", {"Float64"}};
    FunctionDocumentation::Examples examples_linf_distance = {
        {
            "Basic usage",
            R"(
SELECT LinfDistance((1, 2), (2, 3))
            )",
            R"(
┌─LinfDistance((1, 2), (2, 3))─┐
│                            1 │
└──────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_linf_distance = {21, 11};
    FunctionDocumentation::Category category_linf_distance = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_linf_distance = {description_linf_distance, syntax_linf_distance, arguments_linf_distance, returned_value_linf_distance, examples_linf_distance, introduced_in_linf_distance, category_linf_distance};

    factory.registerFunction<TupleOrArrayFunctionLinfDistance>(documentation_linf_distance);

    /// LpDistance documentation
    FunctionDocumentation::Description description_lp_distance = R"(
Calculates the distance between two points (the elements of the vectors are the coordinates) in `Lp` space ([p-norm distance](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)).
    )";
    FunctionDocumentation::Syntax syntax_lp_distance = "LpDistance(vector1, vector2, p)";
    FunctionDocumentation::Arguments arguments_lp_distance = {
        {"vector1", "First vector.", {"Tuple(T)", "Array(T)"}},
        {"vector2", "Second vector.", {"Tuple(T)", "Array(T)"}},
        {"p", "The power. Possible values: real number from `[1; inf)`.", {"UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_lp_distance = {"Returns the p-norm distance.", {"Float64"}};
    FunctionDocumentation::Examples examples_lp_distance = {
        {
            "Basic usage",
            R"(
SELECT LpDistance((1, 2), (2, 3), 3)
            )",
            R"(
┌─LpDistance((1, 2), (2, 3), 3)─┐
│            1.2599210498948732 │
└───────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_lp_distance = {21, 11};
    FunctionDocumentation::Category category_lp_distance = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_lp_distance = {description_lp_distance, syntax_lp_distance, arguments_lp_distance, returned_value_lp_distance, examples_lp_distance, introduced_in_lp_distance, category_lp_distance};

    factory.registerFunction<TupleOrArrayFunctionLpDistance>(documentation_lp_distance);

    // Register aliases for distance functions
    factory.registerAlias("distanceL1", FunctionL1Distance::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("distanceL2", FunctionL2Distance::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("distanceL2Squared", FunctionL2SquaredDistance::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("distanceL2Transposed", FunctionL2DistanceTransposed::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("distanceLinf", FunctionLinfDistance::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("distanceLp", FunctionLpDistance::name, FunctionFactory::Case::Insensitive);

    /// cosineDistance documentation
    FunctionDocumentation::Description description_cosine_distance = R"(
Calculates the cosine distance between two vectors (the elements of the tuples are the coordinates). The smaller the returned value is, the more similar are the vectors.
    )";
    FunctionDocumentation::Syntax syntax_cosine_distance = "cosineDistance(vector1, vector2)";
    FunctionDocumentation::Arguments arguments_cosine_distance = {
        {"vector1", "First tuple.", {"Tuple(T)", "Array(T)"}},
        {"vector2", "Second tuple.", {"Tuple(T)", "Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cosine_distance = {"Returns the cosine of the angle between two vectors subtracted from one.", {"Float64"}};
    FunctionDocumentation::Examples examples_cosine_distance = {
        {
            "Basic usage",
            R"(
SELECT cosineDistance((1, 2), (2, 3));
            )",
            R"(
┌─cosineDistance((1, 2), (2, 3))─┐
│           0.007722123286332261 │
└────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_cosine_distance = {1, 1};
    FunctionDocumentation::Category category_cosine_distance = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_cosine_distance = {description_cosine_distance, syntax_cosine_distance, arguments_cosine_distance, returned_value_cosine_distance, examples_cosine_distance, introduced_in_cosine_distance, category_cosine_distance};

    factory.registerFunction<TupleOrArrayFunctionCosineDistance>(documentation_cosine_distance);

    /// L1Normalize documentation
    FunctionDocumentation::Description description_l1_normalize = R"(
Calculates the unit vector of a given vector (the elements of the tuple are the coordinates) in `L1` space ([taxicab geometry](https://en.wikipedia.org/wiki/Taxicab_geometry)).
    )";
    FunctionDocumentation::Syntax syntax_l1_normalize = "L1Normalize(tuple)";
    FunctionDocumentation::Arguments arguments_l1_normalize = {
        {"tuple", "A tuple of numeric values.", {"Tuple(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l1_normalize = {"Returns the unit vector.", {"Tuple(Float64)"}};
    FunctionDocumentation::Examples examples_l1_normalize = {
        {
            "Basic usage",
            R"(
SELECT L1Normalize((1, 2))
            )",
            R"(
┌─L1Normalize((1, 2))─────────────────────┐
│ (0.3333333333333333,0.6666666666666666) │
└─────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l1_normalize = {21, 11};
    FunctionDocumentation::Category category_l1_normalize = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l1_normalize = {description_l1_normalize, syntax_l1_normalize, arguments_l1_normalize, returned_value_l1_normalize, examples_l1_normalize, introduced_in_l1_normalize, category_l1_normalize};

    factory.registerFunction<FunctionL1Normalize>(documentation_l1_normalize);
    factory.registerAlias("normalizeL1", FunctionL1Normalize::name, FunctionFactory::Case::Insensitive);

    /// L2Normalize documentation
    FunctionDocumentation::Description description_l2_normalize = R"(
Calculates the unit vector of a given vector (the elements of the tuple are the coordinates) in Euclidean space (using [Euclidean distance](https://en.wikipedia.org/wiki/Euclidean_distance)).
    )";
    FunctionDocumentation::Syntax syntax_l2_normalize = "L2Normalize(tuple)";
    FunctionDocumentation::Arguments arguments_l2_normalize = {
        {"tuple", "A tuple of numeric values.", {"Tuple(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_l2_normalize = {"Returns the unit vector.", {"Tuple(Float64)"}};
    FunctionDocumentation::Examples examples_l2_normalize = {
        {
            "Basic usage",
            R"(
SELECT L2Normalize((3, 4))
            )",
            R"(
┌─L2Normalize((3, 4))─┐
│ (0.6,0.8)           │
└─────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_l2_normalize = {21, 11};
    FunctionDocumentation::Category category_l2_normalize = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_l2_normalize = {description_l2_normalize, syntax_l2_normalize, arguments_l2_normalize, returned_value_l2_normalize, examples_l2_normalize, introduced_in_l2_normalize, category_l2_normalize};

    factory.registerFunction<FunctionL2Normalize>(documentation_l2_normalize);
    factory.registerAlias("normalizeL2", FunctionL2Normalize::name, FunctionFactory::Case::Insensitive);

    /// LinfNormalize documentation
    FunctionDocumentation::Description description_linf_normalize = R"(
Calculates the unit vector of a given vector (the elements of the tuple are the coordinates) in `L_{inf}` space (using [maximum norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm))).
    )";
    FunctionDocumentation::Syntax syntax_linf_normalize = "LinfNormalize(tuple)";
    FunctionDocumentation::Arguments arguments_linf_normalize = {
        {"tuple", "A tuple of numeric values.", {"Tuple(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_linf_normalize = {"Returns the unit vector.", {"Tuple(Float64)"}};
    FunctionDocumentation::Examples examples_linf_normalize = {
        {
            "Basic usage",
            R"(
SELECT LinfNormalize((3, 4))
            )",
            R"(
┌─LinfNormalize((3, 4))─┐
│ (0.75,1)              │
└───────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_linf_normalize = {21, 11};
    FunctionDocumentation::Category category_linf_normalize = FunctionDocumentation::Category::Distance;
    FunctionDocumentation documentation_linf_normalize = {description_linf_normalize, syntax_linf_normalize, arguments_linf_normalize, returned_value_linf_normalize, examples_linf_normalize, introduced_in_linf_normalize, category_linf_normalize};

    factory.registerFunction<FunctionLinfNormalize>(documentation_linf_normalize);
    factory.registerAlias("normalizeLinf", FunctionLinfNormalize::name, FunctionFactory::Case::Insensitive);

    /// LpNormalize documentation
    {
        FunctionDocumentation::Description description_lp_normalize = R"(
Calculates the unit vector of a given vector (the elements of the tuple are the coordinates) in `Lp` space (using [p-norm](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)).
        )";
        FunctionDocumentation::Syntax syntax_lp_normalize = "LpNormalize(tuple, p)";
        FunctionDocumentation::Arguments arguments_lp_normalize = {
            {"tuple", "A tuple of numeric values.", {"Tuple(T)"}},
            {"p", "The power. Possible values are any number in the range range from `[1; inf)`.", {"UInt*", "Float*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_lp_normalize = {"Returns the unit vector.", {"Tuple(Float64)"}};
        FunctionDocumentation::Examples examples_lp_normalize = {
            {
                "Usage example",
                R"(
SELECT LpNormalize((3, 4), 5)
                )",
                R"(
┌─LpNormalize((3, 4), 5)──────────────────┐
│ (0.7187302630182624,0.9583070173576831) │
└─────────────────────────────────────────┘
                )"}
        };
        FunctionDocumentation::IntroducedIn introduced_in_lp_normalize = {21, 11};
        FunctionDocumentation::Category category_lp_normalize = FunctionDocumentation::Category::Distance;
        FunctionDocumentation documentation_lp_normalize = {description_lp_normalize, syntax_lp_normalize, arguments_lp_normalize, returned_value_lp_normalize, examples_lp_normalize, introduced_in_lp_normalize, category_lp_normalize};

        factory.registerFunction<FunctionLpNormalize>(documentation_lp_normalize);
    }
    factory.registerAlias("normalizeLp", FunctionLpNormalize::name, FunctionFactory::Case::Insensitive);
}
}
