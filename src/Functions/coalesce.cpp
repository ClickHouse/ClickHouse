#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace
{

/// Implements the function coalesce which takes a set of arguments and
/// returns the value of the leftmost non-null argument. If no such value is
/// found, coalesce() returns NULL.
class FunctionCoalesce : public IFunction
{
public:
    static constexpr auto name = "coalesce";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCoalesce>(context);
    }

    explicit FunctionCoalesce(ContextPtr context_)
        : context(context_)
        , is_not_null(FunctionFactory::instance().get("isNotNull", context))
        , assume_not_null(FunctionFactory::instance().get("assumeNotNull", context))
        , if_function(FunctionFactory::instance().get("if", context))
        , multi_if_function(FunctionFactory::instance().get("multiIf", context))
    {
    }

    std::string getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        ColumnNumbers args;
        for (size_t i = 0; i + 1 < number_of_arguments; ++i)
            args.push_back(i);
        return args;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// Skip all NULL arguments. If any argument is non-Nullable, skip all next arguments.
        DataTypes filtered_args;
        filtered_args.reserve(arguments.size());
        for (const auto & arg : arguments)
        {
            if (arg->onlyNull())
                continue;

            filtered_args.push_back(arg);

            if (!arg->isNullable())
                break;
        }

        DataTypes new_args;
        for (size_t i = 0; i < filtered_args.size(); ++i)
        {
            bool is_last = i + 1 == filtered_args.size();

            if (is_last)
                new_args.push_back(filtered_args[i]);
            else
                new_args.push_back(removeNullable(filtered_args[i]));
        }

        if (new_args.empty())
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
        if (new_args.size() == 1)
            return new_args.front();

        auto res = getLeastSupertype(new_args);

        /// if last argument is not nullable, result should be also not nullable
        if (!new_args.back()->isNullable() && res->isNullable())
            res = removeNullable(res);

        return res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// coalesce(arg0, arg1, ..., argN) is essentially
        /// multiIf(isNotNull(arg0), assumeNotNull(arg0), isNotNull(arg1), assumeNotNull(arg1), ..., argN)
        /// with constant NULL arguments removed.

        ColumnsWithTypeAndName filtered_args;
        filtered_args.reserve(arguments.size());
        for (const auto & arg : arguments)
        {
            const auto & type = arg.type;

            if (type->onlyNull())
                continue;

            filtered_args.push_back(arg);

            if (!type->isNullable())
                break;
        }


        ColumnsWithTypeAndName multi_if_args;
        ColumnsWithTypeAndName tmp_args(1);

        for (size_t i = 0; i < filtered_args.size(); ++i)
        {
            bool is_last = i + 1 == filtered_args.size();

            if (is_last)
            {
                multi_if_args.push_back(filtered_args[i]);
            }
            else
            {
                tmp_args[0] = filtered_args[i];
                auto & cond = multi_if_args.emplace_back(ColumnWithTypeAndName{nullptr, std::make_shared<DataTypeUInt8>(), ""});
                cond.column = is_not_null->build(tmp_args)->execute(tmp_args, cond.type, input_rows_count);

                tmp_args[0] = filtered_args[i];
                auto & val = multi_if_args.emplace_back(ColumnWithTypeAndName{nullptr, removeNullable(filtered_args[i].type), ""});
                val.column = assume_not_null->build(tmp_args)->execute(tmp_args, val.type, input_rows_count);
            }
        }

        /// If all arguments appeared to be NULL.
        if (multi_if_args.empty())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        if (multi_if_args.size() == 1)
            return multi_if_args.front().column;

        /// If there was only two arguments (3 arguments passed to multiIf)
        /// use function "if" instead, because it's implemented more efficient.
        /// TODO: make "multiIf" the same efficient.
        FunctionOverloadResolverPtr if_or_multi_if = multi_if_args.size() == 3 ? if_function : multi_if_function;
        ColumnPtr res = if_or_multi_if->build(multi_if_args)->execute(multi_if_args, result_type, input_rows_count);

        /// if last argument is not nullable, result should be also not nullable
        if (!multi_if_args.back().column->isNullable() && res->isNullable())
        {
            if (const auto * column_lc = checkAndGetColumn<ColumnLowCardinality>(&*res))
                res = checkAndGetColumn<ColumnNullable>(*column_lc->convertToFullColumn()).getNestedColumnPtr();
            else if (const auto * column_const = checkAndGetColumn<ColumnConst>(&*res))
                res = checkAndGetColumn<ColumnNullable>(column_const->getDataColumn()).getNestedColumnPtr();
            else
                res = checkAndGetColumn<ColumnNullable>(&*res)->getNestedColumnPtr();
        }

        return res;
    }

private:
    ContextPtr context;
    FunctionOverloadResolverPtr is_not_null;
    FunctionOverloadResolverPtr assume_not_null;
    FunctionOverloadResolverPtr if_function;
    FunctionOverloadResolverPtr multi_if_function;
};

}

REGISTER_FUNCTION(Coalesce)
{
    factory.registerFunction<FunctionCoalesce>({}, FunctionFactory::Case::Insensitive);
}

}
