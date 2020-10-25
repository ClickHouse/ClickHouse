#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/getLeastSupertype.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>


namespace DB
{

/// Implements the function coalesce which takes a set of arguments and
/// returns the value of the leftmost non-null argument. If no such value is
/// found, coalesce() returns NULL.
class FunctionCoalesce : public IFunction
{
public:
    static constexpr auto name = "coalesce";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionCoalesce>(context);
    }

    explicit FunctionCoalesce(const Context & context_) : context(context_) {}

    std::string getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        /// coalesce(arg0, arg1, ..., argN) is essentially
        /// multiIf(isNotNull(arg0), assumeNotNull(arg0), isNotNull(arg1), assumeNotNull(arg1), ..., argN)
        /// with constant NULL arguments removed.

        ColumnNumbers filtered_args;
        filtered_args.reserve(arguments.size());
        for (const auto & arg : arguments)
        {
            const auto & type = block.getByPosition(arg).type;

            if (type->onlyNull())
                continue;

            filtered_args.push_back(arg);

            if (!type->isNullable())
                break;
        }

        auto is_not_null = FunctionFactory::instance().get("isNotNull", context);
        auto assume_not_null = FunctionFactory::instance().get("assumeNotNull", context);
        auto multi_if = FunctionFactory::instance().get("multiIf", context);

        ColumnNumbers multi_if_args;

        Block temp_block = block;

        for (size_t i = 0; i < filtered_args.size(); ++i)
        {
            size_t res_pos = temp_block.columns();
            bool is_last = i + 1 == filtered_args.size();

            if (is_last)
            {
                multi_if_args.push_back(filtered_args[i]);
            }
            else
            {
                temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
                is_not_null->build({temp_block.getByPosition(filtered_args[i])})->execute(temp_block, {filtered_args[i]}, res_pos, input_rows_count);
                temp_block.insert({nullptr, removeNullable(block.getByPosition(filtered_args[i]).type), ""});
                assume_not_null->build({temp_block.getByPosition(filtered_args[i])})->execute(temp_block, {filtered_args[i]}, res_pos + 1, input_rows_count);

                multi_if_args.push_back(res_pos);
                multi_if_args.push_back(res_pos + 1);
            }
        }

        /// If all arguments appeared to be NULL.
        if (multi_if_args.empty())
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        if (multi_if_args.size() == 1)
        {
            block.getByPosition(result).column = block.getByPosition(multi_if_args.front()).column;
            return;
        }

        ColumnsWithTypeAndName multi_if_args_elems;
        multi_if_args_elems.reserve(multi_if_args.size());
        for (auto column_num : multi_if_args)
            multi_if_args_elems.emplace_back(temp_block.getByPosition(column_num));

        multi_if->build(multi_if_args_elems)->execute(temp_block, multi_if_args, result, input_rows_count);

        ColumnPtr res = std::move(temp_block.getByPosition(result).column);

        /// if last argument is not nullable, result should be also not nullable
        if (!block.getByPosition(multi_if_args.back()).column->isNullable() && res->isNullable())
        {
            if (const auto * column_lc = checkAndGetColumn<ColumnLowCardinality>(*res))
                res = checkAndGetColumn<ColumnNullable>(*column_lc->convertToFullColumn())->getNestedColumnPtr();
            else if (const auto * column_const = checkAndGetColumn<ColumnConst>(*res))
                res = checkAndGetColumn<ColumnNullable>(column_const->getDataColumn())->getNestedColumnPtr();
            else
                res = checkAndGetColumn<ColumnNullable>(*res)->getNestedColumnPtr();
        }

        block.getByPosition(result).column = std::move(res);
    }

private:
    const Context & context;
};


void registerFunctionCoalesce(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCoalesce>(FunctionFactory::CaseInsensitive);
}

}
