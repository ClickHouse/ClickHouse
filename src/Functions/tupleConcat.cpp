#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <base/range.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

/// tupleConcat(tup1, ...) - concatenate tuples.
class FunctionTupleConcat : public IFunction
{
public:
    static constexpr auto name = "tupleConcat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTupleConcat>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one argument.",
                getName());

        DataTypes tuple_arg_types;

        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!isTuple(arg))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}",
                    arg->getName(),
                    arg_idx + 1,
                    getName());

            const auto * type = checkAndGetDataType<DataTypeTuple>(arg);
            for (const auto & elem : type->getElements())
                tuple_arg_types.push_back(elem);
        }

        return std::make_shared<DataTypeTuple>(tuple_arg_types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const size_t num_arguments = arguments.size();
        Columns columns;

        for (size_t i = 0; i < num_arguments; i++)
        {
            const DataTypeTuple * arg_type = checkAndGetDataType<DataTypeTuple>(arguments[i].type.get());

            if (!arg_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}",
                    arguments[i].type->getName(),
                    i + 1,
                    getName());

            ColumnPtr arg_col = arguments[i].column->convertToFullColumnIfConst();
            const ColumnTuple * tuple_col = checkAndGetColumn<ColumnTuple>(arg_col.get());

            if (!tuple_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of argument of function {}",
                    arguments[i].column->getName(),
                    getName());

            for (const auto & inner_col : tuple_col->getColumns())
                columns.push_back(inner_col);
        }

        if (columns.empty())
            return ColumnTuple::create(input_rows_count);

        return ColumnTuple::create(columns);
    }
};

REGISTER_FUNCTION(TupleConcat)
{
    factory.registerFunction<FunctionTupleConcat>();
}

}
