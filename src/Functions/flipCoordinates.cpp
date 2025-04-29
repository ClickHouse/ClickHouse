#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionFlipCoordinates : public IFunction
{
public:
    static constexpr auto name = "flipCoordinates";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFlipCoordinates>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one argument, got {}",
                getName(),
                arguments.size());

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & arg = arguments[0];

        ColumnPtr column = arg.column;
        bool is_const = false;
        size_t const_size = 0;

        if (const auto * const_column = checkAndGetColumn<ColumnConst>(column.get()))
        {
            column = const_column->getDataColumnPtr();
            is_const = true;
            const_size = const_column->size();
        }

        ColumnPtr result;

        if (checkAndGetDataType<DataTypeTuple>(arg.type.get()))
        {
            result = executeForPoint(column);
        }
        else if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.type.get()))
        {
            result = executeForArray(column, array_type);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Expected Point, Ring, Polygon, or MultiPolygon",
                arg.type->getName(),
                getName());
        }

        if (is_const)
            return ColumnConst::create(result, const_size);

        return result;
    }

private:
    ColumnPtr executeForPoint(const ColumnPtr & column) const
    {
        const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get());
        if (!column_tuple)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & tuple_columns = column_tuple->getColumns();

        if (tuple_columns.size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects a Point with exactly 2 coordinates, got {}",
                getName(),
                tuple_columns.size());

        Columns new_columns = {tuple_columns[1], tuple_columns[0]};
        return ColumnTuple::create(new_columns);
    }

    ColumnPtr executeForArray(const ColumnPtr & column, const DataTypeArray * array_type) const
    {
        const auto * column_array = checkAndGetColumn<ColumnArray>(column.get());
        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & nested_type = array_type->getNestedType();
        const auto & nested_column = column_array->getDataPtr();

        ColumnPtr result_nested;

        if (checkAndGetDataType<DataTypeTuple>(nested_type.get()))
        {
            result_nested = executeForPoint(nested_column);
        }
        else if (const auto * nested_array = checkAndGetDataType<DataTypeArray>(nested_type.get()))
        {
            result_nested = executeForArray(nested_column, nested_array);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal nested type {} of argument of function {}",
                nested_type->getName(),
                getName());
        }

        auto offsets_column = column_array->getOffsetsPtr();
        auto result = ColumnArray::create(result_nested, offsets_column);
        return result;
    }
};

REGISTER_FUNCTION(FlipCoordinates)
{
    FunctionDocumentation::Description description = "Flips the coordinates of a Point, Ring, Polygon, or MultiPolygon. For a Point, it swaps the x and y coordinates. For arrays, it recursively applies the same transformation.";
    FunctionDocumentation::Syntax syntax = "flipCoordinates(geometry);";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "The geometry to transform. Supported types: Point (Tuple(Float64, Float64)), Ring (Array(Point)), Polygon (Array(Ring)), MultiPolygon (Array(Polygon))."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The geometry with flipped coordinates. The type is the same as the input.";
    FunctionDocumentation::Examples examples = {
        {"basic_point",
         "SELECT flipCoordinates((1.0, 2.0));",
         "(2.0, 1.0)"},
        {"ring",
         "SELECT flipCoordinates([(1.0, 2.0), (3.0, 4.0)]);",
         "[(2.0, 1.0), (4.0, 3.0)]"},
        {"polygon",
         "SELECT flipCoordinates([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]]);",
         "[[(2.0, 1.0), (4.0, 3.0)], [(6.0, 5.0), (8.0, 7.0)]]"}
    };
    FunctionDocumentation::Category category = {"Other"};

    FunctionDocumentation function_documentation = {
        .description = description,
        .syntax = syntax,
        .arguments = arguments,
        .returned_value = returned_value,
        .examples = examples,
        .category = category
    };

    factory.registerFunction<FunctionFlipCoordinates>(function_documentation);
}

}
