#include "config.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>

#include <Functions/s2_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Internal function used by S2 projection index pruning.
/// Checks whether a single S2 cell intersects any cell in a covering (array of cell IDs).
///
/// __s2CoveringIntersects(cell_id: UInt64, cells: Array(UInt64)) → UInt8
///
/// Returns 1 if the cell identified by `cell_id` intersects at least one cell
/// in the `cells` array, 0 otherwise. Two S2 cells intersect if and only if
/// one contains the other or they are the same cell.
class FunctionS2CoveringIntersects : public IFunction
{
public:
    static constexpr auto name = "__s2CoveringIntersects";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2CoveringIntersects>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 1 of function {}. Must be UInt64",
                arguments[0]->getName(), getName());

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[1].get());
        if (!array_type || !WhichDataType(array_type->getNestedType()).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 2 of function {}. Must be Array(UInt64)",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_cell_id = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_cell_id)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument 1 of function {}. Must be UInt64",
                arguments[0].type->getName(), getName());

        const auto * col_array = checkAndGetColumn<ColumnArray>(non_const_arguments[1].column.get());
        if (!col_array)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument 2 of function {}. Must be Array(UInt64)",
                arguments[1].type->getName(), getName());

        const auto * col_array_data = checkAndGetColumn<ColumnUInt64>(&col_array->getData());
        if (!col_array_data)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal nested type of argument 2 of function {}. Must be Array(UInt64)",
                getName());

        const auto & cell_id_data = col_cell_id->getData();
        const auto & array_data = col_array_data->getData();
        const auto & offsets = col_array->getOffsets();

        auto dst = ColumnUInt8::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 id = cell_id_data[row];
            S2CellId cell(id);
            if (!cell.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cell (id {}) is not valid in function {}", id, getName());

            size_t arr_start = row == 0 ? 0 : offsets[row - 1];
            size_t arr_end = offsets[row];

            UInt8 found = 0;
            for (size_t i = arr_start; i < arr_end; ++i)
            {
                S2CellId covering_cell(array_data[i]);
                if (!covering_cell.is_valid())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Covering cell (id {}) is not valid in function {}", array_data[i], getName());

                if (cell.intersects(covering_cell))
                {
                    found = 1;
                    break;
                }
            }
            dst_data[row] = found;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(S2CoveringIntersects)
{
    FunctionDocumentation::Description description = R"(
Internal function used by S2 projection index pruning.
Checks whether a single S2 cell intersects any cell in a covering (array of cell IDs).
    )";
    FunctionDocumentation::Syntax syntax = "__s2CoveringIntersects(cell_id, covering_cells)";
    FunctionDocumentation::Arguments function_arguments = {
        {"cell_id", "S2 cell identifier.", {"UInt64"}},
        {"covering_cells", "Array of S2 cell identifiers representing a covering.", {"Array(UInt64)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1 if the cell intersects any cell in the covering, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{"Basic usage", "SELECT __s2CoveringIntersects(9926595209846587392, [9926594385212866560])", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, function_arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionS2CoveringIntersects>(documentation);
}


}

#endif
