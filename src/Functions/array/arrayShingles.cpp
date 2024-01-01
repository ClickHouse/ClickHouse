#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

class FunctionArrayShingles : public IFunction
{
public:
    static constexpr auto name = "shingles";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShingles>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be array. Found {} instead.",
                            toString(1), getName(), arguments[0].type->getName());

        if (!isInteger(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be positive integer. Found {} instead.",
                            toString(2), getName(), arguments[1].type->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(array_type->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected array column.");
        const ColumnPtr & length_column = arguments[1].column;

        const IColumn::Offsets & src_offsets = column_array->getOffsets();
        const auto & src_values = column_array->getData();
        auto res_values_column = src_values.cloneEmpty();

        auto column_offsets_2 = ColumnArray::ColumnOffsets::create();
        auto column_offsets_1 = ColumnArray::ColumnOffsets::create();
        IColumn::Offsets & out_offsets_2 = column_offsets_2->getData();
        IColumn::Offsets & out_offsets_1 = column_offsets_1->getData();

        size_t pos1 = 0, pos2 = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto element_length = length_column->getInt(i);
            auto row_size = src_offsets[i] - src_offsets[i - 1];
            if (element_length > 0 && static_cast<unsigned>(element_length) <= row_size)
            {
                for (size_t j = 0; j < row_size - element_length + 1; ++j)
                {
                    res_values_column->insertRangeFrom(src_values, src_offsets[i - 1] + j, element_length);
                    pos1 += element_length;
                    out_offsets_2.push_back(pos1);
                }
                pos2 += row_size - element_length + 1;
            }

            out_offsets_1.push_back(pos2);
        }

        return ColumnArray::create(
            ColumnArray::create(
                std::move(res_values_column),
                std::move(column_offsets_2)
                    ),
            std::move(column_offsets_1)
        );
    }
};

REGISTER_FUNCTION(ArrayShingles)
{
    factory.registerFunction<FunctionArrayShingles>(
        FunctionDocumentation{
            .description = R"(
Returns an array of sub-arrays from the original array.
Each sub-array contains a sequence of elements from the original array. The length of these sequences is specified by the second argument of the function.
[example:simple_int]
[example:simple_string]
)",
            .examples{
                {"simple_int", "SELECT shingles([1, 2, 3, 4, 5], 3)", "[[1,2,3],[2,3,4],[3,4,5]]"},
                {"simple_string", "SELECT shingles(['a', 'b', 'c', 'd', 'e'], 4)", "[['a','b','c','d'],['b','c','d','e']]"}
            },
            .categories = {"Array"},
        });
}

}

