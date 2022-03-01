#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3GetFaces : public IFunction
{
public:
    static constexpr auto name = "h3GetFaces";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetFaces>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto result_column_data = ColumnUInt8::create();
        auto & result_data = result_column_data->getData();

        auto result_column_offsets = ColumnArray::ColumnOffsets::create();
        auto & result_offsets = result_column_offsets->getData();
        result_offsets.resize(input_rows_count);

        auto current_offset = 0;
        std::vector<int> faces;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            int max_faces = maxFaceCount(data[row]);

            faces.resize(max_faces);

            // function name h3GetFaces (v3.x) changed to getIcosahedronFaces (v4.0.0).
            getIcosahedronFaces(data[row], faces.data());

            for (int i = 0; i < max_faces; ++i)
            {
                // valid icosahedron faces are represented by integers 0-19
                if (faces[i] >= 0 && faces[i] <= 19)
                {
                    ++current_offset;
                    result_data.emplace_back(faces[i]);
                }
            }

            result_offsets[row] = current_offset;
            faces.clear();
        }

        return ColumnArray::create(std::move(result_column_data), std::move(result_column_offsets));
    }
};

}

void registerFunctionH3GetFaces(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3GetFaces>();
}

}

#endif
