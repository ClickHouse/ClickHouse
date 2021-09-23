#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <common/range.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
        const auto * column = checkAndGetColumn<ColumnUInt64>(arguments[0].column.get());
        const auto & data = column->getData();

        auto dst = ColumnArray::create(ColumnUInt8::create());
        auto & dst_data = reinterpret_cast<ColumnUInt8 &>(dst->getData());
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);

        auto current_offset = 0;
        std::vector<int> faces;

        for (size_t row = 0; row < input_rows_count; row++)
        {
            int max_faces = maxFaceCount(data[row]);

            faces.resize(max_faces);
            // function name h3GetFaces (v3.x) changed to getIcosahedronFaces (v4.0.0).
            getIcosahedronFaces(data[row], faces.data());

            for (int i = 0; i < max_faces; i++)
            {
                // valid icosahedron faces are represented by integers 0-19
                if (faces[i] >= 0 && faces[i] <= 19)
                {
                    ++current_offset;
                    dst_data.insert(faces[i]);
                }
            }
            dst_offsets[row] = current_offset;
            faces.clear();
        }
        return dst;
    }
};

}

void registerFunctionH3GetFaces(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3GetFaces>();
}

}

#endif
