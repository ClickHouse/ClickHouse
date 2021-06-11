#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

class FunctionH3EdgeAngle : public IFunction
{
public:
    static constexpr auto name = "h3EdgeAngle";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionH3EdgeAngle>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const auto * col_hindex = block.getByPosition(arguments[0]).column.get();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (const auto row : ext::range(0, input_rows_count))
        {
            const int resolution = col_hindex->getUInt(row);
            if (resolution > MAX_H3_RES)
                throw Exception("The argument 'resolution' (" + toString(resolution) + ") of function " + getName()
                    + " is out of bounds because the maximum resolution in H3 library is " + toString(MAX_H3_RES), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            // Numerical constant is 180 degrees / pi / Earth radius, Earth radius is from h3 sources
            Float64 res = 8.99320592271288084e-6 * edgeLengthM(resolution);

            dst_data[row] = res;
        }

        block.getByPosition(result).column = std::move(dst);
    }
};


void registerFunctionH3EdgeAngle(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3EdgeAngle>();
}

}
