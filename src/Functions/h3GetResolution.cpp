#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
class FunctionH3GetResolution : public IFunction
{
public:
    static constexpr auto name = "h3GetResolution";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionH3GetResolution>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const auto * col_hindex = block.getByPosition(arguments[0]).column.get();

        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (const auto row : ext::range(0, input_rows_count))
        {
            const UInt64 hindex = col_hindex->getUInt(row);

            UInt8 res = h3GetResolution(hindex);

            dst_data[row] = res;
        }

        block.getByPosition(result).column = std::move(dst);
    }
};


void registerFunctionH3GetResolution(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3GetResolution>();
}

}
