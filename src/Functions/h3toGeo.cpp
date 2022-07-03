#include "config_functions.h"

#if USE_H3

#include <array>
#include <math.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

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

/// Implements the function h3ToGeo which takes a single argument (h3Index)
/// and returns the longitude and latitude that correspond to the provided h3 index
class FunctionH3ToGeo : public IFunction
{
public:
    static constexpr auto name = "h3ToGeo";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToGeo>(); }

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

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
            Strings{"longitude", "latitude"});
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

        auto latitude = ColumnFloat64::create(input_rows_count);
        auto longitude = ColumnFloat64::create(input_rows_count);

        ColumnFloat64::Container & lon_data = longitude->getData();
        ColumnFloat64::Container & lat_data = latitude->getData();


        for (size_t row = 0; row < input_rows_count; ++row)
        {
            H3Index h3index = data[row];
            LatLng coord{};

            cellToLatLng(h3index,&coord);
            lon_data[row] = radsToDegs(coord.lng);
            lat_data[row] = radsToDegs(coord.lat);
        }

        MutableColumns columns;
        columns.emplace_back(std::move(longitude));
        columns.emplace_back(std::move(latitude));
        return ColumnTuple::create(std::move(columns));
    }
};

}

void registerFunctionH3ToGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3ToGeo>();
}

}

#endif
