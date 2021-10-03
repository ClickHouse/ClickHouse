#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include "s2_fwd.h"

class S2CellId;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/**
 *  Returns a point (longitude, latitude) in degrees
 */
class FunctionS2ToGeo : public IFunction
{
public:
    static constexpr auto name = "s2ToGeo";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2ToGeo>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();

        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arg->getName(), 1, getName());

        DataTypePtr element = std::make_shared<DataTypeFloat64>();

        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_id = arguments[0].column.get();

        auto col_longitude = ColumnFloat64::create();
        auto col_latitude = ColumnFloat64::create();

        auto & longitude = col_longitude->getData();
        longitude.reserve(input_rows_count);

        auto & latitude = col_latitude->getData();
        latitude.reserve(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const auto id = S2CellId(col_id->getUInt(row));

            if (!id.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Point is not valid");

            S2Point point = id.ToPoint();
            S2LatLng ll(point);

            longitude.emplace_back(ll.lng().degrees());
            latitude.emplace_back(ll.lat().degrees());
        }

        return ColumnTuple::create(Columns{std::move(col_longitude), std::move(col_latitude)});
    }

};

}

void registerFunctionS2ToGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2ToGeo>();
}


}

#endif
