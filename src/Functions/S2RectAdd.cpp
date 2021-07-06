#include "config_functions.h"

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <common/range.h>

#include <s2/s2latlng.h>
#include <s2/s2cell_id.h>
#include <s2/s2point.h>
#include <s2/s2latlng_rect.h>

class S2CellId;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// TODO: Comment this
class FunctionS2RectAdd : public IFunction
{
public:
    static constexpr auto name = "S2RectAdd";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2RectAdd>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 4; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 3) {
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(number_of_arguments) + ", should be 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        const auto * arg = arguments[0].get();

        if (!WhichDataType(arg).isUInt64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        arg = arguments[1].get();

        if (!WhichDataType(arg).isUInt64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(2) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        arg = arguments[2].get();

        if (!WhichDataType(arg).isUInt64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(3) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        DataTypePtr element = std::make_shared<DataTypeUInt64>();

        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_lo = arguments[0].column.get();
        const auto * col_hi = arguments[1].column.get();
        const auto * col_point = arguments[2].column.get();

        auto col_res_first = ColumnUInt64::create();
        auto col_res_second = ColumnUInt64::create();

        auto & vec_res_first = col_res_first->getData();
        vec_res_first.resize(input_rows_count);

        auto & vec_res_second = col_res_second->getData();
        vec_res_second.resize(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const UInt64 lo = col_lo->getUInt(row);
            const UInt64 hi = col_hi->getUInt(row);
            const UInt64 point = col_point->getUInt(row);

            S2CellId id_lo(lo);
            S2CellId id_hi(hi);
            S2CellId id_point(point);

            S2LatLngRect rect(id_lo.ToLatLng(), id_hi.ToLatLng());

            rect.AddPoint(id_point.ToPoint());

            vec_res_first[row] = S2CellId(rect.lo()).id();
            vec_res_second[row] = S2CellId(rect.hi()).id();
        }

        return ColumnTuple::create(Columns{std::move(col_res_first), std::move(col_res_second)});
    }

};

}

void registerFunctionS2RectAdd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2RectAdd>();
}


}
