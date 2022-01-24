#include "config_functions.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include "s2_fwd.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{


class FunctionS2RectUnion : public IFunction
{
public:
    static constexpr auto name = "s2RectUnion";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2RectUnion>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 4; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const auto * arg = arguments[i].get();
            if (!WhichDataType(arg).isUInt64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), i + 1, getName());
        }

        DataTypePtr element = std::make_shared<DataTypeUInt64>();

        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_lo1 = arguments[0].column.get();
        const auto * col_hi1 = arguments[1].column.get();
        const auto * col_lo2 = arguments[2].column.get();
        const auto * col_hi2 = arguments[3].column.get();

        auto col_res_first = ColumnUInt64::create();
        auto col_res_second = ColumnUInt64::create();

        auto & vec_res_first = col_res_first->getData();
        vec_res_first.reserve(input_rows_count);

        auto & vec_res_second = col_res_second->getData();
        vec_res_second.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto lo1 = S2CellId(col_lo1->getUInt(row));
            const auto hi1 = S2CellId(col_hi1->getUInt(row));
            const auto lo2 = S2CellId(col_lo2->getUInt(row));
            const auto hi2 = S2CellId(col_hi2->getUInt(row));

            if (!lo1.is_valid() || !hi1.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "First rectangle is not valid");

            if (!lo2.is_valid() || !hi2.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second rectangle is not valid");

            S2LatLngRect rect1(lo1.ToLatLng(), hi1.ToLatLng());
            S2LatLngRect rect2(lo2.ToLatLng(), hi2.ToLatLng());

            S2LatLngRect rect_union = rect1.Union(rect2);

            vec_res_first.emplace_back(S2CellId(rect_union.lo()).id());
            vec_res_second.emplace_back(S2CellId(rect_union.hi()).id());
        }

        return ColumnTuple::create(Columns{std::move(col_res_first), std::move(col_res_second)});
    }

};

}

void registerFunctionS2RectUnion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2RectUnion>();
}


}

#endif
