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
#include <common/range.h>

#include "s2_fwd.h"

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
class FunctionS2RectIntersection : public IFunction
{
public:
    static constexpr auto name = "S2RectIntersection";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2RectIntersection>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 4; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const auto * arg = arguments[i].get();
            if (!WhichDataType(arg).isUInt64()) {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), i, getName()
                    );
            }
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
        vec_res_first.resize(input_rows_count);

        auto & vec_res_second = col_res_second->getData();
        vec_res_second.resize(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const UInt64 lo1 = col_lo1->getUInt(row);
            const UInt64 hi1 = col_hi1->getUInt(row);
            const UInt64 lo2 = col_lo2->getUInt(row);
            const UInt64 hi2 = col_hi2->getUInt(row);

            S2CellId id_lo1(lo1);
            S2CellId id_hi1(hi1);
            S2CellId id_lo2(lo2);
            S2CellId id_hi2(hi2);

            S2LatLngRect rect1(id_lo1.ToLatLng(), id_hi1.ToLatLng());
            S2LatLngRect rect2(id_lo2.ToLatLng(), id_hi2.ToLatLng());

            S2LatLngRect rect_intersection = rect1.Intersection(rect2);

            vec_res_first[row] = S2CellId(rect_intersection.lo()).id();
            vec_res_second[row] = S2CellId(rect_intersection.hi()).id();
        }

        return ColumnTuple::create(Columns{std::move(col_res_first), std::move(col_res_second)});
    }

};

}

void registerFunctionS2RectIntersection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2RectIntersection>();
}


}

#endif
