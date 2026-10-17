#include "config.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <Functions/s2_fwd.h>

class S2CellId;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{


class FunctionS2RectIntersection : public IFunction
{
public:
    static constexpr auto name = "s2RectIntersection";

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
                    arg->getName(), i, getName());
        }

        DataTypePtr element = std::make_shared<DataTypeUInt64>();

        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_lo1 = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_lo1)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_lo1 = col_lo1->getData();

        const auto * col_hi1 = checkAndGetColumn<ColumnUInt64>(non_const_arguments[1].column.get());
        if (!col_hi1)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_hi1 = col_hi1->getData();

        const auto * col_lo2 = checkAndGetColumn<ColumnUInt64>(non_const_arguments[2].column.get());
        if (!col_lo2)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[2].type->getName(),
                3,
                getName());
        const auto & data_lo2 = col_lo2->getData();

        const auto * col_hi2 = checkAndGetColumn<ColumnUInt64>(non_const_arguments[3].column.get());
        if (!col_hi2)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[3].type->getName(),
                4,
                getName());
        const auto & data_hi2 = col_hi2->getData();

        auto col_res_first = ColumnUInt64::create();
        auto col_res_second = ColumnUInt64::create();

        auto & vec_res_first = col_res_first->getData();
        vec_res_first.reserve(input_rows_count);

        auto & vec_res_second = col_res_second->getData();
        vec_res_second.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto lo1 = S2CellId(data_lo1[row]);
            const auto hi1 = S2CellId(data_hi1[row]);
            const auto lo2 = S2CellId(data_lo2[row]);
            const auto hi2 = S2CellId(data_hi2[row]);

            S2LatLngRect rect1(lo1.ToLatLng(), hi1.ToLatLng());
            S2LatLngRect rect2(lo2.ToLatLng(), hi2.ToLatLng());

            if (!rect1.is_valid() || !rect2.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Rectangle is invalid. For valid rectangles the latitude bounds do not exceed "
                    "Pi/2 in absolute value and the longitude bounds do not exceed Pi in absolute value. "
                    "Also, if either the latitude or longitude bound is empty then both must be.");

            S2LatLngRect rect_intersection = rect1.Intersection(rect2);

            vec_res_first.emplace_back(S2CellId(rect_intersection.lo()).id());
            vec_res_second.emplace_back(S2CellId(rect_intersection.hi()).id());
        }

        return ColumnTuple::create(Columns{std::move(col_res_first), std::move(col_res_second)});
    }

};

}

REGISTER_FUNCTION(S2RectIntersection)
{
    FunctionDocumentation::Description description = R"(
Returns the intersection of two S2 latitude-longitude rectangles. Each rectangle is represented by a pair of S2 cell identifiers for its low and high corners.
    )";
    FunctionDocumentation::Syntax syntax = "s2RectIntersection(s2Rect1Low, s2Rect1High, s2Rect2Low, s2Rect2High)";
    FunctionDocumentation::Arguments arguments = {
        {"s2Rect1Low", "S2 cell identifier of the low vertex of the first rectangle.", {"UInt64"}},
        {"s2Rect1High", "S2 cell identifier of the high vertex of the first rectangle.", {"UInt64"}},
        {"s2Rect2Low", "S2 cell identifier of the low vertex of the second rectangle.", {"UInt64"}},
        {"s2Rect2High", "S2 cell identifier of the high vertex of the second rectangle.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (s2RectLow, s2RectHigh) representing the intersection rectangle.", {"Tuple(UInt64, UInt64)"}};
    FunctionDocumentation::Examples examples = {{"Basic usage", "SELECT s2RectIntersection(5765131099823669248, 5765131099956887552, 5765131099880128512, 5765131100088901632)", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionS2RectIntersection>(documentation);
}


}

#endif
