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
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionS2RectAdd : public IFunction
{
public:
    static constexpr auto name = "s2RectAdd";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2RectAdd>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t index = 0; index < getNumberOfArguments(); ++index)
        {
            const auto * arg = arguments[index].get();
            if (!WhichDataType(arg).isUInt64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), index, getName());
        }

        DataTypePtr element = std::make_shared<DataTypeUInt64>();

        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_lo = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_lo)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_low = col_lo->getData();

        const auto * col_hi = checkAndGetColumn<ColumnUInt64>(non_const_arguments[1].column.get());
        if (!col_hi)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_hi = col_hi->getData();

        const auto * col_point = checkAndGetColumn<ColumnUInt64>(non_const_arguments[2].column.get());
        if (!col_point)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[2].type->getName(),
                3,
                getName());
        const auto & data_point = col_point->getData();

        auto col_res_first = ColumnUInt64::create();
        auto col_res_second = ColumnUInt64::create();

        auto & vec_res_first = col_res_first->getData();
        vec_res_first.reserve(input_rows_count);

        auto & vec_res_second = col_res_second->getData();
        vec_res_second.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto lo = S2CellId(data_low[row]);
            const auto hi = S2CellId(data_hi[row]);
            const auto point = S2CellId(data_point[row]);

            if (!lo.is_valid() || !hi.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Rectangle is not valid");

            if (!point.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Point is not valid");

            S2LatLngRect rect(lo.ToLatLng(), hi.ToLatLng());

            rect.AddPoint(point.ToPoint());

            vec_res_first.emplace_back(S2CellId(rect.lo()).id());
            vec_res_second.emplace_back(S2CellId(rect.hi()).id());
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

#endif
