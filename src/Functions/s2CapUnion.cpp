#include "config_functions.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
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

/**
 * The cap represents a portion of the sphere that has been cut off by a plane.
 * See comment for s2CapContains function.
 * This function returns the smallest cap that contains both of input caps.
 * It is represented by identifier of the center and a radius.
 */
class FunctionS2CapUnion : public IFunction
{
public:
    static constexpr auto name = "s2CapUnion";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2CapUnion>();
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
        for (size_t index = 0; index < getNumberOfArguments(); ++index)
        {
            const auto * arg = arguments[index].get();
            if (index == 1 || index == 3)
            {
                if (!WhichDataType(arg).isFloat64())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}. Must be Float64",
                        arg->getName(), index + 1, getName());
            }
            else if (!WhichDataType(arg).isUInt64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be UInt64",
                    arg->getName(), index + 1, getName()
                    );
        }

        DataTypePtr center = std::make_shared<DataTypeUInt64>();
        DataTypePtr radius = std::make_shared<DataTypeFloat64>();

        return std::make_shared<DataTypeTuple>(DataTypes{center, radius});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_center1 = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_center1)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_center1 = col_center1->getData();

        const auto * col_radius1 = checkAndGetColumn<ColumnFloat64>(non_const_arguments[1].column.get());
        if (!col_radius1)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_radius1 = col_radius1->getData();

        const auto * col_center2 = checkAndGetColumn<ColumnUInt64>(non_const_arguments[2].column.get());
        if (!col_center2)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[2].type->getName(),
                3,
                getName());
        const auto & data_center2 = col_center2->getData();

        const auto * col_radius2 = checkAndGetColumn<ColumnFloat64>(non_const_arguments[3].column.get());
        if (!col_radius2)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be Float64",
                arguments[3].type->getName(),
                4,
                getName());
        const auto & data_radius2 = col_radius2->getData();

        auto col_res_center = ColumnUInt64::create();
        auto col_res_radius = ColumnFloat64::create();

        auto & vec_res_center = col_res_center->getData();
        vec_res_center.reserve(input_rows_count);

        auto & vec_res_radius = col_res_radius->getData();
        vec_res_radius.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 first_center = data_center1[row];
            const Float64 first_radius = data_radius1[row];
            const UInt64 second_center = data_center2[row];
            const Float64 second_radius = data_radius2[row];

            if (isNaN(first_radius) || isNaN(second_radius))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Radius of the cap must not be nan");

            if (std::isinf(first_radius) || std::isinf(second_radius))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Radius of the cap must not be infinite");

            auto first_center_cell = S2CellId(first_center);
            auto second_center_cell = S2CellId(second_center);

            if (!first_center_cell.is_valid() || !second_center_cell.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Center of the cap is not valid");

            S2Cap cap1(first_center_cell.ToPoint(), S1Angle::Degrees(first_radius));
            S2Cap cap2(second_center_cell.ToPoint(), S1Angle::Degrees(second_radius));

            S2Cap cap_union = cap1.Union(cap2);

            vec_res_center.emplace_back(S2CellId(cap_union.center()).id());
            vec_res_radius.emplace_back(cap_union.GetRadius().degrees());
        }

        return ColumnTuple::create(Columns{std::move(col_res_center), std::move(col_res_radius)});
    }
};

}

REGISTER_FUNCTION(S2CapUnion)
{
    factory.registerFunction<FunctionS2CapUnion>();
}

}

#endif
