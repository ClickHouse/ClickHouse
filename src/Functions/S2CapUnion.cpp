#include "config_functions.h"

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
class FunctionS2CapUnion : public IFunction
{
public:
    static constexpr auto name = "S2CapUnion";

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 4) {
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(number_of_arguments) + ", should be 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        const auto * arg = arguments[0].get();

        if (!WhichDataType(arg).isUInt64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        arg = arguments[1].get();

        if (!WhichDataType(arg).isFloat64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(2) + " of function " + getName() + ". Must be Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        arg = arguments[2].get();

        if (!WhichDataType(arg).isUInt64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(3) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        arg = arguments[3].get();

        if (!WhichDataType(arg).isFloat64()) {
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(4) + " of function " + getName() + ". Must be Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        DataTypePtr center = std::make_shared<DataTypeUInt64>();
        DataTypePtr radius = std::make_shared<DataTypeFloat64>();

        return std::make_shared<DataTypeTuple>(DataTypes{center, radius});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_center1 = arguments[0].column.get();
        const auto * col_radius1 = arguments[1].column.get();
        const auto * col_center2 = arguments[2].column.get();
        const auto * col_radius2 = arguments[3].column.get();

        auto col_res_first = ColumnUInt64::create();
        auto col_res_second = ColumnFloat64::create();

        auto & vec_res_first = col_res_first->getData();
        vec_res_first.resize(input_rows_count);

        auto & vec_res_second = col_res_second->getData();
        vec_res_second.resize(input_rows_count);

        for (const auto row : collections::range(0, input_rows_count))
        {
            const UInt64 center1 = col_center1->getUInt(row);
            const Float64 radius1 = col_radius1->getFloat64(row);
            const UInt64 center2 = col_center2->getUInt(row);
            const Float64 radius2 = col_radius2->getFloat64(row);

            S2Cap cap1(S2CellId(center1).ToPoint(), S1Angle::Degrees(radius1));
            S2Cap cap2(S2CellId(center2).ToPoint(), S1Angle::Degrees(radius2));

            S2Cap cap_union = cap1.Union(cap2);

            vec_res_first[row] = S2CellId(cap_union.center()).id();
            vec_res_second[row] = cap_union.GetRadius().degrees();
        }

        return ColumnTuple::create(Columns{std::move(col_res_first), std::move(col_res_second)});
    }

};

}

void registerFunctionS2CapUnion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2CapUnion>();
}


}
