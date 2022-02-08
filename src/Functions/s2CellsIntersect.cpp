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

/**
 * Each cell in s2 library is a quadrilateral bounded by four geodesics.
 */
class FunctionS2CellsIntersect : public IFunction
{
public:
    static constexpr auto name = "s2CellsIntersect";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2CellsIntersect>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

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

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_id_first = arguments[0].column.get();
        const auto * col_id_second = arguments[1].column.get();

        auto dst = ColumnUInt8::create();
        auto & dst_data = dst->getData();
        dst_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 id_first = col_id_first->getInt(row);
            const UInt64 id_second = col_id_second->getInt(row);

            auto first_cell = S2CellId(id_first);
            auto second_cell = S2CellId(id_second);

            if (!first_cell.is_valid() || !second_cell.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cell is not valid");

            dst_data.emplace_back(S2CellId(id_first).intersects(S2CellId(id_second)));
        }

        return dst;
    }

};

}

void registerFunctionS2CellsIntersect(FunctionFactory & factory)
{
    factory.registerFunction<FunctionS2CellsIntersect>();
}


}

#endif
