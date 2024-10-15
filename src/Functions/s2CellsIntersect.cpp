#include "config.h"

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

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_id_first = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_id_first)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_id_first = col_id_first->getData();

        const auto * col_id_second = checkAndGetColumn<ColumnUInt64>(non_const_arguments[1].column.get());
        if (!col_id_second)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[1].type->getName(),
                2,
                getName());
        const auto & data_id_second = col_id_second->getData();

        auto dst = ColumnUInt8::create();
        auto & dst_data = dst->getData();
        dst_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 id_first = data_id_first[row];
            const UInt64 id_second = data_id_second[row];

            auto first_cell = S2CellId(id_first);
            if (!first_cell.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "First cell (id {}) is not valid in function {}", id_first, getName());

            auto second_cell = S2CellId(id_second);
            if (!second_cell.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second cell (id {}) is not valid in function {}", id_second, getName());

            dst_data.emplace_back(S2CellId(id_first).intersects(S2CellId(id_second)));
        }

        return dst;
    }

};

}

REGISTER_FUNCTION(S2CellsIntersect)
{
    factory.registerFunction<FunctionS2CellsIntersect>();
}


}

#endif
