#include "config_functions.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
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
 * So, each cell has 4 neighbors
 */
class FunctionS2GetNeighbors : public IFunction
{
public:
    static constexpr auto name = "s2GetNeighbors";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2GetNeighbors>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();

        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(),
                1,
                getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_id = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_id)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_id = col_id->getData();

        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        size_t current_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 id = data_id[row];

            S2CellId cell_id(id);

            if (!cell_id.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cell is not valid");

            S2CellId neighbors[4];
            cell_id.GetEdgeNeighbors(neighbors);

            dst_data.reserve(dst_data.size() + 4);
            for (auto & neighbor : neighbors)
            {
                ++current_offset;
                dst_data.insert(neighbor.id());
            }
            dst_offsets[row] = current_offset;
        }

        return dst;
    }

};

}

REGISTER_FUNCTION(S2GetNeighbors)
{
    factory.registerFunction<FunctionS2GetNeighbors>();
}


}

#endif
