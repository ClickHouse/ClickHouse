#include "config_functions.h"

#if USE_H3

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <h3api.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
}

namespace
{

class FunctionH3HexRing : public IFunction
{
public:
    static constexpr auto name = "h3HexRing";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3HexRing>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt16())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt16",
                arg->getName(),
                2,
                getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_hindex = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_hindex)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data_hindex = col_hindex->getData();

        /// ColumnUInt16 is sufficient as the max value of 2nd arg is checked (arg > 0 < 10000) in implementation below
        const auto * col_k = checkAndGetColumn<ColumnUInt16>(non_const_arguments[1].column.get());
        if (!col_k)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt16.",
                arguments[1].type->getName(),
                2,
                getName());

        const auto & data_k = col_k->getData();

        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = typeid_cast<ColumnUInt64 &>(dst->getData());
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);

        /// First calculate array sizes for all rows and save them in Offsets
        UInt64 current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const int k = data_k[row];

            /// The result size is 6*k. We should not allow to generate too large arrays nevertheless.
            constexpr auto max_k = 10000;
            if (k > max_k)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Too large 'k' argument for {} function, maximum {}", getName(), max_k);
            /// Check is already made while fetching the argument for k (to determine if it's an unsigned integer). Nevertheless, it's checked again here.
            if (k < 0)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Argument 'k' for {} function must be non negative", getName());

            const auto vec_size = (k == 0 ? 1 : 6 * k);  /// Required size according to comments in gridRingUnsafe() source code

            current_offset += vec_size;
            dst_offsets[row] = current_offset;
        }

        /// Allocate based on total size of arrays for all rows
        dst_data.getData().resize(current_offset);

        /// Fill the array for each row with known size
        auto* ptr = dst_data.getData().data();
        current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const H3Index origin_hindex = data_hindex[row];
            const int k = data_k[row];

            H3Error err = gridRingUnsafe(origin_hindex, k, ptr + current_offset);

            if (err)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect arguments h3Index: {}, k: {}, error: {}", origin_hindex, k, err);

            const auto size = dst_offsets[row] - current_offset;
            current_offset += size;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(H3HexRing)
{
    factory.registerFunction<FunctionH3HexRing>();
}

}

#endif
