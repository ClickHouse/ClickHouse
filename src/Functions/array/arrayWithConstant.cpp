#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <base/arithmeticOverflow.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/// Reasonable thresholds.
static constexpr Int64 max_array_size_in_columns_bytes = 1000000000;
static constexpr size_t max_arrays_size_in_columns = 1000000000;


/* arrayWithConstant(num, const) - make array of constants with length num.
 * arrayWithConstant(3, 'hello') = ['hello', 'hello', 'hello']
 * arrayWithConstant(1, 'hello') = ['hello']
 * arrayWithConstant(0, 'hello') = []
 */

class FunctionArrayWithConstant : public IFunction
{
public:
    static constexpr auto name = "arrayWithConstant";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayWithConstant>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNativeNumber(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected Integer",
                arguments[0]->getName(), getName());
        return std::make_shared<DataTypeArray>(arguments[1]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t num_rows) const override
    {
        const auto * col_num = arguments[0].column.get();
        const auto * col_value = arguments[1].column.get();

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnArray::Offsets & offsets = offsets_col->getData();
        offsets.reserve(num_rows);

        ColumnArray::Offset offset = 0;
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto array_size = col_num->getInt(i);

            if (unlikely(array_size < 0))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array size {} cannot be negative: while executing function {}", array_size, getName());

            Int64 estimated_size = 0;
            if (unlikely(common::mulOverflow(array_size, col_value->byteSize(), estimated_size)))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array size {} with element size {} bytes is too large: while executing function {}", array_size, col_value->byteSize(), getName());

            if (unlikely(estimated_size > max_array_size_in_columns_bytes))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array size {} with element size {} bytes is too large: while executing function {}", array_size, col_value->byteSize(), getName());

            offset += array_size;

            if (unlikely(offset > max_arrays_size_in_columns))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size {} (will generate at least {} elements) while executing function {}", array_size, offset, getName());

            offsets.push_back(offset);
        }

        return ColumnArray::create(col_value->replicate(offsets)->convertToFullColumnIfConst(), std::move(offsets_col));
    }
};

REGISTER_FUNCTION(ArrayWithConstant)
{
    factory.registerFunction<FunctionArrayWithConstant>();
}

}
