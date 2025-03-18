#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

// TODO: support for Float64 and BFloat16

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionTransposeBits : public IFunction
{
public:
    static constexpr auto name = "transposeBits";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTransposeBits>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be an array.", getName());

        if (array_type->getNestedType()->getTypeId() != TypeIndex::Float32)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} only supports Array(Float32).", getName());

        return arguments[0];
    }

    // TODO we assume all vectors have the same size. Better check that first. This will likely become unnecessary when this logic moves
    // TODO to the data type itself, where we will demand that all the vectors have the same size
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!array)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());

        auto res_ptr = array->cloneEmpty();
        auto & res = assert_cast<ColumnArray &>(*res_ptr);
        res.getOffsetsPtr() = array->getOffsetsPtr();

        const IColumn & src_data = array->getData();
        const ColumnArray::Offsets & offsets = array->getOffsets();
        IColumn & res_data = res.getData();

        const auto * src_nullable_col = typeid_cast<const ColumnNullable *>(&src_data);
        const auto * res_nullable_col = typeid_cast<ColumnNullable *>(&res_data);

        if (src_nullable_col || res_nullable_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} does not support NULLs in Array(Float32)).", getName());

        const auto * src_data_concrete = checkAndGetColumn<ColumnVector<Float32>>(&src_data);
        if (!src_data_concrete)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}. Function {} expects Array(Float32).", src_data.getName(), getName());

        const PaddedPODArray<Float32> & src_vec = src_data_concrete->getData();
        PaddedPODArray<Float32> & res_vec = typeid_cast<ColumnVector<Float32> &>(res_data).getData();
        res_vec.resize(src_data.size());

        const size_t num_rows = offsets.size();
        ColumnArray::Offset prev_offset = 0;

        for (size_t row = 0; row < num_rows; ++row)
        {
            const size_t curr_offset = offsets[row];
            const size_t sub_len = curr_offset - prev_offset;

            if (sub_len > 0)
            {
                // To avoid situations where two groups end up in the same word, we pad each group to the nearest multiple of 8
                // TODO this is extremely inefficient and is used in POC. In reality, we will pad on when user inserts new data
                if (sub_len % 8 != 0)
                {
                    const size_t remainder = sub_len % 8;
                    const size_t padded_len = sub_len + (8 - remainder);

                    std::vector<Float32> tmp(padded_len, 0.0f);
                    memcpy(tmp.data(), &src_vec[prev_offset], sub_len * sizeof(Float32));

                    transposeBits(tmp.data(), &res_vec[prev_offset], padded_len);
                }
                else
                {
                    transposeBits(&src_vec[prev_offset], &res_vec[prev_offset], sub_len);
                }
            }

            prev_offset = curr_offset;
        }

        chassert(static_cast<bool>(src_nullable_col) == static_cast<bool>(res_nullable_col));
        return res_ptr;
    }


private:
    static void transposeBits(const Float32 * in_floats, Float32 * out_floats, const size_t length)
    {
        const auto * in_u32  = reinterpret_cast<const UInt32 *>(in_floats);
        auto *       out_u32 = reinterpret_cast<UInt32      *>(out_floats);

        for (size_t i = 0; i < length; i++)
            out_u32[i] = 0;

        for (size_t bit = 0; bit < 32; bit++)
        {
            for (size_t j = 0; j < length; j++)
            {
                const UInt32 b = (in_u32[j] >> (31 - bit)) & 1U;

                const size_t out_bit_index   = bit * length + j;
                const size_t out_word_index  = out_bit_index >> 5;      // / 32
                const size_t out_bit_offset  = out_bit_index & 0x1F;    // % 32

                out_u32[out_word_index] |= (b << (31 - out_bit_offset));
            }
        }
    }
};

REGISTER_FUNCTION(TransposeBits) { factory.registerFunction<FunctionTransposeBits>(); }

}
