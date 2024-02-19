#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionSpaceFillingCurveEncode.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>
#include <limits>


namespace DB
{

class FunctionHilbertEncode2DWIthLookupTableImpl {
public:
    static UInt64 encode(UInt64 x, UInt64 y) {
        const auto leading_zeros_count = getLeadingZeroBits(x | y);
        const auto used_bits = std::numeric_limits<UInt64>::digits - leading_zeros_count;

        UInt8 remaind_shift = BIT_STEP - used_bits % BIT_STEP;
        if (remaind_shift == BIT_STEP)
            remaind_shift = 0;
        x <<= remaind_shift;
        y <<= remaind_shift;

        UInt8 current_state = 0;
        UInt64 hilbert_code = 0;
        Int8 current_shift = used_bits + remaind_shift - BIT_STEP;

        while (current_shift > 0)
        {
            const UInt8 x_bits = (x >> current_shift) & STEP_MASK;
            const UInt8 y_bits = (y >> current_shift) & STEP_MASK;
            const auto hilbert_bits = getCodeAndUpdateState(x_bits, y_bits, current_state);
            const UInt8 hilbert_code_shift = static_cast<UInt8>(current_shift) << 1;
            hilbert_code |= (hilbert_bits << hilbert_code_shift);

            current_shift -= BIT_STEP;
        }

        hilbert_code >>= (remaind_shift << 1);
        return hilbert_code;
    }

private:

    // LOOKUP_TABLE[SSXXXYYY] = SSHHHHHH]
    // where SS - 2 bits for state, XXX - 3 bits of x, YYY - 3 bits of y
    static UInt8 getCodeAndUpdateState(UInt8 x_bits, UInt8 y_bits, UInt8& state) {
        const UInt8 table_index = state | (x_bits << BIT_STEP) | y_bits;
        const auto table_code = LOOKUP_TABLE[table_index];
        state = table_code & STATE_MASK;
        return table_code & HILBERT_MASK;
    }

    constexpr static UInt8 BIT_STEP = 3;
    constexpr static UInt8 STEP_MASK = (1 << BIT_STEP) - 1;
    constexpr static UInt8 HILBERT_MASK = (1 << (BIT_STEP << 1)) - 1;
    constexpr static UInt8 STATE_MASK = static_cast<UInt8>(-1) - HILBERT_MASK;

    constexpr static UInt8 LOOKUP_TABLE[256] = {
        64, 1, 206, 79, 16, 211, 84, 21, 131, 2, 205, 140, 81, 82, 151, 22, 4, 199, 8, 203, 158,
        157, 88, 25, 69, 70, 73, 74, 31, 220, 155, 26, 186, 185, 182, 181, 32, 227, 100, 37, 59,
        248, 55, 244, 97, 98, 167, 38, 124, 61, 242, 115, 174, 173, 104, 41, 191, 62, 241, 176, 47,
        236, 171, 42, 0, 195, 68, 5, 250, 123, 60, 255, 65, 66, 135, 6, 249, 184, 125, 126, 142,
        141, 72, 9, 246, 119, 178, 177, 15, 204, 139, 10, 245, 180, 51, 240, 80, 17, 222, 95, 96,
        33, 238, 111, 147, 18, 221, 156, 163, 34, 237, 172, 20, 215, 24, 219, 36, 231, 40, 235, 85,
        86, 89, 90, 101, 102, 105, 106, 170, 169, 166, 165, 154, 153, 150, 149, 43, 232, 39, 228,
        27, 216, 23, 212, 108, 45, 226, 99, 92, 29, 210, 83, 175, 46, 225, 160, 159, 30, 209, 144,
        48, 243, 116, 53, 202, 75, 12, 207, 113, 114, 183, 54, 201, 136, 77, 78, 190, 189, 120, 57,
        198, 71, 130, 129, 63, 252, 187, 58, 197, 132, 3, 192, 234, 107, 44, 239, 112, 49, 254,
        127, 233, 168, 109, 110, 179, 50, 253, 188, 230, 103, 162, 161, 52, 247, 56, 251, 229, 164,
        35, 224, 117, 118, 121, 122, 218, 91, 28, 223, 138, 137, 134, 133, 217, 152, 93, 94, 11,
        200, 7, 196, 214, 87, 146, 145, 76, 13, 194, 67, 213, 148, 19, 208, 143, 14, 193, 128,
    };
};


class FunctionHilbertEncode : public FunctionSpaceFillingCurveEncode
{
public:
    static constexpr auto name = "hilbertEncode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHilbertEncode>();
    }

    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_dimensions = arguments.size();
        if (num_dimensions < 1 || num_dimensions > 2) {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal number of UInt arguments of function {}: should be at least 1 and not more than 2",
                getName());
        }

        size_t vector_start_index = 0;
        const auto * const_col = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const ColumnTuple * mask;
        if (const_col)
            mask = typeid_cast<const ColumnTuple *>(const_col->getDataColumnPtr().get());
        else
            mask = typeid_cast<const ColumnTuple *>(arguments[0].column.get());
        if (mask)
        {
            num_dimensions = mask->tupleSize();
            vector_start_index = 1;
            for (size_t i = 0; i < num_dimensions; i++)
            {
                auto ratio = mask->getColumn(i).getUInt(0);
                if (ratio > 8 || ratio < 1)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Illegal argument {} of function {}, should be a number in range 1-8",
                                    arguments[0].column->getName(), getName());
            }
        }

        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        auto col_res = ColumnUInt64::create();
        ColumnUInt64::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnPtr & col0 = non_const_arguments[0 + vector_start_index].column;
        if (num_dimensions == 1) {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                vec_res[i] = col0->getUInt(i);
            }
            return col_res;
        }

        const ColumnPtr & col1 = non_const_arguments[1 + vector_start_index].column;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            vec_res[i] = FunctionHilbertEncode2DWIthLookupTableImpl::encode(col0->getUInt(i), col1->getUInt(i));
        }
        return col_res;
    }
};


REGISTER_FUNCTION(HilbertEncode)
{
    factory.registerFunction<FunctionHilbertEncode>(FunctionDocumentation{
    .description=R"(

)",
        .examples{
            },
        .categories {}
    });
}

}
