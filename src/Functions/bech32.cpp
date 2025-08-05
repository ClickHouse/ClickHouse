#include "config.h"

#if USE_BECH32

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <bech32.h>

namespace
{

/** Max length of Bech32 or Bech32m encoding is 90 chars, this includes:
 *
 *      HRP: 1 - 83 human readable characters, 'bc' or 'tb' for a SegWit address
 *      separator: always '1'
 *      data: array of 5-bit bytes consisting of a 6 byte checksum, a witness byte, and the actual encoded data
 *
 * max_len = (90 - 1 (HRP) - 1 (sep) - 6 (checksum) - 1 (witness byte)) * 5 // 8
 * max_len = 405 bits or 50 (8-bit) bytes // round up to 55 just in case
 */
constexpr size_t max_address_len = 90;
constexpr size_t max_data_len = 55;
constexpr size_t max_human_readable_part_len = 83; // Note: if we only support segwit addresses, this can be changed to 2

using bech32_data = std::vector<uint8_t>;

/// -------------------------------------------------------------------------------------------------------
/// Function copied from contrib/bech32/ref/c++/segwit_addr.cpp
/// -------------------------------------------------------------------------------------------------------
///
/** Convert from one power-of-2 number base to another.
 *
 *  Function will convert a input vector of <frombits>-bit data to an output vector of <tobit>-bit data,
 *  padding the result if <pad> is true.
 *
 *  Example:
 *  Input:  10010110 11001011 (8-bit numbers)
 *  Output: 10010 11011 00101 10000 (5-bit numbers)
 *
 *  The last 4 "extra" 0s in the output are padding, they will only be added if <pad> is true.
 *  If <pad> is false, no padding will be added and the function will return false if there are bits
 *  left over.
 */
template <int frombits, int tobits, bool pad>
bool convertbits(bech32_data & out, const bech32_data & in)
{
    int acc = 0;
    int bits = 0;
    const int maxv = (1 << tobits) - 1;
    const int max_acc = (1 << (frombits + tobits - 1)) - 1;
    for (int value : in)
    {
        acc = ((acc << frombits) | value) & max_acc;
        bits += frombits;
        while (bits >= tobits)
        {
            bits -= tobits;
            out.push_back((acc >> bits) & maxv);
        }
    }
    if (pad)
    {
        if (bits) // pad leftover bits with 0s and push to 'out'
            out.push_back((acc << (tobits - bits)) & maxv);
    }
    // if pad == false: sanity check, then check if there are significant (non-0) leftover bits
    else if (bits >= frombits || ((acc << (tobits - bits)) & maxv))
    {
        return false;
    }
    return true;
}
/// -------------------------------------------------------------------------------------------------------

void finalizeRow(DB::ColumnString::Offsets & offsets, char *& pos, const char * const begin, const size_t i)
{
    *pos = '\0';
    ++pos;
    offsets[i] = pos - begin;
}

void updatePrevOffset(size_t & prev_offset, const size_t next_offset, const size_t row_width)
{
    prev_offset = row_width == 0 ? next_offset : prev_offset + row_width;
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

/// Encode string to Bech32 or Bech32m address
class EncodeToBech32Representation : public IFunction
{
public:
    static constexpr auto name = "bech32Encode";

    /// Default to the new and improved Bech32m algorithm
    static constexpr int default_witness_version = 1;

    static FunctionPtr create(ContextPtr) { return std::make_shared<EncodeToBech32Representation>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "At least two string arguments (human_readable_part, data) are required for function {}",
                getName());

        if (arguments.size() > 3)
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "A maximum of 3 arguments (human_readable_part, data, witness_version) are allowed for function {}",
                getName());

        /// check first two args, human_readable_part and input string
        for (size_t i = 0; i < 2; ++i)
            if (!WhichDataType(arguments[i]).isStringOrFixedString())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}, expected String or FixedString",
                    arguments[i]->getName(),
                    i + 1,
                    getName());

        /// check 3rd (optional) arg, specifying witness version aka whether to use Bech32 or Bech32m algo
        size_t arg_idx = 2;
        if (arguments.size() == 3 && !WhichDataType(arguments[arg_idx]).isNativeUInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}, expected unsigned integer",
                arguments[arg_idx]->getName(),
                arg_idx + 1,
                getName());

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        bool have_witness_version = arguments.size() == 3;

        ColumnPtr col0 = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr col1 = arguments[1].column->convertToFullColumnIfConst();
        ColumnPtr col2;
        if (have_witness_version)
            col2 = arguments[2].column;

        if (const ColumnString * col0_string = checkAndGetColumn<ColumnString>(col0.get()))
        {
            const ColumnString::Chars & col0_vec = col0_string->getChars();
            const ColumnString::Offsets * col0_offsets = &col0_string->getOffsets();

            return chooseCol1AndExecute(col0_vec, col0_offsets, col1, col2, input_rows_count, have_witness_version);
        }

        if (const ColumnFixedString * col0_fixed_string = checkAndGetColumn<ColumnFixedString>(col0.get()))
        {
            const ColumnString::Chars & col0_vec = col0_fixed_string->getChars();
            const ColumnString::Offsets * col0_offsets = nullptr; /// dummy

            return chooseCol1AndExecute(col0_vec, col0_offsets, col1, col2, input_rows_count, have_witness_version, col0_fixed_string->getN());
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    ColumnPtr chooseCol1AndExecute(
        const ColumnString::Chars & col0_vec,
        const ColumnString::Offsets * col0_offsets,
        const ColumnPtr & col1,
        const ColumnPtr & col2,
        const size_t input_rows_count,
        const bool have_witness_version = false,
        const size_t col0_width = 0) const
    {
        if (const ColumnString * col1_str_ptr = checkAndGetColumn<ColumnString>(col1.get()))
        {
            const ColumnString::Chars & col1_vec = col1_str_ptr->getChars();
            const ColumnString::Offsets * col1_offsets = &col1_str_ptr->getOffsets();

            return execute(col0_vec, col0_offsets, col1_vec, col1_offsets, col2, input_rows_count, have_witness_version, col0_width);
        }

        if (const ColumnFixedString * col1_fstr_ptr = checkAndGetColumn<ColumnFixedString>(col1.get()))
        {
            const ColumnString::Chars & col1_vec = col1_fstr_ptr->getChars();
            const ColumnString::Offsets * col1_offsets = nullptr; /// dummy

            return execute(
                col0_vec, col0_offsets, col1_vec, col1_offsets, col2, input_rows_count, have_witness_version, col0_width, col1_fstr_ptr->getN());
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", col1->getName(), getName());
    }

    static ColumnPtr execute(
        const ColumnString::Chars & human_readable_part_vec,
        const ColumnString::Offsets * human_readable_part_offsets,
        const ColumnString::Chars & data_vec,
        const ColumnString::Offsets * data_offsets,
        const ColumnPtr & witness_version_col,
        const size_t input_rows_count,
        const bool have_witness_version = false,
        const size_t human_readable_part_width = 0,
        const size_t data_width = 0)
    {
        /// outputs
        auto out_col = ColumnString::create();
        ColumnString::Chars & out_vec = out_col->getChars();
        ColumnString::Offsets & out_offsets = out_col->getOffsets();

        out_offsets.resize(input_rows_count);
        out_vec.resize((max_address_len + 1 /* trailing 0 */) * input_rows_count);

        char * out_begin = reinterpret_cast<char *>(out_vec.data());
        char * out_pos = out_begin;

        size_t human_readable_part_prev_offset = 0;
        size_t data_prev_offset = 0;

        /// In ColumnString each value ends with a trailing 0, in ColumnFixedString there is no trailing 0
        size_t human_readable_part_zero_offset = human_readable_part_width == 0 ? 1 : 0;
        size_t data_zero_offset = data_width == 0 ? 1 : 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t human_readable_part_new_offset = human_readable_part_width == 0 ? (*human_readable_part_offsets)[i] : human_readable_part_prev_offset + human_readable_part_width;
            size_t data_new_offset = data_width == 0 ? (*data_offsets)[i] : data_prev_offset + data_width;

            /// NUL chars are used to pad fixed width strings, so we remove them here since they are not valid inputs anyway
            while (human_readable_part_width > 0 && human_readable_part_vec[human_readable_part_new_offset - 1] == 0 && human_readable_part_new_offset > human_readable_part_prev_offset)
                --human_readable_part_new_offset;

            /// max encodable data to stay within 90-char limit on Bech32 output
            /// human_readable_part must be at least 1 character and no more than 83
            auto data_len = data_new_offset - data_prev_offset - data_zero_offset;
            auto human_readable_part_len = human_readable_part_new_offset - human_readable_part_prev_offset - human_readable_part_zero_offset;
            if (data_len > max_data_len || human_readable_part_len > max_human_readable_part_len || human_readable_part_len < 1)
            {
                finalizeRow(out_offsets, out_pos, out_begin, i);

                updatePrevOffset(human_readable_part_prev_offset, human_readable_part_new_offset, human_readable_part_width);
                updatePrevOffset(data_prev_offset, data_new_offset, data_width);
                continue;
            }

            std::string human_readable_part(
                reinterpret_cast<const char *>(&human_readable_part_vec[human_readable_part_prev_offset]),
                reinterpret_cast<const char *>(&human_readable_part_vec[human_readable_part_new_offset - human_readable_part_zero_offset]));

            bech32_data input(
                reinterpret_cast<const uint8_t *>(&data_vec[data_prev_offset]),
                reinterpret_cast<const uint8_t *>(&data_vec[data_new_offset - data_zero_offset]));

            uint8_t witness_version = have_witness_version ? witness_version_col->getUInt(i) : default_witness_version;

            bech32_data input_5bit;
            convertbits<8, 5, true>(input_5bit, input); /// squash input from 8-bit -> 5-bit bytes
            std::string address = bech32::encode(human_readable_part, input_5bit, witness_version > 0 ? bech32::Encoding::BECH32M : bech32::Encoding::BECH32);

            if (address.empty() || address.size() > max_address_len)
            {
                finalizeRow(out_offsets, out_pos, out_begin, i);

                updatePrevOffset(human_readable_part_prev_offset, human_readable_part_new_offset, human_readable_part_width);
                updatePrevOffset(data_prev_offset, data_new_offset, data_width);
                continue;
            }

            /// store address in out_pos
            std::memcpy(out_pos, address.data(), address.size());
            out_pos += address.size();

            finalizeRow(out_offsets, out_pos, out_begin, i);

            updatePrevOffset(human_readable_part_prev_offset, human_readable_part_new_offset, human_readable_part_width);
            updatePrevOffset(data_prev_offset, data_new_offset, data_width);
        }

        chassert(
            static_cast<size_t>(out_pos - out_begin) <= out_vec.size(),
            fmt::format("too small amount of memory was preallocated: needed {}, but have only {}", out_pos - out_begin, out_vec.size()));

        out_vec.resize(out_pos - out_begin);

        return out_col;
    }
};

/// Decode original address from string containing Bech32 or Bech32m address
class DecodeFromBech32Representation : public IFunction
{
public:
    static constexpr auto name = "bech32Decode";
    static constexpr size_t tuple_size = 2; /// (human_readable_part, data)

    static FunctionPtr create(ContextPtr) { return std::make_shared<DecodeFromBech32Representation>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    /// Bech32 and Bech32m are each bijective, but since our decode function accepts either of them,
    /// then decode(bech32(input)) == decode(bech32m(input))
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType dtype(arguments[0]);
        if (!dtype.isStringOrFixedString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            types[i] = std::make_shared<DataTypeString>();

        return std::make_shared<DataTypeTuple>(types);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            const ColumnString::Chars & in_vec = col->getChars();
            const ColumnString::Offsets * in_offsets = &col->getOffsets();

            return execute(in_vec, in_offsets, input_rows_count);
        }

        if (const ColumnFixedString * col_fix_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const ColumnString::Chars & in_vec = col_fix_string->getChars();
            const ColumnString::Offsets * in_offsets = nullptr; /// dummy

            return execute(in_vec, in_offsets, input_rows_count, col_fix_string->getN());
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    static ColumnPtr
    execute(const ColumnString::Chars & in_vec, const ColumnString::Offsets * in_offsets, size_t input_rows_count, size_t col_width = 0)
    {
        auto col0_res = ColumnString::create();
        auto col1_res = ColumnString::create();

        ColumnString::Chars & human_readable_part_vec = col0_res->getChars();
        ColumnString::Offsets & human_readable_part_offsets = col0_res->getOffsets();

        ColumnString::Chars & data_vec = col1_res->getChars();
        ColumnString::Offsets & data_offsets = col1_res->getOffsets();

        human_readable_part_offsets.resize(input_rows_count);
        data_offsets.resize(input_rows_count);

        human_readable_part_vec.resize((max_human_readable_part_len + 1 /* trailing 0 */) * input_rows_count);
        data_vec.resize((max_data_len + 1 /* trailing 0 */) * input_rows_count);

        char * human_readable_part_begin = reinterpret_cast<char *>(human_readable_part_vec.data());
        char * human_readable_part_pos = human_readable_part_begin;

        char * data_begin = reinterpret_cast<char *>(data_vec.data());
        char * data_pos = data_begin;

        size_t prev_offset = 0;

        /// In ColumnString each value ends with a trailing 0, in ColumnFixedString there is no trailing 0
        size_t trailing_zero_offset = col_width == 0 ? 1 : 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t new_offset = col_width == 0 ? (*in_offsets)[i] : prev_offset + col_width;

            /// NUL chars are used to pad fixed width strings, so we remove them here since they are not valid inputs anyway
            while (col_width > 0 && in_vec[new_offset - 1] == 0 && new_offset > prev_offset)
            {
                --new_offset;
            }

            /// enforce char limit
            if ((new_offset - prev_offset - trailing_zero_offset) > max_address_len)
            {
                finalizeRow(human_readable_part_offsets, human_readable_part_pos, human_readable_part_begin, i);
                finalizeRow(data_offsets, data_pos, data_begin, i);

                updatePrevOffset(prev_offset, new_offset, col_width);
                continue;
            }

            std::string input(
                reinterpret_cast<const char *>(&in_vec[prev_offset]),
                reinterpret_cast<const char *>(&in_vec[new_offset - trailing_zero_offset]));

            const auto dec = bech32::decode(input);

            bech32_data data_8bit;
            if (dec.encoding == bech32::Encoding::INVALID
                || !convertbits<5, 8, false>(data_8bit, bech32_data(dec.data.begin(), dec.data.end()))
                || data_8bit.empty())
            {
                finalizeRow(human_readable_part_offsets, human_readable_part_pos, human_readable_part_begin, i);
                finalizeRow(data_offsets, data_pos, data_begin, i);

                updatePrevOffset(prev_offset, new_offset, col_width);
                continue;
            }

            /// store human_readable_part output in human_readable_part_pos
            std::memcpy(human_readable_part_pos, dec.hrp.data(), dec.hrp.size());
            human_readable_part_pos += dec.hrp.size();

            finalizeRow(human_readable_part_offsets, human_readable_part_pos, human_readable_part_begin, i);

            /// store data output in data_pos
            std::memcpy(data_pos, data_8bit.data(), data_8bit.size());
            data_pos += data_8bit.size();

            finalizeRow(data_offsets, data_pos, data_begin, i);

            updatePrevOffset(prev_offset, new_offset, col_width);
        }

        chassert(
            static_cast<size_t>(human_readable_part_pos - human_readable_part_begin) <= human_readable_part_vec.size(),
            fmt::format("too small amount of memory was preallocated: needed {}, but have only {}", human_readable_part_pos - human_readable_part_begin, human_readable_part_vec.size()));
        chassert(
            static_cast<size_t>(data_pos - data_begin) <= data_vec.size(),
            fmt::format(
                "too small amount of memory was preallocated: needed {}, but have only {}", data_pos - data_begin, data_vec.size()));

        human_readable_part_vec.resize(human_readable_part_pos - human_readable_part_begin);
        data_vec.resize(data_pos - data_begin);

        Columns tuple_columns(tuple_size);
        tuple_columns[0] = std::move(col0_res);
        tuple_columns[1] = std::move(col1_res);

        return ColumnTuple::create(tuple_columns);
    }
};

REGISTER_FUNCTION(Bech32Repr)
{
    factory.registerFunction<EncodeToBech32Representation>();
    factory.registerFunction<DecodeFromBech32Representation>();
}

}

#endif
