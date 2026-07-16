#include "config.h"

#if USE_BECH32

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
    extern const int BAD_ARGUMENTS;
}

/// Encode string to Bech32 or Bech32m address
class EncodeToBech32Representation : public IFunction
{
public:
    static constexpr auto name = "bech32Encode";

    /// Default to the new and improved Bech32m algorithm (SegWit mode with witness version byte)
    static constexpr int default_witness_version = 1;

    /// When encoding variant is explicitly specified as a string ('bech32' or 'bech32m'),
    /// we encode raw data without prepending a witness version byte.
    /// This mode is needed for non-SegWit use cases such as Cosmos SDK addresses.

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
                "A maximum of 3 arguments (human_readable_part, data, witness_version | 'bech32' | 'bech32m') are allowed for function {}",
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

        /// check 3rd (optional) arg: either a witness version (UInt*) or encoding variant ('bech32'/'bech32m')
        size_t arg_idx = 2;
        if (arguments.size() == 3 && !WhichDataType(arguments[arg_idx]).isNativeUInt() && !WhichDataType(arguments[arg_idx]).isString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}, expected unsigned integer (witness version) or String ('bech32'/'bech32m')",
                arguments[arg_idx]->getName(),
                arg_idx + 1,
                getName());

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        bool have_witness_version = false;
        bool have_encoding_variant = false;
        bech32::Encoding explicit_encoding = bech32::Encoding::INVALID;

        ColumnPtr col0 = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr col1 = arguments[1].column->convertToFullColumnIfConst();
        ColumnPtr col2;

        if (arguments.size() == 3)
        {
            if (WhichDataType(arguments[2].type).isString())
            {
                /// 3rd arg is encoding variant string — must be constant (mode selector, not per-row data).
                /// useDefaultImplementationForConstants=true materializes const literals into
                /// full columns, so we also accept ColumnString and read from row 0.
                ColumnPtr col2_full = arguments[2].column->convertToFullColumnIfConst();
                const auto * variant_col = checkAndGetColumn<ColumnString>(col2_full.get());
                if (!variant_col)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Encoding variant argument must be a constant String ('bech32' or 'bech32m') for function {}",
                        getName());
                if (input_rows_count == 0)
                {
                    /// For zero rows, try to validate the const value if available.
                    const auto * orig_const = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
                    if (orig_const)
                    {
                        String variant = orig_const->getValue<String>();
                        if (variant != "bech32" && variant != "bech32m")
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Invalid encoding variant '{}' for function {}, expected 'bech32' or 'bech32m'",
                                variant,
                                getName());
                    }
                    have_encoding_variant = true;
                    explicit_encoding = bech32::Encoding::BECH32; /// default doesn't matter for zero rows
                }
                else
                {
                    /// Validate all rows have the same value (guards against non-const column expressions)
                    String variant(variant_col->getDataAt(0));
                    for (size_t row = 1; row < input_rows_count; ++row)
                    {
                        if (variant_col->getDataAt(row) != variant)
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Encoding variant must be constant for function {}, got different values in rows",
                                getName());
                    }
                    if (variant == "bech32")
                        explicit_encoding = bech32::Encoding::BECH32;
                    else if (variant == "bech32m")
                        explicit_encoding = bech32::Encoding::BECH32M;
                    else
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Invalid encoding variant '{}' for function {}, expected 'bech32' or 'bech32m'",
                            variant,
                            getName());
                    have_encoding_variant = true;
                }
            }
            else
            {
                have_witness_version = true;
                col2 = arguments[2].column;
            }
        }

        if (const ColumnString * col0_string = checkAndGetColumn<ColumnString>(col0.get()))
        {
            const ColumnString::Chars & col0_vec = col0_string->getChars();
            const ColumnString::Offsets * col0_offsets = &col0_string->getOffsets();

            return chooseCol1AndExecute(col0_vec, col0_offsets, col1, col2, input_rows_count, have_witness_version, 0, have_encoding_variant, explicit_encoding);
        }

        if (const ColumnFixedString * col0_fixed_string = checkAndGetColumn<ColumnFixedString>(col0.get()))
        {
            const ColumnString::Chars & col0_vec = col0_fixed_string->getChars();
            const ColumnString::Offsets * col0_offsets = nullptr; /// dummy

            return chooseCol1AndExecute(col0_vec, col0_offsets, col1, col2, input_rows_count, have_witness_version, col0_fixed_string->getN(), have_encoding_variant, explicit_encoding);
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
        const size_t col0_width = 0,
        const bool have_encoding_variant = false,
        const bech32::Encoding explicit_encoding = bech32::Encoding::INVALID) const
    {
        if (const ColumnString * col1_str_ptr = checkAndGetColumn<ColumnString>(col1.get()))
        {
            const ColumnString::Chars & col1_vec = col1_str_ptr->getChars();
            const ColumnString::Offsets * col1_offsets = &col1_str_ptr->getOffsets();

            return execute(col0_vec, col0_offsets, col1_vec, col1_offsets, col2, input_rows_count, have_witness_version, col0_width, 0, have_encoding_variant, explicit_encoding);
        }

        if (const ColumnFixedString * col1_fstr_ptr = checkAndGetColumn<ColumnFixedString>(col1.get()))
        {
            const ColumnString::Chars & col1_vec = col1_fstr_ptr->getChars();
            const ColumnString::Offsets * col1_offsets = nullptr; /// dummy

            return execute(
                col0_vec, col0_offsets, col1_vec, col1_offsets, col2, input_rows_count, have_witness_version, col0_width, col1_fstr_ptr->getN(), have_encoding_variant, explicit_encoding);
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
        const size_t data_width = 0,
        const bool have_encoding_variant = false,
        const bech32::Encoding explicit_encoding = bech32::Encoding::INVALID)
    {
        /// outputs
        auto out_col = ColumnString::create();
        ColumnString::Chars & out_vec = out_col->getChars();
        ColumnString::Offsets & out_offsets = out_col->getOffsets();

        out_offsets.resize(input_rows_count);
        out_vec.resize(max_address_len * input_rows_count);

        char * out_begin = reinterpret_cast<char *>(out_vec.data());
        char * out_pos = out_begin;

        size_t human_readable_part_prev_offset = 0;
        size_t data_prev_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t human_readable_part_new_offset = human_readable_part_width == 0 ? (*human_readable_part_offsets)[i] : human_readable_part_prev_offset + human_readable_part_width;
            size_t data_new_offset = data_width == 0 ? (*data_offsets)[i] : data_prev_offset + data_width;

            /// NUL chars are used to pad fixed width strings, so we remove them here since they are not valid inputs anyway
            while (human_readable_part_width > 0 && human_readable_part_vec[human_readable_part_new_offset - 1] == 0 && human_readable_part_new_offset > human_readable_part_prev_offset)
                --human_readable_part_new_offset;

            /// max encodable data to stay within 90-char limit on Bech32 output
            /// human_readable_part must be at least 1 character and no more than 83
            auto data_len = data_new_offset - data_prev_offset;
            auto human_readable_part_len = human_readable_part_new_offset - human_readable_part_prev_offset;
            if (data_len > max_data_len || human_readable_part_len > max_human_readable_part_len || human_readable_part_len < 1)
            {
                finalizeRow(out_offsets, out_pos, out_begin, i);

                updatePrevOffset(human_readable_part_prev_offset, human_readable_part_new_offset, human_readable_part_width);
                updatePrevOffset(data_prev_offset, data_new_offset, data_width);
                continue;
            }

            std::string human_readable_part(
                reinterpret_cast<const char *>(&human_readable_part_vec[human_readable_part_prev_offset]),
                reinterpret_cast<const char *>(&human_readable_part_vec[human_readable_part_new_offset]));

            bech32_data input(
                reinterpret_cast<const uint8_t *>(&data_vec[data_prev_offset]),
                reinterpret_cast<const uint8_t *>(&data_vec[data_new_offset]));

            bech32_data input_5bit;
            bech32::Encoding encoding;

            if (have_encoding_variant)
            {
                /// Raw encoding mode: no witness version byte prepended.
                /// This is used for non-SegWit use cases like Cosmos SDK addresses.
                encoding = explicit_encoding;
                convertbits<8, 5, true>(input_5bit, input);
            }
            else
            {
                uint8_t witness_version = default_witness_version;
                if (have_witness_version)
                {
                    /** Witness version is a versioning mechanism for Bitcoin SegWit addresses:
                      * - Version 0: Original SegWit (BIP-141, BIP-173), uses Bech32 encoding
                      * - Version 1: Taproot (BIP-341, BIP-350), uses Bech32m encoding
                      * - Versions 2-16: Reserved for future protocol upgrades
                      *
                      * The witness version must be in range [0, 16] per the SegWit specification.
                      * It also must fit in the bech32 charset which is 5 bits (0-31), otherwise
                      * indexing into the CHARSET array in bech32::encode will cause a buffer overflow.
                      */
                    auto user_witness_version = witness_version_col->getUInt(i);
                    if (user_witness_version > 16)
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Invalid witness version {} for function {}, expected value in range [0, 16]",
                            user_witness_version,
                            name);
                    }
                    witness_version = static_cast<uint8_t>(user_witness_version);
                }

                input_5bit.push_back(witness_version);
                encoding = witness_version > 0 ? bech32::Encoding::BECH32M : bech32::Encoding::BECH32;
                convertbits<8, 5, true>(input_5bit, input);
            }

            std::string address = bech32::encode(human_readable_part, input_5bit, encoding);

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

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    /// Bech32 and Bech32m are each bijective, but since our decode function accepts either of them,
    /// then decode(bech32(input)) == decode(bech32m(input))
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "At least 1 argument (address) is required for function {}",
                getName());

        if (arguments.size() > 2)
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "A maximum of 2 arguments (address[, 'raw']) are allowed for function {}",
                getName());

        WhichDataType dtype(arguments[0]);
        if (!dtype.isStringOrFixedString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (arguments.size() == 2 && !WhichDataType(arguments[1]).isString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 2 of function {}, expected String ('raw')",
                arguments[1]->getName(),
                getName());

        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            types[i] = std::make_shared<DataTypeString>();

        return std::make_shared<DataTypeTuple>(types);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        bool raw_mode = false;
        if (arguments.size() == 2)
        {
            /// Decode mode must be constant — it's a mode selector, not per-row data.
            ColumnPtr mode_full = arguments[1].column->convertToFullColumnIfConst();
            const auto * mode_col = checkAndGetColumn<ColumnString>(mode_full.get());
            if (!mode_col)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Second argument of function {} must be a constant String ('raw')",
                    getName());
            if (input_rows_count == 0)
            {
                /// For zero rows, try to validate the const value if available.
                const auto * orig_const = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
                if (orig_const)
                {
                    String mode = orig_const->getValue<String>();
                    if (mode != "raw")
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Invalid mode '{}' for function {}, expected 'raw'",
                            mode,
                            getName());
                }
                raw_mode = true; /// default doesn't matter for zero rows
            }
            else
            {
                String mode(mode_col->getDataAt(0));
                /// Validate all rows have the same value (guards against non-const column expressions)
                for (size_t row = 1; row < input_rows_count; ++row)
                {
                    if (mode_col->getDataAt(row) != mode)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Decode mode must be constant for function {}, got different values in rows",
                            getName());
                }
                if (mode == "raw")
                    raw_mode = true;
                else
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid mode '{}' for function {}, expected 'raw'",
                        mode,
                        getName());
            }
        }

        ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            const ColumnString::Chars & in_vec = col->getChars();
            const ColumnString::Offsets * in_offsets = &col->getOffsets();

            return execute(in_vec, in_offsets, input_rows_count, 0, raw_mode);
        }

        if (const ColumnFixedString * col_fix_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const ColumnString::Chars & in_vec = col_fix_string->getChars();
            const ColumnString::Offsets * in_offsets = nullptr; /// dummy

            return execute(in_vec, in_offsets, input_rows_count, col_fix_string->getN(), raw_mode);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    static ColumnPtr
    execute(const ColumnString::Chars & in_vec, const ColumnString::Offsets * in_offsets, size_t input_rows_count, size_t col_width = 0, bool raw_mode = false)
    {
        auto col0_res = ColumnString::create();
        auto col1_res = ColumnString::create();

        ColumnString::Chars & human_readable_part_vec = col0_res->getChars();
        ColumnString::Offsets & human_readable_part_offsets = col0_res->getOffsets();

        ColumnString::Chars & data_vec = col1_res->getChars();
        ColumnString::Offsets & data_offsets = col1_res->getOffsets();

        human_readable_part_offsets.resize(input_rows_count);
        data_offsets.resize(input_rows_count);

        human_readable_part_vec.resize(max_human_readable_part_len * input_rows_count);
        data_vec.resize(max_data_len * input_rows_count);

        char * human_readable_part_begin = reinterpret_cast<char *>(human_readable_part_vec.data());
        char * human_readable_part_pos = human_readable_part_begin;

        char * data_begin = reinterpret_cast<char *>(data_vec.data());
        char * data_pos = data_begin;

        size_t prev_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t new_offset = col_width == 0 ? (*in_offsets)[i] : prev_offset + col_width;

            /// NUL chars are used to pad fixed width strings, so we remove them here since they are not valid inputs anyway
            while (col_width > 0 && in_vec[new_offset - 1] == 0 && new_offset > prev_offset)
                --new_offset;

            /// enforce char limit
            if (new_offset - prev_offset > max_address_len)
            {
                finalizeRow(human_readable_part_offsets, human_readable_part_pos, human_readable_part_begin, i);
                finalizeRow(data_offsets, data_pos, data_begin, i);

                updatePrevOffset(prev_offset, new_offset, col_width);
                continue;
            }

            std::string input(
                reinterpret_cast<const char *>(&in_vec[prev_offset]),
                reinterpret_cast<const char *>(&in_vec[new_offset]));

            const auto dec = bech32::decode(input);

            bech32_data data_8bit;
            if (dec.encoding == bech32::Encoding::INVALID || dec.data.empty())
            {
                finalizeRow(human_readable_part_offsets, human_readable_part_pos, human_readable_part_begin, i);
                finalizeRow(data_offsets, data_pos, data_begin, i);

                updatePrevOffset(prev_offset, new_offset, col_width);
                continue;
            }

            /// In raw mode, don't skip the first byte (no witness version).
            /// In default mode, skip the first byte which is the witness version.
            auto data_start = raw_mode ? dec.data.begin() : dec.data.begin() + 1;
            if (data_start >= dec.data.end()
                || !convertbits<5, 8, false>(data_8bit, bech32_data(data_start, dec.data.end()))
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
    FunctionDocumentation::Description bech32Encode_description = R"(
Encodes a binary data string, along with a human-readable part (HRP), using the [Bech32 or Bech32m](https://en.bitcoin.it/wiki/Bech32) algorithms.

:::note
When using the [`FixedString`](../data-types/fixedstring.md) data type, if a value does not fully fill the row it is padded with null characters.
While the `bech32Encode` function will handle this automatically for the hrp argument, for the data argument the values must not be padded.
For this reason it is not recommended to use the [`FixedString`](../data-types/fixedstring.md) data type for your data values unless you are
certain that they are all the same length and ensure that your `FixedString` column is set to that length as well.
:::
    )";
    FunctionDocumentation::Syntax bech32Encode_syntax = "bech32Encode(hrp, data[, witver | 'bech32' | 'bech32m'])";
    FunctionDocumentation::Arguments bech32Encode_arguments = {
        {"hrp", "A String of `1 - 83` lowercase characters specifying the \"human-readable part\" of the code. Usually 'bc' or 'tb'.", {"String", "FixedString"}},
        {"data", "A String of binary data to encode.", {"String", "FixedString"}},
        {"witver_or_variant", "Optional. Either a UInt* witness version (default = 1, `0` for Bech32, `1`+ for Bech32m) or a String encoding variant: `'bech32'` (BIP173) or `'bech32m'` (BIP350). When a string variant is used, no witness version byte is prepended — this is needed for non-SegWit addresses such as Cosmos SDK.", {"UInt*", "String"}}
    };
    FunctionDocumentation::ReturnedValue bech32Encode_returned_value = {"Returns a Bech32 address string, consisting of the human-readable part, a separator character which is always '1', and a data part. The length of the string will never exceed 90 characters. If the algorithm cannot generate a valid address from the input, it will return an empty string.", {"String"}};
    FunctionDocumentation::Examples bech32Encode_examples = {
        {
            "Default Bech32m",
            R"(
-- When no witness version is supplied, the default is 1, the updated Bech32m algorithm.
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'))
            )",
            "bc1w508d6qejxtdg4y5r3zarvary0c5xw7k8zcwmq"
        },
        {
            "Bech32 algorithm",
            R"(
-- A witness version of 0 will result in a different address string.
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0)
            )",
            "bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z"
        },
        {
            "Custom HRP",
            R"(
-- While 'bc' (Mainnet) and 'tb' (Testnet) are the only allowed hrp values for the
-- SegWit address format, Bech32 allows any hrp that satisfies the above requirements.
SELECT bech32Encode('abcdefg', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 10)
            )",
            "abcdefg1w508d6qejxtdg4y5r3zarvary0c5xw7k9rp8r4"
        },
        {
            "Cosmos SDK address (BIP173, no witness version)",
            R"(
-- Using 'bech32' variant encodes raw data without a witness version byte,
-- compatible with Cosmos SDK, Injective, Osmosis, and other non-SegWit chains.
SELECT bech32Encode('inj', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 'bech32')
            )",
            "inj1w508d6qejxtdg4y5r3zarvary0c5xw7kgj5aqs"
        }
    };
    FunctionDocumentation::IntroducedIn bech32Encode_introduced_in = {25, 6};
    FunctionDocumentation::Category bech32Encode_category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation bech32Encode_documentation = {bech32Encode_description, bech32Encode_syntax, bech32Encode_arguments, {}, bech32Encode_returned_value, bech32Encode_examples, bech32Encode_introduced_in, bech32Encode_category};

    FunctionDocumentation::Description bech32Decode_description = R"(
Decodes a Bech32 address string generated by either the bech32 or bech32m algorithms.

:::note
Unlike the encode function, `bech32Decode` will automatically handle padded FixedStrings.
:::
    )";
    FunctionDocumentation::Syntax bech32Decode_syntax = "bech32Decode(address[, 'raw'])";
    FunctionDocumentation::Arguments bech32Decode_arguments = {
        {"address", "A Bech32 string to decode.", {"String", "FixedString"}},
        {"mode", "Optional. Pass `'raw'` to decode without stripping the first byte as a witness version. Use this for non-SegWit addresses (e.g. Cosmos SDK).", {"String"}}
    };
    FunctionDocumentation::ReturnedValue bech32Decode_returned_value = {"Returns a tuple consisting of `(hrp, data)` that was used to encode the string. The data is in binary format.", {"Tuple(String, String)"}};
    FunctionDocumentation::Examples bech32Decode_examples = {
        {"Decode address", "SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z') AS tup)", "bc   751E76E8199196D454941C45D1B3A323F1433BD6"},
        {"Testnet address", "SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v') AS tup)", "tb   751E76E8199196D454941C45D1B3A323F1433BD6"}
    };
    FunctionDocumentation::IntroducedIn bech32Decode_introduced_in = {25, 6};
    FunctionDocumentation::Category bech32Decode_category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation bech32Decode_documentation = {bech32Decode_description, bech32Decode_syntax, bech32Decode_arguments, {}, bech32Decode_returned_value, bech32Decode_examples, bech32Decode_introduced_in, bech32Decode_category};

    factory.registerFunction<EncodeToBech32Representation>(bech32Encode_documentation);
    factory.registerFunction<DecodeFromBech32Representation>(bech32Decode_documentation);
}

}

#endif
