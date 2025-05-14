#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>
#include <Common/BinStringDecodeHelper.h>
#include <Common/BitHelpers.h>

#include "bech32.h"
#include "segwit_addr.h"

namespace
{

typedef std::vector<uint8_t> bech32_data;

/** Convert from one power-of-2 number base to another. */
template <int frombits, int tobits, bool pad>
bool convertbits(bech32_data & out, const bech32_data & in)
{
    int acc = 0;
    int bits = 0;
    const int maxv = (1 << tobits) - 1;
    const int max_acc = (1 << (frombits + tobits - 1)) - 1;
    for (size_t i = 0; i < in.size(); ++i)
    {
        int value = in[i];
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
        if (bits)
            out.push_back((acc << (tobits - bits)) & maxv);
    }
    else if (bits >= frombits || ((acc << (tobits - bits)) & maxv))
    {
        return false;
    }
    return true;
}

}

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
}

/// Decode original address from string containing Bech32 or Bech32m address
class DecodeFromBech32Representation : public IFunction
{
public:
    static constexpr auto name = "bech32Decode";
    static constexpr size_t tuple_size = 2; // (hrp, data)

    /* Max length of Bech32 or Bech32m encoding is 90 chars, this includes:
     *      HRP: 1 - 83 human readable characters, 'bc' or 'tb' for a SegWit address
     *      separator: always '1'
     *      data: array of 5-bit bytes consisting of a 6 byte checksum, a witness byte, and the actual encoded data
     *
     * max_len = (90 - 1 (HRP) - 1 (sep) - 6 (checksum) - 1 (witness byte)) * 5 // 8
     * max_len = 405 bits or 50 (8-bit) bytes // we must fit in an 8-bit array, so we throw away the last 5 bits
     */
    static constexpr size_t max_data_len = 50;
    static constexpr size_t max_hrp_len = 83; // Note: if we just support segwit addresses, this can be changed to 2

    static FunctionPtr create(ContextPtr) { return std::make_shared<DecodeFromBech32Representation>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    // can return same val for different inputs, f.e. if bech32 and bech32m are applied to the same inputs
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType dType(arguments[0]);
        if (!dType.isStringOrFixedString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            types[i] = std::make_shared<DataTypeString>();

        return std::make_shared<DataTypeTuple>(types);
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
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
            auto col0_res = ColumnString::create();
            auto col1_res = ColumnString::create();

            ColumnString::Chars & hrp_vec = col0_res->getChars();
            ColumnString::Offsets & hrp_offsets = col0_res->getOffsets();

            ColumnString::Chars & data_vec = col1_res->getChars();
            ColumnString::Offsets & data_offsets = col1_res->getOffsets();

            const ColumnString::Chars & in_vec = col->getChars();
            const ColumnString::Offsets & in_offsets = col->getOffsets();

            hrp_offsets.resize(input_rows_count);
            data_offsets.resize(input_rows_count);

            // a ceiling, will resize again later
            hrp_vec.resize((max_hrp_len + 1 /* trailing 0 */) * input_rows_count);
            data_vec.resize((max_data_len + 1 /* trailing 0 */) * input_rows_count);

            char * hrp_begin = reinterpret_cast<char *>(hrp_vec.data());
            char * hrp_pos = hrp_begin;

            char * data_begin = reinterpret_cast<char *>(data_vec.data());
            char * data_pos = data_begin;

            size_t prev_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t new_offset = in_offsets[i];

                // enforce 90 char limit, 91 with trailing zero
                if ((new_offset - prev_offset - 1) > 90)
                {
                    // add empty strings and continue
                    *hrp_pos = '\0';
                    ++hrp_pos;
                    hrp_offsets[i] = hrp_pos - hrp_begin;

                    *data_pos = '\0';
                    ++data_pos;
                    data_offsets[i] = data_pos - data_begin;

                    prev_offset = new_offset;
                    continue;
                }

                // `new_offset - 1` because in ColumnString each string is stored with trailing zero byte
                std::string input(
                    reinterpret_cast<const char *>(&in_vec[prev_offset]), reinterpret_cast<const char *>(&in_vec[new_offset - 1]));

                const auto dec = bech32::decode(input);

                bech32_data data_8bit;
                if (dec.encoding == bech32::Encoding::INVALID
                    || !convertbits<5, 8, false>(data_8bit, bech32_data(dec.data.begin(), dec.data.end())))
                {
                    // add empty strings and continue
                    *hrp_pos = '\0';
                    ++hrp_pos;
                    hrp_offsets[i] = hrp_pos - hrp_begin;

                    *data_pos = '\0';
                    ++data_pos;
                    data_offsets[i] = data_pos - data_begin;

                    prev_offset = new_offset;
                    continue;
                }

                // store hrp output in hrp_pos
                std::memcpy(hrp_pos, dec.hrp.c_str(), dec.hrp.size());
                hrp_pos += dec.hrp.size();
                *hrp_pos = '\0';
                ++hrp_pos;

                hrp_offsets[i] = hrp_pos - hrp_begin;

                // store data output in data_pos
                std::memcpy(data_pos, data_8bit.data(), data_8bit.size());
                data_pos += data_8bit.size();
                *data_pos = '\0';
                ++data_pos;

                data_offsets[i] = data_pos - data_begin;

                prev_offset = new_offset;
            }

            chassert(
                static_cast<size_t>(hrp_pos - hrp_begin) <= hrp_vec.size(),
                fmt::format(
                    "too small amount of memory was preallocated: needed {}, but have only {}", hrp_pos - hrp_begin, hrp_vec.size()));
            chassert(
                static_cast<size_t>(data_pos - data_begin) <= data_vec.size(),
                fmt::format(
                    "too small amount of memory was preallocated: needed {}, but have only {}", data_pos - data_begin, data_vec.size()));

            hrp_vec.resize(hrp_pos - hrp_begin);
            data_vec.resize(data_pos - data_begin);

            Columns tuple_columns(tuple_size);
            tuple_columns[0] = std::move(col0_res);
            tuple_columns[1] = std::move(col1_res);

            return ColumnTuple::create(tuple_columns);
        }

        if (const ColumnFixedString * col_fix_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            // TODO these can be collapsed into a function
            auto col0_res = ColumnString::create();
            auto col1_res = ColumnString::create();

            ColumnString::Chars & hrp_vec = col0_res->getChars();
            ColumnString::Offsets & hrp_offsets = col0_res->getOffsets();

            ColumnString::Chars & data_vec = col1_res->getChars();
            ColumnString::Offsets & data_offsets = col1_res->getOffsets();

            const ColumnString::Chars & in_vec = col_fix_string->getChars();
            const size_t n = col_fix_string->getN();

            hrp_offsets.resize(input_rows_count);
            data_offsets.resize(input_rows_count);

            // a ceiling, will resize again later
            hrp_vec.resize((max_hrp_len + 1 /* trailing 0 */) * input_rows_count);
            data_vec.resize((max_data_len + 1 /* trailing 0 */) * input_rows_count);

            char * hrp_begin = reinterpret_cast<char *>(hrp_vec.data());
            char * hrp_pos = hrp_begin;

            char * data_begin = reinterpret_cast<char *>(data_vec.data());
            char * data_pos = data_begin;

            size_t prev_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t new_offset = prev_offset + n;

                // enforce 90 char limit
                if ((new_offset - prev_offset) > 90)
                {
                    // add empty strings and continue
                    *hrp_pos = '\0';
                    ++hrp_pos;
                    hrp_offsets[i] = hrp_pos - hrp_begin;

                    *data_pos = '\0';
                    ++data_pos;
                    data_offsets[i] = data_pos - data_begin;

                    prev_offset = new_offset;
                    continue;
                }

                // Note: no trailing 0 in fixed string cols
                std::string input(
                    reinterpret_cast<const char *>(&in_vec[prev_offset]), reinterpret_cast<const char *>(&in_vec[new_offset]));

                const auto dec = bech32::decode(input);

                if (dec.encoding == bech32::Encoding::INVALID)
                {
                    // add empty strings and continue
                    *hrp_pos = '\0';
                    ++hrp_pos;
                    hrp_offsets[i] = hrp_pos - hrp_begin;

                    *data_pos = '\0';
                    ++data_pos;
                    data_offsets[i] = data_pos - data_begin;

                    prev_offset = new_offset;
                    continue;
                }

                // store hrp output in hrp_pos
                std::memcpy(hrp_pos, dec.hrp.c_str(), dec.hrp.size());
                hrp_pos += dec.hrp.size();
                *hrp_pos = '\0';
                ++hrp_pos;

                hrp_offsets[i] = hrp_pos - hrp_begin;

                // store data output in data_pos
                std::memcpy(data_pos, dec.data.data(), dec.data.size());
                data_pos += dec.data.size();
                *data_pos = '\0';
                ++data_pos;

                data_offsets[i] = data_pos - data_begin;

                prev_offset = new_offset;
            }

            chassert(
                static_cast<size_t>(hrp_pos - hrp_begin) <= hrp_vec.size(),
                fmt::format(
                    "too small amount of memory was preallocated: needed {}, but have only {}", hrp_pos - hrp_begin, hrp_vec.size()));
            chassert(
                static_cast<size_t>(data_pos - data_begin) <= data_vec.size(),
                fmt::format(
                    "too small amount of memory was preallocated: needed {}, but have only {}", data_pos - data_begin, data_vec.size()));

            hrp_vec.resize(hrp_pos - hrp_begin);
            data_vec.resize(data_pos - data_begin);

            Columns tuple_columns(tuple_size);
            tuple_columns[0] = std::move(col0_res);
            tuple_columns[1] = std::move(col1_res);

            return ColumnTuple::create(tuple_columns);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

REGISTER_FUNCTION(Bech32Repr)
{
    //factory.registerFunction<EncodeToBech32Representation<HexImpl>>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<DecodeFromBech32Representation>({}, FunctionFactory::Case::Insensitive);
}

}
