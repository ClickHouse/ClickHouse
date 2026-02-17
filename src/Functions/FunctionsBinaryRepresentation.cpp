#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>
#include <Common/BinStringDecodeHelper.h>
#include <Common/BitHelpers.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

/*
 * hex(x) - Returns hexadecimal representation; capital letters; there are no prefixes 0x or suffixes h.
 *          For numbers, returns a variable-length string - hex in the "human" (big endian) format, with the leading zeros being cut,
 *          but only by whole bytes. For dates and datetimes - the same as for numbers.
 *          For example, hex(257) = '0101'.
 *
 * unhex(string) - Returns a string, hex of which is equal to `string` with regard of case and discarding one leading zero.
 *                 If such a string does not exist, could return arbitrary implementation specific value.
 *
 * bin(x) - Returns binary representation.
 *
 * unbin(x) - Returns a string, opposite to `bin`.
 *
 */

struct HexImpl
{
    static constexpr auto name = "hex";
    static constexpr size_t word_size = 2;

    template <typename T>
    static void executeOneUIntOrInt(T x, char *& out, bool skip_leading_zero = true)
    {
        bool was_nonzero = false;
        for (int offset = (sizeof(T) - 1) * 8; offset >= 0; offset -= 8)
        {
            UInt8 byte = static_cast<UInt8>(x >> offset);

            /// Skip leading zeros
            if (byte == 0 && !was_nonzero && offset && skip_leading_zero)
                continue;

            was_nonzero = true;
            writeHexByteUppercase(byte, out);
            out += word_size;
        }
    }

    static ALWAYS_INLINE inline void executeOneString(const UInt8 * pos, const UInt8 * end, char *& out, bool reverse_order = false)
    {
        if (!reverse_order)
        {
            const auto raw_size = end - pos;
            constexpr bool lower_case = false;
            hexString<lower_case>(reinterpret_cast<UInt8*>(out), pos, raw_size);
            out += 2 * raw_size;
        }
        else
        {
            const auto * start_pos = pos;
            pos = end - 1;
            while (pos >= start_pos)
            {
                writeHexByteUppercase(*pos, out);
                --pos;
                out += word_size;
            }
        }
    }

    template <typename T>
    static void executeFloatAndDecimal(const T & in_vec, ColumnPtr & col_res, const size_t type_size_in_bytes)
    {
        const size_t hex_length = type_size_in_bytes * word_size;
        auto col_str = ColumnString::create();

        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        size_t size = in_vec.size();
        out_offsets.resize(size);
        out_vec.resize(size * hex_length);

        size_t pos = 0;
        char * out = reinterpret_cast<char *>(out_vec.data());
        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * in_pos = reinterpret_cast<const UInt8 *>(&in_vec[i]);
            bool reverse_order = (std::endian::native == std::endian::big);
            executeOneString(in_pos, in_pos + type_size_in_bytes, out, reverse_order);

            pos += hex_length;
            out_offsets[i] = pos;
        }
        col_res = std::move(col_str);
    }
};

struct UnhexImpl
{
    static constexpr auto name = "unhex";
    static constexpr size_t word_size = 2;

    static void decode(const char * pos, const char * end, char *& out)
    {
        hexStringDecode2(pos, end, out);
    }
};

struct BinImpl
{
    static constexpr auto name = "bin";
    static constexpr size_t word_size = 8;

    template <typename T>
    static void executeOneUIntOrInt(T x, char *& out, bool skip_leading_zero = true)
    {
        bool was_nonzero = false;
        for (int offset = (sizeof(T) - 1) * 8; offset >= 0; offset -= 8)
        {
            UInt8 byte = static_cast<UInt8>(x >> offset);

            /// Skip leading zeros
            if (byte == 0 && !was_nonzero && offset && skip_leading_zero)
                continue;

            was_nonzero = true;
            writeBinByte(byte, out);
            out += word_size;
        }
    }

    template <typename T>
    static void executeFloatAndDecimal(const T & in_vec, ColumnPtr & col_res, const size_t type_size_in_bytes)
    {
        const size_t hex_length = type_size_in_bytes * word_size;
        auto col_str = ColumnString::create();

        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        size_t size = in_vec.size();
        out_offsets.resize(size);
        out_vec.resize(size * hex_length);

        size_t pos = 0;
        char * out = reinterpret_cast<char *>(out_vec.data());
        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * in_pos = reinterpret_cast<const UInt8 *>(&in_vec[i]);

            bool reverse_order = (std::endian::native == std::endian::big);
            executeOneString(in_pos, in_pos + type_size_in_bytes, out, reverse_order);

            pos += hex_length;
            out_offsets[i] = pos;
        }
        col_res = std::move(col_str);
    }

    static void executeOneString(const UInt8 * pos, const UInt8 * end, char *& out, bool reverse_order = false)
    {
        if (!reverse_order)
        {
            while (pos < end)
            {
                writeBinByte(*pos, out);
                ++pos;
                out += word_size;
            }
        }
        else
        {
            const auto * start_pos = pos;
            pos = end - 1;
            while (pos >= start_pos)
            {
                writeBinByte(*pos, out);
                --pos;
                out += word_size;
            }
        }
    }
};

struct UnbinImpl
{
    static constexpr auto name = "unbin";
    static constexpr size_t word_size = 8;

    static void decode(const char * pos, const char * end, char *& out) { binStringDecode(pos, end, out, word_size); }
};

/// Encode number or string to string with binary or hexadecimal representation
template <typename Impl>
class EncodeToBinaryRepresentation : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static constexpr size_t word_size = Impl::word_size;

    static FunctionPtr create(ContextPtr) { return std::make_shared<EncodeToBinaryRepresentation>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which(arguments[0]);

        if (!which.isStringOrFixedString() &&
            !which.isDate() &&
            !which.isDateTime() &&
            !which.isDateTime64() &&
            !which.isUInt() &&
            !which.isInt() &&
            !which.isFloat() &&
            !which.isDecimal() &&
            !which.isUUID() &&
            !which.isIPv4() &&
            !which.isIPv6() &&
            !which.isAggregateFunction())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * column = arguments[0].column.get();
        ColumnPtr res_column;

        WhichDataType which(column->getDataType());
        if (which.isAggregateFunction())
        {
            const ColumnPtr to_string = castColumn(arguments[0], std::make_shared<DataTypeString>());
            const auto * str_column = checkAndGetColumn<ColumnString>(to_string.get());
            tryExecuteString(str_column, res_column);
            return res_column;
        }

        if (tryExecuteUIntOrInt<UInt8>(column, res_column) ||
            tryExecuteUIntOrInt<UInt16>(column, res_column) ||
            tryExecuteUIntOrInt<UInt32>(column, res_column) ||
            tryExecuteUIntOrInt<UInt64>(column, res_column) ||
            tryExecuteUIntOrInt<UInt128>(column, res_column) ||
            tryExecuteUIntOrInt<UInt256>(column, res_column) ||
            tryExecuteUIntOrInt<Int8>(column, res_column) ||
            tryExecuteUIntOrInt<Int16>(column, res_column) ||
            tryExecuteUIntOrInt<Int32>(column, res_column) ||
            tryExecuteUIntOrInt<Int64>(column, res_column) ||
            tryExecuteUIntOrInt<Int128>(column, res_column) ||
            tryExecuteUIntOrInt<Int256>(column, res_column) ||
            tryExecuteString(column, res_column) ||
            tryExecuteFixedString(column, res_column) ||
            tryExecuteFloat<BFloat16>(column, res_column) ||
            tryExecuteFloat<Float32>(column, res_column) ||
            tryExecuteFloat<Float64>(column, res_column) ||
            tryExecuteDecimal<Decimal32>(column, res_column) ||
            tryExecuteDecimal<Decimal64>(column, res_column) ||
            tryExecuteDecimal<Decimal128>(column, res_column) ||
            tryExecuteDecimal<Decimal256>(column, res_column) ||
            tryExecuteUUID(column, res_column) ||
            tryExecuteIPv4(column, res_column) ||
            tryExecuteIPv6(column, res_column))
            return res_column;

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[0].column->getName(), getName());
    }

    template <typename T>
    bool tryExecuteUIntOrInt(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnVector<T> * col_vec = checkAndGetColumn<ColumnVector<T>>(col);

        static constexpr size_t MAX_LENGTH = sizeof(T) * word_size;

        if (col_vec)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const typename ColumnVector<T>::Container & in_vec = col_vec->getData();

            size_t size = in_vec.size();
            out_offsets.resize(size);
            out_vec.resize(size * word_size + MAX_LENGTH);

            size_t pos = 0;
            for (size_t i = 0; i < size; ++i)
            {
                /// Manual exponential growth, so as not to rely on the linear amortized work time of `resize` (no one guarantees it).
                if (pos + MAX_LENGTH > out_vec.size())
                    out_vec.resize(out_vec.size() * word_size + MAX_LENGTH);

                char * begin = reinterpret_cast<char *>(&out_vec[pos]);
                char * end = begin;
                Impl::executeOneUIntOrInt(in_vec[i], end);

                pos += end - begin;
                out_offsets[i] = pos;
            }
            out_vec.resize(pos);

            col_res = std::move(col_str);
            return true;
        }

        return false;
    }

    bool tryExecuteString(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();

            out_offsets.resize(size);
            /// reserve `word_size` bytes for each input byte
            out_vec.resize(in_vec.size() * word_size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = in_offsets[i];

                Impl::executeOneString(&in_vec[prev_offset], &in_vec[new_offset], pos);

                out_offsets[i] = pos - begin;

                prev_offset = new_offset;
            }
            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column size mismatch (internal logical error)");

            col_res = std::move(col_str);
            return true;
        }

        return false;
    }

    template <typename T>
    bool tryExecuteDecimal(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnDecimal<T> * col_dec = checkAndGetColumn<ColumnDecimal<T>>(col);
        if (col_dec)
        {
            const typename ColumnDecimal<T>::Container & in_vec = col_dec->getData();
            Impl::executeFloatAndDecimal(in_vec, col_res, sizeof(T));
            return true;
        }

        return false;
    }

    static bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * word_size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;

            size_t n = col_fstr_in->getN();

            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = prev_offset + n;

                Impl::executeOneString(&in_vec[prev_offset], &in_vec[new_offset], pos);

                out_offsets[i] = pos - begin;
                prev_offset = new_offset;
            }

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column size mismatch (internal logical error)");

            col_res = std::move(col_str);
            return true;
        }

        return false;
    }

    template <typename T>
    bool tryExecuteFloat(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnVector<T> * col_vec = checkAndGetColumn<ColumnVector<T>>(col);
        if (col_vec)
        {
            const typename ColumnVector<T>::Container & in_vec = col_vec->getData();
            Impl::executeFloatAndDecimal(in_vec, col_res, sizeof(T));
            return true;
        }

        return false;
    }

    bool tryExecuteUUID(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnUUID * col_vec = checkAndGetColumn<ColumnUUID>(col);

        static constexpr size_t MAX_LENGTH = sizeof(UUID) * word_size;

        if (col_vec)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const typename ColumnUUID::Container & in_vec = col_vec->getData();
            const UUID* uuid = in_vec.data();

            size_t size = in_vec.size();
            out_offsets.resize(size);
            out_vec.resize(size * word_size + MAX_LENGTH);

            size_t pos = 0;
            for (size_t i = 0; i < size; ++i)
            {
                /// Manual exponential growth, so as not to rely on the linear amortized work time of `resize` (no one guarantees it).
                if (pos + MAX_LENGTH > out_vec.size())
                    out_vec.resize(out_vec.size() * word_size + MAX_LENGTH);

                char * begin = reinterpret_cast<char *>(&out_vec[pos]);
                char * end = begin;

                // use executeOnUInt instead of using executeOneString
                // because the latter one outputs the string in the memory order
                Impl::executeOneUIntOrInt(UUIDHelpers::getHighBytes(uuid[i]), end, false);
                Impl::executeOneUIntOrInt(UUIDHelpers::getLowBytes(uuid[i]), end, false);

                pos += end - begin;
                out_offsets[i] = pos;
            }
            out_vec.resize(pos);

            col_res = std::move(col_str);
            return true;
        }

        return false;
    }

    bool tryExecuteIPv6(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnIPv6 * col_vec = checkAndGetColumn<ColumnIPv6>(col);

        static constexpr size_t MAX_LENGTH = sizeof(IPv6) * word_size;

        if (!col_vec)
            return false;

        auto col_str = ColumnString::create();
        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        const typename ColumnIPv6::Container & in_vec = col_vec->getData();
        const IPv6* ip = in_vec.data();

        size_t size = in_vec.size();
        out_offsets.resize(size);
        out_vec.resize(size * word_size + MAX_LENGTH);

        size_t pos = 0;
        for (size_t i = 0; i < size; ++i)
        {
            /// Manual exponential growth, so as not to rely on the linear amortized work time of `resize` (no one guarantees it).
            if (pos + MAX_LENGTH > out_vec.size())
                out_vec.resize(out_vec.size() * word_size + MAX_LENGTH);

            char * begin = reinterpret_cast<char *>(&out_vec[pos]);
            char * end = begin;

            Impl::executeOneString(reinterpret_cast<const UInt8 *>(&ip[i].toUnderType().items[0]), reinterpret_cast<const UInt8 *>(&ip[i].toUnderType().items[2]), end);

            pos += end - begin;
            out_offsets[i] = pos;
        }
        out_vec.resize(pos);

        col_res = std::move(col_str);
        return true;
    }

    bool tryExecuteIPv4(const IColumn * col, ColumnPtr & col_res) const
    {
        const ColumnIPv4 * col_vec = checkAndGetColumn<ColumnIPv4>(col);

        static constexpr size_t MAX_LENGTH = sizeof(IPv4) * word_size;

        if (!col_vec)
            return false;

        auto col_str = ColumnString::create();
        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        const typename ColumnIPv4::Container & in_vec = col_vec->getData();
        const IPv4* ip = in_vec.data();

        size_t size = in_vec.size();
        out_offsets.resize(size);
        out_vec.resize(size * word_size + MAX_LENGTH);

        size_t pos = 0;
        for (size_t i = 0; i < size; ++i)
        {
            /// Manual exponential growth, so as not to rely on the linear amortized work time of `resize` (no one guarantees it).
            if (pos + MAX_LENGTH > out_vec.size())
                out_vec.resize(out_vec.size() * word_size + MAX_LENGTH);

            char * begin = reinterpret_cast<char *>(&out_vec[pos]);
            char * end = begin;

            Impl::executeOneUIntOrInt(ip[i].toUnderType(), end);

            pos += end - begin;
            out_offsets[i] = pos;
        }
        out_vec.resize(pos);

        col_res = std::move(col_str);
        return true;
    }
};

/// Decode number or string from string with binary or hexadecimal representation
template <typename Impl>
class DecodeFromBinaryRepresentation : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static constexpr size_t word_size = Impl::word_size;
    static FunctionPtr create(ContextPtr) { return std::make_shared<DecodeFromBinaryRepresentation>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which(arguments[0]);
        if (!which.isStringOrFixedString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & out_vec = col_res->getChars();
            ColumnString::Offsets & out_offsets = col_res->getOffsets();

            const ColumnString::Chars & in_vec = col->getChars();
            const ColumnString::Offsets & in_offsets = col->getOffsets();

            out_offsets.resize(input_rows_count);

            size_t max_out_len = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t len = in_offsets[i] - in_offsets[i - 1];
                max_out_len += (len + word_size - 1) / word_size;
            }
            out_vec.resize(max_out_len);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t new_offset = in_offsets[i];

                Impl::decode(reinterpret_cast<const char *>(&in_vec[prev_offset]), reinterpret_cast<const char *>(&in_vec[new_offset]), pos);

                out_offsets[i] = pos - begin;
                prev_offset = new_offset;
            }

            chassert(
                static_cast<size_t>(pos - begin) <= out_vec.size(),
                fmt::format("too small amount of memory was preallocated: needed {}, but have only {}", pos - begin, out_vec.size()));
            out_vec.resize(pos - begin);

            return col_res;
        }
        if (const ColumnFixedString * col_fix_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & out_vec = col_res->getChars();
            ColumnString::Offsets & out_offsets = col_res->getOffsets();

            const ColumnString::Chars & in_vec = col_fix_string->getChars();
            const size_t n = col_fix_string->getN();

            out_offsets.resize(input_rows_count);
            out_vec.resize((n + word_size - 1) / word_size * input_rows_count);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t new_offset = prev_offset + n;

                Impl::decode(
                    reinterpret_cast<const char *>(&in_vec[prev_offset]), reinterpret_cast<const char *>(&in_vec[new_offset]), pos);

                out_offsets[i] = pos - begin;
                prev_offset = new_offset;
            }

            chassert(
                static_cast<size_t>(pos - begin) <= out_vec.size(),
                fmt::format("too small amount of memory was preallocated: needed {}, but have only {}", pos - begin, out_vec.size()));
            out_vec.resize(pos - begin);

            return col_res;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};


REGISTER_FUNCTION(BinaryRepr)
{
    FunctionDocumentation::Description hex_description = R"(
Returns a string containing the argument's hexadecimal representation according
to the following logic for different types:

| Type                       | Description                                                                                                                                                                                                                                                                            |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `(U)Int*`                  | Prints hex digits ("nibbles") from the most significant to least significant (big-endian or "human-readable" order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints both digits of every byte even if the leading digit is zero. |
| `Date` and `DateTime`      | Formatted as corresponding integers (the number of days since epoch for Date and the value of unix timestamp for DateTime).                                                                                                                                                            |
| `String` and `FixedString` | All bytes are simply encoded as two hexadecimal numbers. Zero bytes are not omitted.                                                                                                                                                                                                   |
| `Float*` and `Decimal`     | Encoded as their representation in memory. ClickHouse represents the values internally always as little endian, therefore they are encoded as such. Zero leading/trailing bytes are not omitted.                                                                                                                   |
| `UUID`                     | Encoded as big-endian order string.                                                                                                                                                                                                                                                    |

The function uses uppercase letters `A-F` and not using any prefixes (like `0x`) or suffixes (like `h`).
    )";
    FunctionDocumentation::Syntax hex_syntax = "hex(arg)";
    FunctionDocumentation::Arguments hex_arguments = {{"arg", "A value to convert to hexadecimal.", {"String", "(U)Int*", "Float*", "Decimal", "Date", "DateTime"}}};
    FunctionDocumentation::ReturnedValue hex_returned_value = {"Returns a string with the hexadecimal representation of the argument.", {"String"}};
    FunctionDocumentation::Examples hex_examples =
    {
        {
            "Simple integer",
            "SELECT hex(1)",
            "01"
        },
        {
            "Float32 numbers",
            "SELECT hex(toFloat32(number)) AS hex_presentation FROM numbers(15, 2)",
            R"(
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
            )"
        },
        {
            "Float64 numbers",
            "SELECT hex(toFloat64(number)) AS hex_presentation FROM numbers(15, 2)",
            R"(
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
            )"
        },
        {
            "UUID conversion",
            "SELECT lower(hex(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) AS uuid_hex",
            R"(
┌─uuid_hex─────────────────────────┐
│ 61f0c4045cb311e7907ba6006ad3dba0 │
└──────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn hex_introduced_in = {1, 1};
    FunctionDocumentation::Category hex_category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation hex_documentation = {hex_description, hex_syntax, hex_arguments, {}, hex_returned_value, hex_examples, hex_introduced_in, hex_category};

    FunctionDocumentation::Description unhex_description = R"(
Performs the opposite operation of [`hex`](#hex). It interprets each pair of hexadecimal digits (in the argument) as a number and converts
it to the byte represented by the number. The returned value is a binary string (BLOB).

If you want to convert the result to a number, you can use the `reverse` and `reinterpretAs<Type>` functions.

:::note
`clickhouse-client` interprets strings as UTF-8.
This may cause that values returned by `hex` to be displayed surprisingly.
:::

Supports both uppercase and lowercase letters `A-F`.
The number of hexadecimal digits does not have to be even.
If it is odd, the last digit is interpreted as the least significant half of the `00-0F` byte.
If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn't thrown).
For a numeric argument the inverse of hex(N) is not performed by unhex().
)";
    FunctionDocumentation::Syntax unhex_syntax = "unhex(arg)";
    FunctionDocumentation::Arguments unhex_arguments = {{"arg", "A string containing any number of hexadecimal digits.", {"String", "FixedString"}}};
    FunctionDocumentation::ReturnedValue unhex_returned_value = {"Returns a binary string (BLOB).", {"String"}};
    FunctionDocumentation::Examples unhex_examples =
    {
        {
            "Basic usage",
            "SELECT unhex('303132'), UNHEX('4D7953514C')",
            R"(
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
            )"
        },
        {
            "Convert to number",
            "SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num",
            R"(
┌──num─┐
│ 4095 │
└──────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn unhex_introduced_in = {1, 1};
    FunctionDocumentation::Category unhex_category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation unhex_documentation = {unhex_description, unhex_syntax, unhex_arguments, {}, unhex_returned_value, unhex_examples, unhex_introduced_in, unhex_category};

    FunctionDocumentation::Description bin_description = R"(
Returns a string containing the argument's binary representation according
to the following logic for different types:

| Type                       | Description                                                                                                                                                                                                                                                           |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `(U)Int*`                  | Prints bin digits from the most significant to least significant (big-endian or "human-readable" order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints eight digits of every byte if the leading digit is zero.|
| `Date` and `DateTime`      | Formatted as corresponding integers (the number of days since epoch for Date and the value of unix timestamp for DateTime).                                                                                                                                           |
| `String` and `FixedString` | All bytes are simply encoded as eight binary numbers. Zero bytes are not omitted.                                                                                                                                                                                     |
| `Float*` and `Decimal`     | Encoded as their representation in memory. As we support little-endian architecture, they are encoded in little-endian. Zero leading/trailing bytes are not omitted.                                                                                                  |
| `UUID`                     | Encoded as big-endian order string.                                                                                                                                                                                                                                   |
    )";
    FunctionDocumentation::Syntax bin_syntax = "bin(arg)";
    FunctionDocumentation::Arguments bin_arguments = {{"arg", "A value to convert to binary.", {"String", "FixedString", "(U)Int*", "Float*", "Decimal", "Date", "DateTime"}}};
    FunctionDocumentation::ReturnedValue bin_returned_value = {"Returns a string with the binary representation of the argument.", {"String"}};
    FunctionDocumentation::Examples bin_examples =
    {
        {
            "Simple integer",
            "SELECT bin(14)",
            R"(
┌─bin(14)──┐
│ 00001110 │
└──────────┘
            )"
        },
        {
            "Float32 numbers",
            "SELECT bin(toFloat32(number)) AS bin_presentation FROM numbers(15, 2)",
            R"(
┌─bin_presentation─────────────────┐
│ 00000000000000000111000001000001 │
│ 00000000000000001000000001000001 │
└──────────────────────────────────┘
            )"
        },
        {
            "Float64 numbers",
            "SELECT bin(toFloat64(number)) AS bin_presentation FROM numbers(15, 2)",
            R"(
┌─bin_presentation─────────────────────────────────────────────────┐
│ 0000000000000000000000000000000000000000000000000010111001000000 │
│ 0000000000000000000000000000000000000000000000000011000001000000 │
└──────────────────────────────────────────────────────────────────┘
            )"
        },
        {
            "UUID conversion",
            "SELECT bin(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) AS bin_uuid",
            R"(
┌─bin_uuid─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 01100001111100001100010000000100010111001011001100010001111001111001000001111011101001100000000001101010110100111101101110100000 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn bin_introduced_in = {21, 8};
    FunctionDocumentation::Category bin_category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation bin_documentation = {bin_description, bin_syntax, bin_arguments, {}, bin_returned_value, bin_examples, bin_introduced_in, bin_category};

    FunctionDocumentation::Description unbin_description = R"(
Interprets each pair of binary digits (in the argument) as a number and converts it to the byte represented by the number. The functions performs the opposite operation to bin.

For a numeric argument `unbin()` does not return the inverse of `bin()`. If you want to convert the result to a number, you can use the reverse and `reinterpretAs<Type>` functions.

:::note
If `unbin` is invoked from within the `clickhouse-client`, binary strings are displayed using UTF-8.
:::

Supports binary digits `0` and `1`. The number of binary digits does not have to be multiples of eight. If the argument string contains anything other than binary digits,
the result is undefined (no exception is thrown).
    )";
    FunctionDocumentation::Syntax unbin_syntax = "unbin(arg)";
    FunctionDocumentation::Arguments unbin_arguments = {{"arg", "A string containing any number of binary digits.", {"String"}}};
    FunctionDocumentation::ReturnedValue unbin_returned_value = {"Returns a binary string (BLOB).", {"String"}};
    FunctionDocumentation::Examples unbin_examples =
    {
        {
            "Basic usage",
            "SELECT UNBIN('001100000011000100110010'), UNBIN('0100110101111001010100110101000101001100')",
            R"(
┌─unbin('001100000011000100110010')─┬─unbin('0100110101111001010100110101000101001100')─┐
│ 012                               │ MySQL                                             │
└───────────────────────────────────┴───────────────────────────────────────────────────┘
            )"
        },
        {
            "Convert to number",
            "SELECT reinterpretAsUInt64(reverse(unbin('1110'))) AS num",
            R"(
┌─num─┐
│  14 │
└─────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn unbin_introduced_in = {21, 8};
    FunctionDocumentation::Category unbin_category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation unbin_documentation = {unbin_description, unbin_syntax, unbin_arguments, {}, unbin_returned_value, unbin_examples, unbin_introduced_in, unbin_category};

    factory.registerFunction<EncodeToBinaryRepresentation<HexImpl>>(hex_documentation, FunctionFactory::Case::Insensitive);
    factory.registerFunction<DecodeFromBinaryRepresentation<UnhexImpl>>(unhex_documentation, FunctionFactory::Case::Insensitive);
    factory.registerFunction<EncodeToBinaryRepresentation<BinImpl>>(bin_documentation, FunctionFactory::Case::Insensitive);
    factory.registerFunction<DecodeFromBinaryRepresentation<UnbinImpl>>(unbin_documentation, FunctionFactory::Case::Insensitive);
}

}
