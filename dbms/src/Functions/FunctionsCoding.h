#pragma once

#include <Common/hex.h>
#include <Common/formatIPv6.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionHelpers.h>

#include <arpa/inet.h>

#include <ext/range.h>
#include <array>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int LOGICAL_ERROR;
}


/** TODO This file contains ridiculous amount of copy-paste.
  */

/** Encoding functions:
  *
  * IPv4NumToString (num) - See below.
  * IPv4StringToNum(string) - Convert, for example, '192.168.0.1' to 3232235521 and vice versa.
  *
  * hex(x) - Returns hex; capital letters; there are no prefixes 0x or suffixes h.
  *          For numbers, returns a variable-length string - hex in the "human" (big endian) format, with the leading zeros being cut,
  *          but only by whole bytes. For dates and datetimes - the same as for numbers.
  *          For example, hex(257) = '0101'.
  * unhex(string) - Returns a string, hex of which is equal to `string` with regard of case and discarding one leading zero.
  *                 If such a string does not exist, could return arbitary implementation specific value.
  *
  * bitmaskToArray(x) - Returns an array of powers of two in the binary form of x. For example, bitmaskToArray(50) = [2, 16, 32].
  */


constexpr size_t ipv4_bytes_length = 4;
constexpr size_t ipv6_bytes_length = 16;
constexpr size_t uuid_bytes_length = 16;
constexpr size_t uuid_text_length = 36;


class FunctionIPv6NumToString : public IFunction
{
public:
    static constexpr auto name = "IPv6NumToString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv6NumToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ptr || ptr->getN() != ipv6_bytes_length)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected FixedString(" + toString(ipv6_bytes_length) + ")",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        if (const auto col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != ipv6_bytes_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(ipv6_bytes_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (IPV6_MAX_TEXT_LENGTH + 1));
            offsets_res.resize(size);

            auto begin = reinterpret_cast<char *>(&vec_res[0]);
            auto pos = begin;

            for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += ipv6_bytes_length, ++i)
            {
                formatIPv6(&vec_in[offset], pos);
                offsets_res[i] = pos - begin;
            }

            vec_res.resize(pos - begin);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionCutIPv6 : public IFunction
{
public:
    static constexpr auto name = "cutIPv6";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCutIPv6>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ptr || ptr->getN() != ipv6_bytes_length)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument 1 of function " + getName() +
                            ", expected FixedString(" + toString(ipv6_bytes_length) + ")",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!checkDataType<DataTypeUInt8>(arguments[1].get()))
            throw Exception("Illegal type " + arguments[1]->getName() +
                            " of argument 2 of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!checkDataType<DataTypeUInt8>(arguments[2].get()))
            throw Exception("Illegal type " + arguments[2]->getName() +
                            " of argument 3 of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        const auto & col_ipv6_zeroed_tail_bytes_type = block.getByPosition(arguments[1]);
        const auto & col_ipv6_zeroed_tail_bytes = col_ipv6_zeroed_tail_bytes_type.column;
        const auto & col_ipv4_zeroed_tail_bytes_type = block.getByPosition(arguments[2]);
        const auto & col_ipv4_zeroed_tail_bytes = col_ipv4_zeroed_tail_bytes_type.column;

        if (const auto col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != ipv6_bytes_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(ipv6_bytes_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto ipv6_zeroed_tail_bytes = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv6_zeroed_tail_bytes.get());
            if (!ipv6_zeroed_tail_bytes)
                throw Exception("Illegal type " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
                                " of argument 2 of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            UInt8 ipv6_zeroed_tail_bytes_count = ipv6_zeroed_tail_bytes->getValue<UInt8>();
            if (ipv6_zeroed_tail_bytes_count > ipv6_bytes_length)
                throw Exception("Illegal value for argument 2 " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
                                " of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto ipv4_zeroed_tail_bytes = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv4_zeroed_tail_bytes.get());
            if (!ipv4_zeroed_tail_bytes)
                throw Exception("Illegal type " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
                                " of argument 3 of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            UInt8 ipv4_zeroed_tail_bytes_count = ipv4_zeroed_tail_bytes->getValue<UInt8>();
            if (ipv4_zeroed_tail_bytes_count > ipv6_bytes_length)
                throw Exception("Illegal value for argument 3 " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
                                " of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (IPV6_MAX_TEXT_LENGTH + 1));
            offsets_res.resize(size);

            auto begin = reinterpret_cast<char *>(&vec_res[0]);
            auto pos = begin;

            for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += ipv6_bytes_length, ++i)
            {
                const auto address = &vec_in[offset];
                UInt8 zeroed_tail_bytes_count = isIPv4Mapped(address) ? ipv4_zeroed_tail_bytes_count : ipv6_zeroed_tail_bytes_count;
                cutAddress(address, pos, zeroed_tail_bytes_count);
                offsets_res[i] = pos - begin;
            }

            vec_res.resize(pos - begin);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    bool isIPv4Mapped(const unsigned char * address) const
    {
        return (*reinterpret_cast<const UInt64 *>(address) == 0) &&
            ((*reinterpret_cast<const UInt64 *>(address + 8) & 0x00000000FFFFFFFFull) == 0x00000000FFFF0000ull);
    }

    void cutAddress(const unsigned char * address, char *& dst, UInt8 zeroed_tail_bytes_count)
    {
        formatIPv6(address, dst, zeroed_tail_bytes_count);
    }
};


class FunctionIPv6StringToNum : public IFunction
{
public:
    static constexpr auto name = "IPv6StringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv6StringToNum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(ipv6_bytes_length);
    }


    static bool ipv4_scan(const char * src, unsigned char * dst)
    {
        constexpr auto size = sizeof(UInt32);
        char bytes[size]{};

        for (const auto i : ext::range(0, size))
        {
            UInt32 value = 0;
            size_t len = 0;
            while (isNumericASCII(*src) && len <= 3)
            {
                value = value * 10 + (*src - '0');
                ++len;
                ++src;
            }

            if (len == 0 || value > 255 || (i < size - 1 && *src != '.'))
            {
                memset(dst, 0, size);
                return false;
            }
            bytes[i] = value;
            ++src;
        }

        if (src[-1] != '\0')
        {
            memset(dst, 0, size);
            return false;
        }

        memcpy(dst, bytes, sizeof(bytes));
        return true;
    }

    /// slightly altered implementation from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
    static void ipv6_scan(const char *  src, unsigned char * dst)
    {
        const auto clear_dst = [dst]
        {
            memset(dst, '\0', ipv6_bytes_length);
        };

        /// Leading :: requires some special handling.
        if (*src == ':')
            if (*++src != ':')
                return clear_dst();

        unsigned char tmp[ipv6_bytes_length]{};
        auto tp = tmp;
        auto endp = tp + ipv6_bytes_length;
        auto curtok = src;
        auto saw_xdigit = false;
        UInt32 val{};
        unsigned char * colonp = nullptr;

        /// Assuming zero-terminated string.
        while (const auto ch = *src++)
        {
            const auto num = unhex(ch);

            if (num != '\xff')
            {
                val <<= 4;
                val |= num;
                if (val > 0xffffu)
                    return clear_dst();

                saw_xdigit = 1;
                continue;
            }

            if (ch == ':')
            {
                curtok = src;
                if (!saw_xdigit)
                {
                    if (colonp)
                        return clear_dst();

                    colonp = tp;
                    continue;
                }

                if (tp + sizeof(UInt16) > endp)
                    return clear_dst();

                *tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
                *tp++ = static_cast<unsigned char>(val & 0xffu);
                saw_xdigit = false;
                val = 0;
                continue;
            }

            if (ch == '.' && (tp + ipv4_bytes_length) <= endp)
            {
                if (!ipv4_scan(curtok, tp))
                    return clear_dst();

                tp += ipv4_bytes_length;
                saw_xdigit = false;
                break;    /* '\0' was seen by ipv4_scan(). */
            }

            return clear_dst();
        }

        if (saw_xdigit)
        {
            if (tp + sizeof(UInt16) > endp)
                return clear_dst();

            *tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
            *tp++ = static_cast<unsigned char>(val & 0xffu);
        }

        if (colonp)
        {
            /*
             * Since some memmove()'s erroneously fail to handle
             * overlapping regions, we'll do the shift by hand.
             */
            const auto n = tp - colonp;

            for (int i = 1; i <= n; ++i)
            {
                endp[- i] = colonp[n - i];
                colonp[n - i] = 0;
            }
            tp = endp;
        }

        if (tp != endp)
            return clear_dst();

        memcpy(dst, tmp, sizeof(tmp));
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const auto col_in = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(ipv6_bytes_length);

            auto & vec_res = col_res->getChars();
            vec_res.resize(col_in->size() * ipv6_bytes_length);

            const ColumnString::Chars_t & vec_src = col_in->getChars();
            const ColumnString::Offsets & offsets_src = col_in->getOffsets();
            size_t src_offset = 0;

            for (size_t out_offset = 0, i = 0;
                 out_offset < vec_res.size();
                 out_offset += ipv6_bytes_length, ++i)
            {
                ipv6_scan(reinterpret_cast<const char * >(&vec_src[src_offset]), &vec_res[out_offset]);
                src_offset = offsets_src[i];
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** If mask_tail_octets > 0, the last specified number of octets will be filled with "xxx".
  */
template <size_t mask_tail_octets, typename Name>
class FunctionIPv4NumToString : public IFunction
{
private:
    static void formatIP(UInt32 ip, char *& out)
    {
        char * begin = out;

        for (size_t octet = 0; octet < mask_tail_octets; ++octet)
        {
            if (octet > 0)
            {
                *out = '.';
                ++out;
            }

            memcpy(out, "xxx", 3);  /// Strange choice, but meets the specification.
            out += 3;
        }

        /// Write everything backwards. NOTE The loop is unrolled.
        for (size_t octet = mask_tail_octets; octet < 4; ++octet)
        {
            if (octet > 0)
            {
                *out = '.';
                ++out;
            }

            /// Get the next byte.
            UInt32 value = (ip >> (octet * 8)) & static_cast<UInt32>(0xFF);

            /// Faster than sprintf. NOTE Actually not good enough. LUT will be better.
            if (value == 0)
            {
                *out = '0';
                ++out;
            }
            else
            {
                while (value > 0)
                {
                    *out = '0' + value % 10;
                    ++out;
                    value /= 10;
                }
            }
        }

        /// And reverse.
        std::reverse(begin, out);

        *out = '\0';
        ++out;
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv4NumToString<mask_tail_octets, Name>>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return mask_tail_octets == 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeUInt32>(&*arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected UInt32",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const ColumnUInt32 * col = typeid_cast<const ColumnUInt32 *>(column.get()))
        {
            const ColumnUInt32::Container & vec_in = col->getData();

            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();

            vec_res.resize(vec_in.size() * (IPV4_MAX_TEXT_LENGTH + 1)); /// the longest value is: 255.255.255.255\0
            offsets_res.resize(vec_in.size());
            char * begin = reinterpret_cast<char *>(&vec_res[0]);
            char * pos = begin;

            for (size_t i = 0; i < vec_in.size(); ++i)
            {
                formatIP(vec_in[i], pos);
                offsets_res[i] = pos - begin;
            }

            vec_res.resize(pos - begin);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionIPv4StringToNum : public IFunction
{
public:
    static constexpr auto name = "IPv4StringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv4StringToNum>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt32>();
    }

    static UInt32 parseIPv4(const char * pos)
    {
        UInt32 res = 0;
        for (int offset = 24; offset >= 0; offset -= 8)
        {
            UInt32 value = 0;
            size_t len = 0;
            while (isNumericASCII(*pos) && len <= 3)
            {
                value = value * 10 + (*pos - '0');
                ++len;
                ++pos;
            }
            if (len == 0 || value > 255 || (offset > 0 && *pos != '.'))
                return 0;
            res |= value << offset;
            ++pos;
        }
        if (*(pos - 1) != '\0')
            return 0;
        return res;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt32::create();

            ColumnUInt32::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());

            const ColumnString::Chars_t & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < vec_res.size(); ++i)
            {
                vec_res[i] = parseIPv4(reinterpret_cast<const char *>(&vec_src[prev_offset]));
                prev_offset = offsets_src[i];
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionIPv4ToIPv6 : public IFunction
{
public:
     static constexpr auto name = "IPv4ToIPv6";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv4ToIPv6>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeUInt32>(arguments[0].get()))
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(16);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        if (const auto col_in = typeid_cast<const ColumnUInt32 *>(column.get()))
        {
            auto col_res = ColumnFixedString::create(ipv6_bytes_length);

            auto & vec_res = col_res->getChars();
            vec_res.resize(col_in->size() * ipv6_bytes_length);

            const auto & vec_in = col_in->getData();

            for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += ipv6_bytes_length, ++i)
                mapIPv4ToIPv6(vec_in[i], &vec_res[out_offset]);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    void mapIPv4ToIPv6(UInt32 in, unsigned char * buf) const
    {
        *reinterpret_cast<UInt64 *>(buf) = 0;
        *reinterpret_cast<UInt64 *>(buf + 8) = 0x00000000FFFF0000ull | (static_cast<UInt64>(ntohl(in)) << 32);
    }
};


class FunctionMACNumToString : public IFunction
{
public:
    static constexpr auto name = "MACNumToString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMACNumToString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeUInt64>(&*arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected UInt64",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    static void formatMAC(UInt64 mac, unsigned char * out)
    {
        /// MAC address is represented in UInt64 in natural order (so, MAC addresses are compared in same order as UInt64).
        /// Higher two bytes in UInt64 are just ignored.

        writeHexByteUppercase(mac >> 40, &out[0]);
        out[2] = ':';
        writeHexByteUppercase(mac >> 32, &out[3]);
        out[5] = ':';
        writeHexByteUppercase(mac >> 24, &out[6]);
        out[8] = ':';
        writeHexByteUppercase(mac >> 16, &out[9]);
        out[11] = ':';
        writeHexByteUppercase(mac >> 8, &out[12]);
        out[14] = ':';
        writeHexByteUppercase(mac, &out[15]);
        out[17] = '\0';
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const ColumnUInt64 * col = typeid_cast<const ColumnUInt64 *>(column.get()))
        {
            const ColumnUInt64::Container & vec_in = col->getData();

            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();

            vec_res.resize(vec_in.size() * 18); /// the value is: xx:xx:xx:xx:xx:xx\0
            offsets_res.resize(vec_in.size());

            size_t current_offset = 0;
            for (size_t i = 0; i < vec_in.size(); ++i)
            {
                formatMAC(vec_in[i], &vec_res[current_offset]);
                current_offset += 18;
                offsets_res[i] = current_offset;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};


struct ParseMACImpl
{
    static constexpr size_t min_string_size = 17;
    static constexpr size_t max_string_size = 17;

    /** Example: 01:02:03:04:05:06.
      * There could be any separators instead of : and them are just ignored.
      * The order of resulting integers are correspond to the order of MAC address.
      * If there are any chars other than valid hex digits for bytes, the behaviour is implementation specific.
      */
    static UInt64 parse(const char * pos)
    {
        return (UInt64(unhex(pos[0])) << 44)
             | (UInt64(unhex(pos[1])) << 40)
             | (UInt64(unhex(pos[3])) << 36)
             | (UInt64(unhex(pos[4])) << 32)
             | (UInt64(unhex(pos[6])) << 28)
             | (UInt64(unhex(pos[7])) << 24)
             | (UInt64(unhex(pos[9])) << 20)
             | (UInt64(unhex(pos[10])) << 16)
             | (UInt64(unhex(pos[12])) << 12)
             | (UInt64(unhex(pos[13])) << 8)
             | (UInt64(unhex(pos[15])) << 4)
             | (UInt64(unhex(pos[16])));
    }

    static constexpr auto name = "MACStringToNum";
};

struct ParseOUIImpl
{
    static constexpr size_t min_string_size = 8;
    static constexpr size_t max_string_size = 17;

    /** OUI is the first three bytes of MAC address.
      * Example: 01:02:03.
      */
    static UInt64 parse(const char * pos)
    {
        return (UInt64(unhex(pos[0])) << 20)
             | (UInt64(unhex(pos[1])) << 16)
             | (UInt64(unhex(pos[3])) << 12)
             | (UInt64(unhex(pos[4])) << 8)
             | (UInt64(unhex(pos[6])) << 4)
             | (UInt64(unhex(pos[7])));
    }

    static constexpr auto name = "MACStringToOUI";
};


template <typename Impl>
class FunctionMACStringTo : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMACStringTo<Impl>>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();

            ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());

            const ColumnString::Chars_t & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < vec_res.size(); ++i)
            {
                size_t current_offset = offsets_src[i];
                size_t string_size = current_offset - prev_offset - 1; /// mind the terminating zero byte

                if (string_size >= Impl::min_string_size && string_size <= Impl::max_string_size)
                    vec_res[i] = Impl::parse(reinterpret_cast<const char *>(&vec_src[prev_offset]));
                else
                    vec_res[i] = 0;

                prev_offset = current_offset;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionUUIDNumToString : public IFunction
{

public:
    static constexpr auto name = "UUIDNumToString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUUIDNumToString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ptr || ptr->getN() != uuid_bytes_length)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected FixedString(" + toString(uuid_bytes_length) + ")",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnWithTypeAndName & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        if (const auto col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != uuid_bytes_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_bytes_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (uuid_text_length + 1));
            offsets_res.resize(size);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                formatUUID(&vec_in[src_offset], &vec_res[dst_offset]);
                src_offset += uuid_bytes_length;
                dst_offset += uuid_text_length;
                vec_res[dst_offset] = 0;
                ++dst_offset;
                offsets_res[i] = dst_offset;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionUUIDStringToNum : public IFunction
{
private:
    static void parseHex(const UInt8 * __restrict src, UInt8 * __restrict dst, const size_t num_bytes)
    {
        size_t src_pos = 0;
        size_t dst_pos = 0;
        for (; dst_pos < num_bytes; ++dst_pos)
        {
            dst[dst_pos] = unhex2(reinterpret_cast<const char *>(&src[src_pos]));
            src_pos += 2;
        }
    }

    static void parseUUID(const UInt8 * src36, UInt8 * dst16)
    {
        /// If string is not like UUID - implementation specific behaviour.

        parseHex(&src36[0], &dst16[0], 4);
        parseHex(&src36[9], &dst16[4], 2);
        parseHex(&src36[14], &dst16[6], 2);
        parseHex(&src36[19], &dst16[8], 2);
        parseHex(&src36[24], &dst16[10], 6);
    }

public:
    static constexpr auto name = "UUIDStringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUUIDStringToNum>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// String or FixedString(36)
        if (!arguments[0]->isString())
        {
            const auto ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
            if (!ptr || ptr->getN() != uuid_text_length)
                throw Exception("Illegal type " + arguments[0]->getName() +
                                " of argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_text_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeFixedString>(uuid_bytes_length);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnWithTypeAndName & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        if (const auto col_in = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & vec_in = col_in->getChars();
            const auto & offsets_in = col_in->getOffsets();
            const size_t size = offsets_in.size();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars_t & vec_res = col_res->getChars();
            vec_res.resize(size * uuid_bytes_length);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                /// If string has incorrect length - then return zero UUID.
                /// If string has correct length but contains something not like UUID - implementation specific behaviour.

                size_t string_size = offsets_in[i] - src_offset;
                if (string_size == uuid_text_length + 1)
                    parseUUID(&vec_in[src_offset], &vec_res[dst_offset]);
                else
                    memset(&vec_res[dst_offset], 0, uuid_bytes_length);

                dst_offset += uuid_bytes_length;
                src_offset += string_size;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const auto col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != uuid_text_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_text_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars_t & vec_res = col_res->getChars();
            vec_res.resize(size * uuid_bytes_length);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                parseUUID(&vec_in[src_offset], &vec_res[dst_offset]);
                src_offset += uuid_text_length;
                dst_offset += uuid_bytes_length;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionGenerateUUIDv4 : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv4";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGenerateUUIDv4>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUUID>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        auto col_res = ColumnVector<UInt128>::create();
        typename ColumnVector<UInt128>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);
        Rand64Impl::execute(reinterpret_cast<UInt64 *>(&vec_to[0]), vec_to.size() * 2);

        for (UInt128 & uuid: vec_to)
        {
            /** https://tools.ietf.org/html/rfc4122#section-4.4
             */
            uuid.low = (uuid.low & 0xffffffffffff0fffull) | 0x0000000000004000ull;
            uuid.high = (uuid.high & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        }

        block.getByPosition(result).column = std::move(col_res);
    }
};


class FunctionHex : public IFunction
{
public:
    static constexpr auto name = "hex";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionHex>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString()
            && !arguments[0]->isFixedString()
            && !arguments[0]->isDateOrDateTime()
            && !checkDataType<DataTypeUInt8>(&*arguments[0])
            && !checkDataType<DataTypeUInt16>(&*arguments[0])
            && !checkDataType<DataTypeUInt32>(&*arguments[0])
            && !checkDataType<DataTypeUInt64>(&*arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename T>
    void executeOneUInt(T x, char *& out)
    {
        bool was_nonzero = false;
        for (int offset = (sizeof(T) - 1) * 8; offset >= 0; offset -= 8)
        {
            UInt8 byte = x >> offset;

            /// Leading zeros.
            if (byte == 0 && !was_nonzero && offset)
                continue;

            was_nonzero = true;

            writeHexByteUppercase(byte, out);
            out += 2;
        }
        *out = '\0';
        ++out;
    }

    template <typename T>
    bool tryExecuteUInt(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnVector<T> * col_vec = checkAndGetColumn<ColumnVector<T>>(col);

        static constexpr size_t MAX_UINT_HEX_LENGTH = sizeof(T) * 2 + 1;    /// Including trailing zero byte.

        if (col_vec)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const typename ColumnVector<T>::Container & in_vec = col_vec->getData();

            size_t size = in_vec.size();
            out_offsets.resize(size);
            out_vec.resize(size * 3 + MAX_UINT_HEX_LENGTH); /// 3 is length of one byte in hex plus zero byte.

            size_t pos = 0;
            for (size_t i = 0; i < size; ++i)
            {
                /// Manual exponential growth, so as not to rely on the linear amortized work time of `resize` (no one guarantees it).
                if (pos + MAX_UINT_HEX_LENGTH > out_vec.size())
                    out_vec.resize(out_vec.size() * 2 + MAX_UINT_HEX_LENGTH);

                char * begin = reinterpret_cast<char *>(&out_vec[pos]);
                char * end = begin;
                executeOneUInt<T>(in_vec[i], end);

                pos += end - begin;
                out_offsets[i] = pos;
            }

            out_vec.resize(pos);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    void executeOneString(const UInt8 * pos, const UInt8 * end, char *& out)
    {
        while (pos < end)
        {
            writeHexByteUppercase(*pos, out);
            ++pos;
            out += 2;
        }
        *out = '\0';
        ++out;
    }

    bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars_t & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * 2 - size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = in_offsets[i];

                executeOneString(&in_vec[prev_offset], &in_vec[new_offset - 1], pos);

                out_offsets[i] = pos - begin;

                prev_offset = new_offset;
            }

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars_t & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * 2 + size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;

            size_t n = col_fstr_in->getN();

            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = prev_offset + n;

                executeOneString(&in_vec[prev_offset], &in_vec[new_offset], pos);

                out_offsets[i] = pos - begin;
                prev_offset = new_offset;
            }

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * column = block.getByPosition(arguments[0]).column.get();
        ColumnPtr & res_column = block.getByPosition(result).column;

        if (tryExecuteUInt<UInt8>(column, res_column) ||
            tryExecuteUInt<UInt16>(column, res_column) ||
            tryExecuteUInt<UInt32>(column, res_column) ||
            tryExecuteUInt<UInt64>(column, res_column) ||
            tryExecuteString(column, res_column) ||
            tryExecuteFixedString(column, res_column))
            return;

        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                        + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionUnhex : public IFunction
{
public:
    static constexpr auto name = "unhex";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnhex>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void unhexOne(const char * pos, const char * end, char *& out)
    {
        if ((end - pos) & 1)
        {
            *out = unhex(*pos);
            ++out;
            ++pos;
        }
        while (pos < end)
        {
            *out = unhex2(pos);
            pos += 2;
            ++out;
        }
        *out = '\0';
        ++out;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars_t & out_vec = col_res->getChars();
            ColumnString::Offsets & out_offsets = col_res->getOffsets();

            const ColumnString::Chars_t & in_vec = col->getChars();
            const ColumnString::Offsets & in_offsets = col->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size() / 2 + size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = in_offsets[i];

                unhexOne(reinterpret_cast<const char *>(&in_vec[prev_offset]), reinterpret_cast<const char *>(&in_vec[new_offset - 1]), pos);

                out_offsets[i] = pos - begin;

                prev_offset = new_offset;
            }

            out_vec.resize(pos - begin);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                            + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


class FunctionBitmaskToArray : public IFunction
{
public:
    static constexpr auto name = "bitmaskToArray";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmaskToArray>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isInteger())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    template <typename T>
    bool tryExecute(const IColumn * column, ColumnPtr & out_column)
    {
        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(column))
        {
            auto col_values = ColumnVector<T>::create();
            auto col_offsets = ColumnArray::ColumnOffsets::create();

            typename ColumnVector<T>::Container & res_values = col_values->getData();
            ColumnArray::Offsets & res_offsets = col_offsets->getData();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            res_offsets.resize(size);
            res_values.reserve(size * 2);

            for (size_t row = 0; row < size; ++row)
            {
                T x = vec_from[row];
                while (x)
                {
                    T y = (x & (x - 1));
                    T bit = x ^ y;
                    x = y;
                    res_values.push_back(bit);
                }
                res_offsets[row] = res_values.size();
            }

            out_column = ColumnArray::create(std::move(col_values), std::move(col_offsets));
            return true;
        }
        else
        {
            return false;
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * in_column = block.getByPosition(arguments[0]).column.get();
        ColumnPtr & out_column = block.getByPosition(result).column;

        if (tryExecute<UInt8>(in_column, out_column) ||
            tryExecute<UInt16>(in_column, out_column) ||
            tryExecute<UInt32>(in_column, out_column) ||
            tryExecute<UInt64>(in_column, out_column) ||
            tryExecute<Int8>(in_column, out_column) ||
            tryExecute<Int16>(in_column, out_column) ||
            tryExecute<Int32>(in_column, out_column) ||
            tryExecute<Int64>(in_column, out_column))
            return;

        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                        + " of first argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionToStringCutToZero : public IFunction
{
public:
    static constexpr auto name = "toStringCutToZero";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToStringCutToZero>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars_t & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size());

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;

            ColumnString::Offset current_in_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                const char * pos_in = reinterpret_cast<const char *>(&in_vec[current_in_offset]);
                size_t current_size = strlen(pos_in);
                memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
                pos += current_size;
                *pos = '\0';
                ++pos;
                out_offsets[i] = pos - begin;
                current_in_offset = in_offsets[i];
            }
            out_vec.resize(pos - begin);

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
    {
        const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars_t & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() + size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;
            const char * pos_in = reinterpret_cast<const char *>(&in_vec[0]);

            size_t n = col_fstr_in->getN();

            for (size_t i = 0; i < size; ++i)
            {
                size_t current_size = strnlen(pos_in, n);
                memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
                pos += current_size;
                *pos = '\0';
                out_offsets[i] = ++pos - begin;
                pos_in += n;
            }
            out_vec.resize(pos - begin);

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * column = block.getByPosition(arguments[0]).column.get();
        ColumnPtr & res_column = block.getByPosition(result).column;

        if (tryExecuteFixedString(column, res_column) || tryExecuteString(column, res_column))
            return;

        throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                        + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
