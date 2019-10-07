#pragma once

#include <Common/hex.h>
#include <Common/formatIPv6.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <arpa/inet.h>
#include <ext/range.h>
#include <type_traits>
#include <array>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
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
  *                 If such a string does not exist, could return arbitrary implementation specific value.
  *
  * bitmaskToArray(x) - Returns an array of powers of two in the binary form of x. For example, bitmaskToArray(50) = [2, 16, 32].
  */


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
        if (!ptr || ptr->getN() != IPV6_BINARY_LENGTH)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected FixedString(" + toString(IPV6_BINARY_LENGTH) + ")",
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
            if (col_in->getN() != IPV6_BINARY_LENGTH)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(IPV6_BINARY_LENGTH) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (IPV6_MAX_TEXT_LENGTH + 1));
            offsets_res.resize(size);

            auto begin = reinterpret_cast<char *>(vec_res.data());
            auto pos = begin;

            for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += IPV6_BINARY_LENGTH, ++i)
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
        if (!ptr || ptr->getN() != IPV6_BINARY_LENGTH)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument 1 of function " + getName() +
                            ", expected FixedString(" + toString(IPV6_BINARY_LENGTH) + ")",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1]).isUInt8())
            throw Exception("Illegal type " + arguments[1]->getName() +
                            " of argument 2 of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[2]).isUInt8())
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
            if (col_in->getN() != IPV6_BINARY_LENGTH)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(IPV6_BINARY_LENGTH) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto ipv6_zeroed_tail_bytes = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv6_zeroed_tail_bytes.get());
            if (!ipv6_zeroed_tail_bytes)
                throw Exception("Illegal type " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
                                " of argument 2 of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            UInt8 ipv6_zeroed_tail_bytes_count = ipv6_zeroed_tail_bytes->getValue<UInt8>();
            if (ipv6_zeroed_tail_bytes_count > IPV6_BINARY_LENGTH)
                throw Exception("Illegal value for argument 2 " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
                                " of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto ipv4_zeroed_tail_bytes = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv4_zeroed_tail_bytes.get());
            if (!ipv4_zeroed_tail_bytes)
                throw Exception("Illegal type " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
                                " of argument 3 of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            UInt8 ipv4_zeroed_tail_bytes_count = ipv4_zeroed_tail_bytes->getValue<UInt8>();
            if (ipv4_zeroed_tail_bytes_count > IPV6_BINARY_LENGTH)
                throw Exception("Illegal value for argument 3 " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
                                " of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (IPV6_MAX_TEXT_LENGTH + 1));
            offsets_res.resize(size);

            auto begin = reinterpret_cast<char *>(vec_res.data());
            auto pos = begin;

            for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += IPV6_BINARY_LENGTH, ++i)
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
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const auto col_in = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(IPV6_BINARY_LENGTH);

            auto & vec_res = col_res->getChars();
            vec_res.resize(col_in->size() * IPV6_BINARY_LENGTH);

            const ColumnString::Chars & vec_src = col_in->getChars();
            const ColumnString::Offsets & offsets_src = col_in->getOffsets();
            size_t src_offset = 0;

            for (size_t out_offset = 0, i = 0;
                 out_offset < vec_res.size();
                 out_offset += IPV6_BINARY_LENGTH, ++i)
            {
                //TODO(nemkov): handle failure ?
                parseIPv6(reinterpret_cast<const char *>(&vec_src[src_offset]), &vec_res[out_offset]);
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
        if (!WhichDataType(arguments[0]).isUInt32())
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

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();

            vec_res.resize(vec_in.size() * (IPV4_MAX_TEXT_LENGTH + 1)); /// the longest value is: 255.255.255.255\0
            offsets_res.resize(vec_in.size());
            char * begin = reinterpret_cast<char *>(vec_res.data());
            char * pos = begin;

            for (size_t i = 0; i < vec_in.size(); ++i)
            {
                DB::formatIPv4(reinterpret_cast<const unsigned char*>(&vec_in[i]), pos, mask_tail_octets, "xxx");
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
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt32>();
    }

    static inline UInt32 parseIPv4(const char * pos)
    {
        UInt32 result = 0;
        DB::parseIPv4(pos, reinterpret_cast<unsigned char*>(&result));

        return result;
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

            const ColumnString::Chars & vec_src = col->getChars();
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
            auto col_res = ColumnFixedString::create(IPV6_BINARY_LENGTH);

            auto & vec_res = col_res->getChars();
            vec_res.resize(col_in->size() * IPV6_BINARY_LENGTH);

            const auto & vec_in = col_in->getData();

            for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += IPV6_BINARY_LENGTH, ++i)
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

class FunctionToIPv4 : public FunctionIPv4StringToNum
{
public:
    static constexpr auto name = "toIPv4";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToIPv4>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypeFactory::instance().get("IPv4");
    }
};

class FunctionToIPv6 : public FunctionIPv6StringToNum
{
public:
    static constexpr auto name = "toIPv6";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToIPv6>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypeFactory::instance().get("IPv6");
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
        if (!WhichDataType(arguments[0]).isUInt64())
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

            ColumnString::Chars & vec_res = col_res->getChars();
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
        if (!isString(arguments[0]))
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

            const ColumnString::Chars & vec_src = col->getChars();
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

            ColumnString::Chars & vec_res = col_res->getChars();
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
        if (!isString(arguments[0]))
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

            ColumnString::Chars & vec_res = col_res->getChars();
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
        else if (const auto col_in_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in_fixed->getN() != uuid_text_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in_fixed->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_text_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in_fixed->size();
            const auto & vec_in = col_in_fixed->getChars();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars & vec_res = col_res->getChars();
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
                + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
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
        WhichDataType which(arguments[0]);

        if (!which.isStringOrFixedString()
            && !which.isDateOrDateTime()
            && !which.isUInt())
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
            ColumnString::Chars & out_vec = col_str->getChars();
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
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * 2 - size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
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
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * 2 + size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
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
        if (!isString(arguments[0]))
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

            ColumnString::Chars & out_vec = col_res->getChars();
            ColumnString::Offsets & out_offsets = col_res->getOffsets();

            const ColumnString::Chars & in_vec = col->getChars();
            const ColumnString::Offsets & in_offsets = col->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size() / 2 + size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
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
        if (!isInteger(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    template <typename T>
    bool tryExecute(const IColumn * column, ColumnPtr & out_column)
    {
        using UnsignedT = std::make_unsigned_t<T>;

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
                UnsignedT x = vec_from[row];
                while (x)
                {
                    UnsignedT y = x & (x - 1);
                    UnsignedT bit = x ^ y;
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
        if (!isStringOrFixedString(arguments[0]))
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
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size());

            char * begin = reinterpret_cast<char *>(out_vec.data());
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
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() + size);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;
            const char * pos_in = reinterpret_cast<const char *>(in_vec.data());

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


class FunctionIPv6CIDRToRange : public IFunction
{
private:
    /// TODO Inefficient.
    /// NOTE IPv6 is stored in memory in big endian format that makes some difficulties.
    static void applyCIDRMask(const UInt8 * __restrict src, UInt8 * __restrict dst_lower, UInt8 * __restrict dst_upper, UInt8 bits_to_keep)
    {
        UInt8 mask[16]{};

        UInt8 bytes_to_keep = bits_to_keep / 8;
        UInt8 bits_to_keep_in_last_byte = bits_to_keep % 8;

        for (size_t i = 0; i < bits_to_keep / 8; ++i)
            mask[i] = 0xFFU;

        if (bits_to_keep_in_last_byte)
            mask[bytes_to_keep] = 0xFFU << (8 - bits_to_keep_in_last_byte);

        for (size_t i = 0; i < 16; ++i)
        {
            dst_lower[i] = src[i] & mask[i];
            dst_upper[i] = dst_lower[i] | ~mask[i];
        }
    }

public:
    static constexpr auto name = "IPv6CIDRToRange";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv6CIDRToRange>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto first_argument = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!first_argument || first_argument->getN() != IPV6_BINARY_LENGTH)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of first argument of function " + getName() +
                            ", expected FixedString(" + toString(IPV6_BINARY_LENGTH) + ")",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypePtr & second_argument = arguments[1];
        if (!isUInt8(second_argument))
            throw Exception{"Illegal type " + second_argument->getName()
                            + " of second argument of function " + getName()
                            + ", expected numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        DataTypePtr element = DataTypeFactory::instance().get("IPv6");
        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    bool useDefaultImplementationForConstants() const override { return true; }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto & col_type_name_ip = block.getByPosition(arguments[0]);
        const ColumnPtr & column_ip = col_type_name_ip.column;

        const auto col_ip_in = checkAndGetColumn<ColumnFixedString>(column_ip.get());

        if (!col_ip_in)
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        if (col_ip_in->getN() != IPV6_BINARY_LENGTH)
                throw Exception("Illegal type " + col_type_name_ip.type->getName() +
                                " of column " + col_ip_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(IPV6_BINARY_LENGTH) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto & col_type_name_cidr = block.getByPosition(arguments[1]);
        const ColumnPtr & column_cidr = col_type_name_cidr.column;

        const auto col_const_cidr_in = checkAndGetColumnConst<ColumnUInt8>(column_cidr.get());
        const auto col_cidr_in = checkAndGetColumn<ColumnUInt8>(column_cidr.get());

        if (!col_const_cidr_in && !col_cidr_in)
            throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        const auto & vec_in = col_ip_in->getChars();

        auto col_res_lower_range = ColumnFixedString::create(IPV6_BINARY_LENGTH);
        auto col_res_upper_range = ColumnFixedString::create(IPV6_BINARY_LENGTH);

        ColumnString::Chars & vec_res_lower_range = col_res_lower_range->getChars();
        vec_res_lower_range.resize(input_rows_count * IPV6_BINARY_LENGTH);

        ColumnString::Chars & vec_res_upper_range = col_res_upper_range->getChars();
        vec_res_upper_range.resize(input_rows_count * IPV6_BINARY_LENGTH);

        for (size_t offset = 0; offset < input_rows_count; ++offset)
        {
            const size_t offset_ipv6 = offset * IPV6_BINARY_LENGTH;
            UInt8 cidr = col_const_cidr_in
                ? col_const_cidr_in->getValue<UInt8>()
                : col_cidr_in->getData()[offset];

            applyCIDRMask(&vec_in[offset_ipv6], &vec_res_lower_range[offset_ipv6], &vec_res_upper_range[offset_ipv6], cidr);
        }

        block.getByPosition(result).column = ColumnTuple::create(Columns{std::move(col_res_lower_range), std::move(col_res_upper_range)});
    }
};


class FunctionIPv4CIDRToRange : public IFunction
{
private:
    static inline std::pair<UInt32, UInt32> applyCIDRMask(UInt32 src, UInt8 bits_to_keep)
    {
        if (bits_to_keep >= 8 * sizeof(UInt32))
            return { src, src };
        if (bits_to_keep == 0)
            return { UInt32(0), UInt32(-1) };

        UInt32 mask = UInt32(-1) << (8 * sizeof(UInt32) - bits_to_keep);
        UInt32 lower = src & mask;
        UInt32 upper = lower | ~mask;

        return { lower, upper };
    }

public:
    static constexpr auto name = "IPv4CIDRToRange";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv4CIDRToRange>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isUInt32())
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of first argument of function " + getName() +
                            ", expected UInt32",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


        const DataTypePtr & second_argument = arguments[1];
        if (!isUInt8(second_argument))
            throw Exception{"Illegal type " + second_argument->getName()
                            + " of second argument of function " + getName()
                            + ", expected numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        DataTypePtr element = DataTypeFactory::instance().get("IPv4");
        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    bool useDefaultImplementationForConstants() const override { return true; }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto & col_type_name_ip = block.getByPosition(arguments[0]);
        const ColumnPtr & column_ip = col_type_name_ip.column;

        const auto col_ip_in = checkAndGetColumn<ColumnUInt32>(column_ip.get());
        if (!col_ip_in)
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        const auto & col_type_name_cidr = block.getByPosition(arguments[1]);
        const ColumnPtr & column_cidr = col_type_name_cidr.column;

        const auto col_const_cidr_in = checkAndGetColumnConst<ColumnUInt8>(column_cidr.get());
        const auto col_cidr_in = checkAndGetColumn<ColumnUInt8>(column_cidr.get());

        if (!col_const_cidr_in && !col_cidr_in)
            throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        const auto & vec_in = col_ip_in->getData();

        auto col_res_lower_range = ColumnUInt32::create();
        auto col_res_upper_range = ColumnUInt32::create();

        auto & vec_res_lower_range = col_res_lower_range->getData();
        vec_res_lower_range.resize(input_rows_count);

        auto & vec_res_upper_range = col_res_upper_range->getData();
        vec_res_upper_range.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt8 cidr = col_const_cidr_in
                ? col_const_cidr_in->getValue<UInt8>()
                : col_cidr_in->getData()[i];

            std::tie(vec_res_lower_range[i], vec_res_upper_range[i]) = applyCIDRMask(vec_in[i], cidr);
        }

        block.getByPosition(result).column = ColumnTuple::create(Columns{std::move(col_res_lower_range), std::move(col_res_upper_range)});
    }
};


}
