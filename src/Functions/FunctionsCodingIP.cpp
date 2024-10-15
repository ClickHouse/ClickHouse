#include <functional>
#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <Functions/FunctionsCodingIP.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Common/IPv6ToBinary.h>
#include <Common/formatIPv6.h>
#include <base/hex.h>
#include <Common/typeid_cast.h>

#include <arpa/inet.h>
#include <type_traits>
#include <array>


namespace DB
{
namespace Setting
{
    extern const SettingsBool cast_ipv4_ipv6_default_on_conversion_error;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


/** Encoding functions for network addresses:
  *
  * IPv6NumToString (num) - See below.
  * IPv6StringToNum(string) - Convert, for example, '::1' to 1 and vice versa.
  */
class FunctionIPv6NumToString : public IFunction
{
public:
    static constexpr auto name = "IPv6NumToString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIPv6NumToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg_string = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        const auto * arg_ipv6 = checkAndGetDataType<DataTypeIPv6>(arguments[0].get());
        if (!arg_ipv6 && !(arg_string && arg_string->getN() == IPV6_BINARY_LENGTH))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected IPv6 or FixedString({})",
                arguments[0]->getName(), getName(), IPV6_BINARY_LENGTH
            );

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
        const auto * col_ipv6 = checkAndGetColumn<ColumnIPv6>(column.get());
        const auto * col_string = checkAndGetColumn<ColumnFixedString>(column.get());
        if (!col_ipv6 && !(col_string && col_string->getN() == IPV6_BINARY_LENGTH))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal column {} of argument of function {}, expected IPv6 or FixedString({})",
                arguments[0].name, getName(), IPV6_BINARY_LENGTH
            );

        auto col_res = ColumnString::create();
        ColumnString::Chars & vec_res = col_res->getChars();
        ColumnString::Offsets & offsets_res = col_res->getOffsets();
        vec_res.resize(input_rows_count * (IPV6_MAX_TEXT_LENGTH + 1));
        offsets_res.resize(input_rows_count);

        auto * begin = reinterpret_cast<char *>(vec_res.data());
        auto * pos = begin;

        if (col_ipv6)
        {
            const auto & vec_in = col_ipv6->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                formatIPv6(reinterpret_cast<const unsigned char *>(&vec_in[i]), pos);
                offsets_res[i] = pos - begin;
            }
        }
        else
        {
            const auto & vec_in = col_string->getChars();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                formatIPv6(reinterpret_cast<const unsigned char *>(&vec_in[i * IPV6_BINARY_LENGTH]), pos);
                offsets_res[i] = pos - begin;
            }
        }

        vec_res.resize(pos - begin);
        return col_res;
    }
};


class FunctionCutIPv6 : public IFunction
{
public:
    static constexpr auto name = "cutIPv6";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCutIPv6>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeIPv6>(arguments[0].get()))
        {
            const auto * ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
            if (!ptr || ptr->getN() != IPV6_BINARY_LENGTH)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of argument 1 of function {}, expected FixedString({})",
                                arguments[0]->getName(), getName(), toString(IPV6_BINARY_LENGTH));
        }

        if (!WhichDataType(arguments[1]).isUInt8())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument 2 of function {}",
                            arguments[1]->getName(), getName());

        if (!WhichDataType(arguments[2]).isUInt8())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument 3 of function {}",
                            arguments[2]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        const auto & col_ipv6_zeroed_tail_bytes_type = arguments[1];
        const auto & col_ipv6_zeroed_tail_bytes = col_ipv6_zeroed_tail_bytes_type.column;
        const auto & col_ipv4_zeroed_tail_bytes_type = arguments[2];
        const auto & col_ipv4_zeroed_tail_bytes = col_ipv4_zeroed_tail_bytes_type.column;

        const auto * col_in_str = checkAndGetColumn<ColumnFixedString>(column.get());
        const auto * col_in_ip = checkAndGetColumn<ColumnIPv6>(column.get());

        if (!col_in_str && !col_in_ip)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                            arguments[0].column->getName(), getName());

        if (col_in_str && col_in_str->getN() != IPV6_BINARY_LENGTH)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of column {} argument of function {}, expected FixedString({})",
                            col_type_name.type->getName(), col_in_str->getName(),
                            getName(), toString(IPV6_BINARY_LENGTH));

        const auto * ipv6_zeroed_tail_bytes = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv6_zeroed_tail_bytes.get());
        if (!ipv6_zeroed_tail_bytes)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument 2 of function {}",
                            col_ipv6_zeroed_tail_bytes_type.type->getName(), getName());

        UInt8 ipv6_zeroed_tail_bytes_count = ipv6_zeroed_tail_bytes->getValue<UInt8>();
        if (ipv6_zeroed_tail_bytes_count > IPV6_BINARY_LENGTH)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal value for argument 2 {} of function {}",
                            col_ipv6_zeroed_tail_bytes_type.type->getName(), getName());

        const auto * ipv4_zeroed_tail_bytes = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv4_zeroed_tail_bytes.get());
        if (!ipv4_zeroed_tail_bytes)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument 3 of function {}",
                            col_ipv4_zeroed_tail_bytes_type.type->getName(), getName());

        UInt8 ipv4_zeroed_tail_bytes_count = ipv4_zeroed_tail_bytes->getValue<UInt8>();
        if (ipv4_zeroed_tail_bytes_count > IPV6_BINARY_LENGTH)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal value for argument 3 {} of function {}",
                            col_ipv4_zeroed_tail_bytes_type.type->getName(), getName());

        auto col_res = ColumnString::create();
        ColumnString::Chars & vec_res = col_res->getChars();
        ColumnString::Offsets & offsets_res = col_res->getOffsets();
        vec_res.resize(input_rows_count * (IPV6_MAX_TEXT_LENGTH + 1));
        offsets_res.resize(input_rows_count);

        auto * begin = reinterpret_cast<char *>(vec_res.data());
        auto * pos = begin;

        if (col_in_str)
        {
            const auto & vec_in = col_in_str->getChars();

            for (size_t offset = 0, i = 0; i < input_rows_count; offset += IPV6_BINARY_LENGTH, ++i)
            {
                const auto * address = &vec_in[offset];
                UInt8 zeroed_tail_bytes_count = isIPv4Mapped(address) ? ipv4_zeroed_tail_bytes_count : ipv6_zeroed_tail_bytes_count;
                cutAddress(reinterpret_cast<const unsigned char *>(address), pos, zeroed_tail_bytes_count);
                offsets_res[i] = pos - begin;
            }
        }
        else
        {
            const auto & vec_in = col_in_ip->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto * address = reinterpret_cast<const UInt8 *>(&vec_in[i]);
                UInt8 zeroed_tail_bytes_count = isIPv4Mapped(address) ? ipv4_zeroed_tail_bytes_count : ipv6_zeroed_tail_bytes_count;
                cutAddress(reinterpret_cast<const unsigned char *>(address), pos, zeroed_tail_bytes_count);
                offsets_res[i] = pos - begin;
            }
        }

        vec_res.resize(pos - begin);

        return col_res;
    }

private:
    static bool isIPv4Mapped(const UInt8 * address)
    {
        return (unalignedLoadLittleEndian<UInt64>(address) == 0) &&
               ((unalignedLoadLittleEndian<UInt64>(address + 8) & 0x00000000FFFFFFFFull) == 0x00000000FFFF0000ull);
    }

    static void cutAddress(const unsigned char * address, char *& dst, UInt8 zeroed_tail_bytes_count)
    {
        formatIPv6(address, dst, zeroed_tail_bytes_count);
    }
};

template <IPStringToNumExceptionMode exception_mode>
class FunctionIPv6StringToNum : public IFunction
{
public:
    static constexpr auto name = exception_mode == IPStringToNumExceptionMode::Throw
        ? "IPv6StringToNum"
        : (exception_mode == IPStringToNumExceptionMode::Default ? "IPv6StringToNumOrDefault" : "IPv6StringToNumOrNull");

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionIPv6StringToNum>(context); }

    explicit FunctionIPv6StringToNum(ContextPtr context)
        : cast_ipv4_ipv6_default_on_conversion_error(context->getSettingsRef()[Setting::cast_ipv4_ipv6_default_on_conversion_error])
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(removeNullable(arguments[0])))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());
        }

        auto result_type = std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH);

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        {
            return makeNullable(result_type);
        }

        return arguments[0]->isNullable() ? makeNullable(result_type) : result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        ColumnPtr column = arguments[0].column;
        ColumnPtr null_map_column;
        const NullMap * null_map = nullptr;
        if (column->isNullable())
        {
            const auto * column_nullable = assert_cast<const ColumnNullable *>(column.get());
            column = column_nullable->getNestedColumnPtr();
            null_map_column = column_nullable->getNullMapColumnPtr();
            null_map = &column_nullable->getNullMapData();
        }

        if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
        {
            if (cast_ipv4_ipv6_default_on_conversion_error)
            {
                auto result = convertToIPv6<IPStringToNumExceptionMode::Default, ColumnFixedString>(column, null_map);
                if (null_map && !result->isNullable())
                    return ColumnNullable::create(result, null_map_column);
                return result;
            }
        }

        auto result = convertToIPv6<exception_mode, ColumnFixedString>(column, null_map);
        if (null_map && !result->isNullable())
            return ColumnNullable::create(IColumn::mutate(result), IColumn::mutate(null_map_column));
        return result;
    }

private:
    bool cast_ipv4_ipv6_default_on_conversion_error = false;
};


/** If mask_tail_octets > 0, the last specified number of octets will be filled with "xxx".
  */
template <size_t mask_tail_octets, typename Name>
class FunctionIPv4NumToString : public IFunction
{
private:
    template <typename ArgType>
    ColumnPtr executeTyped(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        using ColumnType = ColumnVector<ArgType>;

        const ColumnPtr & column = arguments[0].column;

        if (const ColumnType * col = typeid_cast<const ColumnType *>(column.get()))
        {
            const typename ColumnType::Container & vec_in = col->getData();

            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();

            vec_res.resize(input_rows_count * (IPV4_MAX_TEXT_LENGTH + 1)); /// the longest value is: 255.255.255.255\0
            offsets_res.resize(input_rows_count);
            char * begin = reinterpret_cast<char *>(vec_res.data());
            char * pos = begin;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                DB::formatIPv4(reinterpret_cast<const unsigned char*>(&vec_in[i]), sizeof(ArgType), pos, mask_tail_octets, "xxx");
                offsets_res[i] = pos - begin;
            }

            vec_res.resize(pos - begin);

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIPv4NumToString<mask_tail_octets, Name>>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return mask_tail_octets == 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType arg_type(arguments[0]);
        if (!(arg_type.isIPv4() || arg_type.isUInt8() || arg_type.isUInt16() || arg_type.isUInt32()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected IPv4 or UInt8 or UInt16 or UInt32",
                arguments[0]->getName(), getName()
            );

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & ret_type, size_t input_rows_count) const override
    {

        switch (arguments[0].type->getTypeId())
        {
            case TypeIndex::IPv4: return executeTyped<IPv4>(arguments, ret_type, input_rows_count);
            case TypeIndex::UInt8: return executeTyped<UInt8>(arguments, ret_type, input_rows_count);
            case TypeIndex::UInt16: return executeTyped<UInt16>(arguments, ret_type, input_rows_count);
            case TypeIndex::UInt32: return executeTyped<UInt32>(arguments, ret_type, input_rows_count);
            default: break;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument of function {}, expected IPv4 or UInt8 or UInt16 or UInt32",
            arguments[0].column->getName(), getName()
        );
    }
};

template <IPStringToNumExceptionMode exception_mode>
class FunctionIPv4StringToNum : public IFunction
{
public:
    static constexpr auto name = exception_mode == IPStringToNumExceptionMode::Throw
        ? "IPv4StringToNum"
        : (exception_mode == IPStringToNumExceptionMode::Default ? "IPv4StringToNumOrDefault" : "IPv4StringToNumOrNull");

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionIPv4StringToNum>(context); }

    explicit FunctionIPv4StringToNum(ContextPtr context)
        : cast_ipv4_ipv6_default_on_conversion_error(context->getSettingsRef()[Setting::cast_ipv4_ipv6_default_on_conversion_error])
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(removeNullable(arguments[0])))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());
        }

        auto result_type = std::make_shared<DataTypeUInt32>();

        if constexpr (exception_mode == IPStringToNumExceptionMode::Null)
        {
            return makeNullable(result_type);
        }

        return arguments[0]->isNullable() ? makeNullable(result_type) : result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        ColumnPtr column = arguments[0].column;
        ColumnPtr null_map_column;
        const NullMap * null_map = nullptr;
        if (column->isNullable())
        {
            const auto * column_nullable = assert_cast<const ColumnNullable *>(column.get());
            column = column_nullable->getNestedColumnPtr();
            null_map_column = column_nullable->getNullMapColumnPtr();
            null_map = &column_nullable->getNullMapData();
        }

        if constexpr (exception_mode == IPStringToNumExceptionMode::Throw)
        {
            if (cast_ipv4_ipv6_default_on_conversion_error)
            {
                auto result = convertToIPv4<IPStringToNumExceptionMode::Default, ColumnUInt32>(column, null_map);
                if (null_map && !result->isNullable())
                    return ColumnNullable::create(result, null_map_column);
                return result;
            }
        }

        auto result = convertToIPv4<exception_mode, ColumnUInt32>(column, null_map);
        if (null_map && !result->isNullable())
            return ColumnNullable::create(IColumn::mutate(result), IColumn::mutate(null_map_column));
        return result;
    }

private:
    bool cast_ipv4_ipv6_default_on_conversion_error = false;
};


class FunctionIPv4ToIPv6 : public IFunction
{
public:
    static constexpr auto name = "IPv4ToIPv6";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIPv4ToIPv6>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// for backward compatibility IPv4ToIPv6 is overloaded, and result type depends on type of argument -
    ///   if it is UInt32 (presenting IPv4) then result is FixedString(16), if IPv4 - result is IPv6
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * dt_uint32 = checkAndGetDataType<DataTypeUInt32>(arguments[0].get());
        const auto * dt_ipv4 = checkAndGetDataType<DataTypeIPv4>(arguments[0].get());
        if (!dt_uint32 && !dt_ipv4)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}", arguments[0]->getName(), getName()
            );

        if (dt_uint32)
            return std::make_shared<DataTypeFixedString>(16);
        return std::make_shared<DataTypeIPv6>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        if (const auto * col_in = checkAndGetColumn<ColumnIPv4>(&*column))
        {
            auto col_res = ColumnIPv6::create();

            auto & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            const auto & vec_in = col_in->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
                mapIPv4ToIPv6(vec_in[i], reinterpret_cast<UInt8 *>(&vec_res[i].toUnderType()));

            return col_res;
        }

        if (const auto * col_in = checkAndGetColumn<ColumnUInt32>(&*column))
        {
            auto col_res = ColumnFixedString::create(IPV6_BINARY_LENGTH);

            auto & vec_res = col_res->getChars();
            vec_res.resize(input_rows_count * IPV6_BINARY_LENGTH);

            const auto & vec_in = col_in->getData();

            for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += IPV6_BINARY_LENGTH, ++i)
                mapIPv4ToIPv6(vec_in[i], &vec_res[out_offset]);

            return col_res;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName()
        );
    }

private:
    static void mapIPv4ToIPv6(UInt32 in, UInt8 * buf)
    {
        unalignedStore<UInt64>(buf, 0);

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            unalignedStoreLittleEndian<UInt64>(buf + 8, 0x00000000FFFF0000ull | (static_cast<UInt64>(ntohl(in)) << 32));
#else
            unalignedStoreLittleEndian<UInt64>(buf + 8, 0x00000000FFFF0000ull | (static_cast<UInt64>(std::byteswap(in)) << 32));
#endif
    }
};

class FunctionMACNumToString : public IFunction
{
public:
    static constexpr auto name = "MACNumToString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMACNumToString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isUInt64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected UInt64",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    static void formatMAC(UInt64 mac, UInt8 * out)
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & column = arguments[0].column;

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

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
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
        return (static_cast<UInt64>(unhex(pos[0])) << 44)
               | (static_cast<UInt64>(unhex(pos[1])) << 40)
               | (static_cast<UInt64>(unhex(pos[3])) << 36)
               | (static_cast<UInt64>(unhex(pos[4])) << 32)
               | (static_cast<UInt64>(unhex(pos[6])) << 28)
               | (static_cast<UInt64>(unhex(pos[7])) << 24)
               | (static_cast<UInt64>(unhex(pos[9])) << 20)
               | (static_cast<UInt64>(unhex(pos[10])) << 16)
               | (static_cast<UInt64>(unhex(pos[12])) << 12)
               | (static_cast<UInt64>(unhex(pos[13])) << 8)
               | (static_cast<UInt64>(unhex(pos[15])) << 4)
               | (static_cast<UInt64>(unhex(pos[16])));
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
        return (static_cast<UInt64>(unhex(pos[0])) << 20)
               | (static_cast<UInt64>(unhex(pos[1])) << 16)
               | (static_cast<UInt64>(unhex(pos[3])) << 12)
               | (static_cast<UInt64>(unhex(pos[4])) << 8)
               | (static_cast<UInt64>(unhex(pos[6])) << 4)
               | (static_cast<UInt64>(unhex(pos[7])));
    }

    static constexpr auto name = "MACStringToOUI";
};


template <typename Impl>
class FunctionMACStringTo : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMACStringTo<Impl>>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();

            ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            const ColumnString::Chars & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t current_offset = offsets_src[i];
                size_t string_size = current_offset - prev_offset - 1; /// mind the terminating zero byte

                if (string_size >= Impl::min_string_size && string_size <= Impl::max_string_size)
                    vec_res[i] = Impl::parse(reinterpret_cast<const char *>(&vec_src[prev_offset]));
                else
                    vec_res[i] = 0;

                prev_offset = current_offset;
            }

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

class FunctionIPv6CIDRToRange : public IFunction
{
private:

#if defined(__SSE2__)

#include <emmintrin.h>

    static void applyCIDRMask(const char * __restrict src, char * __restrict dst_lower, char * __restrict dst_upper, UInt8 bits_to_keep)
    {
        __m128i mask = _mm_loadu_si128(reinterpret_cast<const __m128i *>(getCIDRMaskIPv6(bits_to_keep).data()));
        __m128i lower = _mm_and_si128(_mm_loadu_si128(reinterpret_cast<const __m128i *>(src)), mask);
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst_lower), lower);

        __m128i inv_mask = _mm_xor_si128(mask, _mm_cmpeq_epi32(_mm_setzero_si128(), _mm_setzero_si128()));
        __m128i upper = _mm_or_si128(lower, inv_mask);
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst_upper), upper);
    }

#else

    /// NOTE IPv6 is stored in memory in big endian format that makes some difficulties.
    static void applyCIDRMask(const char * __restrict src, char * __restrict dst_lower, char * __restrict dst_upper, UInt8 bits_to_keep)
    {
        const auto & mask = getCIDRMaskIPv6(bits_to_keep);

        for (size_t i = 0; i < 16; ++i)
        {
            dst_lower[i] = src[i] & mask[i];
            dst_upper[i] = dst_lower[i] | ~mask[i];
        }
    }

#endif

public:
    static constexpr auto name = "IPv6CIDRToRange";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIPv6CIDRToRange>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * ipv6 = checkAndGetDataType<DataTypeIPv6>(arguments[0].get());
        const auto * str = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ipv6 && !(str && str->getN() == IPV6_BINARY_LENGTH))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected IPv6 or FixedString({})",
                arguments[0]->getName(), getName(), IPV6_BINARY_LENGTH
            );

        const DataTypePtr & second_argument = arguments[1];
        if (!isUInt8(second_argument))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected UInt8",
                second_argument->getName(), getName()
            );

        DataTypePtr element = std::make_shared<DataTypeIPv6>();
        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    bool useDefaultImplementationForConstants() const override { return true; }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_type_name_cidr = arguments[1];
        const ColumnPtr & column_cidr = col_type_name_cidr.column;

        const auto * col_const_cidr_in = checkAndGetColumnConst<ColumnUInt8>(column_cidr.get());
        const auto * col_cidr_in = checkAndGetColumn<ColumnUInt8>(column_cidr.get());

        if (!col_const_cidr_in && !col_cidr_in)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[1].column->getName(), getName()
            );

        const auto & col_type_name_ip = arguments[0];
        const ColumnPtr & column_ip = col_type_name_ip.column;

        const auto * col_const_ip_in = checkAndGetColumnConst<ColumnIPv6>(column_ip.get());
        const auto * col_ip_in = checkAndGetColumn<ColumnIPv6>(column_ip.get());

        const auto * col_const_str_in = checkAndGetColumnConst<ColumnFixedString>(column_ip.get());
        const auto * col_str_in = checkAndGetColumn<ColumnFixedString>(column_ip.get());

        std::function<const char *(size_t)> get_ip_data;
        if (col_const_ip_in)
            get_ip_data = [col_const_ip_in](size_t) { return col_const_ip_in->getDataAt(0).data; };
        else if (col_const_str_in)
            get_ip_data = [col_const_str_in](size_t) { return col_const_str_in->getDataAt(0).data; };
        else if (col_ip_in)
            get_ip_data = [col_ip_in](size_t i) { return reinterpret_cast<const char *>(&col_ip_in->getData()[i]); };
        else if (col_str_in)
            get_ip_data = [col_str_in](size_t i) { return reinterpret_cast<const char *>(&col_str_in->getChars().data()[i * IPV6_BINARY_LENGTH]); };
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName()
            );

        auto col_res_lower_range = ColumnIPv6::create();
        auto col_res_upper_range = ColumnIPv6::create();

        auto & vec_res_lower_range = col_res_lower_range->getData();
        vec_res_lower_range.resize(input_rows_count);

        auto & vec_res_upper_range = col_res_upper_range->getData();
        vec_res_upper_range.resize(input_rows_count);

        static constexpr UInt8 max_cidr_mask = IPV6_BINARY_LENGTH * 8;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt8 cidr = col_const_cidr_in
                        ? col_const_cidr_in->getValue<UInt8>()
                        : col_cidr_in->getData()[i];

            cidr = std::min(cidr, max_cidr_mask);

            applyCIDRMask(get_ip_data(i), reinterpret_cast<char *>(&vec_res_lower_range[i]), reinterpret_cast<char *>(&vec_res_upper_range[i]), cidr);
        }

        return ColumnTuple::create(Columns{std::move(col_res_lower_range), std::move(col_res_upper_range)});
    }
};


class FunctionIPv4CIDRToRange : public IFunction
{
private:
    static std::pair<UInt32, UInt32> applyCIDRMask(UInt32 src, UInt8 bits_to_keep)
    {
        if (bits_to_keep >= 8 * sizeof(UInt32))
            return { src, src };
        if (bits_to_keep == 0)
            return { static_cast<UInt32>(0), static_cast<UInt32>(-1) };

        UInt32 mask = static_cast<UInt32>(-1) << (8 * sizeof(UInt32) - bits_to_keep);
        UInt32 lower = src & mask;
        UInt32 upper = lower | ~mask;

        return { lower, upper };
    }

    template <typename ArgType>
    ColumnPtr executeTyped(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        using ColumnType = ColumnVector<ArgType>;
        const auto & col_type_name_ip = arguments[0];
        const ColumnPtr & column_ip = col_type_name_ip.column;

        const auto * col_const_ip_in = checkAndGetColumnConst<ColumnType>(column_ip.get());
        const auto * col_ip_in = checkAndGetColumn<ColumnType>(column_ip.get());

        const auto & col_type_name_cidr = arguments[1];
        const ColumnPtr & column_cidr = col_type_name_cidr.column;

        const auto * col_const_cidr_in = checkAndGetColumnConst<ColumnUInt8>(column_cidr.get());
        const auto * col_cidr_in = checkAndGetColumn<ColumnUInt8>(column_cidr.get());

        auto col_res_lower_range = ColumnIPv4::create();
        auto col_res_upper_range = ColumnIPv4::create();

        auto & vec_res_lower_range = col_res_lower_range->getData();
        vec_res_lower_range.resize(input_rows_count);

        auto & vec_res_upper_range = col_res_upper_range->getData();
        vec_res_upper_range.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ArgType ip = col_const_ip_in
                        ? col_const_ip_in->template getValue<ArgType>()
                        : col_ip_in->getData()[i];

            UInt8 cidr = col_const_cidr_in
                         ? col_const_cidr_in->getValue<UInt8>()
                         : col_cidr_in->getData()[i];

            std::tie(vec_res_lower_range[i], vec_res_upper_range[i]) = applyCIDRMask(ip, cidr);
        }

        return ColumnTuple::create(Columns{std::move(col_res_lower_range), std::move(col_res_upper_range)});
    }

public:
    static constexpr auto name = "IPv4CIDRToRange";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIPv4CIDRToRange>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType arg_type(arguments[0]);
        if (!(arg_type.isIPv4() || arg_type.isUInt8() || arg_type.isUInt16() || arg_type.isUInt32()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected IPv4 or UInt8 or UInt16 or UInt32",
                arguments[0]->getName(), getName()
            );


        const DataTypePtr & second_argument = arguments[1];
        if (!isUInt8(second_argument))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected UInt8",
                second_argument->getName(), getName()
            );

        DataTypePtr element = DataTypeFactory::instance().get("IPv4");
        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    bool useDefaultImplementationForConstants() const override { return true; }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & ret_type, size_t input_rows_count) const override
    {
        if (arguments[1].type->getTypeId() != TypeIndex::UInt8)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected UInt8", arguments[1].type->getName(), getName()
            );

        switch (arguments[0].type->getTypeId())
        {
            case TypeIndex::IPv4: return executeTyped<IPv4>(arguments, ret_type, input_rows_count);
            case TypeIndex::UInt8: return executeTyped<UInt8>(arguments, ret_type, input_rows_count);
            case TypeIndex::UInt16: return executeTyped<UInt16>(arguments, ret_type, input_rows_count);
            case TypeIndex::UInt32: return executeTyped<UInt32>(arguments, ret_type, input_rows_count);
            default: break;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument of function {}, expected IPv4 or UInt8 or UInt16 or UInt32",
            arguments[0].column->getName(), getName()
        );
    }
};

class FunctionIsIPv4String : public IFunction
{
public:
    static constexpr auto name = "isIPv4String";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsIPv4String>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * input_column = checkAndGetColumn<ColumnString>(arguments[0].column.get());

        if (!input_column)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
        }

        auto col_res = ColumnUInt8::create();

        ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnString::Chars & vec_src = input_column->getChars();
        const ColumnString::Offsets & offsets_src = input_column->getOffsets();
        size_t prev_offset = 0;
        UInt32 result = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            vec_res[i] = DB::parseIPv4whole(reinterpret_cast<const char *>(&vec_src[prev_offset]), reinterpret_cast<unsigned char *>(&result));
            prev_offset = offsets_src[i];
        }

        return col_res;
    }
};

class FunctionIsIPv6String : public IFunction
{
public:
    static constexpr auto name = "isIPv6String";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsIPv6String>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * input_column = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!input_column)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
        }

        auto col_res = ColumnUInt8::create();

        ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnString::Chars & vec_src = input_column->getChars();
        const ColumnString::Offsets & offsets_src = input_column->getOffsets();
        size_t prev_offset = 0;
        char buffer[IPV6_BINARY_LENGTH];

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            vec_res[i] = DB::parseIPv6whole(reinterpret_cast<const char *>(&vec_src[prev_offset]),
                                            reinterpret_cast<const char *>(&vec_src[offsets_src[i] - 1]),
                                            reinterpret_cast<unsigned char *>(buffer));
            prev_offset = offsets_src[i];
        }

        return col_res;
    }
};

struct NameFunctionIPv4NumToString { static constexpr auto name = "IPv4NumToString"; };
struct NameFunctionIPv4NumToStringClassC { static constexpr auto name = "IPv4NumToStringClassC"; };

REGISTER_FUNCTION(Coding)
{
    factory.registerFunction<FunctionCutIPv6>();
    factory.registerFunction<FunctionIPv4ToIPv6>();
    factory.registerFunction<FunctionMACNumToString>();
    factory.registerFunction<FunctionMACStringTo<ParseMACImpl>>();
    factory.registerFunction<FunctionMACStringTo<ParseOUIImpl>>();
    factory.registerFunction<FunctionIPv6CIDRToRange>();
    factory.registerFunction<FunctionIPv4CIDRToRange>();
    factory.registerFunction<FunctionIsIPv4String>();
    factory.registerFunction<FunctionIsIPv6String>();

    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>();
    factory.registerFunction<FunctionIPv4NumToString<1, NameFunctionIPv4NumToStringClassC>>();

    factory.registerFunction<FunctionIPv4StringToNum<IPStringToNumExceptionMode::Throw>>();
    factory.registerFunction<FunctionIPv4StringToNum<IPStringToNumExceptionMode::Default>>();
    factory.registerFunction<FunctionIPv4StringToNum<IPStringToNumExceptionMode::Null>>();

    factory.registerFunction<FunctionIPv6NumToString>();
    factory.registerFunction<FunctionIPv6StringToNum<IPStringToNumExceptionMode::Throw>>();
    factory.registerFunction<FunctionIPv6StringToNum<IPStringToNumExceptionMode::Default>>();
    factory.registerFunction<FunctionIPv6StringToNum<IPStringToNumExceptionMode::Null>>();

    /// MySQL compatibility aliases:
    factory.registerAlias("INET_ATON", FunctionIPv4StringToNum<IPStringToNumExceptionMode::Throw>::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("INET6_NTOA", FunctionIPv6NumToString::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("INET6_ATON", FunctionIPv6StringToNum<IPStringToNumExceptionMode::Throw>::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("INET_NTOA", NameFunctionIPv4NumToString::name, FunctionFactory::Case::Insensitive);
}

}
