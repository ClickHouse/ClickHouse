#include <functional>
#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <Functions/FunctionsCodingIP.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
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
#include <Common/typeid_cast.h>

#include <arpa/inet.h>
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
        vec_res.resize(input_rows_count * IPV6_MAX_TEXT_LENGTH);
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
        vec_res.resize(input_rows_count * IPV6_MAX_TEXT_LENGTH);
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

            vec_res.resize(input_rows_count * IPV4_MAX_TEXT_LENGTH); /// the longest value is: 255.255.255.255
            offsets_res.resize(input_rows_count);
            char * begin = reinterpret_cast<char *>(vec_res.data());
            char * pos = begin;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                formatIPv4(reinterpret_cast<const unsigned char*>(&vec_in[i]), sizeof(ArgType), pos, mask_tail_octets, "xxx");
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

        if constexpr (std::endian::native == std::endian::little)
            unalignedStoreLittleEndian<UInt64>(buf + 8, 0x00000000FFFF0000ull | (static_cast<UInt64>(ntohl(in)) << 32));
        else
            unalignedStoreLittleEndian<UInt64>(buf + 8, 0x00000000FFFF0000ull | (static_cast<UInt64>(std::byteswap(in)) << 32));
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

            vec_res.resize(vec_in.size() * 17); /// the value is: xx:xx:xx:xx:xx:xx
            offsets_res.resize(vec_in.size());

            size_t current_offset = 0;
            for (size_t i = 0; i < vec_in.size(); ++i)
            {
                formatMAC(vec_in[i], &vec_res[current_offset]);
                current_offset += 17;
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
                size_t string_size = current_offset - prev_offset;

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
        ColumnString::Offset result = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset new_offset = offsets_src[i];
            vec_res[i] = parseIPv4whole(
                reinterpret_cast<const char *>(&vec_src[prev_offset]),
                reinterpret_cast<const char *>(&vec_src[new_offset]),
                reinterpret_cast<unsigned char *>(&result));
            prev_offset = new_offset;
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
        ColumnString::Offset prev_offset = 0;
        char buffer[IPV6_BINARY_LENGTH];

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset new_offset = offsets_src[i];
            vec_res[i] = parseIPv6Whole(reinterpret_cast<const char *>(&vec_src[prev_offset]),
                                        reinterpret_cast<const char *>(&vec_src[new_offset]),
                                        reinterpret_cast<unsigned char *>(buffer));
            prev_offset = new_offset;
        }

        return col_res;
    }
};

struct NameFunctionIPv4NumToString { static constexpr auto name = "IPv4NumToString"; };
struct NameFunctionIPv4NumToStringClassC { static constexpr auto name = "IPv4NumToStringClassC"; };

REGISTER_FUNCTION(Coding)
{
    /// cutIPv6 function
    FunctionDocumentation::Description description_cutipv6 = R"(
Accepts a `FixedString(16)` value containing the IPv6 address in binary format.
Returns a string containing the address of the specified number of bytes removed in text format.
    )";
    FunctionDocumentation::Syntax syntax_cutipv6 = "cutIPv6(x, bytesToCutForIPv6, bytesToCutForIPv4)";
    FunctionDocumentation::Arguments arguments_cutipv6 = {
        {"x", "IPv6 address in binary format.", {"FixedString(16)", "IPv6"}},
        {"bytesToCutForIPv6", "Number of bytes to cut for IPv6.", {"UInt8"}},
        {"bytesToCutForIPv4", "Number of bytes to cut for IPv4.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cutipv6 = {"Returns a string containing the IPv6 address in text format with specified bytes removed.", {"String"}};
    FunctionDocumentation::Examples examples_cutipv6 = {
        {"Usage example", R"(
WITH
    IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D') AS ipv6,
    IPv4ToIPv6(IPv4StringToNum('192.168.0.1')) AS ipv4
SELECT
    cutIPv6(ipv6, 2, 0),
    cutIPv6(ipv4, 0, 2)
        )", R"(
┌─cutIPv6(ipv6, 2, 0)─────────────────┬─cutIPv6(ipv4, 0, 2)─┐
│ 2001:db8:ac10:fe01:feed:babe:cafe:0 │ ::ffff:192.168.0.0  │
└─────────────────────────────────────┴─────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_cutipv6 = {1, 1};
    FunctionDocumentation::Category category_cutipv6 = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_cutipv6 = {description_cutipv6, syntax_cutipv6, arguments_cutipv6, returned_value_cutipv6, examples_cutipv6, introduced_in_cutipv6, category_cutipv6};

    factory.registerFunction<FunctionCutIPv6>(documentation_cutipv6);

    /// IPv4ToIPv6 function
    FunctionDocumentation::Description description_ipv4toipv6 = R"(
Interprets a (big endian) 32-bit number as an IPv4 address, which is then interpreted as the corresponding IPv6 address in `FixedString(16)` format.
    )";
    FunctionDocumentation::Syntax syntax_ipv4toipv6 = "IPv4ToIPv6(x)";
    FunctionDocumentation::Arguments arguments_ipv4toipv6 = {
        {"x", "IPv4 address.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv4toipv6 = {"Returns an IPv6 address in binary format.", {"FixedString(16)"}};
    FunctionDocumentation::Examples examples_ipv4toipv6 = {
        {"Usage example", R"(
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr;
        )", R"(
┌─addr───────────────┐
│ ::ffff:192.168.0.1 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv4toipv6 = {1, 1};
    FunctionDocumentation::Category category_ipv4toipv6 = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4toipv6 = {description_ipv4toipv6, syntax_ipv4toipv6, arguments_ipv4toipv6, returned_value_ipv4toipv6, examples_ipv4toipv6, introduced_in_ipv4toipv6, category_ipv4toipv6};

    factory.registerFunction<FunctionIPv4ToIPv6>(documentation_ipv4toipv6);

    FunctionDocumentation::Description description_macnumtostring = R"(
Interprets a [`UInt64`](/sql-reference/data-types/int-uint) number as a MAC address in big endian format.
Returns the corresponding MAC address in format `AA:BB:CC:DD:EE:FF` (colon-separated numbers in hexadecimal form) as string.
    )";
    FunctionDocumentation::Syntax syntax_macnumtostring = "MACNumToString(num)";
    FunctionDocumentation::Arguments arguments_macnumtostring = {
        {"num", "UInt64 number.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_macnumtostring = {"Returns a MAC address in format AA:BB:CC:DD:EE:FF.", {"String"}};
    FunctionDocumentation::Examples examples_macnumtostring = {
    {
        "Usage example",
        R"(
SELECT MACNumToString(149809441867716) AS mac_address;
        )",
        R"(
┌─mac_address───────┐
│ 88:00:11:22:33:44 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_macnumtostring = {1, 1};
    FunctionDocumentation::Category category_macnumtostring = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_macnumtostring = {description_macnumtostring, syntax_macnumtostring, arguments_macnumtostring, returned_value_macnumtostring, examples_macnumtostring, introduced_in_macnumtostring, category_macnumtostring};

    factory.registerFunction<FunctionMACNumToString>(documentation_macnumtostring);

    FunctionDocumentation::Description description_macstringtonum = R"(
The inverse function of MACNumToString. If the MAC address has an invalid format, it returns 0.
)";
    FunctionDocumentation::Syntax syntax_macstringtonum = "MACStringToNum(s)";
    FunctionDocumentation::Arguments arguments_macstringtonum = {
        {"s", "MAC address string.", {"String"}}
    };
    FunctionDocumentation::Examples examples_macstringtonum = {
    {
        "Usage example",
        R"(
SELECT MACStringToNum('01:02:03:04:05:06') AS mac_numeric;
        )",
        R"(
1108152157446
        )"
    }
    };
    FunctionDocumentation::ReturnedValue returned_value_macstringtonum = {"Returns a UInt64 number.", {"UInt64"}};
    FunctionDocumentation::Category category_macstringtonum = FunctionDocumentation::Category::Other;
    FunctionDocumentation::IntroducedIn introduced_in_macstringtonum = {1, 1};
    FunctionDocumentation documentation_macstringtonum = {description_macstringtonum, syntax_macstringtonum, arguments_macstringtonum, returned_value_macstringtonum, examples_macstringtonum, introduced_in_macstringtonum, category_macstringtonum};

    factory.registerFunction<FunctionMACStringTo<ParseMACImpl>>(documentation_macstringtonum);

    FunctionDocumentation::Description description_macstringtooui = R"(
Given a MAC address in format AA:BB:CC:DD:EE:FF (colon-separated numbers in hexadecimal form), returns the first three octets as a UInt64 number. If the MAC address has an invalid format, it returns 0.
    )";
    FunctionDocumentation::Syntax syntax_macstringtooui = "MACStringToOUI(s)";
    FunctionDocumentation::Arguments arguments_macstringtooui = {
        {"s", "MAC address string.", {"String"}}
    };
    FunctionDocumentation::Examples examples_macstringtooui = {
    {
        "Usage example",
        R"(
SELECT MACStringToOUI('00:50:56:12:34:56') AS oui;
        )",
        R"(
20566
        )"
    }
    };
    FunctionDocumentation::ReturnedValue returned_value_macstringtooui = {"First three octets as UInt64 number.", {"UInt64"}};
    FunctionDocumentation::Category category_macstringtooui = FunctionDocumentation::Category::Other;
    FunctionDocumentation::IntroducedIn introduced_in_macstringtooui = {1, 1};
    FunctionDocumentation documentation_macstringtooui = {description_macstringtooui, syntax_macstringtooui, arguments_macstringtooui, returned_value_macstringtooui, examples_macstringtooui, introduced_in_macstringtooui, category_macstringtooui};

    factory.registerFunction<FunctionMACStringTo<ParseOUIImpl>>(documentation_macstringtooui);

    /// IPv6CIDRToRange function
    FunctionDocumentation::Description description_ipv6cidr = R"(
Takes an IPv6 address with its Classless Inter-Domain Routing (CIDR) prefix length and returns the subnet's address range as a tuple of two IPv6 values: the lowest and highest addresses in that subnet.
For the IPv4 version see [`IPv4CIDRToRange`](#IPv4CIDRToRange).
    )";
    FunctionDocumentation::Syntax syntax_ipv6cidr = "IPv6CIDRToRange(ipv6, cidr)";
    FunctionDocumentation::Arguments arguments_ipv6cidr = {
        {"ipv6", "IPv6 address.", {"IPv6", "String"}},
        {"cidr", "CIDR value.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv6cidr = {"Returns a tuple with two IPv6 addresses representing the subnet range.", {"Tuple(IPv6, IPv6)"}};
    FunctionDocumentation::Examples examples_ipv6cidr = {
        {"Usage example", R"(
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
        )", R"(
┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
│ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
└────────────────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_ipv6cidr = {20, 1};
    FunctionDocumentation::Category category_ipv6cidr = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv6cidr = {description_ipv6cidr, syntax_ipv6cidr, arguments_ipv6cidr, returned_value_ipv6cidr, examples_ipv6cidr, introduced_ipv6cidr, category_ipv6cidr};

    factory.registerFunction<FunctionIPv6CIDRToRange>(documentation_ipv6cidr);

    // IPv4CIDRToRange function
    FunctionDocumentation::Description description_ipv4cidr = R"(
Takes an IPv4 address with its Classless Inter-Domain Routing (CIDR) prefix length and returns the subnet's address range as a tuple of two IPv4 values: the first and last addresses in that subnet.
For the IPv6 version see [`IPv6CIDRToRange`](#IPv4CIDRToRange).
    )";
    FunctionDocumentation::Syntax syntax_ipv4cidr = "IPv4CIDRToRange(ipv4, cidr)";
    FunctionDocumentation::Arguments arguments_ipv4cidr = {
        {"ipv4", "IPv4 address.", {"IPv4", "String"}},
        {"cidr", "CIDR value.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv4cidr = {"Returns a tuple with two IPv4 addresses representing the subnet range.", {"Tuple(IPv4, IPv4)"}};
    FunctionDocumentation::Examples examples_ipv4cidr = {
        {"Usage example", R"(
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16);
        )", R"(
┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
│ ('192.168.0.0','192.168.255.255')          │
└────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_ipv4cidr = {20, 1};
    FunctionDocumentation::Category category_ipv4cidr = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4cidr = {description_ipv4cidr, syntax_ipv4cidr, arguments_ipv4cidr, returned_value_ipv4cidr, examples_ipv4cidr, introduced_ipv4cidr, category_ipv4cidr};

    factory.registerFunction<FunctionIPv4CIDRToRange>(documentation_ipv4cidr);

    /// isIPv4String function
    FunctionDocumentation::Description description_isipv4 = R"(
Determines whether the input string is an IPv4 address or not.
For the IPv6 version see [`isIPv6String`](#isIPv6String).
    )";
    FunctionDocumentation::Syntax syntax_isipv4 = "isIPv4String(string)";
    FunctionDocumentation::Arguments arguments_isipv4 = {
        {"string", "IP address string to check.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_isipv4 = {"Returns `1` if `string` is IPv4 address, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples_isipv4 = {
    {
        "Usage example",
        R"(
SELECT addr, isIPv4String(addr)
FROM(
SELECT ['0.0.0.0', '127.0.0.1', '::ffff:127.0.0.1'] AS addr
)
ARRAY JOIN addr;
        )",
        R"(
┌─addr─────────────┬─isIPv4String(addr)─┐
│ 0.0.0.0          │                  1 │
│ 127.0.0.1        │                  1 │
│ ::ffff:127.0.0.1 │                  0 │
└──────────────────┴────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_isipv4  = {21, 1};
    FunctionDocumentation::Category category_isipv4 = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_isipv4 = {description_isipv4, syntax_isipv4, arguments_isipv4, returned_value_isipv4, examples_isipv4, introduced_isipv4, category_isipv4};

    factory.registerFunction<FunctionIsIPv4String>(documentation_isipv4);

    /// isIPv6String function
    FunctionDocumentation::Description description_isipv6 = R"(
Determines whether the input string is an IPv6 address or not.
For the IPv4 version see [`isIPv4String`](#isIPv4String).
    )";
    FunctionDocumentation::Syntax syntax_isipv6 = "isIPv6String(string)";
    FunctionDocumentation::Arguments arguments_isipv6 = {
        {"string", "IP address string to check.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_isipv6 = {"Returns `1` if `string` is IPv6 address, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples_isipv6 = {
    {
        "Usage example",
        R"(
SELECT addr, isIPv6String(addr)
FROM(SELECT ['::', '1111::ffff', '::ffff:127.0.0.1', '127.0.0.1'] AS addr)
ARRAY JOIN addr;
        )",
        R"(
┌─addr─────────────┬─isIPv6String(addr)─┐
│ ::               │                  1 │
│ 1111::ffff       │                  1 │
│ ::ffff:127.0.0.1 │                  1 │
│ 127.0.0.1        │                  0 │
└──────────────────┴────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_isipv6  = {21, 1};
    FunctionDocumentation::Category category_isipv6 = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_isipv6 = {description_isipv6, syntax_isipv6, arguments_isipv6, returned_value_isipv6, examples_isipv6, introduced_isipv6, category_isipv6};

    factory.registerFunction<FunctionIsIPv6String>(documentation_isipv6);

    /// IPv4NumToString function
    FunctionDocumentation::Description description_ipv4numtostring = R"(
Converts a 32-bit integer to its IPv4 address string representation in dotted decimal notation (A.B.C.D format).
Interprets the input using big-endian byte ordering.
    )";
    FunctionDocumentation::Syntax syntax_ipv4numtostring = "IPv4NumToString(num)";
    FunctionDocumentation::Arguments arguments_ipv4numtostring = {
        {"num", "IPv4 address as UInt32 number.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv4numtostring = {"Returns a number representing the MAC address, or `0` if the format is invalid.", {"String"}};
    FunctionDocumentation::Examples example_ipv4numtostring = {
    {
        "Usage example",
        "IPv4NumToString(3232235521)",
        "192.168.0.1"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv4numtostring  = {1, 1};
    FunctionDocumentation::Category category_ipv4numtostring = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4numtostring = {description_ipv4numtostring, syntax_ipv4numtostring, arguments_ipv4numtostring, returned_value_ipv4numtostring, example_ipv4numtostring, introduced_in_ipv4numtostring, category_ipv4numtostring};

    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>(documentation_ipv4numtostring);

    /// IPv4NumToStringClassC function
    FunctionDocumentation::Description description_ipv4numtostringclassc = R"(
Converts a 32-bit integer to its IPv4 address string representation in dotted decimal notation (A.B.C.D format),
similar to [`IPv4NumToString`](#IPv4NumToString) but using `xxx` instead of the last octet.
    )";
    FunctionDocumentation::Syntax syntax_ipv4numtostringclassc = "IPv4NumToStringClassC(num)";
    FunctionDocumentation::Arguments arguments_ipv4numtostringclassc = {{"num", "IPv4 address as UInt32 number.", {"UInt32"}}};
    FunctionDocumentation::ReturnedValue returned_value_ipv4numtostringclassc = {"Returns the IPv4 address string with xxx replacing the last octet.", {"String"}};
    FunctionDocumentation::Examples examples_ipv4numtostringclassc = {{"Basic example with aggregation",
        R"(
SELECT
    IPv4NumToStringClassC(ClientIP) AS k,
    count() AS c
FROM test.hits
GROUP BY k
ORDER BY c DESC
LIMIT 10
        )",
        R"(
┌─k──────────────┬─────c─┐
│ 83.149.9.xxx   │ 26238 │
│ 217.118.81.xxx │ 26074 │
│ 213.87.129.xxx │ 25481 │
│ 83.149.8.xxx   │ 24984 │
│ 217.118.83.xxx │ 22797 │
│ 78.25.120.xxx  │ 22354 │
│ 213.87.131.xxx │ 21285 │
│ 78.25.121.xxx  │ 20887 │
│ 188.162.65.xxx │ 19694 │
│ 83.149.48.xxx  │ 17406 │
└────────────────┴───────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv4numtostringclassc  = {1, 1};
    FunctionDocumentation::Category category_ipv4numtostringclassc = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4numtostringclassc = {description_ipv4numtostringclassc, syntax_ipv4numtostringclassc, arguments_ipv4numtostringclassc, returned_value_ipv4numtostringclassc, examples_ipv4numtostringclassc, introduced_in_ipv4numtostringclassc, category_ipv4numtostringclassc};

    factory.registerFunction<FunctionIPv4NumToString<1, NameFunctionIPv4NumToStringClassC>>(documentation_ipv4numtostringclassc);

    /// IPv4StringToNum function
    FunctionDocumentation::Description description_ipv4stringtonum = R"(
Converts an IPv4 address string in dotted decimal notation (A.B.C.D format) to its corresponding 32-bit integer representation. (The reverse of [`IPv4NumToString`](#IPv4NumToString)).
If the IPv4 address has an invalid format, an exception is thrown.
    )";
    FunctionDocumentation::Syntax syntax_ipv4stringtonum = "IPv4StringToNum(string)";
    FunctionDocumentation::Arguments arguments_ipv4stringtonum = {
        {"string", "IPv4 address string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv4stringtonum = {"Returns theIPv4 address.", {"UInt32"}};
    FunctionDocumentation::Examples examples_ipv4stringtonum = {
    {
        "Usage example",
        "IPv4StringToNum('192.168.0.1')",
        "3232235521"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv4stringtonum = {1, 1};
    FunctionDocumentation::Category category_ipv4stringtonum = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4stringtonum = {description_ipv4stringtonum, syntax_ipv4stringtonum, arguments_ipv4stringtonum, returned_value_ipv4stringtonum, examples_ipv4stringtonum, introduced_in_ipv4stringtonum, category_ipv4stringtonum};

    factory.registerFunction<FunctionIPv4StringToNum<IPStringToNumExceptionMode::Throw>>(documentation_ipv4stringtonum);

    /// IPv4StringToNumOrDefault function
    FunctionDocumentation::Description description_ipv4stringtonumordefault = R"(
Converts an IPv4 address string in dotted decimal notation (A.B.C.D format) to its corresponding 32-bit integer representation but if the IPv4 address has an invalid format, it returns `0`.
    )";
    FunctionDocumentation::Syntax syntax_ipv4stringtonumordefault = "IPv4StringToNumOrDefault(string)";
    FunctionDocumentation::Arguments arguments_ipv4stringtonumordefault = {
        {"string", "IPv4 address string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv4stringtonumordefault = {"Returns the IPv4 address, or `0` if invalid.", {"UInt32"}};
    FunctionDocumentation::Examples examples_ipv4stringtonumordefault = {
        {"Example with an invalid address", R"(
SELECT
    IPv4StringToNumOrDefault('127.0.0.1') AS valid,
    IPv4StringToNumOrDefault('invalid') AS invalid;
        )", R"(
┌──────valid─┬─invalid─┐
│ 2130706433 │       0 │
└────────────┴─────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv4stringtonumordefault  = {22, 3};
    FunctionDocumentation::Category category_ipv4stringtonumordefault = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4stringtonumordefault = {description_ipv4stringtonumordefault, syntax_ipv4stringtonumordefault, arguments_ipv4stringtonumordefault, returned_value_ipv4stringtonumordefault, examples_ipv4stringtonumordefault, introduced_in_ipv4stringtonumordefault, category_ipv4stringtonumordefault};

    factory.registerFunction<FunctionIPv4StringToNum<IPStringToNumExceptionMode::Default>>(documentation_ipv4stringtonumordefault);

    /// IPv4StringToNumOrNull function
    FunctionDocumentation::Description description_ipv4stringtonumornull = R"(
Converts a 32-bit integer to its IPv4 address string representation in dotted decimal notation (A.B.C.D format) but if the IPv4 address has an invalid format, it returns `NULL`.
    )";
    FunctionDocumentation::Syntax syntax_ipv4stringtonumornull = "IPv4StringToNumOrNull(string)";
    FunctionDocumentation::Arguments arguments_ipv4stringtonumornull = {
        {"string", "IPv4 address string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv4stringtonumornull = {"Returns the IPv4 address, or `NULL` if invalid.", {"Nullable(UInt32)"}};
    FunctionDocumentation::Examples examples_ipv4stringtonumornull =
    {
    {
        "Example with an invalid address",
        R"(
SELECT
IPv4StringToNumOrNull('127.0.0.1') AS valid,
IPv4StringToNumOrNull('invalid') AS invalid;
        )",
        R"(
┌──────valid─┬─invalid─┐
│ 2130706433 │    ᴺᵁᴸᴸ │
└────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv4stringtonumornull = {22, 3};
    FunctionDocumentation::Category category_ipv4stringtonumornull = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv4stringtonumornull = {description_ipv4stringtonumornull, syntax_ipv4stringtonumornull, arguments_ipv4stringtonumornull, returned_value_ipv4stringtonumornull, examples_ipv4stringtonumornull, introduced_in_ipv4stringtonumornull, category_ipv4stringtonumornull};

    factory.registerFunction<FunctionIPv4StringToNum<IPStringToNumExceptionMode::Null>>(documentation_ipv4stringtonumornull);

    /// IPv6NumToString function
    FunctionDocumentation::Description description_ipv6numtostring = R"(
Converts an IPv6 address from binary format (FixedString(16)) to its standard text representation.
IPv4-mapped IPv6 addresses are displayed in the format `::ffff:111.222.33.44`.
    )";
    FunctionDocumentation::Syntax syntax_ipv6numtostring = "IPv6NumToString(x)";
    FunctionDocumentation::Arguments arguments_ipv6numtostring = {
        {"x", "IPv6 address in binary format.", {"FixedString(16)", "IPv6"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv6numtostring = {"Returns the IPv6 address string in text format.", {"String"}};
    FunctionDocumentation::Examples examples_ipv6numtostring = {
    {
        "Usage example",
        R"(
SELECT IPv6NumToString(toFixedString(unhex('2A0206B8000000000000000000000011'), 16)) AS addr;
        )",
        R"(
┌─addr─────────┐
│ 2a02:6b8::11 │
└──────────────┘
        )"
     },
     {
         "IPv6 with hits analysis",
         R"(
SELECT
    IPv6NumToString(ClientIP6 AS k),
    count() AS c
FROM hits_all
WHERE EventDate = today() AND substring(ClientIP6, 1, 12) != unhex('00000000000000000000FFFF')
GROUP BY k
ORDER BY c DESC
LIMIT 10
         )",
         R"(
┌─IPv6NumToString(ClientIP6)──────────────┬─────c─┐
│ 2a02:2168:aaa:bbbb::2                   │ 24695 │
│ 2a02:2698:abcd:abcd:abcd:abcd:8888:5555 │ 22408 │
│ 2a02:6b8:0:fff::ff                      │ 16389 │
│ 2a01:4f8:111:6666::2                    │ 16016 │
│ 2a02:2168:888:222::1                    │ 15896 │
│ 2a01:7e00::ffff:ffff:ffff:222           │ 14774 │
│ 2a02:8109:eee:ee:eeee:eeee:eeee:eeee    │ 14443 │
│ 2a02:810b:8888:888:8888:8888:8888:8888  │ 14345 │
│ 2a02:6b8:0:444:4444:4444:4444:4444      │ 14279 │
│ 2a01:7e00::ffff:ffff:ffff:ffff          │ 13880 │
└─────────────────────────────────────────┴───────┘
        )"
    },
    {
        "IPv6 mapped IPv4 addresses",
        R"(
SELECT
    IPv6NumToString(ClientIP6 AS k),
    count() AS c
FROM hits_all
WHERE EventDate = today()
GROUP BY k
ORDER BY c DESC
LIMIT 10
        )",
        R"(
┌─IPv6NumToString(ClientIP6)─┬──────c─┐
│ ::ffff:94.26.111.111       │ 747440 │
│ ::ffff:37.143.222.4        │ 529483 │
│ ::ffff:5.166.111.99        │ 317707 │
│ ::ffff:46.38.11.77         │ 263086 │
│ ::ffff:79.105.111.111      │ 186611 │
│ ::ffff:93.92.111.88        │ 176773 │
│ ::ffff:84.53.111.33        │ 158709 │
│ ::ffff:217.118.11.22       │ 154004 │
│ ::ffff:217.118.11.33       │ 148449 │
│ ::ffff:217.118.11.44       │ 148243 │
└────────────────────────────┴────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv6numtostring = {1, 1};
    FunctionDocumentation::Category category_ipv6numtostring = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv6numtostring = {description_ipv6numtostring, syntax_ipv6numtostring, arguments_ipv6numtostring, returned_value_ipv6numtostring, examples_ipv6numtostring, introduced_in_ipv6numtostring, category_ipv6numtostring};

    factory.registerFunction<FunctionIPv6NumToString>(documentation_ipv6numtostring);

    /// IPv6StringToNum function
    FunctionDocumentation::Description description_ipv6stringtonum = R"(
Converts an IPv6 address from its standard text representation to binary format (`FixedString(16)`).
Accepts IPv4-mapped IPv6 addresses in the format `::ffff:111.222.33.44.`.
If the IPv6 address has an invalid format, an exception is thrown.

If the input string contains a valid IPv4 address, returns its IPv6 equivalent.
HEX can be uppercase or lowercase.
    )";
    FunctionDocumentation::Syntax syntax_ipv6stringtonum = "IPv6StringToNum(string)";
    FunctionDocumentation::Arguments arguments_ipv6stringtonum = {
        {"string", "IPv6 address string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv6stringtonum = {"Returns theIPv6 address in binary format.", {"FixedString(16)"}};
    FunctionDocumentation::Examples examples_ipv6stringtonum = {
    {
        "Basic example",
        R"(
SELECT addr, cutIPv6(IPv6StringToNum(addr), 0, 0) FROM (SELECT ['notaddress', '127.0.0.1', '1111::ffff'] AS addr) ARRAY JOIN addr;
        )",
        R"(
┌─addr───────┬─cutIPv6(IPv6StringToNum(addr), 0, 0)─┐
│ notaddress │ ::                                   │
│ 127.0.0.1  │ ::ffff:127.0.0.1                     │
│ 1111::ffff │ 1111::ffff                           │
└────────────┴──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv6stringtonum = {1, 1};
    FunctionDocumentation::Category category_ipv6stringtonum = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv6stringtonum = {description_ipv6stringtonum, syntax_ipv6stringtonum, arguments_ipv6stringtonum, returned_value_ipv6stringtonum, examples_ipv6stringtonum, introduced_in_ipv6stringtonum, category_ipv6stringtonum};

    factory.registerFunction<FunctionIPv6StringToNum<IPStringToNumExceptionMode::Throw>>(documentation_ipv6stringtonum);

    /// IPv6StringToNumOrDefault function
    FunctionDocumentation::Description description_ipv6stringtonumordefault = R"(
Converts an IPv6 address from its standard text representation to binary format (`FixedString(16)`).
Accepts IPv4-mapped IPv6 addresses in the format `::ffff:111.222.33.44.`.
If the IPv6 address has an invalid format, it returns the default value `::`.
    )";
    FunctionDocumentation::Syntax syntax_ipv6stringtonumordefault = "IPv6StringToNumOrDefault(string)";
    FunctionDocumentation::Arguments arguments_ipv6stringtonumordefault = {
        {"string", "IPv6 address string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv6stringtonumordefault = {"IPv6 address in binary format, or zero-filled FixedString(16) if invalid.", {"FixedString(16)"}};
    FunctionDocumentation::Examples examples_ipv6stringtonumordefault = {
        {"Basic example with invalid address", R"(
SELECT
    IPv6NumToString(IPv6StringToNumOrDefault('2001:db8::1')) AS valid,
    IPv6NumToString(IPv6StringToNumOrDefault('invalid')) AS invalid;
        )", R"(
┌─valid───────┬─invalid─┐
│ 2001:db8::1 │ ::      │
└─────────────┴─────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv6stringtonumordefault = {22, 3};
    FunctionDocumentation::Category category_ipv6stringtonumordefault = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv6stringtonumordefault = {description_ipv6stringtonumordefault, syntax_ipv6stringtonumordefault, arguments_ipv6stringtonumordefault, returned_value_ipv6stringtonumordefault, examples_ipv6stringtonumordefault, introduced_in_ipv6stringtonumordefault, category_ipv6stringtonumordefault};

    factory.registerFunction<FunctionIPv6StringToNum<IPStringToNumExceptionMode::Default>>(documentation_ipv6stringtonumordefault);

    /// IPv6StringToNumOrNull function
    FunctionDocumentation::Description description_ipv6stringtonumornull = R"(
Converts an IPv6 address from its standard text representation to binary format (`FixedString(16)`).
Accepts IPv4-mapped IPv6 addresses in the format `::ffff:111.222.33.44.`.
If the IPv6 address has an invalid format, it returns `NULL`.
    )";
    FunctionDocumentation::Syntax syntax_ipv6stringtonumornull = "IPv6StringToNumOrNull(string)";
    FunctionDocumentation::Arguments arguments_ipv6stringtonumornull = {
        {"string", "IPv6 address string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_ipv6stringtonumornull = {"Returns IPv6 address in binary format, or `NULL` if invalid.", {"Nullable(FixedString(16))"}};
    FunctionDocumentation::Examples examples_ipv6stringtonumornull = {
    {
        "Basic example with invalid address",
        R"(
SELECT
    IPv6NumToString(IPv6StringToNumOrNull('2001:db8::1')) AS valid,
    IPv6StringToNumOrNull('invalid') AS invalid;
        )",
        R"(
┌─valid───────┬─invalid─┐
│ 2001:db8::1 │    ᴺᵁᴸᴸ │
└─────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_ipv6stringtonumornull = {22, 3};
    FunctionDocumentation::Category category_ipv6stringtonumornull = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_ipv6stringtonumornull = {description_ipv6stringtonumornull, syntax_ipv6stringtonumornull, arguments_ipv6stringtonumornull, returned_value_ipv6stringtonumornull, examples_ipv6stringtonumornull, introduced_in_ipv6stringtonumornull, category_ipv6stringtonumornull};

    factory.registerFunction<FunctionIPv6StringToNum<IPStringToNumExceptionMode::Null>>(documentation_ipv6stringtonumornull);

    /// MySQL compatibility aliases:
    factory.registerAlias("INET_ATON", FunctionIPv4StringToNum<IPStringToNumExceptionMode::Throw>::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("INET6_NTOA", FunctionIPv6NumToString::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("INET6_ATON", FunctionIPv6StringToNum<IPStringToNumExceptionMode::Throw>::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("INET_NTOA", NameFunctionIPv4NumToString::name, FunctionFactory::Case::Insensitive);
}

}
