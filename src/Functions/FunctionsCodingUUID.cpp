#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/BitHelpers.h>
#include <base/hex.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>

#include <span>

namespace DB::ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
enum class Representation : uint8_t
{
    BigEndian,
    LittleEndian
};

std::pair<int, int> determineBinaryStartIndexWithIncrement(ptrdiff_t num_bytes, Representation representation)
{
    if (representation == Representation::BigEndian)
        return {0, 1};
    if (representation == Representation::LittleEndian)
        return {num_bytes - 1, -1};

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "{} is not handled yet", magic_enum::enum_name(representation));
}

void formatHex(const std::span<const UInt8> src, UInt8 * dst, Representation representation)
{
    const auto src_size = std::ssize(src);
    const auto [src_start_index, src_increment] = determineBinaryStartIndexWithIncrement(src_size, representation);
    for (int src_pos = src_start_index, dst_pos = 0; src_pos >= 0 && src_pos < src_size; src_pos += src_increment, dst_pos += 2)
        writeHexByteLowercase(src[src_pos], dst + dst_pos);
}

void parseHex(const UInt8 * __restrict src, const std::span<UInt8> dst, Representation representation)
{
    const auto dst_size = std::ssize(dst);
    const auto [dst_start_index, dst_increment] = determineBinaryStartIndexWithIncrement(dst_size, representation);
    const auto * src_as_char = reinterpret_cast<const char *>(src);
    for (auto dst_pos = dst_start_index, src_pos = 0; dst_pos >= 0 && dst_pos < dst_size; dst_pos += dst_increment, src_pos += 2)
        dst[dst_pos] = unhex2(src_as_char + src_pos);
}

class UUIDSerializer
{
public:
    enum class Variant : uint8_t
    {
        Default = 1,
        Microsoft = 2
    };

    explicit UUIDSerializer(const Variant variant)
        : first_half_binary_representation(variant == Variant::Microsoft ? Representation::LittleEndian : Representation::BigEndian)
    {
        if (variant != Variant::Default && variant != Variant::Microsoft)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "{} is not handled yet", magic_enum::enum_name(variant));
    }

    void deserialize(const UInt8 * src16, UInt8 * dst36) const
    {
        formatHex({src16, 4}, &dst36[0], first_half_binary_representation);
        dst36[8] = '-';
        formatHex({src16 + 4, 2}, &dst36[9], first_half_binary_representation);
        dst36[13] = '-';
        formatHex({src16 + 6, 2}, &dst36[14], first_half_binary_representation);
        dst36[18] = '-';
        formatHex({src16 + 8, 2}, &dst36[19], Representation::BigEndian);
        dst36[23] = '-';
        formatHex({src16 + 10, 6}, &dst36[24], Representation::BigEndian);
    }

    void serialize(const UInt8 * src36, UInt8 * dst16) const
    {
        /// If string is not like UUID - implementation specific behaviour.
        parseHex(&src36[0], {dst16 + 0, 4}, first_half_binary_representation);
        parseHex(&src36[9], {dst16 + 4, 2}, first_half_binary_representation);
        parseHex(&src36[14], {dst16 + 6, 2}, first_half_binary_representation);
        parseHex(&src36[19], {dst16 + 8, 2}, Representation::BigEndian);
        parseHex(&src36[24], {dst16 + 10, 6}, Representation::BigEndian);
    }

private:
    Representation first_half_binary_representation;
};

void checkArgumentCount(const DB::DataTypes & arguments, const std::string_view function_name)
{
    if (const auto argument_count = std::ssize(arguments); argument_count < 1 || argument_count > 2)
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
            function_name,
            argument_count);
}

void checkFormatArgument(const DB::DataTypes & arguments, const std::string_view function_name)
{
    if (const auto argument_count = std::ssize(arguments);
        argument_count > 1 && !DB::WhichDataType(arguments[1]).isInt8() && !DB::WhichDataType(arguments[1]).isUInt8())
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument of function {}, expected Int8 or UInt8 type",
            arguments[1]->getName(),
            function_name);
}

UUIDSerializer::Variant parseVariant(const DB::ColumnsWithTypeAndName & arguments)
{
    if (arguments.size() < 2)
        return UUIDSerializer::Variant::Default;

    const auto representation = static_cast<magic_enum::underlying_type_t<UUIDSerializer::Variant>>(arguments[1].column->getInt(0));
    const auto as_enum = magic_enum::enum_cast<UUIDSerializer::Variant>(representation);

    if (!as_enum)
        throw DB::Exception(DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Expected UUID variant, got {}", representation);

    return *as_enum;
}
}

namespace DB
{
constexpr size_t uuid_bytes_length = 16;
constexpr size_t uuid_text_length = 36;

class FunctionUUIDNumToString : public IFunction
{
public:
    static constexpr auto name = "UUIDNumToString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDNumToString>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkArgumentCount(arguments, name);

        const auto * ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ptr || ptr->getN() != uuid_bytes_length)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument of function {}, expected FixedString({})",
                            arguments[0]->getName(), getName(), uuid_bytes_length);

        checkFormatArgument(arguments, name);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;
        const auto variant = parseVariant(arguments);
        if (const auto * col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != uuid_bytes_length)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of column {} argument of function {}, expected FixedString({})",
                                col_type_name.type->getName(), col_in->getName(), getName(), uuid_bytes_length);

            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(input_rows_count * (uuid_text_length + 1));
            offsets_res.resize(input_rows_count);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            const UUIDSerializer uuid_serializer(variant);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                uuid_serializer.deserialize(&vec_in[src_offset], &vec_res[dst_offset]);
                src_offset += uuid_bytes_length;
                dst_offset += uuid_text_length;
                vec_res[dst_offset] = 0;
                ++dst_offset;
                offsets_res[i] = dst_offset;
            }

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};


class FunctionUUIDStringToNum : public IFunction
{
public:
    static constexpr auto name = "UUIDStringToNum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDStringToNum>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkArgumentCount(arguments, name);

        /// String or FixedString(36)
        if (!isString(arguments[0]))
        {
            const auto * ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
            if (!ptr || ptr->getN() != uuid_text_length)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of first argument of function {}, expected FixedString({})",
                                arguments[0]->getName(), getName(), uuid_text_length);
        }

        checkFormatArgument(arguments, name);

        return std::make_shared<DataTypeFixedString>(uuid_bytes_length);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        const UUIDSerializer uuid_serializer(parseVariant(arguments));
        if (const auto * col_in = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & vec_in = col_in->getChars();
            const auto & offsets_in = col_in->getOffsets();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars & vec_res = col_res->getChars();
            vec_res.resize(input_rows_count * uuid_bytes_length);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                /// If string has incorrect length - then return zero UUID.
                /// If string has correct length but contains something not like UUID - implementation specific behaviour.

                size_t string_size = offsets_in[i] - src_offset;
                if (string_size == uuid_text_length + 1)
                    uuid_serializer.serialize(&vec_in[src_offset], &vec_res[dst_offset]);
                else
                    memset(&vec_res[dst_offset], 0, uuid_bytes_length);

                dst_offset += uuid_bytes_length;
                src_offset += string_size;
            }

            return col_res;
        }
        if (const auto * col_in_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in_fixed->getN() != uuid_text_length)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of column {} argument of function {}, expected FixedString({})",
                    col_type_name.type->getName(),
                    col_in_fixed->getName(),
                    getName(),
                    uuid_text_length);

            const auto & vec_in = col_in_fixed->getChars();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars & vec_res = col_res->getChars();
            vec_res.resize(input_rows_count * uuid_bytes_length);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                uuid_serializer.serialize(&vec_in[src_offset], &vec_res[dst_offset]);
                src_offset += uuid_text_length;
                dst_offset += uuid_bytes_length;
            }

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};


class FunctionUUIDToNum : public IFunction
{
public:
    static constexpr auto name = "UUIDToNum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDToNum>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkArgumentCount(arguments, name);

        if (!isUUID(arguments[0]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected UUID",
                arguments[0]->getName(),
                getName());
        }

        checkFormatArgument(arguments, name);

        return std::make_shared<DataTypeFixedString>(uuid_bytes_length);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        const bool defaultFormat = (parseVariant(arguments) == UUIDSerializer::Variant::Default);

        if (const auto * col_in = checkAndGetColumn<ColumnUUID>(column.get()))
        {
            const auto & vec_in = col_in->getData();
            const UUID * uuids = vec_in.data();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars & vec_res = col_res->getChars();
            vec_res.resize(input_rows_count * uuid_bytes_length);

            size_t dst_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                uint64_t hiBytes = DB::UUIDHelpers::getHighBytes(uuids[i]);
                uint64_t loBytes = DB::UUIDHelpers::getLowBytes(uuids[i]);
                unalignedStoreBigEndian<uint64_t>(&vec_res[dst_offset], hiBytes);
                unalignedStoreBigEndian<uint64_t>(&vec_res[dst_offset + sizeof(hiBytes)], loBytes);
                if (!defaultFormat)
                {
                    std::swap(vec_res[dst_offset], vec_res[dst_offset + 3]);
                    std::swap(vec_res[dst_offset + 1], vec_res[dst_offset + 2]);
                    std::swap(vec_res[dst_offset + 4], vec_res[dst_offset + 5]);
                    std::swap(vec_res[dst_offset + 6], vec_res[dst_offset + 7]);
                }
                dst_offset += uuid_bytes_length;
            }

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

class FunctionUUIDv7ToDateTime : public IFunction
{
public:
    static constexpr auto name = "UUIDv7ToDateTime";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDv7ToDateTime>(); }

    static constexpr UInt32 datetime_scale = 3;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Wrong number of arguments for function {}: should be 1 or 2", getName());

        if (!checkAndGetDataType<DataTypeUUID>(arguments[0].type.get()))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected UUID",
                arguments[0].type->getName(),
                getName());
        }

        String timezone;
        if (arguments.size() == 2)
        {
            timezone = extractTimeZoneNameFromColumn(arguments[1].column.get(), arguments[1].name);

            if (timezone.empty())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                    getName());
        }

        return std::make_shared<DataTypeDateTime64>(datetime_scale, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        if (const auto * col_in = checkAndGetColumn<ColumnUUID>(column.get()))
        {
            const auto & vec_in = col_in->getData();
            const UUID * uuids = vec_in.data();

            auto col_res = ColumnDateTime64::create(input_rows_count, datetime_scale);
            auto & vec_res = col_res->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const uint64_t hiBytes = DB::UUIDHelpers::getHighBytes(uuids[i]);
                const uint64_t ms = ((hiBytes & 0xf000) == 0x7000) ? (hiBytes >> 16) : 0;

                vec_res[i] = DecimalUtils::decimalFromComponents<DateTime64>(ms / intExp10(datetime_scale), ms % intExp10(datetime_scale), datetime_scale);
            }

            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

REGISTER_FUNCTION(CodingUUID)
{
    factory.registerFunction<FunctionUUIDNumToString>();
    factory.registerFunction<FunctionUUIDStringToNum>();
    factory.registerFunction<FunctionUUIDToNum>(
        FunctionDocumentation{
            .description = R"(
This function accepts a UUID and returns a FixedString(16) as its binary representation, with its format optionally specified by variant (Big-endian by default).
)",
            .examples{
                {"uuid",
                 "select toUUID(UUIDNumToString(toFixedString('a/<@];!~p{jTj={)', 16))) as uuid, UUIDToNum(uuid) as uuidNum, "
                 "UUIDToNum(uuid, 2) as uuidMsNum",
                 R"(
┌─uuid─────────────────────────────────┬─uuidNum──────────┬─uuidMsNum────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │ @</a];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┴──────────────────┘
)"}},
            .categories{"UUID"}});


    factory.registerFunction<FunctionUUIDv7ToDateTime>(
        FunctionDocumentation{
            .description = R"(
This function extracts the timestamp from a UUID and returns it as a DateTime64(3) typed value.
The function expects the UUID having version 7 to be provided as the first argument.
An optional second argument can be passed to specify a timezone for the timestamp.
)",
            .examples{
                {"uuid","select UUIDv7ToDateTime(generateUUIDv7())", ""},
                {"uuid","select generateUUIDv7() as uuid, UUIDv7ToDateTime(uuid), UUIDv7ToDateTime(uuid, 'America/New_York')", ""}},
            .categories{"UUID"}});
}

}
