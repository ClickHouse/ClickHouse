#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Core/callOnTypeIndex.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>

#include <Common/transformEndianness.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Performs byte reinterpretation similar to reinterpret_cast.
 *
 * Following reinterpretations are allowed:
 * 1. Any type that isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion into FixedString.
 * 2. Any type that isValueUnambiguouslyRepresentedInContiguousMemoryRegion into String.
 * 3. Types that can be interpreted as numeric (Integers, Float, Date, DateTime, UUID) into FixedString,
 * String, and types that can be interpreted as numeric (Integers, Float, Date, DateTime, UUID).
 */
class FunctionReinterpret : public IFunction
{
public:
    static constexpr auto name = "reinterpret";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReinterpret>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & column = arguments.back().column;

        DataTypePtr from_type = arguments[0].type;

        const auto * type_col = checkAndGetColumnConst<ColumnString>(column.get());
        if (!type_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument to {} must be a constant string describing type."
                " Instead there is non-constant column of type {}",
                getName(),
                arguments.back().type->getName());

        DataTypePtr to_type = DataTypeFactory::instance().get(type_col->getValue<String>());

        WhichDataType result_reinterpret_type(to_type);

        if (result_reinterpret_type.isFixedString())
        {
            if (!from_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as FixedString because it is not fixed size and contiguous in memory",
                    from_type->getName());
        }
        else if (result_reinterpret_type.isString())
        {
            if (!from_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as String because it is not contiguous in memory",
                    from_type->getName());
        }
        else if (canBeReinterpretedAsNumeric(result_reinterpret_type))
        {
            WhichDataType from_data_type(from_type);

            if (!canBeReinterpretedAsNumeric(from_data_type) && !from_data_type.isStringOrFixedString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as {} because only Numeric, String or FixedString can be reinterpreted in Numeric",
                    from_type->getName(),
                    to_type->getName());
        }
        else if (result_reinterpret_type.isArray())
        {
            WhichDataType from_data_type(from_type);
            if (!from_data_type.isStringOrFixedString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as {} because only String or FixedString can be reinterpreted as array",
                    from_type->getName(),
                    to_type->getName());
            if (!to_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as {} because the array element type is not fixed length",
                    from_type->getName(),
                    to_type->getName());
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Cannot reinterpret {} as {} because only reinterpretation in String, FixedString and Numeric types is supported",
                from_type->getName(),
                to_type->getName());
        }

        return to_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto from_type = arguments[0].type;

        ColumnPtr result;

        if (!callOnTwoTypeIndexes(from_type->getTypeId(), result_type->getTypeId(), [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using FromType = typename Types::LeftType;
            using ToType = typename Types::RightType;

            /// Place this check before std::is_same_v<FromType, ToType> because same FixedString
            /// types does not necessary have the same byte size fixed value.
            if constexpr (std::is_same_v<ToType, DataTypeFixedString>)
            {
                const IColumn & src = *arguments[0].column;
                MutableColumnPtr dst = result_type->createColumn();

                ColumnFixedString * dst_concrete = assert_cast<ColumnFixedString *>(dst.get());

                if (src.isFixedAndContiguous() && src.sizeOfValueIfFixed() == dst_concrete->getN())
                    executeContiguousToFixedString(src, *dst_concrete, dst_concrete->getN(), input_rows_count);
                else
                    executeToFixedString(src, *dst_concrete, dst_concrete->getN(), input_rows_count);

                result = std::move(dst);

                return true;
            }
            else if constexpr (std::is_same_v<FromType, ToType>)
            {
                result = arguments[0].column;

                return true;
            }
            else if constexpr (std::is_same_v<ToType, DataTypeString>)
            {
                const IColumn & src = *arguments[0].column;
                MutableColumnPtr dst = result_type->createColumn();

                ColumnString * dst_concrete = assert_cast<ColumnString *>(dst.get());
                executeToString(src, *dst_concrete, input_rows_count);

                result = std::move(dst);

                return true;
            }
            else if constexpr (CanBeReinterpretedAsNumeric<ToType>)
            {
                using ToFieldType = typename ToType::FieldType;

                if constexpr (std::is_same_v<FromType, DataTypeString>)
                {
                    const auto * col_from = assert_cast<const ColumnString *>(arguments[0].column.get());

                    auto col_res = numericColumnCreateHelper<ToType>(static_cast<const ToType&>(*result_type.get()));

                    const auto & data_from = col_from->getChars();
                    const auto & offsets_from = col_from->getOffsets();
                    auto & vec_res = col_res->getData();
                    vec_res.resize_fill(input_rows_count);

                    size_t offset = 0;
                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        size_t copy_size = std::min(static_cast<UInt64>(sizeof(ToFieldType)), offsets_from[i] - offset);
                        if constexpr (std::endian::native == std::endian::little)
                            memcpy(&vec_res[i],
                                &data_from[offset],
                                copy_size);
                        else
                        {
                            size_t offset_to = sizeof(ToFieldType) > copy_size ? sizeof(ToFieldType) - copy_size : 0;
                            reverseMemcpy(
                                reinterpret_cast<char*>(&vec_res[i]) + offset_to,
                                &data_from[offset],
                                copy_size);
                        }
                        offset = offsets_from[i];
                    }

                    result = std::move(col_res);

                    return true;
                }
                else if constexpr (std::is_same_v<FromType, DataTypeFixedString>)
                {
                    const auto * col_from_fixed = assert_cast<const ColumnFixedString *>(arguments[0].column.get());

                    auto col_res = numericColumnCreateHelper<ToType>(static_cast<const ToType&>(*result_type.get()));

                    const auto& data_from = col_from_fixed->getChars();
                    size_t step = col_from_fixed->getN();
                    auto & vec_res = col_res->getData();

                    size_t offset = 0;
                    size_t copy_size = std::min(step, sizeof(ToFieldType));
                    size_t index = data_from.size() - copy_size;

                    if (sizeof(ToFieldType) <= step)
                        vec_res.resize(input_rows_count);
                    else
                        vec_res.resize_fill(input_rows_count);

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        if constexpr (std::endian::native == std::endian::little)
                            memcpy(&vec_res[i], &data_from[offset], copy_size);
                        else
                        {
                            size_t offset_to = sizeof(ToFieldType) > copy_size ? sizeof(ToFieldType) - copy_size : 0;
                            reverseMemcpy(reinterpret_cast<char*>(&vec_res[i]) + offset_to, &data_from[index - offset], copy_size);
                        }
                        offset += step;
                    }

                    result = std::move(col_res);

                    return true;
                }
                else if constexpr (CanBeReinterpretedAsNumeric<FromType>)
                {
                    using From = typename FromType::FieldType;
                    using To = typename ToType::FieldType;

                    using FromColumnType = ColumnVectorOrDecimal<From>;

                    const auto * column_from = assert_cast<const FromColumnType*>(arguments[0].column.get());

                    auto column_to = numericColumnCreateHelper<ToType>(static_cast<const ToType&>(*result_type.get()));

                    auto & from = column_from->getData();
                    auto & to = column_to->getData();

                    to.resize_fill(input_rows_count);

                    static constexpr size_t copy_size = std::min(sizeof(From), sizeof(To));

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        if constexpr (std::endian::native == std::endian::little)
                            memcpy(static_cast<void*>(&to[i]), static_cast<const void*>(&from[i]), copy_size);
                        else
                        {
                            // Handle the cases of both 128-bit representation to 256-bit and 128-bit to 64-bit or lower.
                            const size_t offset_from = sizeof(From) > sizeof(To) ? sizeof(From) - sizeof(To) : 0;
                            const size_t offset_to = sizeof(To) > sizeof(From) ? sizeof(To) - sizeof(From) : 0;
                            memcpy(reinterpret_cast<char *>(&to[i]) + offset_to, reinterpret_cast<const char *>(&from[i]) + offset_from, copy_size);
                        }

                    }

                    result = std::move(column_to);
                    return true;
                }
            }

            return false;
        }))
        {
            /// Destination could be an array, source has to be String/FixedString
            /// Above lambda block of callOnTwoTypeIndexes only for scalar result type specializations.
            /// All Array(T) result types are handled with a single code block below using insertData().
            if (WhichDataType(result_type).isArray())
            {
                auto inner_type = typeid_cast<const DataTypeArray &>(*result_type).getNestedType();
                const IColumn * col_from = nullptr;

                if (WhichDataType(from_type).isString())
                    col_from = &assert_cast<const ColumnString &>(*arguments[0].column);
                else if (WhichDataType(from_type).isFixedString())
                    col_from = &assert_cast<const ColumnFixedString &>(*arguments[0].column);
                else
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Cannot reinterpret {} as {}, expected String or FixedString as source",
                        from_type->getName(),
                        result_type->getName());

                auto col_res = ColumnArray::create(inner_type->createColumn());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    StringRef ref = col_from->getDataAt(i);
                    col_res->insertData(ref.data, ref.size);
                }

                result = std::move(col_res);
            }
            else
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as {}",
                    from_type->getName(),
                    result_type->getName());
            }
        }

        return result;
    }
private:
    template <typename T>
    static constexpr auto CanBeReinterpretedAsNumeric =
        IsDataTypeDecimalOrNumber<T> ||
        std::is_same_v<T, DataTypeDate> ||
        std::is_same_v<T, DataTypeDateTime> ||
        std::is_same_v<T, DataTypeDateTime64> ||
        std::is_same_v<T, DataTypeUUID>;

    static bool canBeReinterpretedAsNumeric(const WhichDataType & type)
    {
        return type.isUInt() ||
            type.isInt() ||
            type.isDate() ||
            type.isDateTime() ||
            type.isDateTime64() ||
            type.isFloat() ||
            type.isUUID() ||
            type.isDecimal();
    }

    static void NO_INLINE executeToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n, size_t input_rows_count)
    {
        ColumnFixedString::Chars & data_to = dst.getChars();
        data_to.resize_fill(n * input_rows_count);

        ColumnFixedString::Offset offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view data = src.getDataAt(i).toView();

            if constexpr (std::endian::native == std::endian::little)
                memcpy(&data_to[offset], data.data(), std::min(n, data.size()));
            else
                reverseMemcpy(&data_to[offset], data.data(), std::min(n, data.size()));

            offset += n;
        }
    }

    static void NO_INLINE executeContiguousToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n, size_t input_rows_count)
    {
        ColumnFixedString::Chars & data_to = dst.getChars();
        data_to.resize(n * input_rows_count);

        if constexpr (std::endian::native == std::endian::little)
            memcpy(data_to.data(), src.getRawData().data(), data_to.size());
        else
            reverseMemcpy(data_to.data(), src.getRawData().data(), data_to.size());
    }

    static void NO_INLINE executeToString(const IColumn & src, ColumnString & dst, size_t input_rows_count)
    {
        ColumnString::Chars & data_to = dst.getChars();
        ColumnString::Offsets & offsets_to = dst.getOffsets();
        offsets_to.resize(input_rows_count);

        ColumnString::Offset offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef data = src.getDataAt(i);

            /// Cut trailing zero bytes.
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            while (data.size && data.data[data.size - 1] == 0)
                --data.size;
#else
            size_t index = 0;
            while (index < data.size && data.data[index] == 0)
                index++;
            data.size -= index;
#endif
            data_to.resize(offset + data.size);
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            memcpy(&data_to[offset], data.data, data.size);
#else
            reverseMemcpy(&data_to[offset], data.data + index, data.size);
#endif
            offset += data.size;
            offsets_to[i] = offset;
        }
    }

    template <typename Type>
    static typename Type::ColumnType::MutablePtr numericColumnCreateHelper(const Type & type)
    {
        size_t column_size = 0;

        using ColumnType = typename Type::ColumnType;

        if constexpr (IsDataTypeDecimal<Type>)
            return ColumnType::create(column_size, type.getScale());
        else
            return ColumnType::create(column_size);
    }
};

template <typename ToDataType, typename Name>
class FunctionReinterpretAs : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReinterpretAs>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return impl.useDefaultImplementationForConstants(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return impl.isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    static ColumnsWithTypeAndName addTypeColumnToArguments(const ColumnsWithTypeAndName & arguments)
    {
        const auto & argument = arguments[0];

        DataTypePtr data_type;

        if constexpr (std::is_same_v<ToDataType, DataTypeFixedString>)
        {
            const auto & type = argument.type;

            if (!type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as FixedString because it is not fixed size and contiguous in memory",
                    type->getName());

            size_t type_value_size_in_memory = type->getSizeOfValueInMemory();
            data_type = std::make_shared<DataTypeFixedString>(type_value_size_in_memory);
        }
        else
            data_type = std::make_shared<ToDataType>();

        auto type_name_column = DataTypeString().createColumnConst(1, data_type->getName());
        ColumnWithTypeAndName type_column(type_name_column, std::make_shared<DataTypeString>(), "");

        ColumnsWithTypeAndName arguments_with_type
        {
            argument,
            type_column
        };

        return arguments_with_type;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto arguments_with_type = addTypeColumnToArguments(arguments);
        return impl.getReturnTypeImpl(arguments_with_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        auto arguments_with_type = addTypeColumnToArguments(arguments);
        return impl.executeImpl(arguments_with_type, return_type, input_rows_count);
    }

    FunctionReinterpret impl;
};

struct NameReinterpretAsUInt8       { static constexpr auto name = "reinterpretAsUInt8"; };
struct NameReinterpretAsUInt16      { static constexpr auto name = "reinterpretAsUInt16"; };
struct NameReinterpretAsUInt32      { static constexpr auto name = "reinterpretAsUInt32"; };
struct NameReinterpretAsUInt64      { static constexpr auto name = "reinterpretAsUInt64"; };
struct NameReinterpretAsUInt128     { static constexpr auto name = "reinterpretAsUInt128"; };
struct NameReinterpretAsUInt256     { static constexpr auto name = "reinterpretAsUInt256"; };
struct NameReinterpretAsInt8        { static constexpr auto name = "reinterpretAsInt8"; };
struct NameReinterpretAsInt16       { static constexpr auto name = "reinterpretAsInt16"; };
struct NameReinterpretAsInt32       { static constexpr auto name = "reinterpretAsInt32"; };
struct NameReinterpretAsInt64       { static constexpr auto name = "reinterpretAsInt64"; };
struct NameReinterpretAsInt128      { static constexpr auto name = "reinterpretAsInt128"; };
struct NameReinterpretAsInt256      { static constexpr auto name = "reinterpretAsInt256"; };
struct NameReinterpretAsFloat32     { static constexpr auto name = "reinterpretAsFloat32"; };
struct NameReinterpretAsFloat64     { static constexpr auto name = "reinterpretAsFloat64"; };
struct NameReinterpretAsDate        { static constexpr auto name = "reinterpretAsDate"; };
struct NameReinterpretAsDateTime    { static constexpr auto name = "reinterpretAsDateTime"; };
struct NameReinterpretAsUUID        { static constexpr auto name = "reinterpretAsUUID"; };
struct NameReinterpretAsString      { static constexpr auto name = "reinterpretAsString"; };
struct NameReinterpretAsFixedString { static constexpr auto name = "reinterpretAsFixedString"; };

using FunctionReinterpretAsUInt8 = FunctionReinterpretAs<DataTypeUInt8, NameReinterpretAsUInt8>;
using FunctionReinterpretAsUInt16 = FunctionReinterpretAs<DataTypeUInt16, NameReinterpretAsUInt16>;
using FunctionReinterpretAsUInt32 = FunctionReinterpretAs<DataTypeUInt32, NameReinterpretAsUInt32>;
using FunctionReinterpretAsUInt64 = FunctionReinterpretAs<DataTypeUInt64, NameReinterpretAsUInt64>;
using FunctionReinterpretAsUInt128 = FunctionReinterpretAs<DataTypeUInt128, NameReinterpretAsUInt128>;
using FunctionReinterpretAsUInt256 = FunctionReinterpretAs<DataTypeUInt256, NameReinterpretAsUInt256>;
using FunctionReinterpretAsInt8 = FunctionReinterpretAs<DataTypeInt8, NameReinterpretAsInt8>;
using FunctionReinterpretAsInt16 = FunctionReinterpretAs<DataTypeInt16, NameReinterpretAsInt16>;
using FunctionReinterpretAsInt32 = FunctionReinterpretAs<DataTypeInt32, NameReinterpretAsInt32>;
using FunctionReinterpretAsInt64 = FunctionReinterpretAs<DataTypeInt64, NameReinterpretAsInt64>;
using FunctionReinterpretAsInt128 = FunctionReinterpretAs<DataTypeInt128, NameReinterpretAsInt128>;
using FunctionReinterpretAsInt256 = FunctionReinterpretAs<DataTypeInt256, NameReinterpretAsInt256>;
using FunctionReinterpretAsFloat32 = FunctionReinterpretAs<DataTypeFloat32, NameReinterpretAsFloat32>;
using FunctionReinterpretAsFloat64 = FunctionReinterpretAs<DataTypeFloat64, NameReinterpretAsFloat64>;
using FunctionReinterpretAsDate = FunctionReinterpretAs<DataTypeDate, NameReinterpretAsDate>;
using FunctionReinterpretAsDateTime = FunctionReinterpretAs<DataTypeDateTime, NameReinterpretAsDateTime>;
using FunctionReinterpretAsUUID = FunctionReinterpretAs<DataTypeUUID, NameReinterpretAsUUID>;

using FunctionReinterpretAsString = FunctionReinterpretAs<DataTypeString, NameReinterpretAsString>;

using FunctionReinterpretAsFixedString = FunctionReinterpretAs<DataTypeFixedString, NameReinterpretAsFixedString>;

}

REGISTER_FUNCTION(ReinterpretAs)
{
    FunctionDocumentation::Description description_reinterpretAsUInt8 = R"(
Reinterprets the input value as a value of type UInt8.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
)";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt8 = "reinterpretAsUInt8(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt8 = {
        {"x", "Value to reinterpret as UInt8.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt8 = {"Returns the reinterpreted value `x`.", {"UInt8"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt8 = {
    {
        "Usage example",
        R"(
SELECT
    toInt8(-1) AS val,
    toTypeName(val),
    reinterpretAsUInt8(val) AS res,
    toTypeName(res);
        )",
        R"(
┌─val─┬─toTypeName(val)─┬─res─┬─toTypeName(res)─┐
│  -1 │ Int8            │ 255 │ UInt8           │
└─────┴─────────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt8 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt8 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt8 = {description_reinterpretAsUInt8, syntax_reinterpretAsUInt8, arguments_reinterpretAsUInt8, returned_value_reinterpretAsUInt8, examples_reinterpretAsUInt8, introduced_in_reinterpretAsUInt8, category_reinterpretAsUInt8};

    factory.registerFunction<FunctionReinterpretAsUInt8>(documentation_reinterpretAsUInt8);

    FunctionDocumentation::Description description_reinterpretAsUInt16 = R"(
Reinterprets the input value as a value of type UInt16.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt16 = "reinterpretAsUInt16(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt16 = {
        {"x", "Value to reinterpret as UInt16.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt16 = {"Returns the reinterpreted value `x`.", {"UInt16"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt16 = {
    {
        "Usage example",
        R"(
SELECT
    toUInt8(257) AS x,
    toTypeName(x),
    reinterpretAsUInt16(x) AS res,
    toTypeName(res)
        )",
        R"(
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ UInt8         │   1 │ UInt16          │
└───┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt16 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt16 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt16 = {description_reinterpretAsUInt16, syntax_reinterpretAsUInt16, arguments_reinterpretAsUInt16, returned_value_reinterpretAsUInt16, examples_reinterpretAsUInt16, introduced_in_reinterpretAsUInt16, category_reinterpretAsUInt16};

    factory.registerFunction<FunctionReinterpretAsUInt16>(documentation_reinterpretAsUInt16);

    FunctionDocumentation::Description description_reinterpretAsUInt32 = R"(
Reinterprets the input value as a value of type UInt32.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt32 = "reinterpretAsUInt32(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt32 = {
        {"x", "Value to reinterpret as UInt32.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt32 = {"Returns the reinterpreted value `x`.", {"UInt32"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt32 = {
    {
        "Usage example",
        R"(
SELECT
    toUInt16(257) AS x,
    toTypeName(x),
    reinterpretAsUInt32(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt16        │ 257 │ UInt32          │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt32 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt32 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt32 = {description_reinterpretAsUInt32, syntax_reinterpretAsUInt32, arguments_reinterpretAsUInt32, returned_value_reinterpretAsUInt32, examples_reinterpretAsUInt32, introduced_in_reinterpretAsUInt32, category_reinterpretAsUInt32};

    factory.registerFunction<FunctionReinterpretAsUInt32>(documentation_reinterpretAsUInt32);

    FunctionDocumentation::Description description_reinterpretAsUInt64 = R"(
Reinterprets the input value as a value of type UInt64.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt64 = "reinterpretAsUInt64(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt64 = {
        {"x", "Value to reinterpret as UInt64.", {"Int*", "UInt*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt64 = {"Returns the reinterpreted value of `x`.", {"UInt64"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt64 = {
    {
        "Usage example",
        R"(
SELECT
    toUInt32(257) AS x,
    toTypeName(x),
    reinterpretAsUInt64(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt32        │ 257 │ UInt64          │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt64 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt64 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt64 = {description_reinterpretAsUInt64, syntax_reinterpretAsUInt64, arguments_reinterpretAsUInt64, returned_value_reinterpretAsUInt64, examples_reinterpretAsUInt64, introduced_in_reinterpretAsUInt64, category_reinterpretAsUInt64};

    factory.registerFunction<FunctionReinterpretAsUInt64>(documentation_reinterpretAsUInt64);

    FunctionDocumentation::Description description_reinterpretAsUInt128 = R"(
Reinterprets the input value as a value of type UInt128.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt128 = "reinterpretAsUInt128(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt128 = {
        {"x", "Value to reinterpret as UInt128.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt128 = {"Returns the reinterpreted value `x`.", {"UInt128"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt128 = {
    {
        "Usage example",
        R"(
SELECT
    toUInt64(257) AS x,
    toTypeName(x),
    reinterpretAsUInt128(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt64        │ 257 │ UInt128         │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt128 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt128 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt128 = {description_reinterpretAsUInt128, syntax_reinterpretAsUInt128, arguments_reinterpretAsUInt128, returned_value_reinterpretAsUInt128, examples_reinterpretAsUInt128, introduced_in_reinterpretAsUInt128, category_reinterpretAsUInt128};

    factory.registerFunction<FunctionReinterpretAsUInt128>(documentation_reinterpretAsUInt128);

    FunctionDocumentation::Description description_reinterpretAsUInt256 = R"(
Reinterprets the input value as a value of type UInt256.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt256 = "reinterpretAsUInt256(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt256 = {
        {"x", "Value to reinterpret as UInt256.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt256 = {"Returns the reinterpreted value `x`.", {"UInt256"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt256 = {
    {
        "Usage example",
        R"(
SELECT
    toUInt128(257) AS x,
    toTypeName(x),
    reinterpretAsUInt256(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt128       │ 257 │ UInt256         │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt256 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt256 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt256 = {description_reinterpretAsUInt256, syntax_reinterpretAsUInt256, arguments_reinterpretAsUInt256, returned_value_reinterpretAsUInt256, examples_reinterpretAsUInt256, introduced_in_reinterpretAsUInt256, category_reinterpretAsUInt256};

    factory.registerFunction<FunctionReinterpretAsUInt256>(documentation_reinterpretAsUInt256);

    FunctionDocumentation::Description description_reinterpretAsInt8 = R"(
Reinterprets the input value as a value of type Int8.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsInt8 = "reinterpretAsInt8(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsInt8 = {
        {"x", "Value to reinterpret as Int8.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsInt8 = {"Returns the reinterpreted value `x`.", {"Int8"}};
    FunctionDocumentation::Examples examples_reinterpretAsInt8 = {
    {
        "Usage example",
        R"(
SELECT
    toUInt8(257) AS x,
    toTypeName(x),
    reinterpretAsInt8(x) AS res,
    toTypeName(res)
        )",
        R"(
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ UInt8         │   1 │ Int8            │
└───┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsInt8 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsInt8 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsInt8 = {description_reinterpretAsInt8, syntax_reinterpretAsInt8, arguments_reinterpretAsInt8, returned_value_reinterpretAsInt8, examples_reinterpretAsInt8, introduced_in_reinterpretAsInt8, category_reinterpretAsInt8};

    factory.registerFunction<FunctionReinterpretAsInt8>(documentation_reinterpretAsInt8);

    FunctionDocumentation::Description description_reinterpretAsInt16 = R"(
Reinterprets the input value as a value of type Int16.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsInt16 = "reinterpretAsInt16(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsInt16 = {
        {"x", "Value to reinterpret as Int16.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsInt16 = {"Returns the reinterpreted value `x`.", {"Int16"}};
    FunctionDocumentation::Examples examples_reinterpretAsInt16 = {
    {
        "Usage example",
        R"(
SELECT
    toInt8(257) AS x,
    toTypeName(x),
    reinterpretAsInt16(x) AS res,
    toTypeName(res)
        )",
        R"(
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ Int8          │   1 │ Int16           │
└───┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsInt16 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsInt16 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsInt16 = {description_reinterpretAsInt16, syntax_reinterpretAsInt16, arguments_reinterpretAsInt16, returned_value_reinterpretAsInt16, examples_reinterpretAsInt16, introduced_in_reinterpretAsInt16, category_reinterpretAsInt16};

    factory.registerFunction<FunctionReinterpretAsInt16>(documentation_reinterpretAsInt16);

    FunctionDocumentation::Description description_reinterpretAsInt32 = R"(
Reinterprets the input value as a value of type Int32.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsInt32 = "reinterpretAsInt32(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsInt32 = {
        {"x", "Value to reinterpret as Int32.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsInt32 = {"Returns the reinterpreted value `x`.", {"Int32"}};
    FunctionDocumentation::Examples examples_reinterpretAsInt32 = {
    {
        "Usage example",
        R"(
SELECT
    toInt16(257) AS x,
    toTypeName(x),
    reinterpretAsInt32(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int16         │ 257 │ Int32           │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsInt32 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsInt32 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsInt32 = {description_reinterpretAsInt32, syntax_reinterpretAsInt32, arguments_reinterpretAsInt32, returned_value_reinterpretAsInt32, examples_reinterpretAsInt32, introduced_in_reinterpretAsInt32, category_reinterpretAsInt32};

    factory.registerFunction<FunctionReinterpretAsInt32>(documentation_reinterpretAsInt32);

    FunctionDocumentation::Description description_reinterpretAsInt64 = R"(
Reinterprets the input value as a value of type Int64.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsInt64 = "reinterpretAsInt64(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsInt64 = {
        {"x", "Value to reinterpret as Int64.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsInt64 = {"Returns the reinterpreted value `x`.", {"Int64"}};
    FunctionDocumentation::Examples examples_reinterpretAsInt64 = {
    {
        "Usage example",
        R"(
SELECT
    toInt32(257) AS x,
    toTypeName(x),
    reinterpretAsInt64(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int32         │ 257 │ Int64           │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsInt64 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsInt64 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsInt64 = {description_reinterpretAsInt64, syntax_reinterpretAsInt64, arguments_reinterpretAsInt64, returned_value_reinterpretAsInt64, examples_reinterpretAsInt64, introduced_in_reinterpretAsInt64, category_reinterpretAsInt64};

    factory.registerFunction<FunctionReinterpretAsInt64>(documentation_reinterpretAsInt64);

    FunctionDocumentation::Description description_reinterpretAsInt128 = R"(
Reinterprets the input value as a value of type Int128.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsInt128 = "reinterpretAsInt128(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsInt128 = {
        {"x", "Value to reinterpret as Int128.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsInt128 = {"Returns the reinterpreted value `x`.", {"Int128"}};
    FunctionDocumentation::Examples examples_reinterpretAsInt128 = {
    {
        "Usage example",
        R"(
SELECT
    toInt64(257) AS x,
    toTypeName(x),
    reinterpretAsInt128(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int64         │ 257 │ Int128          │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsInt128 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsInt128 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsInt128 = {description_reinterpretAsInt128, syntax_reinterpretAsInt128, arguments_reinterpretAsInt128, returned_value_reinterpretAsInt128, examples_reinterpretAsInt128, introduced_in_reinterpretAsInt128, category_reinterpretAsInt128};

    factory.registerFunction<FunctionReinterpretAsInt128>(documentation_reinterpretAsInt128);

    FunctionDocumentation::Description description_reinterpretAsInt256 = R"(
Reinterprets the input value as a value of type Int256.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsInt256 = "reinterpretAsInt256(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsInt256 = {
        {"x", "Value to reinterpret as Int256.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsInt256 = {"Returns the reinterpreted value `x`.", {"Int256"}};
    FunctionDocumentation::Examples examples_reinterpretAsInt256 = {
    {
        "Usage example",
        R"(
SELECT
    toInt128(257) AS x,
    toTypeName(x),
    reinterpretAsInt256(x) AS res,
    toTypeName(res)
        )",
        R"(
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int128        │ 257 │ Int256          │
└─────┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsInt256 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsInt256 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsInt256 = {description_reinterpretAsInt256, syntax_reinterpretAsInt256, arguments_reinterpretAsInt256, returned_value_reinterpretAsInt256, examples_reinterpretAsInt256, introduced_in_reinterpretAsInt256, category_reinterpretAsInt256};

    factory.registerFunction<FunctionReinterpretAsInt256>(documentation_reinterpretAsInt256);

    FunctionDocumentation::Description description_reinterpretAsFloat32 = R"(
Reinterprets the input value as a value of type Float32.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsFloat32 = "reinterpretAsFloat32(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsFloat32 = {
        {"x", "Value to reinterpret as Float32.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsFloat32 = {"Returns the reinterpreted value `x`.", {"Float32"}};
    FunctionDocumentation::Examples examples_reinterpretAsFloat32 = {
    {
        "Usage example",
        R"(
SELECT reinterpretAsUInt32(toFloat32(0.2)) AS x, reinterpretAsFloat32(x)
        )",
        R"(
┌──────────x─┬─reinterpretAsFloat32(x)─┐
│ 1045220557 │                     0.2 │
└────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsFloat32 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsFloat32 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsFloat32 = {description_reinterpretAsFloat32, syntax_reinterpretAsFloat32, arguments_reinterpretAsFloat32, returned_value_reinterpretAsFloat32, examples_reinterpretAsFloat32, introduced_in_reinterpretAsFloat32, category_reinterpretAsFloat32};

    factory.registerFunction<FunctionReinterpretAsFloat32>(documentation_reinterpretAsFloat32);

    FunctionDocumentation::Description description_reinterpretAsFloat64 = R"(
Reinterprets the input value as a value of type Float64.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is undefined.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsFloat64 = "reinterpretAsFloat64(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsFloat64 = {
        {"x", "Value to reinterpret as Float64.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsFloat64 = {"Returns the reinterpreted value `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_reinterpretAsFloat64 = {
    {
        "Usage example",
        R"(
SELECT reinterpretAsUInt64(toFloat64(0.2)) AS x, reinterpretAsFloat64(x)
        )",
        R"(
┌───────────────────x─┬─reinterpretAsFloat64(x)─┐
│ 4596373779694328218 │                     0.2 │
└─────────────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsFloat64 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsFloat64 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsFloat64 = {description_reinterpretAsFloat64, syntax_reinterpretAsFloat64, arguments_reinterpretAsFloat64, returned_value_reinterpretAsFloat64, examples_reinterpretAsFloat64, introduced_in_reinterpretAsFloat64, category_reinterpretAsFloat64};

    factory.registerFunction<FunctionReinterpretAsFloat64>(documentation_reinterpretAsFloat64);

    FunctionDocumentation::Description description_reinterpretAsDate = R"(
Reinterprets the input value as a Date value (assuming little endian order) which is the number of days since the beginning of the Unix epoch 1970-01-01
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsDate = "reinterpretAsDate(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsDate = {
        {"x", "Number of days since the beginning of the Unix Epoch.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsDate = {"Date.", {"Date"}};
    FunctionDocumentation::Examples examples_reinterpretAsDate = {
    {
        "Usage example",
        R"(
SELECT reinterpretAsDate(65), reinterpretAsDate('A')
        )",
        R"(
┌─reinterpretAsDate(65)─┬─reinterpretAsDate('A')─┐
│            1970-03-07 │             1970-03-07 │
└───────────────────────┴────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsDate = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsDate = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsDate = {description_reinterpretAsDate, syntax_reinterpretAsDate, arguments_reinterpretAsDate, returned_value_reinterpretAsDate, examples_reinterpretAsDate, introduced_in_reinterpretAsDate, category_reinterpretAsDate};

    factory.registerFunction<FunctionReinterpretAsDate>(documentation_reinterpretAsDate);

    FunctionDocumentation::Description description_reinterpretAsDateTime = R"(
Reinterprets the input value as a DateTime value (assuming little endian order) which is the number of days since the beginning of the Unix epoch 1970-01-01
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsDateTime = "reinterpretAsDateTime(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsDateTime = {
        {"x", "Number of seconds since the beginning of the Unix Epoch.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsDateTime = {"Date and Time.", {"DateTime"}};
    FunctionDocumentation::Examples examples_reinterpretAsDateTime = {
    {
        "Usage example",
        R"(
SELECT reinterpretAsDateTime(65), reinterpretAsDateTime('A')
        )",
        R"(
┌─reinterpretAsDateTime(65)─┬─reinterpretAsDateTime('A')─┐
│       1970-01-01 01:01:05 │        1970-01-01 01:01:05 │
└───────────────────────────┴────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsDateTime = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsDateTime = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsDateTime = {description_reinterpretAsDateTime, syntax_reinterpretAsDateTime, arguments_reinterpretAsDateTime, returned_value_reinterpretAsDateTime, examples_reinterpretAsDateTime, introduced_in_reinterpretAsDateTime, category_reinterpretAsDateTime};

    factory.registerFunction<FunctionReinterpretAsDateTime>(documentation_reinterpretAsDateTime);

    FunctionDocumentation::Description description_reinterpretAsUUID = R"(
Accepts a 16 byte string and returns a UUID by interpreting each 8-byte half in little-endian byte order. If the string isn't long enough, the function works as if the string is padded with the necessary number of null bytes to the end. If the string is longer than 16 bytes, the extra bytes at the end are ignored.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUUID = "reinterpretAsUUID(fixed_string)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUUID = {
        {"fixed_string", "Big-endian byte string.", {"FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUUID = {"The UUID type value.", {"UUID"}};
    FunctionDocumentation::Examples examples_reinterpretAsUUID = {
    {
        "String to UUID",
        R"(
SELECT reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')))
        )",
        R"(
┌─reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')))─┐
│                                  08090a0b-0c0d-0e0f-0001-020304050607 │
└───────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUUID = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUUID = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUUID = {description_reinterpretAsUUID, syntax_reinterpretAsUUID, arguments_reinterpretAsUUID, returned_value_reinterpretAsUUID, examples_reinterpretAsUUID, introduced_in_reinterpretAsUUID, category_reinterpretAsUUID};

    factory.registerFunction<FunctionReinterpretAsUUID>(documentation_reinterpretAsUUID);

    FunctionDocumentation::Description description_reinterpretAsString = R"(
Reinterprets the input value as a string (assuming little endian order).
Null bytes at the end are ignored, for example, the function returns for UInt32 value 255 a string with a single character.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsString = "reinterpretAsString(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsString = {
        {"x", "Value to reinterpret to string.", {"(U)Int*", "Float*", "Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsString = {"String containing bytes representing `x`.", {"String"}};
    FunctionDocumentation::Examples examples_reinterpretAsString = {
    {
        "Usage example",
        R"(
SELECT
    reinterpretAsString(toDateTime('1970-01-01 01:01:05')),
    reinterpretAsString(toDate('1970-03-07'))
        )",
        R"(
┌─reinterpretAsString(toDateTime('1970-01-01 01:01:05'))─┬─reinterpretAsString(toDate('1970-03-07'))─┐
│ A                                                      │ A                                         │
└────────────────────────────────────────────────────────┴───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsString = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsString = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsString = {description_reinterpretAsString, syntax_reinterpretAsString, arguments_reinterpretAsString, returned_value_reinterpretAsString, examples_reinterpretAsString, introduced_in_reinterpretAsString, category_reinterpretAsString};

    factory.registerFunction<FunctionReinterpretAsString>(documentation_reinterpretAsString);

    FunctionDocumentation::Description description_reinterpretAsFixedString = R"(
Reinterprets the input value as a fixed string (assuming little endian order).
    Null bytes at the end are ignored, for example, the function returns for UInt32 value 255 a string with a single character.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsFixedString = "reinterpretAsFixedString(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsFixedString = {
        {"x", "Value to reinterpret to string.", {"(U)Int*", "Float*", "Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsFixedString = {"Fixed string containing bytes representing `x`.", {"FixedString"}};
    FunctionDocumentation::Examples examples_reinterpretAsFixedString = {
    {
        "Usage example",
        R"(
SELECT
    reinterpretAsFixedString(toDateTime('1970-01-01 01:01:05')),
    reinterpretAsFixedString(toDate('1970-03-07'))
        )",
        R"(
┌─reinterpretAsFixedString(toDateTime('1970-01-01 01:01:05'))─┬─reinterpretAsFixedString(toDate('1970-03-07'))─┐
│ A                                                           │ A                                              │
└─────────────────────────────────────────────────────────────┴────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsFixedString = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsFixedString = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsFixedString = {description_reinterpretAsFixedString, syntax_reinterpretAsFixedString, arguments_reinterpretAsFixedString, returned_value_reinterpretAsFixedString, examples_reinterpretAsFixedString, introduced_in_reinterpretAsFixedString, category_reinterpretAsFixedString};

    factory.registerFunction<FunctionReinterpretAsFixedString>(documentation_reinterpretAsFixedString);

    FunctionDocumentation::Description description_reinterpret = R"(
Uses the same source in-memory bytes sequence for the provided value `x` and reinterprets it to the destination type.
    )";
    FunctionDocumentation::Syntax syntax_reinterpret = "reinterpret(x, type)";
    FunctionDocumentation::Arguments arguments_reinterpret = {
        {"x", "Any type.", {"Any"}},
        {"type", "Destination type. If it is an array, then the array element type must be a fixed length type.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpret = {"Destination type value.", {"Any"}};
    FunctionDocumentation::Examples examples_reinterpret = {
    {
        "Usage example",
        R"(
SELECT reinterpret(toInt8(-1), 'UInt8') AS int_to_uint,
    reinterpret(toInt8(1), 'Float32') AS int_to_float,
    reinterpret('1', 'UInt32') AS string_to_int
        )",
        R"(
┌─int_to_uint─┬─int_to_float─┬─string_to_int─┐
│         255 │        1e-45 │            49 │
└─────────────┴──────────────┴───────────────┘
        )"
    },
    {
        "Array example",
        R"(
SELECT reinterpret(x'3108b4403108d4403108b4403108d440', 'Array(Float32)') AS string_to_array_of_Float32
        )",
        R"(
┌─string_to_array_of_Float32─┐
│ [5.626,6.626,5.626,6.626]  │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpret = {1, 1};
    FunctionDocumentation::Category category_reinterpret = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpret = {description_reinterpret, syntax_reinterpret, arguments_reinterpret, returned_value_reinterpret, examples_reinterpret, introduced_in_reinterpret, category_reinterpret};

    factory.registerFunction<FunctionReinterpret>(documentation_reinterpret);
}

}
