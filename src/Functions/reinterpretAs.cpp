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
Performs byte reinterpretation by treating the input value as a value of type UInt8.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is meaningless.
)";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt8 = "reinterpretAsUInt8(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt8 = {
        {"x", "Value to byte reinterpret as UInt8.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_reinterpretAsUInt8 = {"Returns the reinterpreted value `x`.", {"UInt8"}};
    FunctionDocumentation::Examples examples_reinterpretAsUInt8 = {
    {
        "Usage example",
        R"(
SELECT
    toInt8(257) AS x,
    toTypeName(x),
    reinterpretAsUInt8(x) AS res,
    toTypeName(res)
        )",
        R"(
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ Int8          │   1 │ UInt8           │
└───┴───────────────┴─────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_reinterpretAsUInt8 = {1, 1};
    FunctionDocumentation::Category category_reinterpretAsUInt8 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_reinterpretAsUInt8 = {description_reinterpretAsUInt8, syntax_reinterpretAsUInt8, arguments_reinterpretAsUInt8, returned_value_reinterpretAsUInt8, examples_reinterpretAsUInt8, introduced_in_reinterpretAsUInt8, category_reinterpretAsUInt8};

    factory.registerFunction<FunctionReinterpretAsUInt8>(documentation_reinterpretAsUInt8);

    FunctionDocumentation::Description description_reinterpretAsUInt16 = R"(
Performs byte reinterpretation by treating the input value as a value of type UInt16.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is meaningless.
)";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt16 = "reinterpretAsUInt16(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt16 = {
        {"x", "Value to byte reinterpret as UInt16.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
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
Performs byte reinterpretation by treating the input value as a value of type UInt32.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is meaningless.
)";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt32 = "reinterpretAsUInt32(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt32 = {
        {"x", "Value to byte reinterpret as UInt32.", {"(U)Int*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
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
Performs byte reinterpretation by treating the input value as a value of type UInt64.
Unlike [`CAST`](#cast), the function does not attempt to preserve the original value - if the target type is not able to represent the input type, the output is meaningless.
    )";
    FunctionDocumentation::Syntax syntax_reinterpretAsUInt64 = "reinterpretAsUInt64(x)";
    FunctionDocumentation::Arguments arguments_reinterpretAsUInt64 = {
        {"x", "Value to byte reinterpret as UInt64.", {"Int*", "UInt*", "Float*", "Date", "DateTime", "UUID", "String", "FixedString"}}
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

    factory.registerFunction<FunctionReinterpretAsUInt128>();
    factory.registerFunction<FunctionReinterpretAsUInt256>();
    factory.registerFunction<FunctionReinterpretAsInt8>();
    factory.registerFunction<FunctionReinterpretAsInt16>();
    factory.registerFunction<FunctionReinterpretAsInt32>();
    factory.registerFunction<FunctionReinterpretAsInt64>();
    factory.registerFunction<FunctionReinterpretAsInt128>();
    factory.registerFunction<FunctionReinterpretAsInt256>();
    factory.registerFunction<FunctionReinterpretAsFloat32>();
    factory.registerFunction<FunctionReinterpretAsFloat64>();
    factory.registerFunction<FunctionReinterpretAsDate>();
    factory.registerFunction<FunctionReinterpretAsDateTime>();
    factory.registerFunction<FunctionReinterpretAsUUID>();

    factory.registerFunction<FunctionReinterpretAsString>();

    factory.registerFunction<FunctionReinterpretAsFixedString>();

    factory.registerFunction<FunctionReinterpret>();
}

}
