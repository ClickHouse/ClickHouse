#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Core/AccurateComparison.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}

struct NameMakeDecimal
{
    static constexpr auto name = "makeDecimal";
};
struct NameMakeDecimalOrNull
{
    static constexpr auto name = "makeDecimalOrNull";
};

enum class ConvertExceptionMode
{
    Throw, /// Throw exception if value cannot be parsed.
    Null /// Return ColumnNullable with NULLs when value cannot be parsed.
};

template <typename From, typename To>
Field convertNumericTypeImpl(const Field & from)
{
    To result;
    if (!accurate::convertNumeric(from.get<From>(), result))
        return {};
    return result;
}

template <typename To>
Field convertNumericType(const Field & from)
{
    if (from.getType() == Field::Types::UInt64)
        return convertNumericTypeImpl<UInt64, To>(from);
    if (from.getType() == Field::Types::Int64)
        return convertNumericTypeImpl<Int64, To>(from);
    if (from.getType() == Field::Types::UInt128)
        return convertNumericTypeImpl<UInt128, To>(from);
    if (from.getType() == Field::Types::Int128)
        return convertNumericTypeImpl<Int128, To>(from);
    if (from.getType() == Field::Types::UInt256)
        return convertNumericTypeImpl<UInt256, To>(from);
    if (from.getType() == Field::Types::Int256)
        return convertNumericTypeImpl<Int256, To>(from);

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch. Expected: Integer. Got: {}", from.getType());
}

inline UInt32 extractArgument(const ColumnWithTypeAndName & named_column)
{
    Field from;
    named_column.column->get(0, from);
    Field to = convertNumericType<UInt32>(from);
    if (to.isNull())
    {
        throw Exception(
            ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow, precision/scale value must in UInt32", named_column.type->getName());
    }
    return static_cast<UInt32>(to.get<UInt32>());
}

namespace
{
    /// Create decimal with nested value, precision and scale. Required 3 arguments.
    /// If overflow, throw exceptions by default. Else use 'orNull' function will return null.
    template <typename Name, ConvertExceptionMode mode>
    class FunctionMakeDecimal : public IFunction
    {
    public:
        static constexpr auto name = Name::name;
        static constexpr auto exception_mode = mode;

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDecimal>(); }

        String getName() const override { return name; }
        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 3; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (arguments.empty() || arguments.size() != 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 3.",
                    getName(),
                    arguments.size());

            if (!isInteger(arguments[0].type) || !isInteger(arguments[1].type) || !isInteger(arguments[2].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot format {} {} {} as decimal",
                    arguments[0].type->getName(),
                    arguments[1].type->getName(),
                    arguments[2].type->getName());

            if (!arguments[1].column || !arguments[2].column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

            DataTypePtr res = createDecimal<DataTypeDecimal>(extractArgument(arguments[1]), extractArgument(arguments[2]));
            if constexpr (exception_mode == ConvertExceptionMode::Null)
                return std::make_shared<DataTypeNullable>(res);
            else
                return res;
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            const auto & unscale_column = arguments[0];
            if (!unscale_column.column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

            auto precision_value = extractArgument(arguments[1]);
            auto scale_value = extractArgument(arguments[2]);

            if (precision_value <= DecimalUtils::max_precision<Decimal32>)
                return executeInternal<DataTypeDecimal<Decimal32>>(arguments, result_type, input_rows_count, scale_value);
            else if (precision_value <= DecimalUtils::max_precision<Decimal64>)
                return executeInternal<DataTypeDecimal<Decimal64>>(arguments, result_type, input_rows_count, scale_value);
            else if (precision_value <= DecimalUtils::max_precision<Decimal128>)
                return executeInternal<DataTypeDecimal<Decimal128>>(arguments, result_type, input_rows_count, scale_value);
            else
                return executeInternal<DataTypeDecimal<Decimal256>>(arguments, result_type, input_rows_count, scale_value);
        }

    private:
        template <typename DataType>
            requires(IsDataTypeDecimal<DataType>)
        static ColumnPtr
        executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, UInt32 scale)
        {
            const auto & src_column = arguments[0];

            ColumnPtr result_column;

            auto call = [&](const auto & types) -> bool //-V657
            {
                using Types = std::decay_t<decltype(types)>;
                using FromDataType = typename Types::LeftType;
                using ToDataType = typename Types::RightType;

                if constexpr (IsDataTypeNumber<FromDataType>)
                {
                    ColumnUInt8::MutablePtr col_null_map_to;
                    ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
                    if constexpr (exception_mode == ConvertExceptionMode::Null)
                    {
                        col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                        vec_null_map_to = &col_null_map_to->getData();
                    }

                    using ToFieldType = typename ToDataType::FieldType;
                    using ToNativeType = typename ToFieldType::NativeType;
                    using ToColumnType = typename ToDataType::ColumnType;
                    typename ToColumnType::MutablePtr col_to = ToColumnType::create(input_rows_count, scale);

                    auto & vec_to = col_to->getData();
                    vec_to.resize(input_rows_count);

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        ToNativeType result;
                        Field field;
                        src_column.column->get(i, field);
                        bool convert_result = false;

                        if constexpr (std::is_signed_v<typename FromDataType::FieldType>)
                            convert_result = convertDecimalsFromIntegerImpl<Int64, ToNativeType>(field, result);
                        else if constexpr (std::is_unsigned_v<typename FromDataType::FieldType>)
                            convert_result = convertDecimalsFromIntegerImpl<UInt64, ToNativeType>(field, result);
                        else if constexpr (is_big_int_v<typename FromDataType::FieldType>)
                            convert_result = convertDecimalsFromIntegerImpl<typename FromDataType::FieldType, ToNativeType>(field, result);

                        if (convert_result)
                        {
                            vec_to[i] = static_cast<ToFieldType>(result);
                        }
                        else
                        {
                            if constexpr (exception_mode == ConvertExceptionMode::Null)
                            {
                                vec_to[i] = static_cast<ToFieldType>(0);
                                (*vec_null_map_to)[i] = true;
                            }
                            else
                                throw Exception(
                                    ErrorCodes::ILLEGAL_COLUMN,
                                    "Cannot parse {} as {}",
                                    src_column.type->getName(),
                                    result_type->getName());
                        }
                    }

                    if constexpr (exception_mode == ConvertExceptionMode::Null)
                        result_column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
                    else
                        result_column = std::move(col_to);

                    return true;
                }
                else
                {
                    return false;
                }
            };

            bool r = callOnIndexAndDataType<DataType>(src_column.type->getTypeId(), call);

            if (!r)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", src_column.type->getName(), name);
            }

            return result_column;
        }

        template <typename FromNativeType, typename ToNativeType>
        static bool convertDecimalsFromIntegerImpl(const Field & from, ToNativeType & result)
        {
            Field convert_to = convertNumericTypeImpl<FromNativeType, ToNativeType>(from);
            if (convert_to.isNull())
            {
                if constexpr (ConvertExceptionMode::Throw == exception_mode)
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow", from.getTypeName());
                else
                    return false;
            }
            result = static_cast<ToNativeType>(convert_to.get<ToNativeType>());
            return true;
        }
    };

    using FunctionMakeDecimalThrow = FunctionMakeDecimal<NameMakeDecimal, ConvertExceptionMode::Throw>;
    using FunctionMakeDecimalOrNull = FunctionMakeDecimal<NameMakeDecimalOrNull, ConvertExceptionMode::Null>;
}

REGISTER_FUNCTION(MakeDecimal)
{
    factory.registerFunction<FunctionMakeDecimalThrow>({
        R"(
Create a decimal value by use nested type. If overflow throws exception.
)",
        Documentation::Examples{{"makeDecimal", "SELECT makeDecimal(10987654321, 40, 3)"}},
        Documentation::Categories{"OtherFunctions"}
    });
    factory.registerFunction<FunctionMakeDecimalOrNull>({
        R"(
Create a decimal value by use nested type. If overflow return `NULL`.
)",
        Documentation::Examples{{"makeDecimalOrNull", "SELECT makeDecimalOrNull(toInt128(10987654321),8,3)"}},
        Documentation::Categories{"OtherFunctions"}
    });
}
}
