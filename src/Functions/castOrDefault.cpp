#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionCastOrDefault final : public IFunction
{
public:
    static constexpr auto name = "accurateCastOrDefault";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCastOrDefault>(context);
    }

    explicit FunctionCastOrDefault(ContextPtr context_)
        : keep_nullable(context_->getSettingsRef().cast_keep_nullable)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t arguments_size = arguments.size();
        if (arguments_size != 2 && arguments_size != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} expected 2 or 3 arguments. Actual {}",
                getName(),
                arguments_size);

        const auto & type_column = arguments[1].column;
        const auto * type_column_typed = checkAndGetColumnConst<ColumnString>(type_column.get());

        if (!type_column_typed)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument to {} must be a constant string describing type. Actual {}",
                getName(),
                arguments[1].type->getName());

        DataTypePtr result_type = DataTypeFactory::instance().get(type_column_typed->getValue<String>());

        if (keep_nullable && arguments.front().type->isNullable())
            result_type = makeNullable(result_type);

        if (arguments.size() == 3)
        {
            auto default_value_type = arguments[2].type;

            if (!areTypesEqual(result_type, default_value_type))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Default value type should be same as cast type. Expected {}. Actual {}",
                    result_type->getName(),
                    default_value_type->getName());
            }
        }

        return result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t) const override
    {
        const ColumnWithTypeAndName & column_to_cast = arguments[0];
        auto non_const_column_to_cast = column_to_cast.column->convertToFullColumnIfConst();
        ColumnWithTypeAndName column_to_cast_non_const { non_const_column_to_cast, column_to_cast.type, column_to_cast.name };

        auto cast_result = castColumnAccurateOrNull(column_to_cast_non_const, return_type);

        const auto & cast_result_nullable = assert_cast<const ColumnNullable &>(*cast_result);
        const auto & null_map_data = cast_result_nullable.getNullMapData();
        size_t null_map_data_size = null_map_data.size();
        const auto & nested_column = cast_result_nullable.getNestedColumn();
        auto result = return_type->createColumn();
        result->reserve(null_map_data_size);

        ColumnNullable * result_nullable = nullptr;
        if (result->isNullable())
            result_nullable = assert_cast<ColumnNullable *>(&*result);

        size_t start_insert_index = 0;

        Field default_value;
        ColumnPtr default_column;

        if (arguments.size() == 3)
        {
            auto default_values_column = arguments[2].column;

            if (isColumnConst(*default_values_column))
                default_value = (*default_values_column)[0];
            else
                default_column = default_values_column->convertToFullColumnIfConst();
        }
        else
        {
            default_value = return_type->getDefault();
        }

        for (size_t i = 0; i < null_map_data_size; ++i)
        {
            bool is_current_index_null = null_map_data[i];
            if (!is_current_index_null)
                continue;

            if (i != start_insert_index)
            {
                if (result_nullable)
                    result_nullable->insertRangeFromNotNullable(nested_column, start_insert_index, i - start_insert_index);
                else
                    result->insertRangeFrom(nested_column, start_insert_index, i - start_insert_index);
            }

            if (default_column)
                result->insertFrom(*default_column, i);
            else
                result->insert(default_value);

            start_insert_index = i + 1;
        }

        if (null_map_data_size != start_insert_index)
        {
            if (result_nullable)
                result_nullable->insertRangeFromNotNullable(nested_column, start_insert_index, null_map_data_size - start_insert_index);
            else
                result->insertRangeFrom(nested_column, start_insert_index, null_map_data_size - start_insert_index);
        }

        return result;
    }

private:

    bool keep_nullable;
};

template <typename Type, typename Name>
class FunctionCastOrDefaultTyped final : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionCastOrDefaultTyped>(context);
    }

    explicit FunctionCastOrDefaultTyped(ContextPtr context_)
        : impl(context_)
    {
    }

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return impl.useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForNothing() const override { return impl.useDefaultImplementationForNothing(); }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return impl.useDefaultImplementationForLowCardinalityColumns();}
    bool useDefaultImplementationForConstants() const override { return impl.useDefaultImplementationForConstants();}
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return impl.isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args = {{"Value", nullptr, nullptr, nullptr}};
        FunctionArgumentDescriptors optional_args;

        if constexpr (IsDataTypeDecimal<Type>)
            mandatory_args.push_back({"scale", &isNativeInteger<IDataType>, &isColumnConst, "const Integer"});

        if (std::is_same_v<Type, DataTypeDateTime> || std::is_same_v<Type, DataTypeDateTime64>)
            optional_args.push_back({"timezone", &isString<IDataType>, isColumnConst, "const String"});

        optional_args.push_back({"default_value", nullptr, nullptr, nullptr});

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        size_t additional_argument_index = 1;

        size_t scale = 0;
        std::string time_zone;

        if constexpr (IsDataTypeDecimal<Type>)
        {
            const auto & scale_argument = arguments[additional_argument_index];

            WhichDataType scale_argument_type(scale_argument.type);

            if (!scale_argument_type.isNativeUInt())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Function {} decimal scale should have native UInt type. Actual {}",
                    getName(), scale_argument.type->getName());
            }

            scale = arguments[additional_argument_index].column->getUInt(0);
            ++additional_argument_index;
        }

        if constexpr (std::is_same_v<Type, DataTypeDateTime> || std::is_same_v<Type, DataTypeDateTime64>)
        {
            if (additional_argument_index < arguments.size())
            {
                time_zone = extractTimeZoneNameFromColumn(*arguments[additional_argument_index].column);
                ++additional_argument_index;
            }
        }

        std::shared_ptr<Type> cast_type;

        if constexpr (std::is_same_v<Type, DataTypeDateTime64>)
            cast_type = std::make_shared<Type>(scale, time_zone);
        else if constexpr (IsDataTypeDecimal<Type>)
            cast_type = std::make_shared<Type>(Type::maxPrecision(), scale);
        else if constexpr (std::is_same_v<Type, DataTypeDateTime> || std::is_same_v<Type, DataTypeDateTime64>)
            cast_type = std::make_shared<Type>(time_zone);
        else
            cast_type = std::make_shared<Type>();

        ColumnWithTypeAndName type_argument =
        {
            DataTypeString().createColumnConst(1, cast_type->getName()),
            std::make_shared<DataTypeString>(),
            ""
        };

        ColumnsWithTypeAndName arguments_with_cast_type;
        arguments_with_cast_type.reserve(arguments.size());

        arguments_with_cast_type.emplace_back(arguments[0]);
        arguments_with_cast_type.emplace_back(type_argument);

        if (additional_argument_index < arguments.size())
        {
            arguments_with_cast_type.emplace_back(arguments[additional_argument_index]);
            ++additional_argument_index;
        }

        if (additional_argument_index < arguments.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} wrong arguments size", getName());

        return impl.getReturnTypeImpl(arguments_with_cast_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_size) const override
    {
        size_t additional_arguments_size = IsDataTypeDecimal<Type> + (std::is_same_v<Type, DataTypeDateTime> || std::is_same_v<Type, DataTypeDateTime64>);

        ColumnWithTypeAndName second_argument =
        {
            DataTypeString().createColumnConst(arguments.begin()->column->size(), result_type->getName()),
            std::make_shared<DataTypeString>(),
            ""
        };

        ColumnsWithTypeAndName arguments_with_cast_type;
        arguments_with_cast_type.reserve(arguments.size());

        arguments_with_cast_type.emplace_back(arguments[0]);
        arguments_with_cast_type.emplace_back(second_argument);

        size_t default_column_argument = 1 + additional_arguments_size;
        if (default_column_argument < arguments.size())
            arguments_with_cast_type.emplace_back(arguments[default_column_argument]);

        return impl.executeImpl(arguments_with_cast_type, result_type, input_rows_size);
    }

    FunctionCastOrDefault impl;
};

struct NameToUInt8OrDefault { static constexpr auto name = "toUInt8OrDefault"; };
struct NameToUInt16OrDefault { static constexpr auto name = "toUInt16OrDefault"; };
struct NameToUInt32OrDefault { static constexpr auto name = "toUInt32OrDefault"; };
struct NameToUInt64OrDefault { static constexpr auto name = "toUInt64OrDefault"; };
struct NameToUInt256OrDefault { static constexpr auto name = "toUInt256OrDefault"; };
struct NameToInt8OrDefault { static constexpr auto name = "toInt8OrDefault"; };
struct NameToInt16OrDefault { static constexpr auto name = "toInt16OrDefault"; };
struct NameToInt32OrDefault { static constexpr auto name = "toInt32OrDefault"; };
struct NameToInt64OrDefault { static constexpr auto name = "toInt64OrDefault"; };
struct NameToInt128OrDefault { static constexpr auto name = "toInt128OrDefault"; };
struct NameToInt256OrDefault { static constexpr auto name = "toInt256OrDefault"; };
struct NameToFloat32OrDefault { static constexpr auto name = "toFloat32OrDefault"; };
struct NameToFloat64OrDefault { static constexpr auto name = "toFloat64OrDefault"; };
struct NameToDateOrDefault { static constexpr auto name = "toDateOrDefault"; };
struct NameToDate32OrDefault { static constexpr auto name = "toDate32OrDefault"; };
struct NameToDateTimeOrDefault { static constexpr auto name = "toDateTimeOrDefault"; };
struct NameToDateTime64OrDefault { static constexpr auto name = "toDateTime64OrDefault"; };
struct NameToDecimal32OrDefault { static constexpr auto name = "toDecimal32OrDefault"; };
struct NameToDecimal64OrDefault { static constexpr auto name = "toDecimal64OrDefault"; };
struct NameToDecimal128OrDefault { static constexpr auto name = "toDecimal128OrDefault"; };
struct NameToDecimal256OrDefault { static constexpr auto name = "toDecimal256OrDefault"; };
struct NameToUUIDOrDefault { static constexpr auto name = "toUUIDOrDefault"; };

using FunctionToUInt8OrDefault = FunctionCastOrDefaultTyped<DataTypeUInt8, NameToUInt8OrDefault>;
using FunctionToUInt16OrDefault = FunctionCastOrDefaultTyped<DataTypeUInt16, NameToUInt16OrDefault>;
using FunctionToUInt32OrDefault = FunctionCastOrDefaultTyped<DataTypeUInt32, NameToUInt32OrDefault>;
using FunctionToUInt64OrDefault = FunctionCastOrDefaultTyped<DataTypeUInt64, NameToUInt64OrDefault>;
using FunctionToUInt256OrDefault = FunctionCastOrDefaultTyped<DataTypeUInt256, NameToUInt256OrDefault>;

using FunctionToInt8OrDefault = FunctionCastOrDefaultTyped<DataTypeInt8, NameToInt8OrDefault>;
using FunctionToInt16OrDefault = FunctionCastOrDefaultTyped<DataTypeInt16, NameToInt16OrDefault>;
using FunctionToInt32OrDefault = FunctionCastOrDefaultTyped<DataTypeInt32, NameToInt32OrDefault>;
using FunctionToInt64OrDefault = FunctionCastOrDefaultTyped<DataTypeInt64, NameToInt64OrDefault>;
using FunctionToInt128OrDefault = FunctionCastOrDefaultTyped<DataTypeInt128, NameToInt128OrDefault>;
using FunctionToInt256OrDefault = FunctionCastOrDefaultTyped<DataTypeInt256, NameToInt256OrDefault>;

using FunctionToFloat32OrDefault = FunctionCastOrDefaultTyped<DataTypeFloat32, NameToFloat32OrDefault>;
using FunctionToFloat64OrDefault = FunctionCastOrDefaultTyped<DataTypeFloat64, NameToFloat64OrDefault>;

using FunctionToDateOrDefault = FunctionCastOrDefaultTyped<DataTypeDate, NameToDateOrDefault>;
using FunctionToDate32OrDefault = FunctionCastOrDefaultTyped<DataTypeDate32, NameToDate32OrDefault>;
using FunctionToDateTimeOrDefault = FunctionCastOrDefaultTyped<DataTypeDateTime, NameToDateTimeOrDefault>;
using FunctionToDateTime64OrDefault = FunctionCastOrDefaultTyped<DataTypeDateTime64, NameToDateTime64OrDefault>;

using FunctionToDecimal32OrDefault = FunctionCastOrDefaultTyped<DataTypeDecimal<Decimal32>, NameToDecimal32OrDefault>;
using FunctionToDecimal64OrDefault = FunctionCastOrDefaultTyped<DataTypeDecimal<Decimal64>, NameToDecimal64OrDefault>;
using FunctionToDecimal128OrDefault = FunctionCastOrDefaultTyped<DataTypeDecimal<Decimal128>, NameToDecimal128OrDefault>;
using FunctionToDecimal256OrDefault = FunctionCastOrDefaultTyped<DataTypeDecimal<Decimal256>, NameToDecimal256OrDefault>;

using FunctionToUUIDOrDefault = FunctionCastOrDefaultTyped<DataTypeUUID, NameToUUIDOrDefault>;

void registerFunctionCastOrDefault(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCastOrDefault>();

    factory.registerFunction<FunctionToUInt8OrDefault>();
    factory.registerFunction<FunctionToUInt16OrDefault>();
    factory.registerFunction<FunctionToUInt32OrDefault>();
    factory.registerFunction<FunctionToUInt64OrDefault>();
    factory.registerFunction<FunctionToUInt256OrDefault>();

    factory.registerFunction<FunctionToInt8OrDefault>();
    factory.registerFunction<FunctionToInt16OrDefault>();
    factory.registerFunction<FunctionToInt32OrDefault>();
    factory.registerFunction<FunctionToInt64OrDefault>();
    factory.registerFunction<FunctionToInt128OrDefault>();
    factory.registerFunction<FunctionToInt256OrDefault>();

    factory.registerFunction<FunctionToFloat32OrDefault>();
    factory.registerFunction<FunctionToFloat64OrDefault>();

    factory.registerFunction<FunctionToDateOrDefault>();
    factory.registerFunction<FunctionToDate32OrDefault>();
    factory.registerFunction<FunctionToDateTimeOrDefault>();
    factory.registerFunction<FunctionToDateTime64OrDefault>();

    factory.registerFunction<FunctionToDecimal32OrDefault>();
    factory.registerFunction<FunctionToDecimal64OrDefault>();
    factory.registerFunction<FunctionToDecimal128OrDefault>();
    factory.registerFunction<FunctionToDecimal256OrDefault>();

    factory.registerFunction<FunctionToUUIDOrDefault>();
}

}
