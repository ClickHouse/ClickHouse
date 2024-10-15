#include <Core/Settings.h>
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
#include <DataTypes/DataTypeIPv4andIPv6.h>
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
namespace Setting
{
    extern const SettingsBool cast_keep_nullable;
}

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

    explicit FunctionCastOrDefault(ContextPtr context_) : keep_nullable(context_->getSettingsRef()[Setting::cast_keep_nullable]) { }

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

            if (!result_type->equals(*default_value_type))
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

class FunctionCastOrDefaultTyped final : public IFunction
{
public:
    explicit FunctionCastOrDefaultTyped(ContextPtr context_, String name_, DataTypePtr type_)
        : impl(context_), name(std::move(name_)), type(std::move(type_)), which(type)
    {
    }

    String getName() const override { return name; }

private:
    FunctionCastOrDefault impl;
    String name;
    DataTypePtr type;
    WhichDataType which;

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
        FunctionArgumentDescriptors mandatory_args = {{"Value", nullptr, nullptr, "any type"}};
        FunctionArgumentDescriptors optional_args;

        if (isDecimal(type) || isDateTime64(type))
            mandatory_args.push_back({"scale", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), &isColumnConst, "const Integer"});

        if (isDateTimeOrDateTime64(type))
            optional_args.push_back({"timezone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"});

        optional_args.push_back({"default_value", nullptr, nullptr, "any type"});

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        size_t additional_argument_index = 1;

        size_t scale = 0;
        std::string time_zone;

        if (isDecimal(type) || isDateTime64(type))
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

        if (isDateTimeOrDateTime64(type))
        {
            if (additional_argument_index < arguments.size())
            {
                time_zone = extractTimeZoneNameFromColumn(arguments[additional_argument_index].column.get(),
                                                          arguments[additional_argument_index].name);
                ++additional_argument_index;
            }
        }

        DataTypePtr cast_type;

        if (which.isDateTime64())
            cast_type = std::make_shared<DataTypeDateTime64>(scale, time_zone);
        else if (which.isDateTime())
            cast_type = std::make_shared<DataTypeDateTime>(time_zone);
        else if (which.isDecimal32())
            cast_type = createDecimalMaxPrecision<Decimal32>(scale);
        else if (which.isDecimal64())
            cast_type = createDecimalMaxPrecision<Decimal64>(scale);
        else if (which.isDecimal128())
            cast_type = createDecimalMaxPrecision<Decimal128>(scale);
        else if (which.isDecimal256())
            cast_type = createDecimalMaxPrecision<Decimal256>(scale);
        else
            cast_type = type;

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
        /// Scale and time zone
        size_t additional_arguments_size = (which.isDecimal() || which.isDateTime64()) + which.isDateTimeOrDateTime64();

        ColumnWithTypeAndName second_argument =
        {
            DataTypeString().createColumnConst(arguments.begin()->column->size(), result_type->getName()),
            std::make_shared<DataTypeString>(),
            ""
        };

        ColumnsWithTypeAndName arguments_with_cast_type;
        arguments_with_cast_type.reserve(arguments.size() + 1);

        arguments_with_cast_type.emplace_back(arguments[0]);
        arguments_with_cast_type.emplace_back(second_argument);

        size_t default_column_argument = 1 + additional_arguments_size;
        if (default_column_argument < arguments.size())
            arguments_with_cast_type.emplace_back(arguments[default_column_argument]);

        return impl.executeImpl(arguments_with_cast_type, result_type, input_rows_size);
    }
};

REGISTER_FUNCTION(CastOrDefault)
{
    factory.registerFunction<FunctionCastOrDefault>();

    factory.registerFunction("toUInt8OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt8OrDefault", std::make_shared<DataTypeUInt8>()); });
    factory.registerFunction("toUInt16OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt16OrDefault", std::make_shared<DataTypeUInt16>()); });
    factory.registerFunction("toUInt32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt32OrDefault", std::make_shared<DataTypeUInt32>()); });
    factory.registerFunction("toUInt64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt64OrDefault", std::make_shared<DataTypeUInt64>()); });
    factory.registerFunction("toUInt128OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt128OrDefault", std::make_shared<DataTypeUInt128>()); },
        FunctionDocumentation{
            .description=R"(
Converts a string in the first argument of the function to UInt128 by parsing it.
If it cannot parse the value, returns the default value, which can be provided as the second function argument, and if provided, must be of UInt128 type.
If the default value is not provided in the second argument, it is assumed to be zero.
)",
            .examples{
                {"Successful conversion", "SELECT toUInt128OrDefault('1', 2::UInt128)", "1"},
                {"Default value", "SELECT toUInt128OrDefault('upyachka', 123456789012345678901234567890::UInt128)", "123456789012345678901234567890"},
                {"Implicit default value", "SELECT toUInt128OrDefault('upyachka')", "0"}},
            .categories{"ConversionFunctions"}
        });
    factory.registerFunction("toUInt256OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt256OrDefault", std::make_shared<DataTypeUInt256>()); });

    factory.registerFunction("toInt8OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt8OrDefault", std::make_shared<DataTypeInt8>()); });
    factory.registerFunction("toInt16OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt16OrDefault", std::make_shared<DataTypeInt16>()); });
    factory.registerFunction("toInt32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt32OrDefault", std::make_shared<DataTypeInt32>()); });
    factory.registerFunction("toInt64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt64OrDefault", std::make_shared<DataTypeInt64>()); });
    factory.registerFunction("toInt128OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt128OrDefault", std::make_shared<DataTypeInt128>()); });
    factory.registerFunction("toInt256OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt256OrDefault", std::make_shared<DataTypeInt256>()); });

    factory.registerFunction("toFloat32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toFloat32OrDefault", std::make_shared<DataTypeFloat32>()); });
    factory.registerFunction("toFloat64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toFloat64OrDefault", std::make_shared<DataTypeFloat64>()); });

    factory.registerFunction("toDateOrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDateOrDefault", std::make_shared<DataTypeDate>()); });
    factory.registerFunction("toDate32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDate32OrDefault", std::make_shared<DataTypeDate32>()); });
    factory.registerFunction("toDateTimeOrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDateTimeOrDefault", std::make_shared<DataTypeDateTime>()); });
    factory.registerFunction("toDateTime64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDateTime64OrDefault", std::make_shared<DataTypeDateTime64>(3 /* default scale */)); });

    factory.registerFunction("toDecimal32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal32OrDefault", createDecimalMaxPrecision<Decimal32>(0)); });
    factory.registerFunction("toDecimal64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal64OrDefault", createDecimalMaxPrecision<Decimal64>(0)); });
    factory.registerFunction("toDecimal128OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal128OrDefault", createDecimalMaxPrecision<Decimal128>(0)); });
    factory.registerFunction("toDecimal256OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal256OrDefault", createDecimalMaxPrecision<Decimal256>(0)); });

    factory.registerFunction("toUUIDOrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUUIDOrDefault", std::make_shared<DataTypeUUID>()); });
    factory.registerFunction("toIPv4OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toIPv4OrDefault", std::make_shared<DataTypeIPv4>()); });
    factory.registerFunction("toIPv6OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toIPv6OrDefault", std::make_shared<DataTypeIPv6>()); });
}

}
