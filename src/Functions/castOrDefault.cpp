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
    /// accurateCastOrDefault documentation
    FunctionDocumentation::Description accurateCastOrDefault_description = R"(
Converts a value to a specified data type.
Like [`accurateCast`](#accurateCast), but returns a default value instead of throwing an exception if the conversion cannot be performed accurately.

If a default value is provided as the second argument, it must be of the target type.
If no default value is provided, the default value of the target type is used.
    )";
    FunctionDocumentation::Syntax accurateCastOrDefault_syntax = "accurateCastOrDefault(x, T[, default_value])";
    FunctionDocumentation::Arguments accurateCastOrDefault_arguments = {
        {"x", "A value to convert.", {"Any"}},
        {"T", "The target data type name.", {"const String"}},
        {"default_value", "Optional. Default value to return if conversion fails.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue accurateCastOrDefault_returned_value = {"Returns the converted value with the target data type, or the default value if conversion is not possible.", {"Any"}};
    FunctionDocumentation::Examples accurateCastOrDefault_examples = {
    {
        "Successful conversion",
        R"(
SELECT accurateCastOrDefault(42, 'String')
        )",
        R"(
┌─accurateCastOrDefault(42, 'String')─┐
│ 42                                  │
└─────────────────────────────────────┘
        )"
    },
    {
        "Failed conversion with explicit default",
        R"(
SELECT accurateCastOrDefault('abc', 'UInt32', 999::UInt32)
        )",
        R"(
┌─accurateCastOrDefault('abc', 'UInt32', 999)─┐
│                                         999 │
└─────────────────────────────────────────────┘
        )"
    },
    {
        "Failed conversion with implicit default",
        R"(
SELECT accurateCastOrDefault('abc', 'UInt32')
        )",
        R"(
┌─accurateCastOrDefault('abc', 'UInt32')─┐
│                                      0 │
└────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn accurateCastOrDefault_introduced_in = {21, 1};
    FunctionDocumentation::Category accurateCastOrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation accurateCastOrDefault_documentation = {accurateCastOrDefault_description, accurateCastOrDefault_syntax, accurateCastOrDefault_arguments, accurateCastOrDefault_returned_value, accurateCastOrDefault_examples, accurateCastOrDefault_introduced_in, accurateCastOrDefault_category};

    factory.registerFunction<FunctionCastOrDefault>(accurateCastOrDefault_documentation);

    FunctionDocumentation::Description toUInt8OrDefault_description = R"(
Like [`toUInt8`](#toUInt8), this function converts an input value to a value of type [UInt8](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toUInt8OrDefault_syntax = "toUInt8OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toUInt8OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue toUInt8OrDefault_returned_value = {"Returns a value of type UInt8 if successful, otherwise returns the default value if passed, or 0 if not.", {"UInt8"}};
    FunctionDocumentation::Examples toUInt8OrDefault_examples = {
        {"Successful conversion", "SELECT toUInt8OrDefault('8', CAST('0', 'UInt8'))", "8"},
        {"Failed conversion", "SELECT toUInt8OrDefault('abc', CAST('0', 'UInt8'))", "0"}
    };
    FunctionDocumentation::Category toUInt8OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toUInt8OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toUInt8OrDefault_documentation = {toUInt8OrDefault_description, toUInt8OrDefault_syntax, toUInt8OrDefault_arguments, toUInt8OrDefault_returned_value, toUInt8OrDefault_examples, toUInt8OrDefault_introduced_in, toUInt8OrDefault_category};

    factory.registerFunction("toUInt8OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt8OrDefault", std::make_shared<DataTypeUInt8>()); },
        toUInt8OrDefault_documentation);
    FunctionDocumentation::Description toUInt16OrDefault_description = R"(
Like [`toUInt16`](#toUInt16), this function converts an input value to a value of type [UInt16](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toUInt16OrDefault_syntax = "toUInt16OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toUInt16OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"UInt16"}}
    };
    FunctionDocumentation::ReturnedValue toUInt16OrDefault_returned_value = {"Returns a value of type UInt16 if successful, otherwise returns the default value if passed, or 0 if not.", {"UInt16"}};
    FunctionDocumentation::Examples toUInt16OrDefault_examples = {
        {"Successful conversion", "SELECT toUInt16OrDefault('16', CAST('0', 'UInt16'))", "16"},
        {"Failed conversion", "SELECT toUInt16OrDefault('abc', CAST('0', 'UInt16'))", "0"}
    };
    FunctionDocumentation::Category toUInt16OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toUInt16OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toUInt16OrDefault_documentation = {toUInt16OrDefault_description, toUInt16OrDefault_syntax, toUInt16OrDefault_arguments, toUInt16OrDefault_returned_value, toUInt16OrDefault_examples, toUInt16OrDefault_introduced_in, toUInt16OrDefault_category};

    factory.registerFunction("toUInt16OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt16OrDefault", std::make_shared<DataTypeUInt16>()); },
        toUInt16OrDefault_documentation);
    FunctionDocumentation::Description toUInt32OrDefault_description = R"(
Like [`toUInt32`](#toUInt32), this function converts an input value to a value of type [UInt32](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toUInt32OrDefault_syntax = "toUInt32OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toUInt32OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue toUInt32OrDefault_returned_value = {"Returns a value of type UInt32 if successful, otherwise returns the default value if passed, or 0 if not.", {"UInt32"}};
    FunctionDocumentation::Examples toUInt32OrDefault_examples = {
        {"Successful conversion", "SELECT toUInt32OrDefault('32', CAST('0', 'UInt32'))", "32"},
        {"Failed conversion", "SELECT toUInt32OrDefault('abc', CAST('0', 'UInt32'))", "0"}
    };
    FunctionDocumentation::Category toUInt32OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toUInt32OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toUInt32OrDefault_documentation = {toUInt32OrDefault_description, toUInt32OrDefault_syntax, toUInt32OrDefault_arguments, toUInt32OrDefault_returned_value, toUInt32OrDefault_examples, toUInt32OrDefault_introduced_in, toUInt32OrDefault_category};

    factory.registerFunction("toUInt32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt32OrDefault", std::make_shared<DataTypeUInt32>()); },
        toUInt32OrDefault_documentation);
    FunctionDocumentation::Description toUInt64OrDefault_description = R"(
Like [`toUInt64`](#toUInt64), this function converts an input value to a value of type [UInt64](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toUInt64OrDefault_syntax = "toUInt64OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toUInt64OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue toUInt64OrDefault_returned_value = {"Returns a value of type UInt64 if successful, otherwise returns the default value if passed, or 0 if not.", {"UInt64"}};
    FunctionDocumentation::Examples toUInt64OrDefault_examples = {
        {"Successful conversion", "SELECT toUInt64OrDefault('64', CAST('0', 'UInt64'))", "64"},
        {"Failed conversion", "SELECT toUInt64OrDefault('abc', CAST('0', 'UInt64'))", "0"}
    };
    FunctionDocumentation::Category toUInt64OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toUInt64OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toUInt64OrDefault_documentation = {toUInt64OrDefault_description, toUInt64OrDefault_syntax, toUInt64OrDefault_arguments, toUInt64OrDefault_returned_value, toUInt64OrDefault_examples, toUInt64OrDefault_introduced_in, toUInt64OrDefault_category};

    factory.registerFunction("toUInt64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt64OrDefault", std::make_shared<DataTypeUInt64>()); },
        toUInt64OrDefault_documentation);
    FunctionDocumentation::Description toUInt128OrDefault_description = R"(
Like [`toUInt128`](#toUInt128), this function converts an input value to a value of type [`UInt128`](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toUInt128OrDefault_syntax = "toUInt128OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toUInt128OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"UInt128"}}
    };
    FunctionDocumentation::ReturnedValue toUInt128OrDefault_returned_value = {"Returns a value of type UInt128 if successful, otherwise returns the default value if passed, or 0 if not.", {"UInt128"}};
    FunctionDocumentation::Examples toUInt128OrDefault_examples = {
        {"Successful conversion", "SELECT toUInt128OrDefault('128', CAST('0', 'UInt128'))", "128"},
        {"Failed conversion", "SELECT toUInt128OrDefault('abc', CAST('0', 'UInt128'))", "0"}
    };
    FunctionDocumentation::Category toUInt128OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toUInt128OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toUInt128OrDefault_documentation = {toUInt128OrDefault_description, toUInt128OrDefault_syntax, toUInt128OrDefault_arguments, toUInt128OrDefault_returned_value, toUInt128OrDefault_examples, toUInt128OrDefault_introduced_in, toUInt128OrDefault_category};

    factory.registerFunction("toUInt128OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt128OrDefault", std::make_shared<DataTypeUInt128>()); },
        toUInt128OrDefault_documentation);
    FunctionDocumentation::Description toUInt256OrDefault_description = R"(
Like [`toUInt256`](#toUInt256), this function converts an input value to a value of type [UInt256](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toUInt256OrDefault_syntax = "toUInt256OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toUInt256OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"UInt256"}}
    };
    FunctionDocumentation::ReturnedValue toUInt256OrDefault_returned_value = {"Returns a value of type UInt256 if successful, otherwise returns the default value if passed, or 0 if not.", {"UInt256"}};
    FunctionDocumentation::Examples toUInt256OrDefault_examples = {
        {"Successful conversion", "SELECT toUInt256OrDefault('-256', CAST('0', 'UInt256'))", "0"},
        {"Failed conversion", "SELECT toUInt256OrDefault('abc', CAST('0', 'UInt256'))", "0"}
    };
    FunctionDocumentation::Category toUInt256OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toUInt256OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toUInt256OrDefault_documentation = {toUInt256OrDefault_description, toUInt256OrDefault_syntax, toUInt256OrDefault_arguments, toUInt256OrDefault_returned_value, toUInt256OrDefault_examples, toUInt256OrDefault_introduced_in, toUInt256OrDefault_category};

    factory.registerFunction("toUInt256OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUInt256OrDefault", std::make_shared<DataTypeUInt256>()); },
        toUInt256OrDefault_documentation);

    FunctionDocumentation::Description toInt8OrDefault_description = R"(
Like [`toInt8`](#toInt8), this function converts an input value to a value of type [Int8](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toInt8OrDefault_syntax = "toInt8OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toInt8OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Int8"}}
    };
    FunctionDocumentation::ReturnedValue toInt8OrDefault_returned_value = {"Returns a value of type Int8 if successful, otherwise returns the default value if passed, or 0 if not.", {"Int8"}};
    FunctionDocumentation::Examples toInt8OrDefault_examples = {
        {"Successful conversion", "SELECT toInt8OrDefault('-8', CAST('-1', 'Int8'))", "-8"},
        {"Failed conversion", "SELECT toInt8OrDefault('abc', CAST('-1', 'Int8'))", "-1"}
    };
    FunctionDocumentation::Category toInt8OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toInt8OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toInt8OrDefault_documentation = {toInt8OrDefault_description, toInt8OrDefault_syntax, toInt8OrDefault_arguments, toInt8OrDefault_returned_value, toInt8OrDefault_examples, toInt8OrDefault_introduced_in, toInt8OrDefault_category};

    factory.registerFunction("toInt8OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt8OrDefault", std::make_shared<DataTypeInt8>()); },
        toInt8OrDefault_documentation);
    FunctionDocumentation::Description toInt16OrDefault_description = R"(
Like [`toInt16`](#toInt16), this function converts an input value to a value of type [Int16](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toInt16OrDefault_syntax = "toInt16OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toInt16OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Int16"}}
    };
    FunctionDocumentation::ReturnedValue toInt16OrDefault_returned_value = {"Returns a value of type Int16 if successful, otherwise returns the default value if passed, or 0 if not.", {"Int16"}};
    FunctionDocumentation::Examples toInt16OrDefault_examples = {
        {"Successful conversion", "SELECT toInt16OrDefault('-16', CAST('-1', 'Int16'))", "-16"},
        {"Failed conversion", "SELECT toInt16OrDefault('abc', CAST('-1', 'Int16'))", "-1"}
    };
    FunctionDocumentation::Category toInt16OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toInt16OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toInt16OrDefault_documentation = {toInt16OrDefault_description, toInt16OrDefault_syntax, toInt16OrDefault_arguments, toInt16OrDefault_returned_value, toInt16OrDefault_examples, toInt16OrDefault_introduced_in, toInt16OrDefault_category};

    factory.registerFunction("toInt16OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt16OrDefault", std::make_shared<DataTypeInt16>()); },
        toInt16OrDefault_documentation);
    FunctionDocumentation::Description toInt32OrDefault_description = R"(
Like [`toInt32`](#toInt32), this function converts an input value to a value of type [Int32](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toInt32OrDefault_syntax = "toInt32OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toInt32OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Int32"}}
    };
    FunctionDocumentation::ReturnedValue toInt32OrDefault_returned_value = {"Returns a value of type Int32 if successful, otherwise returns the default value if passed or 0 if not.", {"Int32"}};
    FunctionDocumentation::Examples toInt32OrDefault_examples = {
        {"Successful conversion", "SELECT toInt32OrDefault('-32', CAST('-1', 'Int32'))", "-32"},
        {"Failed conversion", "SELECT toInt32OrDefault('abc', CAST('-1', 'Int32'))", "-1"}
    };
    FunctionDocumentation::Category toInt32OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toInt32OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toInt32OrDefault_documentation = {toInt32OrDefault_description, toInt32OrDefault_syntax, toInt32OrDefault_arguments, toInt32OrDefault_returned_value, toInt32OrDefault_examples, toInt32OrDefault_introduced_in, toInt32OrDefault_category};

    factory.registerFunction("toInt32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt32OrDefault", std::make_shared<DataTypeInt32>()); },
        toInt32OrDefault_documentation);
    FunctionDocumentation::Description toInt64OrDefault_description = R"(
Like [`toInt64`](#toInt64), this function converts an input value to a value of type [Int64](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toInt64OrDefault_syntax = "toInt64OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toInt64OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Int64"}}
    };
    FunctionDocumentation::ReturnedValue toInt64OrDefault_returned_value = {"Returns a value of type Int64 if successful, otherwise returns the default value if passed, or 0 if not.", {"Int64"}};
    FunctionDocumentation::Examples toInt64OrDefault_examples = {
        {"Successful conversion", "SELECT toInt64OrDefault('-64', CAST('-1', 'Int64'))", "-64"},
        {"Failed conversion", "SELECT toInt64OrDefault('abc', CAST('-1', 'Int64'))", "-1"}
    };
    FunctionDocumentation::Category toInt64OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toInt64OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toInt64OrDefault_documentation = {toInt64OrDefault_description, toInt64OrDefault_syntax, toInt64OrDefault_arguments, toInt64OrDefault_returned_value, toInt64OrDefault_examples, toInt64OrDefault_introduced_in, toInt64OrDefault_category};

    factory.registerFunction("toInt64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt64OrDefault", std::make_shared<DataTypeInt64>()); },
        toInt64OrDefault_documentation);
    FunctionDocumentation::Description toInt128OrDefault_description = R"(
Like [`toInt128`](#toInt128), this function converts an input value to a value of type [Int128](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toInt128OrDefault_syntax = "toInt128OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toInt128OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Int128"}}
    };
    FunctionDocumentation::ReturnedValue toInt128OrDefault_returned_value = {"Returns a value of type Int128 if successful, otherwise returns the default value if passed, or 0 if not.", {"Int128"}};
    FunctionDocumentation::Examples toInt128OrDefault_examples = {
        {"Successful conversion", "SELECT toInt128OrDefault('-128', CAST('-1', 'Int128'))", "-128"},
        {"Failed conversion", "SELECT toInt128OrDefault('abc', CAST('-1', 'Int128'))", "-1"}
    };
    FunctionDocumentation::Category toInt128OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toInt128OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toInt128OrDefault_documentation = {toInt128OrDefault_description, toInt128OrDefault_syntax, toInt128OrDefault_arguments, toInt128OrDefault_returned_value, toInt128OrDefault_examples, toInt128OrDefault_introduced_in, toInt128OrDefault_category};

    factory.registerFunction("toInt128OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt128OrDefault", std::make_shared<DataTypeInt128>()); },
        toInt128OrDefault_documentation);
    FunctionDocumentation::Description toInt256OrDefault_description = R"(
Like [`toInt256`](#toInt256), this function converts an input value to a value of type [Int256](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toInt256OrDefault_syntax = "toInt256OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toInt256OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Int256"}}
    };
    FunctionDocumentation::ReturnedValue toInt256OrDefault_returned_value = {"Returns a value of type Int256 if successful, otherwise returns the default value if passed, or 0 if not.", {"Int256"}};
    FunctionDocumentation::Examples toInt256OrDefault_examples = {
        {"Successful conversion", "SELECT toInt256OrDefault('-256', CAST('-1', 'Int256'))", "-256"},
        {"Failed conversion", "SELECT toInt256OrDefault('abc', CAST('-1', 'Int256'))", "-1"}
    };
    FunctionDocumentation::Category toInt256OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toInt256OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toInt256OrDefault_documentation = {toInt256OrDefault_description, toInt256OrDefault_syntax, toInt256OrDefault_arguments, toInt256OrDefault_returned_value, toInt256OrDefault_examples, toInt256OrDefault_introduced_in, toInt256OrDefault_category};

    factory.registerFunction("toInt256OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toInt256OrDefault", std::make_shared<DataTypeInt256>()); },
        toInt256OrDefault_documentation);

    FunctionDocumentation::Description toFloat32OrDefault_description = R"(
Like [`toFloat32`](#toFloat32), this function converts an input value to a value of type [Float32](../data-types/float.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toFloat32OrDefault_syntax = "toFloat32OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toFloat32OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Float32"}}
    };
    FunctionDocumentation::ReturnedValue toFloat32OrDefault_returned_value = {"Returns a value of type Float32 if successful, otherwise returns the default value if passed or 0 if not.", {"Float32"}};
    FunctionDocumentation::Examples toFloat32OrDefault_examples = {
        {"Successful conversion", "SELECT toFloat32OrDefault('8', CAST('0', 'Float32'))", "8"},
        {"Failed conversion", "SELECT toFloat32OrDefault('abc', CAST('0', 'Float32'))", "0"}
    };
    FunctionDocumentation::Category toFloat32OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toFloat32OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toFloat32OrDefault_documentation = {toFloat32OrDefault_description, toFloat32OrDefault_syntax, toFloat32OrDefault_arguments, toFloat32OrDefault_returned_value, toFloat32OrDefault_examples, toFloat32OrDefault_introduced_in, toFloat32OrDefault_category};

    factory.registerFunction("toFloat32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toFloat32OrDefault", std::make_shared<DataTypeFloat32>()); },
        toFloat32OrDefault_documentation);
    FunctionDocumentation::Description toFloat64OrDefault_description = R"(
Like [`toFloat64`](#toFloat64), this function converts an input value to a value of type [Float64](../data-types/float.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.
    )";
    FunctionDocumentation::Syntax toFloat64OrDefault_syntax = "toFloat64OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toFloat64OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue toFloat64OrDefault_returned_value = {"Returns a value of type Float64 if successful, otherwise returns the default value if passed or 0 if not.", {"Float64"}};
    FunctionDocumentation::Examples toFloat64OrDefault_examples = {
        {"Successful conversion", "SELECT toFloat64OrDefault('8', CAST('0', 'Float64'))", "8"},
        {"Failed conversion", "SELECT toFloat64OrDefault('abc', CAST('0', 'Float64'))", "0"}
    };
    FunctionDocumentation::Category toFloat64OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toFloat64OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toFloat64OrDefault_documentation = {toFloat64OrDefault_description, toFloat64OrDefault_syntax, toFloat64OrDefault_arguments, toFloat64OrDefault_returned_value, toFloat64OrDefault_examples, toFloat64OrDefault_introduced_in, toFloat64OrDefault_category};

    factory.registerFunction("toFloat64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toFloat64OrDefault", std::make_shared<DataTypeFloat64>()); },
        toFloat64OrDefault_documentation);

    FunctionDocumentation::Description toDateOrDefault_description = R"(
Like [toDate](#toDate) but if unsuccessful, returns a default value which is either the second argument (if specified), or otherwise the lower boundary of [Date](../data-types/date.md).
    )";
    FunctionDocumentation::Syntax toDateOrDefault_syntax = "toDateOrDefault(expr[, default])";
    FunctionDocumentation::Arguments toDateOrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Date"}}
    };
    FunctionDocumentation::ReturnedValue toDateOrDefault_returned_value = {"Value of type Date if successful, otherwise returns the default value if passed or 1970-01-01 if not.", {"Date"}};
    FunctionDocumentation::Examples toDateOrDefault_examples = {
        {"Successful conversion", "SELECT toDateOrDefault('2022-12-30')", "2022-12-30"},
        {"Failed conversion", "SELECT toDateOrDefault('', CAST('2023-01-01', 'Date'))", "2023-01-01"}
    };
    FunctionDocumentation::Category toDateOrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDateOrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDateOrDefault_documentation = {toDateOrDefault_description, toDateOrDefault_syntax, toDateOrDefault_arguments, toDateOrDefault_returned_value, toDateOrDefault_examples, toDateOrDefault_introduced_in, toDateOrDefault_category};

    factory.registerFunction("toDateOrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDateOrDefault", std::make_shared<DataTypeDate>()); },
        toDateOrDefault_documentation);
    FunctionDocumentation::Description toDate32OrDefault_description = R"(
Converts the argument to the [Date32](../data-types/date32.md) data type. If the value is outside the range, `toDate32OrDefault` returns the lower border value supported by [Date32](../data-types/date32.md). If the argument has [Date](../data-types/date.md) type, it's borders are taken into account. Returns default value if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax toDate32OrDefault_syntax = "toDate32OrDefault(expr[, default])";
    FunctionDocumentation::Arguments toDate32OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"Date32"}}
    };
    FunctionDocumentation::ReturnedValue toDate32OrDefault_returned_value = {"Value of type Date32 if successful, otherwise returns the default value if passed or 1900-01-01 if not.", {"Date32"}};
    FunctionDocumentation::Examples toDate32OrDefault_examples = {
        {"Successful conversion", "SELECT toDate32OrDefault('1930-01-01', toDate32('2020-01-01'))", "1930-01-01"},
        {"Failed conversion", "SELECT toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'))", "2020-01-01"}
    };
    FunctionDocumentation::Category toDate32OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDate32OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDate32OrDefault_documentation = {toDate32OrDefault_description, toDate32OrDefault_syntax, toDate32OrDefault_arguments, toDate32OrDefault_returned_value, toDate32OrDefault_examples, toDate32OrDefault_introduced_in, toDate32OrDefault_category};

    factory.registerFunction("toDate32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDate32OrDefault", std::make_shared<DataTypeDate32>()); },
        toDate32OrDefault_documentation);
    FunctionDocumentation::Description toDateTimeOrDefault_description = R"(
Like [toDateTime](#todatetime) but if unsuccessful, returns a default value which is either the third argument (if specified), or otherwise the lower boundary of [DateTime](../data-types/datetime.md).
    )";
    FunctionDocumentation::Syntax toDateTimeOrDefault_syntax = "toDateTimeOrDefault(expr[, timezone, default])";
    FunctionDocumentation::Arguments toDateTimeOrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"timezone", "Optional. Time zone.", {"String"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"DateTime"}}
    };
    FunctionDocumentation::ReturnedValue toDateTimeOrDefault_returned_value = {"Value of type DateTime if successful, otherwise returns the default value if passed or 1970-01-01 00:00:00 if not.", {"DateTime"}};
    FunctionDocumentation::Examples toDateTimeOrDefault_examples = {
        {"Successful conversion", "SELECT toDateTimeOrDefault('2022-12-30 13:44:17')", "2022-12-30 13:44:17"},
        {"Failed conversion", "SELECT toDateTimeOrDefault('', 'UTC', CAST('2023-01-01', 'DateTime(\\'UTC\\')'))", "2023-01-01 00:00:00"}
    };
    FunctionDocumentation::Category toDateTimeOrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDateTimeOrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDateTimeOrDefault_documentation = {toDateTimeOrDefault_description, toDateTimeOrDefault_syntax, toDateTimeOrDefault_arguments, toDateTimeOrDefault_returned_value, toDateTimeOrDefault_examples, toDateTimeOrDefault_introduced_in, toDateTimeOrDefault_category};
    factory.registerFunction("toDateTimeOrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDateTimeOrDefault", std::make_shared<DataTypeDateTime>()); },
        toDateTimeOrDefault_documentation);
    FunctionDocumentation::Description toDateTime64OrDefault_description = R"(
Like [toDateTime64](#todatetime64), this function converts an input value to a value of type [DateTime64](../data-types/datetime64.md),
but returns either the default value of [DateTime64](../data-types/datetime64.md)
or the provided default if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax toDateTime64OrDefault_syntax = "toDateTime64OrDefault(expr, scale[, timezone, default])";
    FunctionDocumentation::Arguments toDateTime64OrDefault_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"String", "(U)Int*", "Float*"}},
        {"scale", "Tick size (precision): 10^-precision seconds.", {"UInt8"}},
        {"timezone", "Optional. Time zone.", {"String"}},
        {"default", "Optional. The default value to return if parsing is unsuccessful.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue toDateTime64OrDefault_returned_value = {"Value of type DateTime64 if successful, otherwise returns the default value if passed or 1970-01-01 00:00:00.000 if not.", {"DateTime64"}};
    FunctionDocumentation::Examples toDateTime64OrDefault_examples = {
        {"Successful conversion", "SELECT toDateTime64OrDefault('1976-10-18 00:00:00.30', 3)", "1976-10-18 00:00:00.300"},
        {"Failed conversion", "SELECT toDateTime64OrDefault('1976-10-18 00:00:00 30', 3, 'UTC', toDateTime64('2001-01-01 00:00:00.00',3))", "2000-12-31 23:00:00.000"}
    };
    FunctionDocumentation::Category toDateTime64OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDateTime64OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDateTime64OrDefault_documentation = {toDateTime64OrDefault_description, toDateTime64OrDefault_syntax, toDateTime64OrDefault_arguments, toDateTime64OrDefault_returned_value, toDateTime64OrDefault_examples, toDateTime64OrDefault_introduced_in, toDateTime64OrDefault_category};

    factory.registerFunction("toDateTime64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDateTime64OrDefault", std::make_shared<DataTypeDateTime64>(3)); },
        toDateTime64OrDefault_documentation);

    FunctionDocumentation::Description toDecimal32OrDefault_description = R"(
Like [`toDecimal32`](#toDecimal32), this function converts an input value to a value of type [Decimal(9, S)](../data-types/decimal.md) but returns the default value in case of an error.
    )";
    FunctionDocumentation::Syntax toDecimal32OrDefault_syntax = "toDecimal32OrDefault(expr, S[, default])";
    FunctionDocumentation::Arguments toDecimal32OrDefault_arguments = {
        {"expr", "A String representation of a number.", {"String"}},
        {"S", "Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have.", {"UInt8"}},
        {"default", "Optional. The default value to return if parsing to type Decimal32(S) is unsuccessful.", {"Decimal32(S)"}}
    };
    FunctionDocumentation::ReturnedValue toDecimal32OrDefault_returned_value = {"Value of type Decimal(9, S) if successful, otherwise returns the default value if passed or 0 if not.", {"Decimal32(S)"}};
    FunctionDocumentation::Examples toDecimal32OrDefault_examples = {
        {"Successful conversion", "SELECT toDecimal32OrDefault(toString(0.0001), 5)", "0.0001"},
        {"Failed conversion", "SELECT toDecimal32OrDefault('Inf', 0, CAST('-1', 'Decimal32(0)'))", "-1"}
    };
    FunctionDocumentation::Category toDecimal32OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDecimal32OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDecimal32OrDefault_documentation = {toDecimal32OrDefault_description, toDecimal32OrDefault_syntax, toDecimal32OrDefault_arguments, toDecimal32OrDefault_returned_value, toDecimal32OrDefault_examples, toDecimal32OrDefault_introduced_in, toDecimal32OrDefault_category};

    factory.registerFunction("toDecimal32OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal32OrDefault", createDecimalMaxPrecision<Decimal32>(0)); },
        toDecimal32OrDefault_documentation);

    FunctionDocumentation::Description toDecimal64OrDefault_description = R"(
Like [`toDecimal64`](#toDecimal64), this function converts an input value to a value of type [Decimal(18, S)](../data-types/decimal.md) but returns the default value in case of an error.
    )";
    FunctionDocumentation::Syntax toDecimal64OrDefault_syntax = "toDecimal64OrDefault(expr, S[, default])";
    FunctionDocumentation::Arguments toDecimal64OrDefault_arguments = {
        {"expr", "A String representation of a number.", {"String"}},
        {"S", "Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have.", {"UInt8"}},
        {"default", "Optional. The default value to return if parsing to type Decimal64(S) is unsuccessful.", {"Decimal64(S)"}}
    };
    FunctionDocumentation::ReturnedValue toDecimal64OrDefault_returned_value = {"Value of type Decimal(18, S) if successful, otherwise returns the default value if passed or 0 if not.", {"Decimal64(S)"}};
    FunctionDocumentation::Examples toDecimal64OrDefault_examples = {
        {"Successful conversion", "SELECT toDecimal64OrDefault(toString(0.0001), 18)", "0.0001"},
        {"Failed conversion", "SELECT toDecimal64OrDefault('Inf', 0, CAST('-1', 'Decimal64(0)'))", "-1"}
    };
    FunctionDocumentation::Category toDecimal64OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDecimal64OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDecimal64OrDefault_documentation = {toDecimal64OrDefault_description, toDecimal64OrDefault_syntax, toDecimal64OrDefault_arguments, toDecimal64OrDefault_returned_value, toDecimal64OrDefault_examples, toDecimal64OrDefault_introduced_in, toDecimal64OrDefault_category};

    factory.registerFunction("toDecimal64OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal64OrDefault", createDecimalMaxPrecision<Decimal64>(0)); },
        toDecimal64OrDefault_documentation);

    FunctionDocumentation::Description toDecimal128OrDefault_description = R"(
Like [`toDecimal128`](#toDecimal128), this function converts an input value to a value of type [Decimal(38, S)](../data-types/decimal.md) but returns the default value in case of an error.
    )";
    FunctionDocumentation::Syntax toDecimal128OrDefault_syntax = "toDecimal128OrDefault(expr, S[, default])";
    FunctionDocumentation::Arguments toDecimal128OrDefault_arguments = {
        {"expr", "A String representation of a number.", {"String"}},
        {"S", "Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have.", {"UInt8"}},
        {"default", "Optional. The default value to return if parsing to type Decimal128(S) is unsuccessful.", {"Decimal128(S)"}}
    };
    FunctionDocumentation::ReturnedValue toDecimal128OrDefault_returned_value = {"Value of type Decimal(38, S) if successful, otherwise returns the default value if passed or 0 if not.", {"Decimal128(S)"}};
    FunctionDocumentation::Examples toDecimal128OrDefault_examples = {
        {"Successful conversion", "SELECT toDecimal128OrDefault(toString(1/42), 18)", "0.023809523809523808"},
        {"Failed conversion", "SELECT toDecimal128OrDefault('Inf', 0, CAST('-1', 'Decimal128(0)'))", "-1"}
    };
    FunctionDocumentation::Category toDecimal128OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDecimal128OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDecimal128OrDefault_documentation = {toDecimal128OrDefault_description, toDecimal128OrDefault_syntax, toDecimal128OrDefault_arguments, toDecimal128OrDefault_returned_value, toDecimal128OrDefault_examples, toDecimal128OrDefault_introduced_in, toDecimal128OrDefault_category};

    factory.registerFunction("toDecimal128OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal128OrDefault", createDecimalMaxPrecision<Decimal128>(0)); },
        toDecimal128OrDefault_documentation);

    FunctionDocumentation::Description toDecimal256OrDefault_description = R"(
Like [`toDecimal256`](#toDecimal256), this function converts an input value to a value of type [Decimal(76, S)](../data-types/decimal.md) but returns the default value in case of an error.
    )";
    FunctionDocumentation::Syntax toDecimal256OrDefault_syntax = "toDecimal256OrDefault(expr, S[, default])";
    FunctionDocumentation::Arguments toDecimal256OrDefault_arguments = {
        {"expr", "A String representation of a number.", {"String"}},
        {"S", "Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have.", {"UInt8"}},
        {"default", "Optional. The default value to return if parsing to type Decimal256(S) is unsuccessful.", {"Decimal256(S)"}}
    };
    FunctionDocumentation::ReturnedValue toDecimal256OrDefault_returned_value = {"Value of type Decimal(76, S) if successful, otherwise returns the default value if passed or 0 if not.", {"Decimal256(S)"}};
    FunctionDocumentation::Examples toDecimal256OrDefault_examples = {
        {"Successful conversion", "SELECT toDecimal256OrDefault(toString(1/42), 76)", "0.023809523809523808"},
        {"Failed conversion", "SELECT toDecimal256OrDefault('Inf', 0, CAST('-1', 'Decimal256(0)'))", "-1"}
    };
    FunctionDocumentation::Category toDecimal256OrDefault_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn toDecimal256OrDefault_introduced_in = {21, 11};
    FunctionDocumentation toDecimal256OrDefault_documentation = {toDecimal256OrDefault_description, toDecimal256OrDefault_syntax, toDecimal256OrDefault_arguments, toDecimal256OrDefault_returned_value, toDecimal256OrDefault_examples, toDecimal256OrDefault_introduced_in, toDecimal256OrDefault_category};

    factory.registerFunction("toDecimal256OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toDecimal256OrDefault", createDecimalMaxPrecision<Decimal256>(0)); },
        toDecimal256OrDefault_documentation);

    FunctionDocumentation::Description toUUIDOrDefault_description = R"(
Converts a String value to UUID type. If the conversion fails, returns a default UUID value instead of throwing an error.

This function attempts to parse a string of 36 characters in the standard UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
If the string cannot be converted to a valid UUID, the function returns the provided default UUID value.
    )";
    FunctionDocumentation::Syntax toUUIDOrDefault_syntax = "toUUIDOrDefault(string, default)";
    FunctionDocumentation::Arguments toUUIDOrDefault_arguments = {
        {"string", "String of 36 characters or FixedString(36) to be converted to UUID."},
        {"default", "UUID value to be returned if the first argument cannot be converted to UUID type."}
    };
    FunctionDocumentation::ReturnedValue toUUIDOrDefault_returned_value = {
        "Returns the converted UUID if successful, or the default UUID if conversion fails.",
        {"UUID"}
    };
    FunctionDocumentation::Examples toUUIDOrDefault_examples = {
    {
        "Successful conversion returns the parsed UUID",
        R"(
SELECT toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', toUUID('59f0c404-5cb3-11e7-907b-a6006ad3dba0'));
        )",
        R"(
┌─toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', toUUID('59f0c404-5cb3-11e7-907b-a6006ad3dba0'))─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                     │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Failed conversion returns the default UUID",
        R"(
SELECT toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', toUUID('59f0c404-5cb3-11e7-907b-a6006ad3dba0'));
        )",
        R"(
┌─toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', toUUID('59f0c404-5cb3-11e7-907b-a6006ad3dba0'))─┐
│ 59f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                          │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUUIDOrDefault_introduced_in = {21, 1};
    FunctionDocumentation::Category toUUIDOrDefault_category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation toUUIDOrDefault_documentation = {toUUIDOrDefault_description, toUUIDOrDefault_syntax, toUUIDOrDefault_arguments, toUUIDOrDefault_returned_value, toUUIDOrDefault_examples, toUUIDOrDefault_introduced_in, toUUIDOrDefault_category};

    factory.registerFunction("toUUIDOrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toUUIDOrDefault", std::make_shared<DataTypeUUID>()); }, toUUIDOrDefault_documentation);


    FunctionDocumentation::Description toIPv4OrDefault_description = R"(
Converts a string or a UInt32 form of an IPv4 address to [`IPv4`](../data-types/ipv4.md) type.
If the IPv4 address has an invalid format, it returns `0.0.0.0` (0 IPv4), or the provided IPv4 default.
    )";
    FunctionDocumentation::Syntax toIPv4OrDefault_syntax = "toIPv4OrDefault(string[, default])";
    FunctionDocumentation::Arguments toIPv4OrDefault_arguments = {
        {"string", "IP address string to convert.", {"String"}},
        {"default", "Optional. The value to return if string is an invalid IPv4 address.", {"IPv4"}}
    };
    FunctionDocumentation::ReturnedValue toIPv4OrDefault_returned_value = {"Returns a string converted to the current IPv4 address, or the default value if conversion fails.", {"IPv4"}};
    FunctionDocumentation::Examples toIPv4OrDefault_examples = {
    {
        "Valid and invalid IPv4 strings",
        R"(
WITH
    '192.168.1.1' AS valid_IPv4_string,
    '999.999.999.999' AS invalid_IPv4_string,
    'not_an_ip' AS malformed_string
SELECT
    toIPv4OrDefault(valid_IPv4_string) AS valid,
    toIPv4OrDefault(invalid_IPv4_string) AS default_value,
    toIPv4OrDefault(malformed_string, toIPv4('8.8.8.8')) AS provided_default;
        )",
        R"(
┌─valid─────────┬─default_value─┬─provided_default─┐
│ 192.168.1.1   │ 0.0.0.0       │ 8.8.8.8          │
└───────────────┴───────────────┴──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toIPv4OrDefault_introduced_in = {22, 3};
    FunctionDocumentation::Category toIPv4OrDefault_category = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation toIPv4OrDefault_documentation = {toIPv4OrDefault_description, toIPv4OrDefault_syntax, toIPv4OrDefault_arguments, toIPv4OrDefault_returned_value, toIPv4OrDefault_examples, toIPv4OrDefault_introduced_in, toIPv4OrDefault_category};

    factory.registerFunction("toIPv4OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toIPv4OrDefault", std::make_shared<DataTypeIPv4>()); },
        toIPv4OrDefault_documentation);

    FunctionDocumentation::Description toIPv6OrDefault_description = R"(
Converts a string or a UInt128 form of IPv6 address to [`IPv6`](../data-types/ipv6.md) type.
If the IPv6 address has an invalid format, it returns `::` (0 IPv6) or the provided IPv6 default.
    )";
    FunctionDocumentation::Syntax toIPv6OrDefault_syntax = "toIPv6OrDefault(string[, default])";
    FunctionDocumentation::Arguments toIPv6OrDefault_arguments = {
        {"string", "IP address string to convert."},
        {"default", "Optional. The value to return if string has an invalid format."}
    };
    FunctionDocumentation::ReturnedValue toIPv6OrDefault_returned_value = {"Returns the IPv6 address, otherwise `::` or the provided optional default if argument `string` has an invalid format.", {"IPv6"}};
    FunctionDocumentation::Examples toIPv6OrDefault_examples = {
    {
        "Valid and invalid IPv6 strings",
        R"(
WITH
    '2001:0db8:85a3:0000:0000:8a2e:0370:7334' AS valid_IPv6_string,
    '2001:0db8:85a3::8a2e:370g:7334' AS invalid_IPv6_string,
    'not_an_ipv6' AS malformed_string
SELECT
    toIPv6OrDefault(valid_IPv6_string) AS valid,
    toIPv6OrDefault(invalid_IPv6_string) AS default_value,
    toIPv6OrDefault(malformed_string, toIPv6('::1')) AS provided_default;
        )",
        R"(
┌─valid──────────────────────────────────┬─default_value─┬─provided_default─┐
│ 2001:db8:85a3::8a2e:370:7334           │ ::            │ ::1              │
└────────────────────────────────────────┴───────────────┴──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toIPv6OrDefault_introduced_in = {22, 3};
    FunctionDocumentation::Category toIPv6OrDefault_category = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation toIPv6OrDefault_documentation = {toIPv6OrDefault_description, toIPv6OrDefault_syntax, toIPv6OrDefault_arguments, toIPv6OrDefault_returned_value, toIPv6OrDefault_examples, toIPv6OrDefault_introduced_in, toIPv6OrDefault_category};

    factory.registerFunction("toIPv6OrDefault", [](ContextPtr context)
        { return std::make_shared<FunctionCastOrDefaultTyped>(context, "toIPv6OrDefault", std::make_shared<DataTypeIPv6>()); },
        toIPv6OrDefault_documentation);
}

}
