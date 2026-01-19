#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool cast_keep_nullable;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

FunctionBasePtr createFunctionBaseCast(
    ContextPtr context,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior);

/** CastInternal does not preserve nullability of the data type,
  * i.e. CastInternal(toNullable(toInt8(1)) as Int32) will be Int32(1).
  *
  * Cast preserves nullability according to setting `cast_keep_nullable`,
  * i.e. Cast(toNullable(toInt8(1)) as Int32) will be Nullable(Int32(1)) if `cast_keep_nullable` == 1.
  */
class CastOverloadResolverImpl : public IFunctionOverloadResolver
{
public:
    static const char * getNameImpl(CastType cast_type, bool internal)
    {
        if (cast_type == CastType::accurate)
            return "accurateCast";
        if (cast_type == CastType::accurateOrNull)
            return "accurateCastOrNull";
        if (internal)
            return "_CAST";
        return "CAST";
    }

    String getName() const override
    {
        return getNameImpl(cast_type, internal);
    }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    explicit CastOverloadResolverImpl(ContextPtr context_, CastType cast_type_, bool internal_, std::optional<CastDiagnostic> diagnostic_, bool keep_nullable_, const DataTypeValidationSettings & data_type_validation_settings_)
        : context(context_)
        , cast_type(cast_type_)
        , internal(internal_)
        , diagnostic(std::move(diagnostic_))
        , keep_nullable(keep_nullable_)
        , data_type_validation_settings(data_type_validation_settings_)
    {
    }

    static FunctionOverloadResolverPtr create(ContextPtr context, CastType cast_type, bool internal, std::optional<CastDiagnostic> diagnostic)
    {
        if (internal)
            return std::make_unique<CastOverloadResolverImpl>(context, cast_type, internal, diagnostic, false /*keep_nullable*/, DataTypeValidationSettings{});

        const auto & settings_ref = context->getSettingsRef();
        return std::make_unique<CastOverloadResolverImpl>(
            context, cast_type, internal, diagnostic, settings_ref[Setting::cast_keep_nullable], DataTypeValidationSettings(settings_ref));
    }

    static FunctionBasePtr createInternalCast(
        ColumnWithTypeAndName from,
        DataTypePtr to,
        CastType cast_type,
        std::optional<CastDiagnostic> diagnostic,
        ContextPtr context)
    {
        if (cast_type == CastType::accurateOrNull && !isVariant(to))
            to = makeNullable(to);

        ColumnsWithTypeAndName arguments;
        arguments.emplace_back(std::move(from));
        arguments.emplace_back().type = std::make_unique<DataTypeString>();

        return createFunctionBaseCast(
            context, getNameImpl(cast_type, true), arguments, to, diagnostic, cast_type, FormatSettings::DateTimeOverflowBehavior::Saturate);
    }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        return createFunctionBaseCast(
            context,
            getNameImpl(cast_type, internal),
            arguments,
            return_type,
            diagnostic,
            cast_type,
            FormatSettings::DateTimeOverflowBehavior::Ignore);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & column = arguments.back().column;
        if (!column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant string describing type. "
                "Instead there is non-constant column of type {}", getName(), arguments.back().type->getName());

        const auto * type_col = checkAndGetColumnConst<ColumnString>(column.get());
        if (!type_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant string describing type. "
                "Instead there is a column with the following structure: {}", getName(), column->dumpStructure());

        DataTypePtr type = DataTypeFactory::instance().get(type_col->getValue<String>());
        validateDataType(type, data_type_validation_settings);

        if (cast_type == CastType::accurateOrNull)
        {
            /// Variant handles NULLs by itself during conversions.
            if (!isVariant(type))
                return makeNullable(type);
        }

        if (internal)
            return type;

        if (keep_nullable && arguments.front().type->isNullable() && type->canBeInsideNullable())
            return makeNullable(type);

        return type;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    ContextPtr context;
    CastType cast_type;
    bool internal;
    std::optional<CastDiagnostic> diagnostic;
    bool keep_nullable;
    DataTypeValidationSettings data_type_validation_settings;
};


FunctionBasePtr createInternalCast(ColumnWithTypeAndName from, DataTypePtr to, CastType cast_type, std::optional<CastDiagnostic> diagnostic, ContextPtr context)
{
    return CastOverloadResolverImpl::createInternalCast(std::move(from), std::move(to), cast_type, std::move(diagnostic), context);
}

REGISTER_FUNCTION(CastOverloadResolvers)
{
    factory.registerFunction("_CAST", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::nonAccurate, true, {}); }, {}, FunctionFactory::Case::Insensitive);
    /// Note: "internal" (not affected by null preserving setting) versions of accurate cast functions are unneeded.

    /// CAST documentation
    FunctionDocumentation::Description CAST_description = R"(
Converts a value to a specified data type.
Unlike the reinterpret function, CAST tries to generate the same value in the target type.
If that is not possible, an exception is raised.
    )";
    FunctionDocumentation::Syntax CAST_syntax = R"(
CAST(x, T)
or CAST(x AS T)
or x::T
    )";
    FunctionDocumentation::Arguments CAST_arguments = {
        {"x", "A value of any type.", {"Any"}},
        {"T", "The target data type.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue CAST_returned_value = {"Returns the converted value with the target data type.", {"Any"}};
    FunctionDocumentation::Examples CAST_examples = {
    {
        "Basic usage",
        R"(
SELECT CAST(42, 'String')
        )",
        R"(
┌─CAST(42, 'String')─┐
│ 42                 │
└────────────────────┘
        )"
    },
    {
        "Using AS syntax",
        R"(
SELECT CAST('2025-01-01' AS Date)
        )",
        R"(
┌─CAST('2025-01-01', 'Date')─┐
│                 2025-01-01 │
└────────────────────────────┘
        )"
    },
    {
        "Using :: syntax",
        R"(
SELECT '123'::UInt32
        )",
        R"(
┌─CAST('123', 'UInt32')─┐
│                   123 │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn CAST_introduced_in = {1, 1};
    FunctionDocumentation::Category CAST_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation CAST_documentation = {CAST_description, CAST_syntax, CAST_arguments, CAST_returned_value, CAST_examples, CAST_introduced_in, CAST_category};

    /// accurateCast documentation
    FunctionDocumentation::Description accurateCast_description = R"(
Converts a value to a specified data type. Unlike [`CAST`](#CAST), `accurateCast` performs stricter type checking and throws an exception if the conversion would result in a loss of data precision or if the conversion is not possible.

This function is safer than regular `CAST` as it prevents precision loss and invalid conversions.
    )";
    FunctionDocumentation::Syntax accurateCast_syntax = "accurateCast(x, T)";
    FunctionDocumentation::Arguments accurateCast_arguments = {
        {"x", "A value to convert.", {"Any"}},
        {"T", "The target data type name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue accurateCast_returned_value = {"Returns the converted value with the target data type.", {"Any"}};
    FunctionDocumentation::Examples accurateCast_examples = {
    {
        "Successful conversion",
        R"(
SELECT accurateCast(42, 'UInt16')
        )",
        R"(
┌─accurateCast(42, 'UInt16')─┐
│                        42 │
└───────────────────────────┘
        )"
    },
    {
        "String to number",
        R"(
SELECT accurateCast('123.45', 'Float64')
        )",
        R"(
┌─accurateCast('123.45', 'Float64')─┐
│                            123.45 │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn accurateCast_introduced_in = {1, 1};
    FunctionDocumentation::Category accurateCast_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation accurateCast_documentation = {accurateCast_description, accurateCast_syntax, accurateCast_arguments, accurateCast_returned_value, accurateCast_examples, accurateCast_introduced_in, accurateCast_category};

    /// accurateCastOrNull documentation
    FunctionDocumentation::Description accurateCastOrNull_description = R"(
Converts a value to a specified data type.
Like [`accurateCast`](#accurateCast), but returns `NULL` instead of throwing an exception if the conversion cannot be performed accurately.

This function combines the safety of [`accurateCast`](#accurateCast) with graceful error handling.
    )";
    FunctionDocumentation::Syntax accurateCastOrNull_syntax = "accurateCastOrNull(x, T)";
    FunctionDocumentation::Arguments accurateCastOrNull_arguments = {
        {"x", "A value to convert.", {"Any"}},
        {"T", "The target data type name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue accurateCastOrNull_returned_value = {"Returns the converted value with the target data type, or `NULL` if conversion is not possible.", {"Any"}};
    FunctionDocumentation::Examples accurateCastOrNull_examples = {
    {
        "Successful conversion",
        R"(
SELECT accurateCastOrNull(42, 'String')
        )",
        R"(
┌─accurateCastOrNull(42, 'String')─┐
│ 42                               │
└──────────────────────────────────┘
        )"
    },
    {
        "Failed conversion returns NULL",
        R"(
SELECT accurateCastOrNull('abc', 'UInt32')
        )",
        R"(
┌─accurateCastOrNull('abc', 'UInt32')─┐
│                                ᴺᵁᴸᴸ │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn accurateCastOrNull_introduced_in = {1, 1};
    FunctionDocumentation::Category accurateCastOrNull_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation accurateCastOrNull_documentation = {accurateCastOrNull_description, accurateCastOrNull_syntax, accurateCastOrNull_arguments, accurateCastOrNull_returned_value, accurateCastOrNull_examples, accurateCastOrNull_introduced_in, accurateCastOrNull_category};

    factory.registerFunction("CAST", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::nonAccurate, false, {}); }, CAST_documentation, FunctionFactory::Case::Insensitive);
    factory.registerFunction("accurateCast", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::accurate, false, {}); }, accurateCast_documentation);
    factory.registerFunction("accurateCastOrNull", [](ContextPtr context){ return CastOverloadResolverImpl::create(context, CastType::accurateOrNull, false, {}); }, accurateCastOrNull_documentation);
}

}
