#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_variant_as_common_type;
}

namespace
{

/// Implements the function ifNull which takes 2 arguments and returns
/// the value of the 1st argument if it is not null. Otherwise it returns
/// the value of the 2nd argument.
class FunctionIfNull final : public IFunction
{
public:
    static constexpr auto name = "ifNull";

    explicit FunctionIfNull(ContextPtr context, bool use_variant_as_common_type_)
        : is_not_null(FunctionFactory::instance().get("isNotNull", context))
        , assume_not_null(FunctionFactory::instance().get("assumeNotNull", context))
        , if_function(FunctionFactory::instance().get("if", context))
        , use_variant_as_common_type(use_variant_as_common_type_)
    {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIfNull>(context, context->getSettingsRef()[Setting::use_variant_as_common_type]);
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    /// Three shapes:
    /// 1. `ifNull(NULL, alt)`            -> alt's type (literal NULL always falls through).
    /// 2. `ifNull(Nullable(T), alt)`     -> `leastSupertype{,OrVariant}(T, alt)`.
    /// 3. `ifNull(non-nullable, alt)`    -> the non-nullable type unchanged.
    /// The `OrVariant` form is selected by `use_variant_as_common_type`. The
    /// runtime *also* uses the Variant supertype when either argument is itself
    /// a Variant, regardless of the setting; the DSL doesn't model that
    /// auto-detection, so when the setting is off the third alternative goes
    /// through plain `leastSupertype` (and the runtime override stays
    /// authoritative when a Variant slips in).
    String getSignatureString() const override
    {
        if (use_variant_as_common_type)
            return "(Nothing, T) -> T"
                   " OR (Nullable(T), U) -> leastSupertypeOrVariant(T, U)"
                   " OR (T, Any) -> T";
        return "(Nothing, T) -> T"
               " OR (Nullable(T), U) -> leastSupertype(T, U)"
               " OR (T, Any) -> T";
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & /*left*/, const Field & right) const override
    {
        /// ifNull() is identity when its first argument cannot be NULL, so it preserves ordering and thus monotonic.
        /// For Nullable types, ifNull() substitutes NULLs with the second argument and is not
        /// monotonic in general. We treat it as monotonic only when the analyzed range is guaranteed to not contain
        /// NULLs. NULLs always represented as POSITIVE_INFINITY and they will always be at the end of ordering.
        /// So, we do not need to check left.isNull().
        bool can_contain_null = canContainNull(type);
        if (can_contain_null && right.isNull())
            return {};

        return { .is_monotonic = true, .is_positive = true, .is_always_monotonic = !can_contain_null };
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[1];

        if (!canContainNull(*arguments[0]))
            return arguments[0];

        auto args = DataTypes{removeNullable(arguments[0]), arguments[1]};
        bool has_variant = std::any_of(args.begin(), args.end(), [](const auto & t) { return isVariant(t); });
        if (use_variant_as_common_type || has_variant)
            return getLeastSupertypeOrVariant(args);
        return getLeastSupertype(args);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Always null.
        if (arguments[0].type->onlyNull())
            return arguments[1].column;

        /// Could not contain nulls, so nullIf makes no sense.
        if (!canContainNull(*arguments[0].type))
            return arguments[0].column;

        /// ifNull(col1, col2) == if(isNotNull(col1), assumeNotNull(col1), col2)

        ColumnsWithTypeAndName columns{arguments[0]};

        auto is_not_null_type = std::make_shared<DataTypeUInt8>();
        auto is_not_null_res = is_not_null->build(columns)->execute(columns, is_not_null_type, input_rows_count, /* dry_run = */ false);

        auto assume_not_null_type = removeNullable(arguments[0].type);
        auto assume_not_null_res = assume_not_null->build(columns)->execute(columns, assume_not_null_type, input_rows_count, /* dry_run = */ false);

        ColumnsWithTypeAndName if_columns
        {
                {is_not_null_res, is_not_null_type, ""},
                {assume_not_null_res, assume_not_null_type, ""},
                arguments[1],
        };

        return if_function->build(if_columns)->execute(if_columns, result_type, input_rows_count, /* dry_run = */ false);
    }

private:
    FunctionOverloadResolverPtr is_not_null;
    FunctionOverloadResolverPtr assume_not_null;
    FunctionOverloadResolverPtr if_function;
    bool use_variant_as_common_type = false;
};

}

REGISTER_FUNCTION(IfNull)
{
    FunctionDocumentation::Description description = R"(
Returns an alternative value if the first argument is `NULL`.
    )";
    FunctionDocumentation::Syntax syntax = "ifNull(x, alt)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value to check for `NULL`.", {"Any"}},
        {"alt", "The value that the function returns if `x` is `NULL`.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value of `x` if it is not `NULL`, otherwise `alt`.", {"Any"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example",
         R"(
SELECT ifNull('a', 'b'), ifNull(NULL, 'b');
        )",
         R"(
┌─ifNull('a', 'b')─┬─ifNull(NULL, 'b')─┐
│ a                │ b                 │
└──────────────────┴───────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in{1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIfNull>(documentation, FunctionFactory::Case::Insensitive);
}

}
