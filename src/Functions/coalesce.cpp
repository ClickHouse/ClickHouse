#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/MaskOperations.h>
#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_variant_as_common_type;
    extern const SettingsBool allow_lossy_numeric_supertype;
}

namespace
{

/// Implements the function coalesce which takes a set of arguments and
/// returns the value of the leftmost non-null argument. If no such value is
/// found, coalesce() returns NULL.
class FunctionCoalesce final : public IFunction
{
public:
    static constexpr auto name = "coalesce";

    static FunctionPtr create(ContextPtr context)
    {
        const auto & settings = context->getSettingsRef();
        return std::make_shared<FunctionCoalesce>(
            context, settings[Setting::use_variant_as_common_type], settings[Setting::allow_lossy_numeric_supertype]);
    }

    explicit FunctionCoalesce(ContextPtr context, bool use_variant_as_common_type_, bool allow_lossy_numeric_supertype_)
        : is_not_null(FunctionFactory::instance().get("isNotNull", context))
        , assume_not_null(FunctionFactory::instance().get("assumeNotNull", context))
        , if_function(FunctionFactory::instance().get("if", context))
        , multi_if_function(FunctionFactory::instance().get("multiIf", context))
        , use_variant_as_common_type(use_variant_as_common_type_)
        , allow_lossy_numeric_supertype(allow_lossy_numeric_supertype_)
    {
    }

    std::string getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override
    {
        /// The first argument is always needed. Every other argument is needed only for the rows
        /// where all the preceding arguments are NULL, so it is evaluated lazily.
        settings.arguments_with_disabled_lazy_execution.insert(0);
        /// A node that is a common descendant of two lazily executed arguments is still not needed
        /// for the rows where the first argument is not NULL. It happens only with at least three arguments:
        /// with two arguments the only common descendants are shared with the first argument,
        /// which is always evaluated.
        settings.enable_lazy_execution_for_common_descendants_of_arguments = number_of_arguments > 2;
        settings.force_enable_lazy_execution = false;
        return true;
    }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        ColumnNumbers args;
        for (size_t i = 0; i + 1 < number_of_arguments; ++i)
            args.push_back(i);
        return args;
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & /*left*/, const Field & right) const override
    {
        /// coalesce() is identity when its first argument cannot be NULL, so it preserves ordering and thus monotonic.
        /// For Nullable types, coalesce() substitutes NULLs with other arguments and is not
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
        /// Skip all NULL arguments. If any argument is non-Nullable, skip all next arguments.
        DataTypes filtered_args;
        filtered_args.reserve(arguments.size());
        for (const auto & arg : arguments)
        {
            if (arg->onlyNull())
                continue;

            filtered_args.push_back(arg);

            if (!canContainNull(*arg))
                break;
        }

        DataTypes new_args;
        for (size_t i = 0; i < filtered_args.size(); ++i)
        {
            bool is_last = i + 1 == filtered_args.size();

            if (is_last)
                new_args.push_back(filtered_args[i]);
            else
                new_args.push_back(removeNullable(filtered_args[i]));
        }

        if (new_args.empty())
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
        if (new_args.size() == 1)
            return new_args.front();

        bool has_variant = std::any_of(new_args.begin(), new_args.end(), [](const auto & t) { return isVariant(t); });
        auto res = (use_variant_as_common_type || has_variant)
            ? getLeastSupertypeOrVariant(new_args, allow_lossy_numeric_supertype)
            : getLeastSupertype(new_args, allow_lossy_numeric_supertype);

        /// if last argument is not nullable, result should be also not nullable
        if (!canContainNull(*new_args.back()) && res->isNullable())
            res = removeNullable(res);

        return res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// coalesce(arg0, arg1, ..., argN) is essentially
        /// multiIf(isNotNull(arg0), assumeNotNull(arg0), isNotNull(arg1), assumeNotNull(arg1), ..., argN)
        /// with constant NULL arguments removed.

        ColumnsWithTypeAndName filtered_args;
        filtered_args.reserve(arguments.size());
        for (const auto & arg : arguments)
        {
            const auto & type = arg.type;

            if (type->onlyNull())
                continue;

            filtered_args.push_back(arg);

            if (!canContainNull(*type))
                break;
        }

        /// Short-circuit evaluation: the arguments after the first one can be lazily executed
        /// (see `isShortCircuit`). The i-th argument is needed only for the rows where all the
        /// preceding arguments are NULL, so we execute it under the mask of exactly these rows.
        const int last_lazy_argument_index = checkShortCircuitArguments(filtered_args);
        /// The first argument is always needed, but it can still be lazily executed when `coalesce`
        /// itself is an argument of an enclosing short-circuit function.
        if (last_lazy_argument_index >= 0)
            executeColumnIfNeeded(filtered_args[0]);

        const bool has_lazy_arguments = last_lazy_argument_index > 0;
        IColumn::Filter mask;
        MaskInfo mask_info{.has_ones = true, .has_zeros = false};
        if (has_lazy_arguments)
            mask.resize_fill(input_rows_count, 1);

        ColumnsWithTypeAndName multi_if_args;
        ColumnsWithTypeAndName tmp_args(1);

        for (size_t i = 0; i < filtered_args.size(); ++i)
        {
            bool is_last = i + 1 == filtered_args.size();

            if (has_lazy_arguments && i > 0)
                maskedExecute(filtered_args[i], mask, mask_info);

            if (is_last)
            {
                multi_if_args.push_back(filtered_args[i]);
            }
            else
            {
                tmp_args[0] = filtered_args[i];

                DataTypePtr cond_type = std::make_shared<DataTypeUInt8>();
                ColumnPtr cond_column
                    = is_not_null->build(tmp_args)->execute(tmp_args, cond_type, input_rows_count, /* dry_run = */ false);

                DataTypePtr val_type = removeNullable(filtered_args[i].type);
                ColumnPtr val_column
                    = assume_not_null->build(tmp_args)->execute(tmp_args, val_type, input_rows_count, /* dry_run = */ false);

                multi_if_args.emplace_back(cond_column, cond_type, "");
                multi_if_args.emplace_back(val_column, val_type, "");

                /// Narrow the mask down to the rows where this argument is NULL - only these rows
                /// need the next arguments. When the mask becomes all zeros, `maskedExecute` does not
                /// execute the remaining arguments at all and substitutes default values instead.
                /// A lazily executed argument can be reduced to a sparse column (and then `isNotNull`
                /// keeps it sparse), while mask extraction supports only full and constant columns.
                if (has_lazy_arguments && static_cast<int>(i) < last_lazy_argument_index)
                    mask_info = extractInvertedMask(mask, recursiveRemoveSparse(cond_column));
            }
        }

        /// If all arguments appeared to be NULL.
        if (multi_if_args.empty())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        if (multi_if_args.size() == 1)
            return multi_if_args.front().column;

        /// If there was only two arguments (3 arguments passed to multiIf)
        /// use function "if" instead, because it's implemented more efficient.
        /// TODO: make "multiIf" the same efficient.
        FunctionOverloadResolverPtr if_or_multi_if = multi_if_args.size() == 3 ? if_function : multi_if_function;
        ColumnPtr res = if_or_multi_if->build(multi_if_args)->execute(multi_if_args, result_type, input_rows_count, /* dry_run = */ false);

        /// if last argument is not nullable, result should be also not nullable
        if (!multi_if_args.back().column->isNullable() && res->isNullable())
        {
            if (const auto * column_lc = checkAndGetColumn<ColumnLowCardinality>(&*res))
                res = checkAndGetColumn<ColumnNullable>(*column_lc->convertToFullColumn()).getNestedColumnPtr();
            else if (const auto * column_const = checkAndGetColumn<ColumnConst>(&*res))
                res = checkAndGetColumn<ColumnNullable>(column_const->getDataColumn()).getNestedColumnPtr();
            else
                res = checkAndGetColumn<ColumnNullable>(&*res)->getNestedColumnPtr();
        }

        return res;
    }

private:
    FunctionOverloadResolverPtr is_not_null;
    FunctionOverloadResolverPtr assume_not_null;
    FunctionOverloadResolverPtr if_function;
    FunctionOverloadResolverPtr multi_if_function;
    bool use_variant_as_common_type = false;
    bool allow_lossy_numeric_supertype = false;
};

}

REGISTER_FUNCTION(Coalesce)
{
    FunctionDocumentation::Description description = R"(
Returns the leftmost non-`NULL` argument.

The setting [`short_circuit_function_evaluation`](/reference/settings/session-settings/short-circuit-function-evaluation#short_circuit_function_evaluation) controls whether short-circuit evaluation is used.

If enabled, an argument is evaluated only on the rows where all the preceding arguments are `NULL`.

For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the following query:

```sql
SELECT coalesce(if(number = 0, toNullable(0), NULL), intDiv(42, number)) FROM numbers(10)
```
    )";
    FunctionDocumentation::Syntax syntax = "coalesce(x[, y, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"x[, y, ...]", "Any number of parameters of non-compound type. All parameters must be of mutually compatible data types.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the first non-`NULL` argument, otherwise `NULL`, if all arguments are `NULL`.", {"Any", "NULL"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example",
         R"(
-- Consider a list of contacts that may specify multiple ways to contact a customer.

CREATE TABLE aBook
(
    name String,
    mail Nullable(String),
    phone Nullable(String),
    telegram Nullable(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO aBook VALUES ('client 1', NULL, '123-45-67', 123), ('client 2', NULL, NULL, NULL);

-- The mail and phone fields are of type String, but the telegram field is UInt32 so it needs to be converted to String.

-- Get the first available contact method for the customer from the contact list

SELECT name, coalesce(mail, phone, CAST(telegram,'Nullable(String)')) FROM aBook;
        )",
         R"(
┌─name─────┬─coalesce(mail, phone, CAST(telegram, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                                 │
│ client 2 │ ᴺᵁᴸᴸ                                                      │
└──────────┴───────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCoalesce>(documentation, FunctionFactory::Case::Insensitive);
}

}
