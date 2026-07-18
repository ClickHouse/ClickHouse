#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionVariantAdapter.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>

static constexpr size_t MAX_AGGREGATE_FUNCTION_NAME_LENGTH = 1000;


namespace DB
{
struct Settings;
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_AGGREGATION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

/// An aggregate-function creator signals "these argument types are not supported" with one of a few error codes:
/// most use ILLEGAL_TYPE_OF_ARGUMENT, but some reject unsupported types with BAD_ARGUMENTS (e.g. analysisOfVariance,
/// the *TTest family) or NOT_IMPLEMENTED (e.g. rankCorr). For the Variant adapter they all mean the same thing: the
/// native call did not accept the argument types, so it is worth retrying after adapting the Variant arguments to
/// their supertype. Any other error code is a genuine failure and must propagate unchanged.
static bool isUnsupportedArgumentTypeError(int code)
{
    return code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
        || code == ErrorCodes::BAD_ARGUMENTS
        || code == ErrorCodes::NOT_IMPLEMENTED;
}

const String & getAggregateFunctionCanonicalNameIfAny(const String & name)
{
    return AggregateFunctionFactory::instance().getCanonicalNameIfAny(name);
}

void AggregateFunctionFactory::registerFunction(const String & name, Value creator_with_properties, Case case_sensitiveness)
{
    if (creator_with_properties.creator == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory: "
            "the aggregate function {} has been provided  a null constructor", name);

    if (!aggregate_functions.emplace(name, creator_with_properties).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory: the aggregate function name '{}' is not unique",
            name);

    if (case_sensitiveness == Case::Insensitive)
    {
        auto key = Poco::toLower(name);
        if (!case_insensitive_aggregate_functions.emplace(key, creator_with_properties).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory: "
                "the case insensitive aggregate function name '{}' is not unique", name);
        case_insensitive_name_mapping[key] = name;
    }
}

void AggregateFunctionFactory::registerNullsActionTransformation(const String & source_ignores_nulls, const String & target_respect_nulls)
{
    if (!aggregate_functions.contains(source_ignores_nulls))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Source aggregation '{}' not found", source_ignores_nulls);

    if (!aggregate_functions.contains(target_respect_nulls))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Target aggregation '{}' not found", target_respect_nulls);

    if (!respect_nulls.emplace(source_ignores_nulls, target_respect_nulls).second)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Assignment from '{}' is not unique", source_ignores_nulls);

    if (!ignore_nulls.emplace(target_respect_nulls, source_ignores_nulls).second)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Assignment from '{}' is not unique", target_respect_nulls);
}

static DataTypes convertLowCardinalityTypesToNested(const DataTypes & types)
{
    DataTypes res_types;
    res_types.reserve(types.size());
    for (const auto & type : types)
        res_types.emplace_back(recursiveRemoveLowCardinality(type));

    return res_types;
}

AggregateFunctionPtr AggregateFunctionFactory::get(
    const String & name,
    NullsAction action,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    AggregateFunctionStateVariant state_variant) const
{
    /// This to prevent costly string manipulation in parsing the aggregate function combinators.
    /// Example: avgArrayArrayArrayArray...(1000 times)...Array
    if (name.size() > MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long name of aggregate function, maximum: {}", MAX_AGGREGATE_FUNCTION_NAME_LENGTH);

    auto types_without_low_cardinality = convertLowCardinalityTypesToNested(argument_types);

    /// If one of the arguments is a Variant, and the requested function does not accept it natively,
    /// we aggregate over the least common supertype of the variants (see AggregateFunctionVariantAdapter).
    if (std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
        [](const auto & type) { return isVariant(type); }))
    {
        auto properties = tryGetProperties(name, action);
        /// Window functions must handle their argument types themselves, so don't adapt them.
        bool is_window_function = properties.has_value() && properties->is_window_function;
        if (!is_window_function)
        {
            /// Whether the function accepts a Variant argument natively is declared by
            /// AggregateFunctionProperties::support_variant_argument, so we do not need to attempt native
            /// resolution and catch its failure to find out. A function that does not accept a Variant natively
            /// goes straight to the adapter below; one that does is resolved natively first. argMin / argMax
            /// accept a Variant only in the returned "arg" and reject it in the comparison key, so a function
            /// that resolves natively for some Variant positions but not the given ones still falls through to
            /// the adapter, which adapts only the rejected positions.
            bool supports_variant_natively = properties.has_value() && properties->support_variant_argument;
            if (supports_variant_natively)
            {
                /// The Variant is present in the top-level arguments, so the adapter (if needed) is applied here
                /// as the outermost wrapper via tryGetVariantAdapter below; do not let combinators apply it again
                /// inside. tryResolveNatively returns nullptr when the function rejects the given Variant
                /// positions (e.g. argMin with a Variant comparison key), so we fall through to the adapter.
                if (auto native = tryResolveNatively(
                        name, action, types_without_low_cardinality, parameters, out_properties, state_variant))
                    return native;
            }

            if (auto adapter = tryGetVariantAdapter(name, action, types_without_low_cardinality, parameters, out_properties, state_variant))
                return adapter;

            /// Neither native resolution nor the supertype adapter can handle the Variant argument. Resolve
            /// natively once more so the function's original, specific error is reported unchanged.
            return getWithoutVariantAdapter(
                name, action, types_without_low_cardinality, parameters, out_properties, state_variant,
                /*apply_variant_adapter_to_nested=*/ false);
        }
    }

    /// No Variant in the top-level arguments. A combinator may still reintroduce one from a stored aggregate state
    /// type (e.g. sumMerge over AggregateFunction(sum, Variant(...))), in which case the nested function needs the
    /// adapter to reconstruct the matching state layout, so allow it.
    return getWithoutVariantAdapter(
        name, action, types_without_low_cardinality, parameters, out_properties, state_variant,
        /*apply_variant_adapter_to_nested=*/ true);
}

AggregateFunctionPtr AggregateFunctionFactory::getWithoutVariantAdapter(
    const String & name,
    NullsAction action,
    const DataTypes & types_without_low_cardinality,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    AggregateFunctionStateVariant state_variant,
    bool apply_variant_adapter_to_nested) const
{
    /// If one of the types is Nullable, we apply aggregate function combinator "Null" if it's not window function.
    /// Window functions are not real aggregate functions. Applying combinators doesn't make sense for them,
    /// they must handle the nullability themselves.
    /// Aggregate functions such as any_value_respect_nulls are considered window functions in that sense
    auto properties = tryGetProperties(name, action);
    bool is_window_function = properties.has_value() && properties->is_window_function;
    if (!is_window_function && std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
        [](const auto & type) { return type->isNullable(); }))
    {
        AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("Null");
        if (!combinator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find aggregate function combinator "
                            "to apply a function to Nullable arguments.");

        DataTypes nested_types = combinator->transformArguments(types_without_low_cardinality);
        Array nested_parameters = combinator->transformParameters(parameters);

        bool has_null_arguments = std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
            [](const auto & type) { return type->onlyNull(); });

        AggregateFunctionPtr nested_function = getImpl(name, action, nested_types, nested_parameters, out_properties, has_null_arguments, state_variant, apply_variant_adapter_to_nested);

        // Pure window functions are not real aggregate functions. Applying
        // combinators doesn't make sense for them, they must handle the
        // nullability themselves. Another special case is functions from Nothing
        // that are rewritten to AggregateFunctionNothing, in this case
        // nested_function is nullptr.
        if (!nested_function || !nested_function->isOnlyWindowFunction())
            return combinator->transformAggregateFunction(nested_function, out_properties, types_without_low_cardinality, parameters);
    }

    auto with_original_arguments = getImpl(name, action, types_without_low_cardinality, parameters, out_properties, false, state_variant, apply_variant_adapter_to_nested);

    if (!with_original_arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory returned nullptr");
    return with_original_arguments;
}

AggregateFunctionPtr AggregateFunctionFactory::tryResolveNatively(
    const String & name,
    NullsAction action,
    const DataTypes & types_without_low_cardinality,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    AggregateFunctionStateVariant state_variant) const
{
    try
    {
        return getWithoutVariantAdapter(
            name, action, types_without_low_cardinality, parameters, out_properties, state_variant,
            /*apply_variant_adapter_to_nested=*/ false);
    }
    catch (const Exception & e)
    {
        /// A creator may reject an unsupported argument type with any of a few error codes, not only
        /// ILLEGAL_TYPE_OF_ARGUMENT (see isUnsupportedArgumentTypeError). For our purposes all of them mean "the
        /// function does not accept these argument types", not a hard failure. Any other error is genuine and
        /// must propagate.
        if (isUnsupportedArgumentTypeError(e.code()))
            return nullptr;
        throw;
    }
}

AggregateFunctionPtr AggregateFunctionFactory::tryGetVariantAdapter(
    const String & name,
    NullsAction action,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    AggregateFunctionStateVariant state_variant) const
{
    /// Whether the Float64 fallback below is safe for this aggregate is declared by
    /// AggregateFunctionProperties::is_float_promoting: it is true only when the function's result is a
    /// floating-point value computed by arithmetic/statistics over its numeric input, so that aggregating a numeric
    /// mix with no lossless common supertype in Float64 is exactly what the function already does internally.
    /// Combinator suffixes are stripped and aliases resolved by tryGetProperties, so this classifies the base
    /// function (sumIf / sumArray / sumMerge / ... -> sum). A function whose properties cannot be resolved (or that
    /// is not float-promoting) is treated as not float-promoting -- kept fail-closed on purpose.
    auto properties = tryGetProperties(name, action);
    bool is_float_promoting = properties.has_value() && properties->is_float_promoting;

    /// is_float_promoting classifies the base function (tryGetProperties strips the combinator suffixes: sumArgMax ->
    /// sum), so a `-ArgMin` / `-ArgMax` combinator over a float-promoting base would inherit the Float64 fallback for
    /// its comparison key too. That key is compared exactly (AggregateFunctionCombinatorArgMinArgMax rejects a Variant
    /// key), so a lossy Float64 cast of the key would silently return the wrong argMax/argMin row -- e.g. sumArgMax(v, k)
    /// with k in {9007199254740992, 9007199254740993} would collapse both keys to the same Float64. So the Float64
    /// fallback must never apply to the key position (its lossless-supertype adaptation below stays allowed).
    std::optional<size_t> argminmax_key_argument = getArgMinArgMaxKeyArgument(name, argument_types.size());

    /// The type each Variant argument would be adapted to: Nullable(least common supertype of its nested types).
    /// Nullable is used so that the implicit NULLs of the Variant become ordinary NULLs which the aggregation skips.
    /// A non-Variant argument keeps its type; a Variant with no Nullable-wrappable supertype has no adapted type
    /// (stays null) and therefore cannot be adapted at all.
    DataTypes adapted_types(argument_types.size());
    size_t num_variant_arguments = 0;
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!isVariant(argument_types[i]))
        {
            adapted_types[i] = argument_types[i];
            continue;
        }

        ++num_variant_arguments;
        const auto & variants = assert_cast<const DataTypeVariant &>(*argument_types[i]).getVariants();
        auto supertype = tryGetLeastSupertype(variants);
        /// getLeastSupertype is strict and reports no common type for e.g. Decimal + Float64 or Int64 + Float64
        /// (there is no lossless conversion). For an aggregate whose result is a floating-point value computed by
        /// arithmetic over its input (sum/avg/...), a mix of numeric types is naturally computed in Float64
        /// (exactly as arithmetic does: Int64 + Float64 -> Float64), so fall back to Float64 when all the variants
        /// are numeric. This fallback is deliberately NOT applied to exact/order-based aggregates (min/max/argMin/
        /// argMax/...): a lossy Float64 cast would silently return wrong results for them (two distinct integers
        /// above 2^53 collapse to the same Float64), so they keep reporting the original error when there is no
        /// lossless common supertype. See AggregateFunctionProperties::is_float_promoting. The same applies to the
        /// exact comparison key of the `-ArgMin` / `-ArgMax` combinators, so it is excluded here as well.
        if (!supertype
            && std::all_of(variants.begin(), variants.end(), [](const auto & v) { return isNumber(v); })
            && is_float_promoting
            && i != argminmax_key_argument)
            supertype = std::make_shared<DataTypeFloat64>();
        /// The supertype must be wrappable in Nullable: the adapter relies on Nullable to carry the implicit NULLs
        /// of the Variant (which the aggregation then skips). This is not possible when there is no common supertype,
        /// when it is itself a composite type (e.g. Variant), or when it is a container type such as Array/Tuple/Map.
        /// So a top-level Variant whose supertype is a container is out of scope even for an orderable aggregate such
        /// as min/max over Variant(Array(...), Array(...)): the original error is reported. Supporting it would require
        /// tracking the Variant NULLs separately from the value column (a natural follow-up).
        if (supertype && supertype->canBeInsideNullable())
            adapted_types[i] = makeNullable(supertype);
    }

    /// Build the argument list with each Variant position for which adapt[i] is set replaced by its adapted type.
    /// Returns nullopt when a position that must be adapted has no adapted type.
    auto build_nested_types = [&](const std::vector<bool> & adapt) -> std::optional<DataTypes> // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        DataTypes nested(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (isVariant(argument_types[i]) && adapt[i])
            {
                if (!adapted_types[i])
                    return std::nullopt;
                nested[i] = adapted_types[i];
            }
            else
                nested[i] = argument_types[i];
        }
        return nested;
    };

    /// Resolve the function over the given argument types, returning nullptr when it rejects them. Any Variant left in
    /// place is one the function accepts natively, so combinators need not (and must not) apply the adapter again.
    auto try_resolve = [&](const DataTypes & nested) -> AggregateFunctionPtr
    {
        return tryResolveNatively(name, action, nested, parameters, out_properties, state_variant);
    };

    /// Adapt every Variant argument by default. Most functions accept a Variant in none of their positions, so this is
    /// the whole story. But some aggregates (argMin / argMax and the *ArgMin / *ArgMax combinators) natively accept a
    /// Variant in some positions (the returned "arg") and reject it only in others (the comparable key). Adapting a
    /// natively-accepted position would needlessly change the result type (Variant(...) -> Nullable(supertype)) or
    /// reject a call that only needed part of the signature adapted. So, when more than one argument is a Variant, keep
    /// as Variant every position the function still resolves with, adapting only the rest.
    std::vector<bool> adapt(argument_types.size(), true); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    if (num_variant_arguments > 1)
    {
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (!isVariant(argument_types[i]))
                continue;

            adapt[i] = false; /// tentatively keep this Variant argument as-is
            /// Keeping every Variant argument would be the native resolution that already failed, so only probe when
            /// at least one Variant argument is still being adapted.
            bool any_variant_still_adapted = false;
            for (size_t j = 0; j < argument_types.size(); ++j)
                if (isVariant(argument_types[j]) && adapt[j])
                {
                    any_variant_still_adapted = true;
                    break;
                }

            std::optional<DataTypes> nested;
            if (any_variant_still_adapted)
                nested = build_nested_types(adapt);
            if (!nested || !try_resolve(*nested))
                adapt[i] = true; /// keeping it does not resolve -> adapt it after all
        }
    }

    auto nested_argument_types = build_nested_types(adapt);
    if (!nested_argument_types)
        return nullptr;

    /// Resolve the function over the (partially) adapted argument types. If it does not support them either, there is
    /// nothing we can do.
    auto nested_function = try_resolve(*nested_argument_types);
    if (!nested_function)
        return nullptr;

    return std::make_shared<AggregateFunctionVariantAdapter>(nested_function, argument_types, *nested_argument_types, parameters);
}

std::optional<size_t> AggregateFunctionFactory::getArgMinArgMaxKeyArgument(const String & name, size_t num_arguments) const
{
    /// Peel combinator suffixes off the name (the same way tryGetProperties does), stopping at the `-ArgMin` /
    /// `-ArgMax` combinator if present. Its comparison key is the last argument of that combinator's call, which stays
    /// the last top-level argument: the `-ArgMin` / `-ArgMax` combinator is the outermost value combinator in the
    /// standard forms (sumArgMax, avgArgMin, corrArgMax, ...), and combinators that could wrap it strip or unwrap
    /// arguments in place rather than reorder them.
    String current_name = name;
    while (true)
    {
        current_name = getAliasToOrName(current_name);
        if (aggregate_functions.contains(current_name)
            || case_insensitive_aggregate_functions.contains(Poco::toLower(current_name)))
            return {};

        AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(current_name);
        if (!combinator)
            return {};

        const String & combinator_name = combinator->getName();
        if (combinator_name == "ArgMin" || combinator_name == "ArgMax")
            return num_arguments == 0 ? std::optional<size_t>{} : std::optional<size_t>{num_arguments - 1};

        current_name = current_name.substr(0, current_name.size() - combinator_name.size());
    }
}

std::optional<AggregateFunctionWithProperties>
AggregateFunctionFactory::getAssociatedFunctionByNullsAction(const String & name, NullsAction action) const
{
    if (action == NullsAction::RESPECT_NULLS)
    {
        auto it = respect_nulls.find(name);
        if (it == respect_nulls.end())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} does not support RESPECT NULLS", name);
        if (auto associated_it = aggregate_functions.find(it->second); associated_it != aggregate_functions.end())
            return {associated_it->second};
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to find the function {} (equivalent to '{} RESPECT NULLS')", it->second, name);
    }

    if (action == NullsAction::IGNORE_NULLS)
    {
        if (auto it = ignore_nulls.find(name); it != ignore_nulls.end())
        {
            if (auto associated_it = aggregate_functions.find(it->second); associated_it != aggregate_functions.end())
                return {associated_it->second};
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unable to find the function {} (equivalent to '{} IGNORE NULLS')", it->second, name);
        }
        /// We don't throw for IGNORE NULLS of other functions because that's the default in CH
    }

    return {};
}


AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const String & name_param,
    NullsAction action,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    bool has_null_arguments,
    AggregateFunctionStateVariant state_variant,
    bool apply_variant_adapter_to_nested) const
{
    String name = getAliasToOrName(name_param);
    String case_insensitive_name;
    bool is_case_insensitive = false;
    Value found;

    /// Find by exact match.
    if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
    {
        found = it->second;
    }

    if (!found.creator)
    {
        case_insensitive_name = Poco::toLower(name);
        if (auto jt = case_insensitive_aggregate_functions.find(case_insensitive_name); jt != case_insensitive_aggregate_functions.end())
        {
            found = jt->second;
            is_case_insensitive = true;
        }
    }

    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().tryGetQueryContext();

    if (found.creator)
    {
        auto opt = getAssociatedFunctionByNullsAction(is_case_insensitive ? case_insensitive_name : name, action);
        if (opt)
            found = *opt;

        out_properties = found.properties;
        if (query_context && query_context->getSettingsRef()[Setting::log_queries])
            query_context->addQueryFactoriesInfo(
                Context::QueryLogFactories::AggregateFunction, is_case_insensitive ? case_insensitive_name : name);

        /// The case when aggregate function should return NULL on NULL arguments. This case is handled in "get" method.
        if (!out_properties.returns_default_when_only_null && has_null_arguments)
            return nullptr;

        const Settings * settings = query_context ? &query_context->getSettingsRef() : nullptr;

        AggregateFunctionPtr function;
        if (state_variant == AggregateFunctionStateVariant::Window && found.window_creator)
            function = found.window_creator(name, argument_types, parameters, settings);
        else
            function = found.creator(name, argument_types, parameters, settings);

        /// Invariant: For any aggregation function IAggregateFunction::getParameters() should return exactly
        /// the parameters used to create the aggregation function. Aggregation functions are not allowed to change
        /// or normalize these parameters.
        /// (Otherwise it would become a different DataTypeAggregateFunction (see DataTypeAggregateFunction::strictEquals),
        /// and could fail to reattach a table because decodeDataType() would reconstruct a type different from the one
        /// recorded in the column metadata; or fail on parallel replicas under serialize_query_plan=1 because the replica's
        /// decodeDataType() would reconstruct a type different from the one the coordinator sent in the plan.)
        ///
        /// TODO: Some aggregation functions (at least `kolmogorovSmirnovTest`, `mannWhitneyUTest`, `groupArrayMovingSum`, `groupArrayMovingAvg`)
        /// drop their parameters completely, so the check below has to tolerate `function->getParameters().empty()`.
        /// They should be fixed to preserve their parameters like every other aggregation function.
        chassert(function && (function->getParameters().empty() || function->getParameters() == parameters),
            "function->getParameters() must equal the parameters passed to the factory");

        return function;
    }

    /// Combinators of aggregate functions.
    /// For every aggregate function 'agg' and combiner '-Comb' there is a combined aggregate function with the name 'aggComb',
    ///  that can have different number and/or types of arguments, different result type and different behaviour.

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        const std::string & combinator_name = combinator->getName();

        if (combinator->isForInternalUsageOnly())
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function combinator '{}' is only for internal usage",
                combinator_name);

        if (query_context && query_context->getSettingsRef()[Setting::log_queries])
            query_context->addQueryFactoriesInfo(Context::QueryLogFactories::AggregateFunctionCombinator, combinator_name);

        String nested_name = name.substr(0, name.size() - combinator_name.size());
        /// Nested identical combinators (i.e. uniqCombinedIfIf) is not
        /// supported (since they don't work -- silently).
        ///
        /// But non-identical is supported and works. For example,
        /// uniqCombinedIfMergeIf is useful in cases when the underlying
        /// storage stores AggregateFunction(uniqCombinedIf) and in SELECT you
        /// need to filter aggregation result based on another column.

        if (!combinator->supportsNesting() && nested_name.ends_with(combinator_name))
        {
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Nested identical combinator '{}' is not supported",
                combinator_name);
        }

        Array nested_parameters = combinator->transformParameters(parameters);

        AggregateFunctionPtr combined_function;
        if (combinator->transformsMultipleNestedFunctions())
        {
            /// A combinator that wraps one nested function per argument list (e.g. -Tuple: one per tuple
            /// element). Like the single-nested path below, resolve each nested function without the Variant
            /// fallback adapter: a Variant appearing inside a tuple element is a nested (not a top-level)
            /// Variant and stays out of scope, so e.g. sumTuple(tuple(CAST(1 AS Variant(UInt8, UInt64)))) keeps
            /// throwing ILLEGAL_TYPE_OF_ARGUMENT like the other nested-Variant cases (-Array etc.). This is the
            /// only multiple-nested combinator (-Tuple) and it does not consume an aggregate-function state, so
            /// the -Merge state-reintroduction special case below does not arise here; we still propagate
            /// apply_variant_adapter_to_nested so a deeper state-reintroducing combinator remains consistent.
            /// (For non-Variant arguments this is equivalent to the plain get() master used here.)
            auto nested_arguments_list = combinator->transformArgumentsForMultipleNestedFunctions(argument_types);

            VectorWithMemoryTracking<AggregateFunctionPtr> nested_functions;
            nested_functions.reserve(nested_arguments_list.size());
            for (const auto & nested_arguments : nested_arguments_list)
                nested_functions.push_back(getWithoutVariantAdapter(
                    nested_name, action, nested_arguments, nested_parameters, out_properties, state_variant, apply_variant_adapter_to_nested));

            combined_function = combinator->transformAggregateFunctionFromMultipleNestedFunctions(
                getAliasToOrName(nested_name), std::move(nested_functions), out_properties, argument_types, parameters);
        }
        else
        {
            DataTypes nested_types = combinator->transformArguments(argument_types);

            /// Resolve the combinator's nested function. Normally without the Variant fallback adapter: this keeps the
            /// adapter (when needed for a Variant argument in the top-level call) as the outermost wrapper, so combinators
            /// like -If/-Array are applied inside it in the usual order (e.g. Adapter(Null(If(sum))) rather than
            /// If(Adapter(Null(sum)))). The argument types here are already recursively free of LowCardinality (stripped
            /// by the top-level get()).
            ///
            /// The exception is a combinator that reintroduces a Variant argument from a stored aggregate-function state
            /// type -- most importantly -Merge, whose nested argument types come from an AggregateFunction(f, Variant(...))
            /// state (produced earlier by this same Variant adapter). There the nested function itself must go through the
            /// adapter, so that the merge side reconstructs the same Adapter(f) state layout the state was produced with
            /// (otherwise resolving f over the Variant throws). We recognize this narrowly by the combinator consuming an
            /// AggregateFunction state type (as -Merge/-MergeState do). Combinators that merely expose a nested Variant
            /// from ordinary user data (e.g. -Array turning Array(Variant(...)) into a nested Variant argument) are NOT
            /// adapted: nested Variant arguments stay out of scope -- only top-level Variant arguments are handled, which
            /// keeps the documented "top-level Variant only; nested Array/Tuple Variant still rejected" contract.
            bool consumes_aggregate_state = std::any_of(
                argument_types.begin(), argument_types.end(),
                [](const auto & type) { return typeid_cast<const DataTypeAggregateFunction *>(type.get()) != nullptr; });
            bool nested_has_variant = std::any_of(
                nested_types.begin(), nested_types.end(), [](const auto & type) { return isVariant(type); });
            AggregateFunctionPtr nested_function = (apply_variant_adapter_to_nested && nested_has_variant && consumes_aggregate_state)
                ? get(nested_name, action, nested_types, nested_parameters, out_properties, state_variant)
                : getWithoutVariantAdapter(nested_name, action, nested_types, nested_parameters, out_properties, state_variant, apply_variant_adapter_to_nested);
            combined_function = combinator->transformAggregateFunction(nested_function, out_properties, argument_types, parameters);
        }

        /// Same invariant as above.
        chassert(combined_function && (combined_function->getParameters().empty() || combined_function->getParameters() == parameters),
            "function->getParameters() must equal the parameters passed to the factory");
        return combined_function;
    }


    String extra_info;
    if (FunctionFactory::instance().hasNameOrAlias(name))
        extra_info = ". There is an ordinary function with the same name, but aggregate function is expected here";

    auto hints = this->getHints(name);
    if (!hints.empty())
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                        "Unknown aggregate function {}{}. Maybe you meant: {}", name, extra_info, toString(hints));
    throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Unknown aggregate function {}{}", name, extra_info);
}

std::optional<AggregateFunctionProperties> AggregateFunctionFactory::tryGetProperties(String name, NullsAction action) const
{
    if (name.size() > MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long name of aggregate function, maximum: {}", MAX_AGGREGATE_FUNCTION_NAME_LENGTH);

    while (true)
    {
        name = getAliasToOrName(name);
        Value found;
        String lower_case_name;
        bool is_case_insensitive = false;

        /// Find by exact match.
        if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
        {
            found = it->second;
        }

        if (!found.creator)
        {
            lower_case_name = Poco::toLower(name);
            if (auto jt = case_insensitive_aggregate_functions.find(lower_case_name); jt != case_insensitive_aggregate_functions.end())
            {
                is_case_insensitive = true;
                found = jt->second;
            }
        }

        if (found.creator)
        {
            auto opt = getAssociatedFunctionByNullsAction(is_case_insensitive ? lower_case_name : name, action);
            if (opt)
                return opt->properties;
            return found.properties;
        }

        /// Combinators of aggregate functions.
        /// For every aggregate function 'agg' and combiner '-Comb' there is a combined aggregate function with the name 'aggComb',
        ///  that can have different number and/or types of arguments, different result type and different behaviour.

        if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
        {
            if (combinator->isForInternalUsageOnly())
                return {};

            /// NOTE: It's reasonable to also allow to transform properties by combinator.
            name = name.substr(0, name.size() - combinator->getName().size());
        }
        else
            return {};
    }
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name_) const
{
    if (name_.size() > MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long name of aggregate function, maximum: {}", MAX_AGGREGATE_FUNCTION_NAME_LENGTH);

    if (aggregate_functions.contains(name_) || isAlias(name_))
        return true;

    String name_lowercase = Poco::toLower(name_);
    if (case_insensitive_aggregate_functions.contains(name_lowercase) || isAlias(name_lowercase))
        return true;

    String name = name_;
    while (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        name = name.substr(0, name.size() - combinator->getName().size());
        name_lowercase = name_lowercase.substr(0, name_lowercase.size() - combinator->getName().size());

        if (aggregate_functions.contains(name) || isAlias(name) || case_insensitive_aggregate_functions.contains(name_lowercase)
            || isAlias(name_lowercase))
            return true;
    }
    return false;
}

AggregateFunctionFactory & AggregateFunctionFactory::instance()
{
    static AggregateFunctionFactory ret;
    return ret;
}


FunctionDocumentation AggregateFunctionFactory::getDocumentation(const String & name) const
{
    String canonical_name = getAliasToOrName(name);

    if (auto it = aggregate_functions.find(canonical_name); it != aggregate_functions.end())
        return it->second.documentation;

    String name_lowercase = Poco::toLower(canonical_name);
    if (auto it = case_insensitive_aggregate_functions.find(name_lowercase); it != case_insensitive_aggregate_functions.end())
        return it->second.documentation;

    return {};
}

bool AggregateUtils::isAggregateFunction(const ASTFunction & node)
{
    return AggregateFunctionFactory::instance().isAggregateFunctionName(node.name);
}
}
