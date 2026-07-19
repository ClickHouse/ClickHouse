#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Parsers/NullsAction.h>
#include <Common/FunctionDocumentation.h>
#include <Common/IFactoryWithAliases.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Names.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>


namespace DB
{
struct Settings;

class Context;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;

class ASTFunction;

/**
 * The invoker has arguments: name of aggregate function, types of arguments, values of parameters.
 * Parameters are for "parametric" aggregate functions.
 * For example, in quantileWeighted(0.9)(x, weight), 0.9 is "parameter" and x, weight are "arguments".
 */
using AggregateFunctionCreator = std::function<AggregateFunctionPtr(const String &, const DataTypes &, const Array &, const Settings *)>;

struct AggregateFunctionWithProperties
{
    AggregateFunctionCreator creator;
    /// Optional override to prefer in window context (OVER (...))
    /// See TheilsU aggregate function for an example.
    AggregateFunctionCreator window_creator;
    FunctionDocumentation documentation;
    AggregateFunctionProperties properties;

    AggregateFunctionWithProperties() = default;
    AggregateFunctionWithProperties(const AggregateFunctionWithProperties &) = default;
    AggregateFunctionWithProperties & operator = (const AggregateFunctionWithProperties &) = default;

    template <typename Creator>
    requires (!std::is_same_v<Creator, AggregateFunctionWithProperties>)
    AggregateFunctionWithProperties(Creator creator_, FunctionDocumentation documentation_, AggregateFunctionProperties properties_ = {}, AggregateFunctionCreator window_creator_ = {}) /// NOLINT
        : creator(std::forward<Creator>(creator_)), window_creator(std::move(window_creator_)), documentation(std::move(documentation_)), properties(std::move(properties_))
    {
    }
};


/** Creates an aggregate function by name.
  */
class AggregateFunctionFactory final : private boost::noncopyable, public IFactoryWithAliases<AggregateFunctionWithProperties>
{
public:
    static AggregateFunctionFactory & instance();

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const String & name,
        Value creator,
        Case case_sensitiveness = Case::Sensitive);

    /// Register how to transform from one aggregate function to other based on NullsAction
    /// Registers them both ways:
    /// SOURCE + RESPECT NULLS will be transformed to TARGET
    /// TARGET + IGNORE NULLS will be transformed to SOURCE
    void registerNullsActionTransformation(const String & source_ignores_nulls, const String & target_respect_nulls);

    /// Throws an exception if not found.
    ///
    /// The `state_variant` parameter selects which implementation to resolve for functions
    /// that have a dedicated implementation for window execution (OVER (...)) with different
    /// performance characteristics on add()/getResult().
    ///   - Aggregation: resolve the normal GROUP BY implementation.
    ///   - Window: prefer a window-specific implementation if registered (via `window_creator`),
    ///     falling back to the normal implementation if absent.
    AggregateFunctionPtr
    get(const String & name,
        NullsAction action,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        AggregateFunctionStateVariant state_variant = AggregateFunctionStateVariant::Aggregation) const;

    /// Get properties if the aggregate function exists.
    std::optional<AggregateFunctionProperties> tryGetProperties(String name, NullsAction action) const;

    bool isAggregateFunctionName(const String & name) const;

    FunctionDocumentation getDocumentation(const String & name) const;

private:
    AggregateFunctionPtr getImpl(
        const String & name,
        NullsAction action,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        bool has_null_arguments,
        AggregateFunctionStateVariant state_variant,
        bool apply_variant_adapter_to_nested) const;

    /// Resolve the function applying only the LowCardinality/Nullable/combinator handling, without the
    /// Variant fallback. `types_without_low_cardinality` must already have LowCardinality removed.
    ///
    /// `apply_variant_adapter_to_nested` controls what happens when a combinator reintroduces a Variant
    /// argument in its nested types (most importantly `-Merge`, whose nested argument types come from a stored
    /// `AggregateFunction(f, Variant(...))` state type): if set, the nested function is resolved through the
    /// Variant adapter so such states round-trip; if not, the Variant is passed to the nested function as-is
    /// (which keeps the adapter as the outermost wrapper on the forward path, where it is applied by `get`).
    AggregateFunctionPtr getWithoutVariantAdapter(
        const String & name,
        NullsAction action,
        const DataTypes & types_without_low_cardinality,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        AggregateFunctionStateVariant state_variant,
        bool apply_variant_adapter_to_nested) const;

    /// Try to wrap the function in AggregateFunctionVariantAdapter so it can be applied to Variant arguments by
    /// aggregating over the least common supertype of the variants. Returns nullptr if that is not possible.
    AggregateFunctionPtr tryGetVariantAdapter(
        const String & name,
        NullsAction action,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        AggregateFunctionStateVariant state_variant) const;

    /// Position of the `-ArgMin` / `-ArgMax` combinator comparison key in the top-level argument list, if the function
    /// has such a combinator (nullopt otherwise). That key is compared exactly, so it must never be adapted through the
    /// lossy Float64 fallback (see `tryGetVariantAdapter` and `AggregateFunctionProperties::is_float_promoting`). The
    /// key is the last argument of the `-ArgMin` / `-ArgMax` call itself, which is not necessarily the last top-level
    /// argument: an outer combinator may append its own trailing argument (e.g. `-If`, `-Resample`), so the position is
    /// computed by replaying the wrapping combinators' argument transforms on `argument_types`.
    std::optional<size_t> getArgMinArgMaxKeyArgument(const String & name, const DataTypes & argument_types) const;

    /// Resolve the function over `types_without_low_cardinality` without the Variant adapter, returning nullptr
    /// if the creator rejects them with an "unsupported argument type" error (any other error propagates). This
    /// encapsulates the "does the function accept these argument types" probe, so callers can branch on native
    /// acceptance without a raw try/catch. `types_without_low_cardinality` must already have LowCardinality removed.
    AggregateFunctionPtr tryResolveNatively(
        const String & name,
        NullsAction action,
        const DataTypes & types_without_low_cardinality,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        AggregateFunctionStateVariant state_variant) const;

    using AggregateFunctions = std::unordered_map<String, Value>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    using ActionMap = NameToNameMap;

    AggregateFunctions aggregate_functions;
    /// Mapping from functions with `RESPECT NULLS` modifier to actual aggregate function names
    /// Example: `any(x) RESPECT NULLS` should be executed as function `any_respect_nulls`
    ActionMap respect_nulls;
    /// Same as above for `IGNORE NULLS` modifier
    ActionMap ignore_nulls;
    std::optional<AggregateFunctionWithProperties> getAssociatedFunctionByNullsAction(const String & name, NullsAction action) const;
    /// Name-only variant: the registered name that `name` resolves to under `action` (see the definition).
    String getAssociatedNameByNullsAction(const String & name, NullsAction action) const;

    /// Case insensitive aggregate functions will be additionally added here with lowercased name.
    AggregateFunctions case_insensitive_aggregate_functions;

    const AggregateFunctions & getMap() const override { return aggregate_functions; }

    const AggregateFunctions & getCaseInsensitiveMap() const override { return case_insensitive_aggregate_functions; }

    String getFactoryName() const override { return "AggregateFunctionFactory"; }

};

struct AggregateUtils
{
    static bool isAggregateFunction(const ASTFunction & node);
};

const String & getAggregateFunctionCanonicalNameIfAny(const String & name);

}
