#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/// Special adapter classes that implement functions execution with Variant arguments.

class ExecutableFunctionVariantAdaptor final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionVariantAdaptor(
        std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_,
        size_t variant_argument_index_);

    String getName() const override { return function_overload_resolver->getName(); }

protected:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const final;
    ColumnPtr executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const final;

    /// Disable all default implementations, they will be used later.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

private:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    /// We remember the original IFunctionOverloadResolver to be able to build function for types inside Variant column.
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver;
    size_t variant_argument_index;
    /// When true (default), throw an exception if a variant type is incompatible with the function.
    /// When false, return NULL for incompatible rows instead.
    /// Resolved at construction time from the `variant_throw_on_type_mismatch` setting or the strictness
    /// override pinned by the caller, see `TypeMismatchStrictness.h`.
    bool throw_on_type_mismatch = true;
};

class FunctionBaseVariantAdaptor final : public IFunctionBase
{
public:
    /// variant_argument_index_ is the argument the adaptor executes per alternative. The resolver
    /// picks it, because a function may handle some `Variant` arguments itself (see
    /// `IFunctionOverloadResolver::useDefaultImplementationForVariantWithCustomName`).
    FunctionBaseVariantAdaptor(
        std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_,
        ColumnsWithTypeAndName arguments_with_type_,
        size_t variant_argument_index_);

    String getName() const override { return function_overload_resolver->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_unique<ExecutableFunctionVariantAdaptor>(function_overload_resolver, variant_argument_index);
    }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isStateful() const override { return function_overload_resolver->isStateful(); }

    bool isDeterministic() const override { return function_overload_resolver->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function_overload_resolver->isDeterministicInScopeOfQuery(); }

    bool isServerConstant() const override { return function_overload_resolver->isServerConstant(); }

    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override { return function_overload_resolver->isShortCircuit(settings, number_of_arguments); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    bool isSpatialPredicate() const override { return function_overload_resolver->isSpatialPredicate(); }

    /// These delegate to the resolver rather than a per-alternative concrete function: the
    /// underlying IFunction is already constructed before overload resolution ever wraps it here,
    /// so the resolver's answer is the same regardless of which Variant alternative ends up
    /// dispatched to at the row level -- there is no dependency on the concrete alternative type,
    /// only on the constant Field arguments and construction-time settings.
    ///
    /// A geometry kind the wrapped function rejects means "evaluating this argument is guaranteed to
    /// raise `ILLEGAL_TYPE_OF_ARGUMENT`", which is what makes `spatial_bbox` pruning fail closed (see
    /// `Common/GeoBbox.h`). This forwards it unconditionally, including when
    /// `variant_throw_on_type_mismatch` is off. Leniency is NOT a reason to drop the rejection:
    /// `ExecutableFunctionVariantAdaptor::try_execute` swallows only a rejection raised while BUILDING the
    /// wrapped function for an incompatible alternative, and deliberately re-throws one raised while
    /// executing it. `polygonsIntersectCartesian`/`polygonsWithinCartesian` reject `Point`,
    /// `LineString`, `MultiLineString` and `MultiPoint` only in `executeImpl`, so under a lenient
    /// session they still raise -- and masking the rejection here let a sibling conjunct prune the
    /// granule away and answer `0` instead of raising. Losing pruning in a lenient session is a cost;
    /// answering `0` where the query must raise is a wrong result.
    bool rejectsColumnGeometryKind(std::string_view kind_name, size_t arg_index) const override
    {
        return function_overload_resolver->rejectsColumnGeometryKind(kind_name, arg_index);
    }
    bool treatsConstTupleAsPoint(size_t arg_index) const override { return function_overload_resolver->treatsConstTupleAsPoint(arg_index); }

private:
    /// We remember the original IFunctionOverloadResolver to be able to build function for types inside Variant column.
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver;
    DataTypes arguments;
    DataTypePtr return_type;
    size_t variant_argument_index;
};


}
