#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/// Special adapter classes that implement functions execution with Dynamic arguments.

class ExecutableFunctionDynamicAdaptor final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionDynamicAdaptor(
        std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_,
        size_t dynamic_argument_index_);

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

    /// We remember the original IFunctionOverloadResolver to be able to build function for types inside Dynamic column.
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver;
    size_t dynamic_argument_index;
    /// When true, throw an exception if a dynamic variant type is incompatible with the function.
    /// When false (default), return NULL for incompatible rows instead.
    /// Resolved at construction time from the `dynamic_throw_on_type_mismatch` setting or the strictness
    /// override pinned by the caller, see `TypeMismatchStrictness.h`.
    bool throw_on_type_mismatch = true;
};

class FunctionBaseDynamicAdaptor final : public IFunctionBase
{
public:
    FunctionBaseDynamicAdaptor(std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_, DataTypes arguments_);

    String getName() const override { return function_overload_resolver->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_unique<ExecutableFunctionDynamicAdaptor>(function_overload_resolver, dynamic_argument_index);
    }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isStateful() const override { return function_overload_resolver->isStateful(); }

    bool isDeterministic() const override { return function_overload_resolver->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function_overload_resolver->isDeterministicInScopeOfQuery(); }

    bool isServerConstant() const override { return function_overload_resolver->isServerConstant(); }

    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override { return function_overload_resolver->isShortCircuit(settings, number_of_arguments); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    bool isSpatialPredicate() const override { return function_overload_resolver->isSpatialPredicate(); }

    /// These delegate to the resolver rather than a per-alternative concrete function: for the
    /// builtins that answer them non-trivially (e.g. `pointInPolygon`), the underlying IFunction is
    /// already constructed (from settings such as `validate_polygons`) before overload resolution
    /// ever wraps it here, so the resolver's answer is the same regardless of which Dynamic
    /// alternative ends up dispatched to at the row level -- there is no dependency on the concrete
    /// alternative type, only on the constant Field arguments and construction-time settings.
    bool requiresValidConstGeometry() const override { return function_overload_resolver->requiresValidConstGeometry(); }
    bool rejectsConstGeometryKind(std::string_view kind_name) const override { return function_overload_resolver->rejectsConstGeometryKind(kind_name); }
    /// A geometry kind the wrapped function rejects normally means "evaluating this argument is
    /// guaranteed to raise `ILLEGAL_TYPE_OF_ARGUMENT`", which is what makes `spatial_bbox` pruning
    /// fail closed (see `hasDeferredGeometryKindRejection` in `Common/GeoBbox.h`). That holds only
    /// while this adaptor actually raises: with `dynamic_throw_on_type_mismatch` off,
    /// `ExecutableFunctionDynamicAdaptor` swallows the build-time `ILLEGAL_TYPE_OF_ARGUMENT` for an incompatible alternative and
    /// resolves those rows to NULL instead, so there is no exception left for pruning to hide and
    /// reporting a rejection would cost pruning for nothing. Only a BUILD-time rejection at the
    /// argument this adaptor dispatches per alternative is covered: an execute-time one (see
    /// `rejectsColumnGeometryKindDuringBuild`) still escapes leniency, and so does a rejection at
    /// any other argument position, which the wrapped function raises itself.
    bool rejectsColumnGeometryKindDuringBuild(size_t arg_index) const override { return function_overload_resolver->rejectsColumnGeometryKindDuringBuild(arg_index); }
    bool rejectsColumnGeometryKind(std::string_view kind_name, size_t arg_index) const override
    {
        if (!throw_on_type_mismatch && arg_index == dynamic_argument_index
            && function_overload_resolver->rejectsColumnGeometryKindDuringBuild(arg_index))
            return false;

        return function_overload_resolver->rejectsColumnGeometryKind(kind_name, arg_index);
    }
    bool treatsConstTupleAsPoint(size_t arg_index) const override { return function_overload_resolver->treatsConstTupleAsPoint(arg_index); }
    bool rejectsNonGeometryArgument(size_t arg_index) const override { return function_overload_resolver->rejectsNonGeometryArgument(arg_index); }

private:
    /// We remember the original IFunctionOverloadResolver to be able to build function for types inside Dynamic column.
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver;
    DataTypes arguments;
    DataTypePtr return_type;
    size_t dynamic_argument_index;
    /// Resolved at construction time, like `ExecutableFunctionDynamicAdaptor`'s own flag: whether an alternative
    /// incompatible with the wrapped function raises or resolves the row to NULL.
    bool throw_on_type_mismatch = true;
};


}
