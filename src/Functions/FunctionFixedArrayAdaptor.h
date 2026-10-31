#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/** The default implementation for `FixedArray`, see `IFunctionOverloadResolver::useDefaultImplementationForFixedArray`.
  *
  * The function is built for the arguments where `FixedArray` is replaced with `Array` (recursively), and the adaptor
  * converts the argument columns in the same way before executing it. The conversion shares the elements and only
  * generates the offsets, so a function which is not specialized for `FixedArray` processes it as `Array` cheaply.
  */
class FunctionBaseFixedArrayAdaptor final : public IFunctionBase
{
public:
    /// Returns the arguments with `FixedArray` replaced with `Array`, or nothing if there is no `FixedArray` in them.
    static std::optional<ColumnsWithTypeAndName> convertArguments(const ColumnsWithTypeAndName & arguments);

    FunctionBaseFixedArrayAdaptor(FunctionBasePtr function_, DataTypes argument_types_)
        : function(std::move(function_)), argument_types(std::move(argument_types_))
    {
    }

    String getName() const override { return function->getName(); }
    bool isEqual(const IFunctionBase & rhs) const override;
    void updateHash(SipHash & hash) const override { function->updateHash(hash); }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return function->getResultType(); }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const override;

    bool isStateful() const override { return function->isStateful(); }
    bool hasObservableSideEffects() const override { return function->hasObservableSideEffects(); }
    bool isVolumeReducing() const override { return function->isVolumeReducing(); }
    bool isExpensive() const override { return function->isExpensive(); }
    bool isSpatialPredicate() const override { return function->isSpatialPredicate(); }
    bool isSuitableForConstantFolding() const override { return function->isSuitableForConstantFolding(); }
    bool isDeterministic() const override { return function->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }
    bool isServerConstant() const override { return function->isServerConstant(); }
    bool isNameInsensitive() const override { return function->isNameInsensitive(); }

    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override
    {
        return function->isShortCircuit(settings, number_of_arguments);
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return function->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    bool canThrow(const DataTypesWithConstInfo & arguments) const override { return function->canThrow(arguments); }

    bool isInjective(const ColumnsWithTypeAndName & sample_columns) const override;

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override;

private:
    FunctionBasePtr function;
    DataTypes argument_types;
};

/** Replaces `FixedArray(N, T)` with `Array(T)` in the type and the column of the argument (also inside `ColumnConst`),
  * but only at the first level: `T` keeps its type. It is used by the functions that are specialized for `FixedArray`,
  * so that an element of a multidimensional `FixedArray` is still a `FixedArray`. Other arguments are returned as is.
  */
ColumnWithTypeAndName removeOuterFixedArray(const ColumnWithTypeAndName & argument);
DataTypePtr removeOuterFixedArray(const DataTypePtr & type);

}
