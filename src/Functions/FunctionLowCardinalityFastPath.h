#pragma once

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context_fwd.h>

#include <concepts>
#include <memory>

namespace DB
{

/// A function provides its specialized LowCardinality execution path by implementing this hook.
/// It returns the result column, or nullptr when it cannot handle this particular call, in which
/// case the arguments are processed as if the hook did not exist.
template <typename Base>
concept HasLowCardinalityFastPath = requires(
    const Base & base, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
{
    { base.tryExecuteLowCardinality(arguments, result_type, input_rows_count) } -> std::same_as<ColumnPtr>;
};

/** Adds a specialized LowCardinality execution path to an existing function.
  *
  * Normally LowCardinality arguments are converted to full columns before executeImpl runs (or
  * the function is executed once per dictionary entry, when possible). A function that wants to
  * see the LowCardinality columns themselves has to disable that conversion, and with it loses
  * everything else the generic code does for LowCardinality arguments: wrapping the result type
  * in LowCardinality, making the result Nullable for LowCardinality(Nullable) arguments,
  * execution on the dictionary, constant folding. This mixin restores those behaviors, so that
  * Base only implements the fast path itself: a tryExecuteLowCardinality method that returns
  * nullptr when it does not apply (see HasLowCardinalityFastPath above). Base must otherwise be
  * an ordinary stateless, default-constructible IFunction with all default implementations left
  * enabled. The function is registered as FunctionWithLowCardinalityFastPath<Base>.
  *
  * The mixin inherits from Base instead of wrapping it, because IFunction has dozens of other
  * virtual methods (isDeterministic, getMonotonicityForRange, ...) that a wrapper would have to
  * forward one by one, and a missed one would go unnoticed. Only four methods are overridden:
  *
  * - useDefaultImplementationForLowCardinalityColumns() returns false, so executeImpl sees the
  *   original LowCardinality columns.
  *
  * - useDefaultImplementationForConstants() must then also return false. The generic code
  *   handles LowCardinality before constants, and execution on the dictionary requires constant
  *   arguments to still be constants (it resizes them to the dictionary size). If constants were
  *   already unwrapped to full columns here, redoing the call (below) would run the dictionary
  *   code on mismatched row counts.
  *
  * - executeImpl() tries the fast path first. If it declines and any argument type contains
  *   LowCardinality (or all arguments are constant, since that default is off too), the call is
  *   redone on a plain Base instance with the defaults enabled, which behaves exactly like the
  *   function did before the fast path existed. Redoing the call is deliberately preferred over
  *   reimplementing the default handling here: the declared type and the produced column must
  *   agree, and the rules for wrapping results in LowCardinality and for handling
  *   Nullable inside LowCardinality are easy to get wrong.
  *
  * - getReturnTypeImpl(ColumnsWithTypeAndName) is likewise redone on the plain Base instance
  *   whenever an argument type contains LowCardinality, since disabling the conversion also
  *   changes how the return type is deduced (a LowCardinality(Nullable) argument no longer makes
  *   the result Nullable, and the result type is no longer wrapped in LowCardinality).
  */
template <typename Base>
requires HasLowCardinalityFastPath<Base>
class FunctionWithLowCardinalityFastPath : public Base
{
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionWithLowCardinalityFastPath>(); }

    FunctionWithLowCardinalityFastPath() : delegate(std::make_shared<Base>()) { }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    using Base::getReturnTypeImpl;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (hasLowCardinalityTypes(arguments))
            return FunctionToOverloadResolverAdaptor(delegate).getReturnType(arguments);

        return Base::getReturnTypeImpl(arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (auto result = Base::tryExecuteLowCardinality(arguments, result_type, input_rows_count))
            return result;

        if (hasLowCardinalityTypes(arguments) || allArgumentColumnsAreConstant(arguments))
            return FunctionToExecutableFunctionAdaptor(delegate).execute(arguments, result_type, input_rows_count, /*dry_run=*/ false);

        return Base::executeImpl(arguments, result_type, input_rows_count);
    }

private:
    /// The delegate is stateless, so it is created once and shared across calls.
    std::shared_ptr<Base> delegate;
};

}
