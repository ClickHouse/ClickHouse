#pragma once

#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

/// Old function interface. Check documentation in IFunction.h
class IFunction
{
public:

    virtual ~IFunction() = default;

    virtual String getName() const = 0;

    virtual ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const = 0;
    virtual ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        return executeImpl(arguments, result_type, input_rows_count);
    }

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for columns,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** If function arguments has single low cardinality column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all low cardinality columns to ordinary columns.
      * Returns ColumnLowCardinality if at least one argument is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /** True if function can be called on default arguments (include Nullable's) and won't throw.
      * Counterexample: modulo(0, 0)
      */
    virtual bool canBeExecutedOnDefaultArguments() const { return true; }

    /// Properties from IFunctionBase (see IFunction.h)
    virtual bool isSuitableForConstantFolding() const { return true; }
    virtual ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const ColumnsWithTypeAndName & /*arguments*/) const { return nullptr; }
    virtual bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const { return false; }
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool isStateful() const { return false; }
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    using Monotonicity = IFunctionBase::Monotonicity;
    virtual Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
    {
        throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const
    {
        throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnTypeImpl(data_types);
    }

    virtual bool isVariadic() const { return false; }

    virtual void getLambdaArgumentTypes(DataTypes & /*arguments*/) const
    {
        throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const { return {}; }


#if USE_EMBEDDED_COMPILER

    bool isCompilable(const DataTypes & arguments) const;

    llvm::Value * compile(llvm::IRBuilderBase &, const DataTypes & arguments, Values values) const;

#endif

protected:

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilableImpl(const DataTypes &) const { return false; }

    virtual llvm::Value * compileImpl(llvm::IRBuilderBase &, const DataTypes &, Values) const
    {
        throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif
};

using FunctionPtr = std::shared_ptr<IFunction>;

}
