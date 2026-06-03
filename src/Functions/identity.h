#pragma once
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Exception.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class FunctionIdentityBase : public IFunction
{
public:
    FunctionIdentityBase(const char * name_, [[maybe_unused]] bool is_identity_)
        : function_name(name_)
#if USE_EMBEDDED_COMPILER
        , is_identity(is_identity_)
#endif
    {}

    static FunctionPtr create(ContextPtr, const char * name, bool is_identity)
    {
        return std::make_shared<FunctionIdentityBase>(name, is_identity);
    }

    String getName() const override { return function_name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments.front();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        return arguments.front().column;
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & /*types*/, const DataTypePtr & result_type) const override
    {
        return is_identity && canBeNativeType(result_type);
    }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override
    {
        return arguments[0].value;
    }
#endif

private:
    const char * function_name;
#if USE_EMBEDDED_COMPILER
    bool is_identity;
#endif
};


/// Default-constructible identity function, used as a template argument in FunctionMapToArrayAdapter
class FunctionIdentity final : public FunctionIdentityBase
{
public:
    FunctionIdentity() : FunctionIdentityBase("identity", true) {}
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIdentity>(); }
};


class FunctionActionName final : public FunctionIdentityBase
{
public:
    FunctionActionName() : FunctionIdentityBase("__actionName", false) {}
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionActionName>(); }
    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    /// Do not allow any argument to have type other than String
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & arg : arguments)
        {
            if (WhichDataType(arg).isString())
                continue;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function __actionName is internal nad should not be used directly");
        }

        return FunctionIdentityBase::getReturnTypeImpl(arguments);
    }
};

struct AliasMarkerName
{
    static constexpr auto name = "__aliasMarker";
};

/**
 * __aliasMarker is an internal function used to enforce an alias-preserving projection step exactly
 * where it appears in distributed SQL transport.
 *
 * It is injected only when a pushed-down expression must still behave like a real column from the
 * initiator's point of view, rather than as an arbitrary expression produced on the initiator. This
 * typically happens after expanding an ALIAS column to its underlying expression for distributed SQL
 * transport. Conceptually, if the initiator has `SELECT foo AS bar FROM distr` and `foo` is an ALIAS
 * column such as `1 + x`, the remote query should look like
 * `SELECT __aliasMarker(1 + x, 'table1.foo') AS bar FROM local AS table1`.
 *
 * The user-facing SQL alias (`bar` in the example above) is separate and must stay untouched.
 * __aliasMarker carries only the low-level column identity that says "treat this expression as the
 * expanded form of that logical column". Preserving that identity is important because otherwise remote
 * headers may diverge from initiator expectations, leading to header mismatch, wrong column association,
 * or column-count mismatch.
 *
 * This must not be confused with normal SQL aliases that appear in the query text: those participate
 * in user-visible query semantics and may or may not be materialized depending on the execution stage.
 * A normal SQL alias is not enough here because it may interfere with user query logic, clash with
 * existing names, and in the mergeable-state path the final projection step that normally assigns
 * aliases is intentionally skipped (see the conditional createComputeAliasColumnsStep(...) path in
 * PlannerJoinTree::buildQueryPlanForTableExpression()).
 *
 * This is also why __aliasMarker is not the same as __actionName. For this use case we need the
 * wrapper to be consumed into an alias/projection boundary on top of the child expression, so the
 * expression keeps behaving like a distinct logical column. __actionName would instead survive as a
 * normal function node with a forced result name, which is a different semantic contract and a worse
 * fit for distributed alias transport.
 *
 * The marker also prevents distinct logical columns with the same expression from collapsing into one
 * transport column. For example, `SELECT 2 * x AS x, 2 * x AS y` must still travel as two columns;
 * otherwise both expressions may collapse to a single `multiply(2, x)` output and break distributed
 * header reconciliation.
 *
 * Lifecycle/invariants:
 * 1) Injected around rewritten alias expressions that need stable transport identity.
 * 2) Materialized before the query is sent to the shard in serialized form: the marker id is converted
 *    to a String alias identifier.
 * 3) Consumed on the receiver by adding a projection step where it appears, so that identity is enforced
 *    in actions without changing the user-facing aliasing logic.
 * 4) Preserved while forwarding to the next hop. Nested marker chains are allowed and each marker may
 *    contribute an alias step during actions construction.
 *
 * This is a temporary bridge while distributed plan transport still relies on SQL text in these paths.
 * As query plan serialization fully replaces that boundary, this marker path should become unnecessary.
 */
class FunctionAliasMarker final : public FunctionIdentityBase
{
public:
    static constexpr auto name = AliasMarkerName::name;
    FunctionAliasMarker() : FunctionIdentityBase(AliasMarkerName::name, /*is_identity=*/ false) {}
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionAliasMarker>(); }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function __aliasMarker expects 2 arguments (expression and column id)");

        return FunctionIdentityBase::getReturnTypeImpl(arguments);
    }
};

}
