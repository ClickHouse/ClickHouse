#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Regression coverage for the `isTrivialCast` widening in
/// `src/Storages/MergeTree/KeyCondition.cpp` (issue #105291).
///
/// The analyzer emits the internal name `_CAST` instead of the user-facing
/// `CAST` when it inserts a type-carrying wrapper around a resolved value
/// (for example, the planner-folded result of a scalar subquery on the
/// right-hand side of a comparison). Before the fix `isTrivialCast` only
/// matched the name `CAST`, so the `_CAST` wrapper survived into
/// `KeyCondition::cloneDAGWithInversionPushDown` and the downstream
/// `RPNBuilder::tryGetConstant` could not extract the constant when the
/// wrapper's own `node.column` was not populated (the
/// non-folded shape produced by some planner paths). The pruner then
/// fell back to a full scan.
///
/// The end-to-end Spark + Iceberg shape from issue #105291 is hard to
/// reproduce in a unit test (it depends on the analyzer / planner path that
/// produces a `_CAST` `FUNCTION` node with `column == nullptr`). The tests
/// below pin the targeted behavior at the level of the function that was
/// actually changed: `isTrivialCast` is exercised through the public
/// `ActionsDAGWithInversionPushDown` adapter, and we assert on the
/// structure of the cloned DAG (presence or absence of the `_CAST`
/// function node), which is the only thing the patch can affect.
///
/// Without the patch:
///   - `equals(key, _CAST(value, 'T'_String))` is preserved in the cloned
///     DAG and the partition pruner sees an opaque `_CAST` `FUNCTION` node.
/// With the patch:
///   - `_CAST` is stripped exactly like `CAST` and the cloned DAG only
///     contains `equals(key, value)`.

namespace
{

/// Find a `FUNCTION` node by name in the cloned DAG.
bool dagContainsFunction(const ActionsDAG & dag, const std::string & name)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION
            && node.function_base
            && node.function_base->getName() == name)
            return true;
    }
    return false;
}

struct CastShape
{
    ActionsDAG dag;
    const ActionsDAG::Node * predicate = nullptr;
};

/// Build `equals(key, cast_name(value_child, 'target_type'_String))`. The
/// outer cast wrapper takes its name from `cast_name` (`"CAST"` or
/// `"_CAST"`) and is constructed via `createInternalCast` so the resulting
/// `FUNCTION` node has the same shape the analyzer would produce. The
/// `value_child` is an `INPUT` so the wrapper itself stays un-folded
/// (`column == nullptr`), which is the un-folded shape the issue describes.
CastShape buildEqualsWithCast(
    ContextPtr context,
    const DataTypePtr & key_type,
    const DataTypePtr & value_type,
    const DataTypePtr & target_type,
    CastType cast_type)
{
    CastShape shape;
    auto & dag = shape.dag;

    const auto & key_node = dag.addInput("key", key_type);
    const auto & value_node = dag.addInput("value", value_type);

    /// The type-literal child carries the target-type name as a String constant.
    /// `isTrivialCast` checks (a) that this is a COLUMN node and (b) that the
    /// literal value equals the source-child's `result_type->getName()`.
    auto type_name = target_type->getName();
    const auto & type_literal_node = dag.addColumn(
        DataTypeString().createColumnConst(0, type_name),
        std::make_shared<DataTypeString>(),
        "'" + type_name + "'_String");

    /// Build the cast `FunctionBase` and add it as a `FUNCTION` node. We use
    /// `createInternalCast` for the `_CAST` shape (the analyzer-internal
    /// name) and the public `CAST` overload via `FunctionFactory` for the
    /// `CAST` shape.
    FunctionBasePtr cast_function;
    if (cast_type == CastType::nonAccurate)
    {
        /// Internal `_CAST`. The patched code path widens `isTrivialCast` to
        /// also match this name.
        cast_function = createInternalCast(
            ColumnWithTypeAndName{value_node.result_type, value_node.result_name},
            target_type,
            CastType::nonAccurate,
            std::nullopt,
            context);
    }
    else
    {
        /// Sanity: the user-facing `CAST` shape (matched by `isTrivialCast`
        /// before the patch as well). Built through the standard overload
        /// resolver so the function gets the public name `CAST`.
        auto resolver = FunctionFactory::instance().get("CAST", context);
        ColumnsWithTypeAndName arguments{
            {nullptr, value_node.result_type, value_node.result_name},
            {type_literal_node.column, type_literal_node.result_type, type_literal_node.result_name},
        };
        cast_function = resolver->build(arguments);
    }

    const auto & cast_node = dag.addFunction(
        cast_function,
        {&value_node, &type_literal_node},
        cast_function->getName() + "(" + value_node.result_name + ", " + type_literal_node.result_name + ")");

    /// Since `value_child` is an `INPUT` (non-const), `addFunctionImpl`'s
    /// `all_const` branch is not taken and `getConstantResultForNonConstArguments`
    /// for `_CAST` / `CAST` returns null, so `cast_node.column` is already
    /// `nullptr`. Assert this so the test fails loudly if a future
    /// `addFunction` change starts pre-folding non-const arguments.
    EXPECT_EQ(cast_node.column, nullptr);

    auto equals_resolver = FunctionFactory::instance().get("equals", context);
    ColumnsWithTypeAndName equals_arguments{
        {nullptr, key_node.result_type, key_node.result_name},
        {nullptr, cast_node.result_type, cast_node.result_name},
    };
    auto equals_function = equals_resolver->build(equals_arguments);

    const auto & equals_node = dag.addFunction(
        equals_function,
        {&key_node, &cast_node},
        "equals(" + key_node.result_name + ", " + cast_node.result_name + ")");

    dag.getOutputs().push_back(&equals_node);
    shape.predicate = &equals_node;
    return shape;
}

ContextMutablePtr getRegisteredContext()
{
    tryRegisterFunctions();
    return Context::createCopy(getContext().context);
}

}

/// `_CAST(value, 'T')` with matching source and target types must be
/// stripped by `cloneDAGWithInversionPushDown` (via the widened
/// `isTrivialCast`). Without the patch, `isTrivialCast` only matched the
/// public name `CAST` and left `_CAST` in the cloned DAG, which is what
/// blocked partition pruning in issue #105291.
TEST(KeyConditionIsTrivialCast, InternalCastWithMatchingTypeIsStripped)
{
    auto context = getRegisteredContext();
    auto nullable_int32 = makeNullable(std::make_shared<DataTypeInt32>());

    auto shape = buildEqualsWithCast(context, nullable_int32, nullable_int32, nullable_int32, CastType::nonAccurate);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context);
    ASSERT_TRUE(filter_dag.dag.has_value());

    /// `_CAST` is the wrapper that the patch teaches `isTrivialCast` to
    /// recognize. Its presence in the cloned DAG means the wrapper survived
    /// and the partition pruner would have to treat it as an opaque
    /// function on the RHS of the comparison.
    EXPECT_FALSE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    /// The `equals` node is preserved; the patch only removes the cast.
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

/// Sanity: the existing behavior for the public name `CAST` is unchanged.
/// `isTrivialCast` matched `CAST` before the patch and must keep matching
/// it after the patch.
TEST(KeyConditionIsTrivialCast, PublicCastWithMatchingTypeIsStillStripped)
{
    auto context = getRegisteredContext();
    auto nullable_int32 = makeNullable(std::make_shared<DataTypeInt32>());

    /// `CastType::accurate` here selects the helper's non-internal branch,
    /// which builds via `FunctionFactory::get("CAST", context)`. The
    /// resulting `FUNCTION` node has the public name `CAST`.
    auto shape = buildEqualsWithCast(context, nullable_int32, nullable_int32, nullable_int32, CastType::accurate);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_FALSE(dagContainsFunction(*filter_dag.dag, "CAST"));
    EXPECT_FALSE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

/// Negative case: when the inner type does not match the target type, the
/// cast is not a no-op and `isTrivialCast` must leave it in place. This
/// guards against the patch over-widening and dropping casts that actually
/// change the value's type.
TEST(KeyConditionIsTrivialCast, InternalCastWithMismatchedTypeIsNotStripped)
{
    auto context = getRegisteredContext();
    auto int32 = std::make_shared<DataTypeInt32>();
    auto nullable_int32 = makeNullable(std::make_shared<DataTypeInt32>());

    /// `_CAST(Int32, 'Nullable(Int32)')` is a real type change (it wraps in
    /// `Nullable`), so `isTrivialCast` must return false and the cast must
    /// survive cloning.
    auto shape = buildEqualsWithCast(context, nullable_int32, int32, nullable_int32, CastType::nonAccurate);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

}
