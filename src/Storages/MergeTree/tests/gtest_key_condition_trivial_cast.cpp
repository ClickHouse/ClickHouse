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
const ActionsDAG::Node * findFunction(const ActionsDAG & dag, const std::string & name)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION
            && node.function_base
            && node.function_base->getName() == name)
            return &node;
    }
    return nullptr;
}

bool dagContainsFunction(const ActionsDAG & dag, const std::string & name)
{
    return findFunction(dag, name) != nullptr;
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

/// Build the analyzer-inserted wrapper shape that `optimize_extract_common_expressions`
/// produces: `_CAST(equals(key, value), 'target_type')`. Here the cast sits directly on a
/// condition, so its value is only truth-tested by the consumer and a `Nullable`-widening
/// cast may be dropped.
CastShape buildCastOverEquals(ContextPtr context, const DataTypePtr & argument_type, const DataTypePtr & target_type)
{
    CastShape shape;
    auto & dag = shape.dag;

    const auto & key_node = dag.addInput("key", argument_type);
    const auto & value_node = dag.addInput("value", argument_type);

    auto equals_resolver = FunctionFactory::instance().get("equals", context);
    ColumnsWithTypeAndName equals_arguments{
        {nullptr, key_node.result_type, key_node.result_name},
        {nullptr, value_node.result_type, value_node.result_name},
    };
    const auto & equals_node = dag.addFunction(
        equals_resolver->build(equals_arguments),
        {&key_node, &value_node},
        "equals(" + key_node.result_name + ", " + value_node.result_name + ")");

    auto type_name = target_type->getName();
    const auto & type_literal_node = dag.addColumn(
        DataTypeString().createColumnConst(0, type_name),
        std::make_shared<DataTypeString>(),
        "'" + type_name + "'_String");

    auto cast_function = createInternalCast(
        ColumnWithTypeAndName{equals_node.result_type, equals_node.result_name},
        target_type,
        CastType::nonAccurate,
        std::nullopt,
        context);

    const auto & cast_node = dag.addFunction(
        cast_function,
        {&equals_node, &type_literal_node},
        cast_function->getName() + "(" + equals_node.result_name + ", " + type_literal_node.result_name + ")");

    dag.getOutputs().push_back(&cast_node);
    shape.predicate = &cast_node;
    return shape;
}

/// Build `equals(isNullable(_CAST(key, 'Nullable(UInt32)')), 1)`. `isNullable` derives its
/// constant result from the argument's type alone, so the `_CAST` here is in a value position:
/// dropping it would let the clone re-resolve `isNullable` over the narrower `UInt32` and fold
/// the always-true predicate to `equals(0, 1)`.
CastShape buildIsNullableOverCast(ContextPtr context)
{
    CastShape shape;
    auto & dag = shape.dag;

    auto uint32 = std::make_shared<DataTypeUInt32>();
    auto nullable_uint32 = makeNullable(uint32);

    const auto & key_node = dag.addInput("key", uint32);

    auto type_name = nullable_uint32->getName();
    const auto & type_literal_node = dag.addColumn(
        DataTypeString().createColumnConst(0, type_name),
        std::make_shared<DataTypeString>(),
        "'" + type_name + "'_String");

    auto cast_function = createInternalCast(
        ColumnWithTypeAndName{key_node.result_type, key_node.result_name},
        nullable_uint32,
        CastType::nonAccurate,
        std::nullopt,
        context);

    const auto & cast_node = dag.addFunction(
        cast_function,
        {&key_node, &type_literal_node},
        cast_function->getName() + "(" + key_node.result_name + ", " + type_literal_node.result_name + ")");

    auto is_nullable_resolver = FunctionFactory::instance().get("isNullable", context);
    const auto & is_nullable_node = dag.addFunction(
        is_nullable_resolver->build({{nullptr, cast_node.result_type, cast_node.result_name}}),
        {&cast_node},
        "isNullable(" + cast_node.result_name + ")");

    auto uint8 = std::make_shared<DataTypeUInt8>();
    const auto & one_node = dag.addColumn(uint8->createColumnConst(0, UInt64(1)), uint8, "1_UInt8");

    auto equals_resolver = FunctionFactory::instance().get("equals", context);
    ColumnsWithTypeAndName equals_arguments{
        {nullptr, is_nullable_node.result_type, is_nullable_node.result_name},
        {one_node.column, one_node.result_type, one_node.result_name},
    };
    const auto & equals_node = dag.addFunction(
        equals_resolver->build(equals_arguments),
        {&is_nullable_node, &one_node},
        "equals(" + is_nullable_node.result_name + ", " + one_node.result_name + ")");

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

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, /* boolean_context */ false);
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

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, /* boolean_context */ false);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_FALSE(dagContainsFunction(*filter_dag.dag, "CAST"));
    EXPECT_FALSE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

/// `_CAST(A, 'Nullable(UInt8)')` sitting directly on a condition only widens a non-nullable
/// type into `Nullable` of that very same underlying type, so it can never turn a non-`NULL`
/// value into `NULL`. This is the shape `optimize_extract_common_expressions` produces, and in
/// a truth-tested (boolean) position the wrapper must be stripped so `KeyCondition` sees `A`.
TEST(KeyConditionIsTrivialCast, InternalCastWideningToNullableIsStrippedInBooleanContext)
{
    auto context = getRegisteredContext();
    auto uint32 = std::make_shared<DataTypeUInt32>();
    auto nullable_uint8 = makeNullable(std::make_shared<DataTypeUInt8>());

    auto shape = buildCastOverEquals(context, uint32, nullable_uint8);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, /* boolean_context */ true);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_FALSE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

/// The same widening cast in a value position must survive: it is value-preserving but not
/// type-preserving, and the enclosing function may observe the argument's type.
TEST(KeyConditionIsTrivialCast, InternalCastWideningToNullableIsKeptOutsideBooleanContext)
{
    auto context = getRegisteredContext();
    auto uint32 = std::make_shared<DataTypeUInt32>();
    auto nullable_uint8 = makeNullable(std::make_shared<DataTypeUInt8>());

    auto shape = buildCastOverEquals(context, uint32, nullable_uint8);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, /* boolean_context */ false);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

/// Regression for the wrong-result hazard of the widening rule: a function whose constant result
/// is derived from the argument's type alone (`isNullable`) must keep seeing `Nullable(UInt32)`.
/// If the cast were stripped here, the clone would re-resolve `isNullable` over `UInt32`, fold it
/// to `0`, and turn the always-true `isNullable(...) = 1` into an always-false `KeyCondition`
/// that prunes every granule.
TEST(KeyConditionIsTrivialCast, InternalCastWideningIsKeptUnderTypeObservingFunction)
{
    auto context = getRegisteredContext();

    auto shape = buildIsNullableOverCast(context);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, /* boolean_context */ true);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "_CAST"));

    /// The folded value of `isNullable` must stay `1`, exactly as in the original DAG.
    const auto * is_nullable_node = findFunction(*filter_dag.dag, "isNullable");
    ASSERT_TRUE(is_nullable_node != nullptr);
    ASSERT_TRUE(is_nullable_node->column != nullptr);
    EXPECT_EQ(is_nullable_node->column->getUInt(0), 1);
}

/// Negative case: when the cast changes the value's underlying type (not merely wrapping it in
/// `Nullable`), it is not value-preserving and `isTrivialCast` must leave it in place even in a
/// boolean context. This guards against the widening rule dropping casts that change the value.
TEST(KeyConditionIsTrivialCast, InternalCastWithMismatchedUnderlyingTypeIsNotStripped)
{
    auto context = getRegisteredContext();
    auto uint32 = std::make_shared<DataTypeUInt32>();
    auto nullable_int64 = makeNullable(std::make_shared<DataTypeInt64>());

    /// `_CAST(UInt8, 'Nullable(Int64)')` changes the underlying type, so the cast must survive.
    auto shape = buildCastOverEquals(context, uint32, nullable_int64);

    ActionsDAGWithInversionPushDown filter_dag(shape.predicate, context, /* boolean_context */ true);
    ASSERT_TRUE(filter_dag.dag.has_value());

    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "_CAST"));
    EXPECT_TRUE(dagContainsFunction(*filter_dag.dag, "equals"));
}

}
