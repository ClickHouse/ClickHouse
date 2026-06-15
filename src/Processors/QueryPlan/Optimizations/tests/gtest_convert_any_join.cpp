#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <atomic>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

std::atomic_bool g_test_not_ready_set_probe_triggered = false;

/// `FutureSet` which is intentionally not ready.
class MockNotReadyFutureSet : public FutureSet
{
public:
    explicit MockNotReadyFutureSet(DataTypes types_)
        : types(std::move(types_))
    {}

    SetPtr get() const override
    {
        return nullptr;
    }

    DataTypes getTypes() const override { return types; }
    SetPtr buildOrderedSetInplace(const ContextPtr &) override { return nullptr; }
    Hash getHash() const override { return Hash{0, 0}; }
    ASTPtr getSourceAST() const override { return nullptr; }

private:
    DataTypes types;
};

class FunctionTestNotReadySetProbe : public IFunction
{
public:
    static constexpr auto name = "testNotReadySetProbe";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTestNotReadySetProbe>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        g_test_not_ready_set_probe_triggered = true;
        return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
    }
};

void registerTestFunctions()
{
    auto & factory = FunctionFactory::instance();
    if (!factory.has(FunctionTestNotReadySetProbe::name))
        factory.registerFunction<FunctionTestNotReadySetProbe>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

ActionsDAG makePreActionsDagWithNotReadySetFunction(const FunctionOverloadResolverPtr & function, const String & set_column_name)
{
    ActionsDAG dag;

    ColumnWithTypeAndName lhs_col;
    lhs_col.name = "lhs";
    lhs_col.type = std::make_shared<DataTypeUInt32>();
    lhs_col.column = ColumnConst::create(ColumnUInt32::create(1, 1), 1);
    const auto & lhs_node = dag.addColumn(lhs_col);

    ColumnWithTypeAndName set_column;
    set_column.name = set_column_name;
    set_column.type = std::make_shared<DataTypeSet>();
    set_column.column = ColumnSet::create(
        1,
        std::make_shared<MockNotReadyFutureSet>(DataTypes{std::make_shared<DataTypeUInt32>()}));
    const auto & set_node = dag.addColumn(set_column);

    ActionsDAG::NodeRawConstPtrs children = {&lhs_node, &set_node};
    const auto & flag_node = dag.addFunction(function, children, "flag");

    dag.getOutputs().clear();
    dag.getOutputs().push_back(&flag_node);

    return dag;
}

ActionsDAG makeFlagFilterDag()
{
    ActionsDAG dag;
    const auto & flag_input = dag.addInput("flag", std::make_shared<DataTypeUInt8>());
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&flag_input);
    return dag;
}

}

GTEST_TEST(ConvertAnyJoin, PreActionsDagNotReadySetReturnsBeforeEvaluatingFilter)
{
    tryRegisterFunctions();
    registerTestFunctions();
    auto context = getContext().context;

    auto probe_function = FunctionFactory::instance().get(FunctionTestNotReadySetProbe::name, context);
    auto pre_actions_dag = makePreActionsDagWithNotReadySetFunction(probe_function, "__probe_not_ready_set");
    auto filter_dag = makeFlagFilterDag();

    g_test_not_ready_set_probe_triggered = false;
    FilterResult result = filterResultForMatchedRows(std::move(pre_actions_dag), filter_dag, "flag");

    EXPECT_EQ(result, FilterResult::UNKNOWN);
    EXPECT_FALSE(g_test_not_ready_set_probe_triggered)
        << "`filterResultForMatchedRows` must return before evaluating `pre_actions_dag` with a not-ready `Set`";
}

GTEST_TEST(ConvertAnyJoin, FixedFilterResultForMatchedRows_ReturnsUnknown)
{
    tryRegisterFunctions();
    auto context = getContext().context;

    auto in_function = FunctionFactory::instance().get("in", context);
    auto pre_actions_dag = makePreActionsDagWithNotReadySetFunction(in_function, "__not_ready_set");
    auto filter_dag = makeFlagFilterDag();

    // After PR #103029 fix, `filterResultForMatchedRows` should return UNKNOWN instead of throwing an exception
    FilterResult result = filterResultForMatchedRows(std::move(pre_actions_dag), filter_dag, "flag");

    EXPECT_EQ(result, FilterResult::UNKNOWN)
        << "After PR #103029 fix, should return UNKNOWN when pre_actions_dag contains not-ready Set";
}
