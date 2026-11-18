#include <gtest/gtest.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMemory.h>

using namespace DB;

namespace
{

/// Two tables: one whose column needs quoting in a plan column identifier, one whose does not.
/// Reading the latter registers no quoted identifier, which is what made branch order observable.
struct State
{
    State(const State &) = delete;

    ContextMutablePtr context;

    static const State & instance()
    {
        static State state;
        return state;
    }

private:
    explicit State()
        : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        /// Unique name: the DatabaseCatalog is process-wide and shared with every other gtest.
        static constexpr auto database_name = "collect_legacy_action_node_names_test_db";

        DatabasePtr database = std::make_shared<DatabaseMemory>(database_name, context);

        auto attach = [&](const String & table_name, const NamesAndTypesList & columns)
        {
            database->attachTable(
                context,
                table_name,
                std::make_shared<StorageMemory>(
                    StorageID(database_name, table_name),
                    ColumnsDescription{columns},
                    ConstraintsDescription{},
                    String{},
                    MemorySettings{}),
                {});
        };

        auto u64 = std::make_shared<DataTypeUInt64>();
        attach("quoted_ids", NamesAndTypesList{{"a.b", u64}, {"v", u64}});
        attach("plain_only", NamesAndTypesList{{"p", u64}, {"v", u64}});

        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);
        context->setCurrentDatabase(database_name);
    }
};

QueryTreeNodePtr analyzedQueryTree(const String & query, const ContextMutablePtr & context)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000, 1000000);
    auto query_tree = buildQueryTree(ast, context);
    QueryTreePassManager pass_manager(context);
    addQueryTreePasses(pass_manager);
    pass_manager.run(query_tree);
    return query_tree;
}

}

/// `collectLegacyActionNodeNames` must record every query node planned at an intermediate stage,
/// whatever the shared `GlobalPlannerContext` already holds.
///
/// UNION branches are planned one after another against ONE such context
/// (`Planner::buildPlanForUnionNode`), so a collection gate that consults its contents answers
/// differently depending on which branch is planned first: a branch registering no quoted
/// identifier would be skipped, and the names it puts on the wire left uncollected.
///
/// This is asserted here rather than in
/// `tests/integration/test_distributed_quoted_identifier_compatibility` because the defect is not
/// observable from SQL: every UNION shape reachable from a query plans its branches at `Complete`
/// (35 of 35 shapes measured, including `view(UNION)`, a `VIEW` holding the UNION, `Distributed`
/// and `Merge` over that view, `distributed_group_by_no_merge` 1 and 2,
/// `distributed_push_down_limit`, parallel replicas, a JOIN of two aliased remote subqueries, and
/// a UNION inside an `IN` subquery), and `Complete` is excluded from collection anyway. Per the
/// standing preference for SQL-based tests, a gtest is used only because of that measurement.
TEST(CollectLegacyActionNodeNames, CollectionDoesNotDependOnWhatTheSharedContextHolds)
{
    const auto & state = State::instance();
    auto context = Context::createCopy(state.context);

    /// The order that used to hide the defect: the branch registering no quoted identifier first.
    auto first = analyzedQueryTree("SELECT p FROM plain_only", context);
    auto second = analyzedQueryTree("SELECT `a.b` FROM quoted_ids", context);

    /// WithMergeableState is what puts plan column identifiers on the wire; Complete does not.
    SelectQueryOptions options(QueryProcessingStage::WithMergeableState);

    Planner first_planner(first, options);
    first_planner.buildQueryPlanIfNeeded();

    /// Exactly what the UNION branch loop does: the second branch shares the first's global context.
    auto shared_global_context = first_planner.getPlannerContext()->getGlobalPlannerContext();
    Planner second_planner(second, options, shared_global_context);
    second_planner.buildQueryPlanIfNeeded();

    const auto & collected = shared_global_context->getLegacyActionNodeNames();

    /// Both branches must be represented. `plain_only`'s column is spelled identically by both
    /// conventions, so it is Unchanged rather than Unique; the assertion is only that it was not
    /// skipped, which is the invariant the branch order could break.
    EXPECT_NE(collected.getLegacyName("__table1.p").state, LegacyActionNodeNames::State::Absent)
        << "the branch planned first was skipped: collection consulted the shared context";

    auto quoted = collected.getLegacyName("__table1.`a.b`");
    EXPECT_EQ(quoted.state, LegacyActionNodeNames::State::Unique);
    EXPECT_EQ(quoted.legacy_name, "__table1.a.b");
}
