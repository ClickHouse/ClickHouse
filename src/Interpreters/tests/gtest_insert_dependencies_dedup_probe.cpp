#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMemory.h>

#include <fmt/format.h>

using namespace DB;

namespace
{

/// A minimal in-memory database in the process-wide `DatabaseCatalog` holding the sources, the
/// targets and the materialized views between them that the probe walks. The views are real
/// `StorageMaterializedView`s pointing at their `TO` tables, and their dependency on the source is
/// registered the way `InterpreterCreateQuery` registers it - which is what `getDependentViews`,
/// and therefore the probe, answers from.
struct State
{
    /// Unique, so that it cannot collide with the databases other gtests attach to the same catalog.
    static constexpr auto database_name = "insert_dependencies_dedup_probe_test_db";

    static const State & instance()
    {
        static State state;
        return state;
    }

    ContextMutablePtr context;
    DatabasePtr database;

    void createMemoryTable(const std::string & name) const
    {
        database->attachTable(
            context,
            name,
            std::make_shared<StorageMemory>(
                StorageID(database_name, name), columns(), ConstraintsDescription{}, String{}, MemorySettings{}),
            {});
    }

    void createMaterializedView(const std::string & name, const std::string & source, const std::string & target) const
    {
        const auto query_text
            = fmt::format("CREATE MATERIALIZED VIEW {0}.{1} TO {0}.{2} AS SELECT x FROM {0}.{3}", database_name, name, target, source);
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, query_text, 1000, 1000, 1000000);

        auto view = std::make_shared<StorageMaterializedView>(
            StorageID(database_name, name),
            context,
            ast->as<ASTCreateQuery &>(),
            columns(),
            LoadingStrictnessLevel::CREATE,
            /*comment=*/ "",
            /*is_restore_from_backup=*/ false);
        database->attachTable(context, name, view, {});

        /// The view depends on its source: `getDependentViews(source)` now lists it.
        DatabaseCatalog::instance().addDependencies(StorageID(database_name, name), {}, {}, {StorageID(database_name, source)});
    }

private:
    State() : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        database = std::make_shared<DatabaseMemory>(database_name, context);
        DatabaseCatalog::instance().attachDatabase(database_name, database);
    }

    static ColumnsDescription columns()
    {
        return ColumnsDescription{NamesAndTypesList{{"x", std::make_shared<DataTypeUInt64>()}}};
    }
};

bool dependentViewsDeduplicate(const std::string & source)
{
    return InsertDependenciesBuilder::dependentViewsDeduplicateBlocksOnInsert(
        StorageID(State::database_name, source), State::instance().context);
}

bool dependentViewsCertainlyDeduplicate(const std::string & source)
{
    return InsertDependenciesBuilder::dependentViewsCertainlyDeduplicateBlocksOnInsert(
        StorageID(State::database_name, source), State::instance().context);
}

}

/// `dependentViewsDeduplicateBlocksOnInsert` decides whether a source that streams into materialized
/// views needs a per-chunk deduplication token at all. A token only matters to a target sink that
/// deduplicates, so a source whose views feed nothing but `Memory` tables must report `false`, while
/// anything the probe cannot see through fails closed.

TEST(InsertDependenciesDeduplicationProbe, SourceWithoutDependentViewsDoesNotDeduplicate)
{
    const auto & state = State::instance();
    state.createMemoryTable("lonely_source");

    EXPECT_FALSE(dependentViewsDeduplicate("lonely_source"));
}

TEST(InsertDependenciesDeduplicationProbe, ViewIntoMemoryDoesNotDeduplicate)
{
    /// `Memory` never consults deduplication block ids, so the token is dead weight here and any
    /// requirement attached to producing it (a strong `ETag` on the object storage queue) must not
    /// apply.
    const auto & state = State::instance();
    state.createMemoryTable("memory_source");
    state.createMemoryTable("memory_target");
    state.createMaterializedView("view_into_memory", "memory_source", "memory_target");

    EXPECT_FALSE(dependentViewsDeduplicate("memory_source"));
}

TEST(InsertDependenciesDeduplicationProbe, UnresolvableTargetFailsClosed)
{
    /// Whether the target deduplicates cannot be told when it is not there: keep the token.
    const auto & state = State::instance();
    state.createMemoryTable("dangling_source");
    state.createMaterializedView("view_into_nothing", "dangling_source", "table_that_does_not_exist");

    EXPECT_TRUE(dependentViewsDeduplicate("dangling_source"));
}

TEST(InsertDependenciesDeduplicationProbe, ViewsBehindTheFirstTargetAreFollowed)
{
    /// The insert into a view's target pushes to that target's own dependent views in turn, so a
    /// deduplicating (here: unresolvable, hence fail-closed) sink two hops away still counts. The
    /// first hop alone would report `false` - it is a `Memory` table, as in the test above.
    const auto & state = State::instance();
    state.createMemoryTable("cascade_source");
    state.createMemoryTable("cascade_middle");
    state.createMaterializedView("cascade_first_hop", "cascade_source", "cascade_middle");
    ASSERT_FALSE(dependentViewsDeduplicate("cascade_source"));

    state.createMaterializedView("cascade_second_hop", "cascade_middle", "another_table_that_does_not_exist");
    EXPECT_TRUE(dependentViewsDeduplicate("cascade_source"));
}

/// `dependentViewsCertainlyDeduplicateBlocksOnInsert` decides whether a partially inserted batch may be
/// replayed. That is only safe when the repeated rows are certainly dropped, so it fails the other way:
/// whatever the probe cannot see through counts as not deduplicating.

TEST(InsertDependenciesDeduplicationProbe, CertainProbeSourceWithoutDependentViews)
{
    const auto & state = State::instance();
    state.createMemoryTable("certain_lonely_source");

    EXPECT_FALSE(dependentViewsCertainlyDeduplicate("certain_lonely_source"));
}

TEST(InsertDependenciesDeduplicationProbe, CertainProbeViewIntoMemory)
{
    const auto & state = State::instance();
    state.createMemoryTable("certain_memory_source");
    state.createMemoryTable("certain_memory_target");
    state.createMaterializedView("certain_view_into_memory", "certain_memory_source", "certain_memory_target");

    EXPECT_FALSE(dependentViewsCertainlyDeduplicate("certain_memory_source"));
}

TEST(InsertDependenciesDeduplicationProbe, CertainProbeUnresolvableTargetDoesNotCount)
{
    /// The fail-closed probe keeps the deduplication token for such a source, but a replay must not
    /// rely on a target that is not there.
    const auto & state = State::instance();
    state.createMemoryTable("certain_dangling_source");
    state.createMaterializedView("certain_view_into_nothing", "certain_dangling_source", "certain_table_that_does_not_exist");

    EXPECT_TRUE(dependentViewsDeduplicate("certain_dangling_source"));
    EXPECT_FALSE(dependentViewsCertainlyDeduplicate("certain_dangling_source"));
}
