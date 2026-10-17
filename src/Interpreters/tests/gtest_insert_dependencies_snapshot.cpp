#include <gtest/gtest.h>

#include <algorithm>
#include <ranges>
#include <thread>
#include <vector>

#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

using namespace DB;

namespace
{

constexpr auto DATABASE_NAME = "insert_dependencies_snapshot_test_db";

StorageID srcId() { return StorageID(DATABASE_NAME, "src"); }
StorageID tgtId() { return StorageID(DATABASE_NAME, "tgt"); }

/// Widens its column list on every re-read, standing in for a concurrent `ALTER ADD COLUMN` or an
/// object-storage schema refresh. Driving the drift from the accessor makes the race deterministic,
/// since the traversal observes a shared target once per path.
class DriftingStorage : public StorageWithCommonVirtualColumns
{
public:
    DriftingStorage(const StorageID & table_id_, const ColumnsDescription & columns_)
        : StorageWithCommonVirtualColumns(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "DriftingStorage"; }

    StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr context, bool bypass_metadata_cache) const override
    {
        if (drift_enabled && reads++ > 0)
        {
            auto current = IStorage::getInMemoryMetadataPtr(context, bypass_metadata_cache);
            StorageInMemoryMetadata widened = *current;
            ColumnsDescription columns = widened.getColumns();
            columns.add(ColumnDescription(fmt::format("w{}", reads), std::make_shared<DataTypeUInt32>()));
            widened.setColumns(std::move(columns));
            const_cast<DriftingStorage *>(this)->setInMemoryMetadata(widened);
        }
        auto result = IStorage::getInMemoryMetadataPtr(context, bypass_metadata_cache);
        if (drift_enabled)
            observed.push_back(result->getColumns().size());
        return result;
    }

    void enableDrift()
    {
        reads = 0;
        observed.clear();
        drift_enabled = true;
    }
    void disableDrift() { drift_enabled = false; }
    size_t readCount() const { return reads; }

    /// Column counts of the handles returned while drifting, in call order.
    const std::vector<size_t> & observedColumnCounts() const { return observed; }

private:
    mutable size_t reads = 0;
    mutable std::vector<size_t> observed;
    bool drift_enabled = false;
};

/// `src -> {mv_a, mv_b} -> tgt`, so `tgt` is reached by two paths and observed twice. The database
/// name must be unique: `DatabaseCatalog` is process-wide and shared with the other gtests here.
struct Fixture
{
    ContextMutablePtr context;
    StoragePtr src;
    std::shared_ptr<DriftingStorage> tgt;

    static const Fixture & instance()
    {
        static Fixture fixture;
        return fixture;
    }

private:
    Fixture()
        : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        DatabasePtr database = std::make_shared<DatabaseMemory>(DATABASE_NAME, context);
        DatabaseCatalog::instance().attachDatabase(DATABASE_NAME, database);
        context->setCurrentDatabase(DATABASE_NAME);

        NamesAndTypesList src_columns{{"k", std::make_shared<DataTypeUInt32>()}};
        NamesAndTypesList tgt_columns{
            {"k", std::make_shared<DataTypeUInt32>()},
            {"v", std::make_shared<DataTypeUInt32>()},
        };

        src = std::make_shared<StorageMemory>(
            srcId(), ColumnsDescription{src_columns}, ConstraintsDescription{}, String{}, MemorySettings{});
        database->attachTable(context, "src", src, {});

        tgt = std::make_shared<DriftingStorage>(tgtId(), ColumnsDescription{tgt_columns});
        database->attachTable(context, "tgt", std::static_pointer_cast<IStorage>(tgt), {});

        for (const auto * view_name : {"mv_a", "mv_b"})
        {
            const String create_sql = fmt::format(
                "CREATE MATERIALIZED VIEW {0}.{1} TO {0}.tgt AS SELECT k, 1 AS v FROM {0}.src", DATABASE_NAME, view_name);
            ParserCreateQuery parser;
            ASTPtr ast = parseQuery(parser, create_sql, 100000, 1000, 1000000);

            auto view = std::make_shared<StorageMaterializedView>(
                StorageID(DATABASE_NAME, view_name),
                context,
                ast->as<ASTCreateQuery &>(),
                ColumnsDescription{tgt_columns},
                LoadingStrictnessLevel::CREATE,
                String{},
                /*is_restore_from_backup=*/false);
            database->attachTable(context, view_name, view, {});

            /// `collectAllDependencies` finds views via `getDependentViews`, which
            /// `InterpreterCreateQuery` normally fills in; no DDL interpreter runs here.
            DatabaseCatalog::instance().addDependencies(
                StorageID(DATABASE_NAME, view_name), /*new_referential_dependencies=*/{},
                /*new_loading_dependencies=*/{}, /*new_view_dependencies=*/{srcId()});
        }
    }
};

}

/// `createSinkImpl` pairs `output_headers.at(view_id)` with `metadata_snapshots.at(target)`, so both
/// must come from one snapshot or `Chain::addSink` throws `Block structure mismatch`. Asserts the
/// stored snapshot is the first observation, and that both views' headers equal its sample block.
TEST(InsertDependenciesSnapshot, TargetSharedByTwoViewsKeepsFirstSnapshot)
{
    /// Run in a dedicated thread so `current_thread` starts as nullptr, independent of whatever
    /// `ThreadStatus` other tests in `unit_tests_dbms` left behind. One is needed because
    /// `ThreadGroup::ThreadGroup`, reached per view, reads `CurrentThread::get()`.
    std::thread worker([]
    {
        ThreadStatus thread_status;

        const auto & fixture = Fixture::instance();

        auto insert_context = Context::createCopy(fixture.context);
        insert_context->makeQueryContext();

        /// Restore afterwards: `DatabaseCatalog` is shared with every other test in this binary.
        auto original_handle = fixture.tgt->getInMemoryMetadataPtr(nullptr, /*bypass_metadata_cache=*/true);
        const StorageInMemoryMetadata & original = *original_handle;
        struct Restore
        {
            std::shared_ptr<DriftingStorage> tgt;
            const StorageInMemoryMetadata & metadata;
            ~Restore()
            {
                tgt->disableDrift();
                tgt->setInMemoryMetadata(metadata);
            }
        } restore{fixture.tgt, original};

        auto insert_ast = make_intrusive<ASTInsertQuery>();
        insert_ast->table_id = srcId();
        ASTPtr insert_query = insert_ast;
        auto insert_header = std::make_shared<const Block>(ColumnsWithTypeAndName{
            {std::make_shared<DataTypeUInt32>()->createColumn(), std::make_shared<DataTypeUInt32>(), "k"}});

        fixture.tgt->enableDrift();
        auto builder = InsertDependenciesBuilder::create(
            fixture.src, insert_query, insert_header, /*async_insert_=*/false, /*skip_destination_table_=*/false,
            /*max_insert_threads=*/size_t{1}, insert_context);
        fixture.tgt->disableDrift();

        ASSERT_GE(fixture.tgt->readCount(), 2u) << "the fixture must observe the shared target more than once";

        const auto & metadata_snapshots = builder->metadata_snapshots;
        const auto & output_headers = builder->output_headers;

        ASSERT_TRUE(metadata_snapshots.contains(tgtId()));
        auto stored = metadata_snapshots.at(tgtId());

        /// Drift only adds columns, so the column count identifies which observation a snapshot came
        /// from, and the widest is the last one. The guards below keep a non-drifting fixture from
        /// satisfying the assertions vacuously.
        const auto & observed = fixture.tgt->observedColumnCounts();
        ASSERT_GE(observed.size(), 2u) << "the fixture must observe the shared target more than once";
        ASSERT_EQ(observed, [&]{ auto sorted = observed; std::ranges::sort(sorted); return sorted; }())
            << "the drift hook must return monotonically widening column lists";
        ASSERT_LT(observed.front(), observed.back()) << "the fixture never produced two different column lists";

        EXPECT_LT(stored->getColumns().size(), observed.back())
            << "the stored snapshot of " << tgtId().getNameForLogs()
            << " is the LAST observation - a later visit replaced the first one";

        size_t views_checked = 0;
        for (const auto & [id, header] : output_headers)
        {
            if (id.empty() || !builder->inner_tables.contains(id) || builder->inner_tables.at(id) != StorageIDMaybeEmpty(tgtId()))
                continue;
            ++views_checked;
            EXPECT_EQ(header->dumpStructure(), stored->getSampleBlock().dumpStructure())
                << "output header of view " << id.getNameForLogs() << " disagrees with the stored snapshot of "
                << tgtId().getNameForLogs();
        }
        EXPECT_EQ(views_checked, 2u) << "the fixture must reach the shared target through two views";
    });
    worker.join();
}
