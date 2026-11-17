#include <gtest/gtest.h>

#include <functional>

#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>
#include <Common/tests/gtest_global_context.h>
#include <base/scope_guard.h>

/// StorageSnapshot refers to its storage without owning it, which is sound only while the caller that
/// took the snapshot keeps a StoragePtr. StorageAlias breaks that: it hands out a snapshot of the table
/// it points at, which its own caller neither owns nor locks, so the target could be dropped and
/// destroyed under a running query (heap-use-after-free in IStorage::getStorageID, STID 2350-8243).
/// withStorageHolder is the opt-in ownership token that closes it. These tests assert the token
/// mechanism through weak_ptr expiry, which is deterministic and needs no sanitizer.

namespace
{

using namespace DB;

/// The invariant is a property of StorageSnapshot itself and holds for any engine, so the cheapest
/// storage that can serve the base getStorageSnapshot is enough; a real engine would only add a
/// Context and a disk to the fixture.
class StorageForSnapshotHolderTest final : public IStorage
{
public:
    explicit StorageForSnapshotHolderTest(const StorageID & id) : IStorage(id) { }
    std::string getName() const override { return "StorageForSnapshotHolderTest"; }
};

StoragePtr makeStorage(const String & database, const String & table)
{
    return std::make_shared<StorageForSnapshotHolderTest>(StorageID{database, table});
}

StoragePtr makeStorage(const String & table)
{
    return makeStorage("test_db", table);
}

StorageSnapshotPtr takeSnapshot(const StoragePtr & storage)
{
    return storage->getStorageSnapshot(std::make_shared<StorageInMemoryMetadata>(), nullptr);
}

}

TEST(StorageSnapshotStorageHolder, PinsTheReferentUntilTheLastSnapshotIsGone)
{
    auto storage = makeStorage("pinned");
    std::weak_ptr<const IStorage> weak = storage;
    const String expected_name = storage->getStorageID().getFullTableName();

    auto original = takeSnapshot(storage);
    auto pinned = original->withStorageHolder(storage);

    original.reset();
    storage.reset();

    /// The token is the only owner left, so the referent is still alive and readable.
    ASSERT_FALSE(weak.expired());
    EXPECT_EQ(pinned->storage.getStorageID().getFullTableName(), expected_name);

    auto cloned = pinned->clone(nullptr);
    pinned.reset();

    /// clone has to carry the token, or every projection and metadata re-wrap would silently drop it.
    ASSERT_FALSE(weak.expired());
    EXPECT_EQ(cloned->storage.getStorageID().getFullTableName(), expected_name);

    cloned.reset();

    /// And the token has to be released with the last snapshot, or the table would never be dropped.
    EXPECT_TRUE(weak.expired());
}

TEST(StorageSnapshotStorageHolder, DoesNotInstallTheTokenInTheSourceSnapshot)
{
    auto storage = makeStorage("shared");
    std::weak_ptr<const IStorage> weak = storage;

    auto original = takeSnapshot(storage);
    auto pinned = original->withStorageHolder(storage);

    /// One snapshot is shared between the readers of a query (the query metadata cache and the pinned
    /// snapshot of CREATE MATERIALIZED VIEW ... POPULATE both hand out the same object), so the token
    /// must land on a new snapshot instead of being written into shared state.
    EXPECT_NE(original.get(), pinned.get());

    storage.reset();
    pinned.reset();

    /// `original` is what such a cache would still be handing out, and it must be exactly as
    /// non-owning as it was before the call. It is deliberately not dereferenced here.
    EXPECT_TRUE(weak.expired());
}

TEST(StorageSnapshotStorageHolder, IsNonOwningWithoutAToken)
{
    auto storage = makeStorage("unpinned");
    std::weak_ptr<const IStorage> weak = storage;

    auto snapshot = takeSnapshot(storage);
    storage.reset();

    /// The token is opt-in: a snapshot consumed inside the scope of an owning StoragePtr must not
    /// delay the drop of its table. This is also the control for the assertions above.
    EXPECT_TRUE(weak.expired());
}

TEST(StorageSnapshotStorageHolder, AliasChainKeepsTheInnermostReferentPinned)
{
    auto base = makeStorage("base");
    auto intermediate = makeStorage("intermediate_alias");
    std::weak_ptr<const IStorage> weak_base = base;
    std::weak_ptr<const IStorage> weak_intermediate = intermediate;
    const String expected_name = base->getStorageID().getFullTableName();

    /// An `outer_alias -> inner_alias -> base` chain is supported (04946_storage_alias_attach_stored
    /// _definition.sh), and it resolves innermost-first: the inner link pins the referent it took the
    /// snapshot from, then the outer link offers its own target, which is the inner alias instead.
    auto inner = takeSnapshot(base)->withStorageHolder(base);
    auto outer = inner->withStorageHolder(intermediate);

    inner.reset();
    base.reset();
    intermediate.reset();

    /// The first token wins, so the referent stays pinned. An unconditional overwrite would pin the
    /// intermediate alias instead and leave the referent to be freed under the query.
    ASSERT_FALSE(weak_base.expired());
    EXPECT_EQ(outer->storage.getStorageID().getFullTableName(), expected_name);

    /// The intermediate is dropped rather than kept, so a chain does not accumulate pins.
    EXPECT_TRUE(weak_intermediate.expired());
}

TEST(StorageSnapshotStorageHolder, CloneWithMetadataCarriesTheToken)
{
    auto storage = makeStorage("re_wrapped");
    std::weak_ptr<const IStorage> weak = storage;
    const String expected_name = storage->getStorageID().getFullTableName();

    auto pinned = takeSnapshot(storage)->withStorageHolder(storage);

    /// TableNode::updateStorage and TableNode::setTableExpressionModifiers re-wrap a snapshot through
    /// the two-argument overload, so a table expression carrying `FINAL` or `SAMPLE` reaches it on an
    /// ordinary read.
    auto rewrapped = pinned->clone(std::make_shared<StorageInMemoryMetadata>(), pinned->data);

    pinned.reset();
    storage.reset();

    ASSERT_FALSE(weak.expired());
    EXPECT_EQ(rewrapped->storage.getStorageID().getFullTableName(), expected_name);

    rewrapped.reset();
    EXPECT_TRUE(weak.expired());
}

namespace
{

/// Both StorageAlias accessors have to pin, so the two arms below differ only in which one they call.
/// `take_snapshot` receives the alias rather than capturing it, because the alias must be constructed
/// after the target is attached.
void assertStorageAliasPinsItsTarget(
    const String & database_name,
    const std::function<StorageSnapshotPtr(const StorageAlias &, ContextPtr)> & take_snapshot)
{
    auto context = getContext().context;

    auto database = std::make_shared<DatabaseMemory>(database_name, context);
    DatabaseCatalog::instance().attachDatabase(database_name, database);
    SCOPE_EXIT({ DatabaseCatalog::instance().detachDatabase(context, database_name, false, false); });

    auto target = makeStorage(database_name, "target");
    std::weak_ptr<const IStorage> weak_target = target;
    database->attachTable(context, "target", target, {});
    target.reset();

    /// An alias owns nothing and resolves its target through DatabaseCatalog on every access, so this
    /// is the whole ownership state a query has when it reads through one.
    auto alias = std::make_shared<StorageAlias>(StorageID{database_name, "alias"}, context, database_name, "target");
    auto snapshot = take_snapshot(*alias, context);

    /// Detaching releases the catalog's own StoragePtr, which is what DROP does before the background
    /// worker destroys the table: DatabaseCatalog::getTablesToDrop frees it as soon as no one else
    /// owns it. So from here the snapshot's token is the only thing that can keep the target alive.
    database->detachTable(context, "target");

    ASSERT_FALSE(weak_target.expired());
    EXPECT_EQ(snapshot->storage.getStorageID().getFullTableName(), database_name + ".target");

    snapshot.reset();
    EXPECT_TRUE(weak_target.expired());
}

}

TEST(StorageSnapshotStorageHolder, StorageAliasPinsTheTargetItHandsOut)
{
    assertStorageAliasPinsItsTarget(
        "test_storage_alias_holder_db",
        [](const StorageAlias & alias, ContextPtr context)
        { return alias.getStorageSnapshot(std::make_shared<StorageInMemoryMetadata>(), context); });
}

TEST(StorageSnapshotStorageHolder, StorageAliasPinsTheTargetForASnapshotWithoutData)
{
    assertStorageAliasPinsItsTarget(
        "test_storage_alias_holder_without_data_db",
        [](const StorageAlias & alias, ContextPtr context)
        { return alias.getStorageSnapshotWithoutData(std::make_shared<StorageInMemoryMetadata>(), context); });
}
