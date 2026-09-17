#include <Databases/DatabaseAtomic.h>
#include <Databases/IDatabase.h>

#include <DataTypes/UDT/IAuthorityAdapter.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <utility>

namespace DB::UDT
{
namespace
{

static_assert(noexcept(std::declval<DatabaseAtomic &>().activateUDTAuthorityAfterFirstPublication()));

class UnsupportedDatabaseForTest final : public IDatabase
{
public:
    explicit UnsupportedDatabaseForTest(String name)
        : IDatabase(std::move(name))
    {
    }

    String getEngineName() const override { return "UnsupportedForTest"; }
    bool isTableExist(const String &, ContextPtr) const override { return false; }
    StoragePtr tryGetTable(const String &, ContextPtr) const override { return {}; }
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const override { return {}; }
    bool empty() const override { return true; }
    void shutdown() override { }

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override { return {}; }
};

TEST(DatabaseUDTAuthority, DefaultAdapterIsOneAllocationFreeFailClosedInstance)
{
    UnsupportedDatabaseForTest first_database("first");
    UnsupportedDatabaseForTest second_database("second");

    const auto & first = first_database.getUDTAuthorityAdapter();
    const auto & second = second_database.getUDTAuthorityAdapter();
    const auto & first_supported = first_database.getSupportedUDTAuthorityCapabilities();
    const auto & second_supported = second_database.getSupportedUDTAuthorityCapabilities();
    EXPECT_EQ(&first, &second);
    EXPECT_EQ(&first, &getUnsupportedAuthorityAdapter());
    EXPECT_EQ(&first_supported, &second_supported);
    EXPECT_EQ(first.getDatabaseUUID(), UUIDHelpers::Nil);
    EXPECT_EQ(first.getCapabilities(), TypeAuthorityCapabilities{});
    EXPECT_EQ(first_supported, TypeAuthorityCapabilities{});

    auto owning_api_compatibility_pointer = makeUnsupportedAuthorityAdapter();
    EXPECT_EQ(owning_api_compatibility_pointer.get(), &first);
    EXPECT_EQ(owning_api_compatibility_pointer.use_count(), 0);

    EXPECT_NO_THROW(first.requireCapabilities(0, "capability-free operation"));
    EXPECT_THROW(
        first.requireCapabilities(typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution), "type binding"), Exception);
    EXPECT_THROW(static_cast<void>(first.beginResolutionSession()), Exception);
}

}
}
