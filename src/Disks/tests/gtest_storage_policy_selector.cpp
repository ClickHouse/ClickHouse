#include <gtest/gtest.h>

#include <Disks/DiskSelector.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <string>

using namespace DB;

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> makeEmptyStorageConfiguration()
{
    std::string xml(R"END(<clickhouse>
    <storage_configuration>
        <disks/>
        <policies/>
    </storage_configuration>
</clickhouse>)END");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    return new Poco::Util::XMLConfiguration(document);
}

/// A selector holding only the implicitly created `default` policy over the implicit `default` disk.
StoragePolicySelector makeSelector(const Poco::Util::AbstractConfiguration & config)
{
    auto disk_selector = std::make_shared<DiskSelector>();
    disk_selector->initialize(config, "storage_configuration.disks", getContext().context);
    return StoragePolicySelector(config, "storage_configuration.policies", disk_selector);
}

StoragePolicyPtr makePolicy(const String & name, const StoragePolicySelector & selector)
{
    auto volume = std::make_shared<SingleDiskVolume>("_volume_" + name, selector.get("default")->getDisks().front());
    return std::make_shared<StoragePolicy>(name, Volumes{volume}, /* move_factor_= */ 0.0);
}

const String temporary_name = String(StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX) + "test_policy";

}

TEST(StoragePolicySelector, AddOrReplaceTemporaryInsertsWhenAbsent)
{
    auto config = makeEmptyStorageConfiguration();
    auto selector = makeSelector(*config);

    ASSERT_EQ(selector.tryGet(temporary_name), nullptr);

    auto policy = makePolicy(temporary_name, selector);
    selector.addOrReplaceTemporary(policy);

    EXPECT_EQ(selector.tryGet(temporary_name), policy);
}

TEST(StoragePolicySelector, AddOrReplaceTemporaryReplacesExisting)
{
    auto config = makeEmptyStorageConfiguration();
    auto selector = makeSelector(*config);

    auto first = makePolicy(temporary_name, selector);
    auto second = makePolicy(temporary_name, selector);
    ASSERT_NE(first, second);

    selector.addOrReplaceTemporary(first);
    ASSERT_EQ(selector.tryGet(temporary_name), first);

    selector.addOrReplaceTemporary(second);
    EXPECT_EQ(selector.tryGet(temporary_name), second);
}

/// Skipped under debug/sanitizers: LOGICAL_ERROR aborts there, so EXPECT_THROW can't catch it.
#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(StoragePolicySelector, AddOrReplaceTemporaryRejectsNonTemporaryName)
{
    auto config = makeEmptyStorageConfiguration();
    auto selector = makeSelector(*config);

    auto default_policy = selector.get("default");
    auto policy = makePolicy("default", selector);

    EXPECT_THROW(selector.addOrReplaceTemporary(policy), Exception);
    EXPECT_EQ(selector.tryGet("default"), default_policy);
}

/// `add` refuses a duplicate, which is why replacing needs its own entry point.
TEST(StoragePolicySelector, AddRefusesDuplicate)
{
    auto config = makeEmptyStorageConfiguration();
    auto selector = makeSelector(*config);

    selector.addOrReplaceTemporary(makePolicy(temporary_name, selector));
    EXPECT_THROW(selector.add(makePolicy(temporary_name, selector)), Exception);
}

#endif
