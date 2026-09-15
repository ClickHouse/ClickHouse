#include <Access/AccessControl.h>
#include <Access/EnabledSettings.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/User.h>
#include <Core/Settings.h>
#include <Core/SettingsSnapshot.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <gtest/gtest.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <base/scope_guard.h>

#include <atomic>
#include <thread>
#include <type_traits>

namespace DB::Setting
{
extern const SettingsMap additional_table_filters;
extern const SettingsString log_comment;
extern const SettingsUInt64 max_query_size;
extern const SettingsMaxThreads max_threads;
}

namespace DB::ErrorCodes
{
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{
using namespace DB;

static_assert(std::is_const_v<std::remove_reference_t<decltype(std::declval<Settings &>()[Setting::log_comment])>>);

String serialize(const Settings & settings, SettingsWriteFormat format)
{
    WriteBufferFromOwnString output;
    settings.write(output, format);
    return output.str();
}
}

GTEST_TEST(SettingsSnapshot, CopiesShareUntilExplicitMutation)
{
    Settings parent;
    parent.set(Setting::max_query_size, UInt64(100000));
    parent.set(Setting::log_comment, String(256, 'p'));
    Settings child(parent);
    Settings sibling(parent);

    EXPECT_EQ(&parent[Setting::max_query_size], &child[Setting::max_query_size]);
    EXPECT_EQ(&parent[Setting::log_comment], &child[Setting::log_comment]);

    child.set(Setting::max_query_size, UInt64(100001));
    EXPECT_EQ(parent[Setting::max_query_size].value, 100000);
    EXPECT_EQ(sibling[Setting::max_query_size].value, 100000);
    EXPECT_EQ(child[Setting::max_query_size].value, 100001);
    EXPECT_EQ(&parent[Setting::log_comment], &child[Setting::log_comment]);

    child.set(Setting::log_comment, child[Setting::log_comment].value);
    EXPECT_EQ(child[Setting::log_comment].value, String(256, 'p'));
    child.set(Setting::log_comment, String(257, 'c'));
    EXPECT_EQ(parent[Setting::log_comment].value, String(256, 'p'));
    EXPECT_EQ(sibling[Setting::log_comment].value, String(256, 'p'));
}

GTEST_TEST(SettingsSnapshot, FlagsAndHiddenAutoStateArePreserved)
{
    Settings parent;
    const auto expected = parent[Setting::max_threads];
    Settings child(parent);
    child.setChanged(Setting::max_threads, expected.changed);
    EXPECT_EQ(&parent[Setting::max_threads], &child[Setting::max_threads]);

    child.set(Setting::max_threads, UInt64(3));
    EXPECT_EQ(parent[Setting::max_threads].is_auto, expected.is_auto);
    EXPECT_EQ(parent[Setting::max_threads].value, expected.value);
    EXPECT_EQ(parent[Setting::max_threads].changed, expected.changed);
    EXPECT_EQ(child[Setting::max_threads].value, 3);

    child.set(Setting::max_threads, expected);
    EXPECT_EQ(child[Setting::max_threads].is_auto, expected.is_auto);
    EXPECT_EQ(child[Setting::max_threads].value, expected.value);
    EXPECT_EQ(child[Setting::max_threads].changed, expected.changed);

    child.setChanged(Setting::max_threads, !expected.changed);
    EXPECT_EQ(parent[Setting::max_threads].changed, expected.changed);
    EXPECT_EQ(child[Setting::max_threads].changed, !expected.changed);
}

GTEST_TEST(SettingsSnapshot, ContainerReplacementPreservesFlagsAndIsolation)
{
    Settings parent;
    Settings child(parent);
    auto filters = child[Setting::additional_table_filters];
    filters.value.push_back(Tuple{String("table"), String("key > 1")});
    child.set(Setting::additional_table_filters, std::move(filters));
    EXPECT_TRUE(parent[Setting::additional_table_filters].value.empty());
    EXPECT_EQ(child[Setting::additional_table_filters].value.size(), 1);
    EXPECT_EQ(child[Setting::additional_table_filters].changed, parent[Setting::additional_table_filters].changed);
}

GTEST_TEST(SettingsSnapshot, AssignmentAndParentDestruction)
{
    Settings surviving;
    auto * wrapper = &surviving;
    {
        Settings parent;
        parent.set("compatibility", "22.8");
        parent.set(Setting::log_comment, String(512, 'p'));
        surviving = parent;
        EXPECT_EQ(&surviving[Setting::log_comment], &parent[Setting::log_comment]);
    }
    EXPECT_EQ(&surviving, wrapper);
    EXPECT_EQ(surviving[Setting::log_comment].value, String(512, 'p'));
    Settings sibling(surviving);
    surviving.set(Setting::log_comment, String(513, 's'));
    EXPECT_EQ(sibling[Setting::log_comment].value, String(512, 'p'));
    EXPECT_EQ(surviving[Setting::log_comment].value, String(513, 's'));
}

GTEST_TEST(SettingsSnapshot, NamedInputsRemainValidAcrossAllocationDomains)
{
    MemoryTracker query_tracker(&total_memory_tracker, VariableContext::Process, false);
    std::thread(
        [&]
        {
            ThreadStatus thread_status;
            thread_status.memory_tracker.setParent(&query_tracker);
            const Map expected{Tuple{String(256, 't'), String(512, 'f')}};
            Settings settings;
            settings.set(Setting::additional_table_filters, Map{Tuple{String("container"), Field(expected)}});
            const auto & input = settings[Setting::additional_table_filters].value.front().safeGet<Tuple>()[1];
            {
                MemoryTrackerBlockerInThread guard;
                settings.set("additional_table_filters", input);
            }
            EXPECT_EQ(settings[Setting::additional_table_filters].value, expected);
        })
        .join();
}

GTEST_TEST(SettingsSnapshot, CompatibilityAndSerializationRemainIsolated)
{
    Settings parent;
    parent.set("compatibility", "22.8");
    parent.set(Setting::max_threads, UInt64(8));
    parent.set(Setting::log_comment, String(256, 'p'));
    const auto expected = serialize(parent, SettingsWriteFormat::DEFAULT);
    Settings child(parent);
    child.markSettingsChangedByCompatibilityAsUnchanged();
    child.set("compatibility", "");
    child.set(Setting::log_comment, "child");
    EXPECT_EQ(serialize(parent, SettingsWriteFormat::DEFAULT), expected);
    EXPECT_TRUE(parent.hasSettingsChangedByCompatibility());
    EXPECT_FALSE(child.hasSettingsChangedByCompatibility());

    for (const auto format : {SettingsWriteFormat::BINARY, SettingsWriteFormat::STRINGS_WITH_FLAGS})
    {
        const auto bytes = serialize(parent, format);
        Settings decoded;
        ReadBufferFromString input(bytes);
        decoded.read(input, format);
        EXPECT_EQ(serialize(decoded, format), bytes);
        decoded.set(Setting::log_comment, "decoded");
        EXPECT_EQ(serialize(parent, SettingsWriteFormat::DEFAULT), expected);
    }
}

GTEST_TEST(SettingsSnapshot, IndependentConcurrentDescendants)
{
    Settings parent;
    parent.set("compatibility", "22.8");
    parent.set(Setting::max_threads, UInt64(8));
    parent.set(Setting::log_comment, String(256, 'p'));
    const auto expected = serialize(parent, SettingsWriteFormat::DEFAULT);
    std::atomic<size_t> failures = 0;
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < 8; ++worker)
    {
        workers.emplace_back(
            [&, worker]
            {
                for (size_t iteration = 0; iteration < 1000; ++iteration)
                {
                    Settings child(parent);
                    child.set(Setting::max_threads, UInt64(worker + 1));
                    child.set(Setting::log_comment, std::to_string(iteration));
                    Settings descendant(child);
                    descendant.set(Setting::log_comment, "descendant");
                    if (child[Setting::log_comment].value != std::to_string(iteration) || child[Setting::max_threads].value != worker + 1
                        || descendant[Setting::log_comment].value != "descendant")
                        ++failures;
                }
            });
    }
    for (auto & worker : workers)
        worker.join();
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(serialize(parent, SettingsWriteFormat::DEFAULT), expected);
}

GTEST_TEST(SettingsSnapshot, CacheKeyPreservesExactCustomState)
{
    Settings parent;
    parent.setCustom("custom_snapshot", Field(Float64(0.0)));
    Settings child(parent);
    EXPECT_TRUE(parent.sharesSnapshotWith(child));
    child.setCustom("custom_snapshot", Field(Float64(-0.0)));
    EXPECT_FALSE(parent.sharesSnapshotWith(child));
    child.setCustom("custom_snapshot", Field(Float64(0.0)));
    EXPECT_TRUE(parent.sharesSnapshotWith(child));

    parent.setCustom("custom_snapshot", Field(DecimalField<Decimal64>(Decimal64(10), 1)));
    child = parent;
    child.setCustom("custom_snapshot", Field(DecimalField<Decimal64>(Decimal64(100), 2)));
    EXPECT_FALSE(parent.sharesSnapshotWith(child));

    parent.setCustom("custom_snapshot", Field(Array{Field(Null{})}));
    child = parent;
    child.setCustom("custom_snapshot", Field(Array{Field(POSITIVE_INFINITY)}));
    EXPECT_FALSE(parent.sharesSnapshotWith(child));
    child.setCustom("custom_snapshot", Field(Array{Field(NEGATIVE_INFINITY)}));
    EXPECT_FALSE(parent.sharesSnapshotWith(child));
    child.setCustom("custom_snapshot", Field(Array{Field(Null{})}));
    EXPECT_TRUE(parent.sharesSnapshotWith(child));

    parent.setCustom("custom_snapshot", Field(POSITIVE_INFINITY));
    child = parent;
    EXPECT_TRUE(parent.sharesSnapshotWith(child));
    child.setCustom("custom_snapshot", Field(NEGATIVE_INFINITY));
    EXPECT_FALSE(parent.sharesSnapshotWith(child));
    parent.setCustom("custom_snapshot", Field(NEGATIVE_INFINITY));
    EXPECT_TRUE(parent.sharesSnapshotWith(child));

    parent.setCustom("custom_snapshot", Field(UInt64(0)));
    child = parent;
    child.setCustom("custom_snapshot", Field(Int64(0)));
    EXPECT_FALSE(parent.sharesSnapshotWith(child));

    Settings inherited;
    Settings pinned(inherited);
    EXPECT_TRUE(inherited.sharesSnapshotWith(pinned));
    inherited.set(Setting::max_query_size, UInt64(100000));
    EXPECT_FALSE(inherited.sharesSnapshotWith(pinned));
}

GTEST_TEST(SettingsSnapshot, ResolvedProfileCacheSharesAndReclaimsReplacedEntries)
{
    AccessControl access_control;
    SettingsProfilesInfo profile(access_control);
    Settings input;
    Settings output(input);
    output.set(Setting::max_threads, UInt64(7));
    output.set(Setting::log_comment, String(256, 'p'));
    profile.cacheSettings(input, output, true);

    auto cached = profile.tryGetCachedSettings(input, true);
    ASSERT_TRUE(cached);
    EXPECT_TRUE(cached->sharesSnapshotWith(output));
    EXPECT_EQ(cached.get(), profile.tryGetCachedSettings(input, true).get());
    EXPECT_FALSE(profile.tryGetCachedSettings(input, false));
    Settings different_input(input);
    different_input.set(Setting::max_query_size, UInt64(100000));
    EXPECT_FALSE(profile.tryGetCachedSettings(different_input, true));
    SettingsProfilesInfo other_generation(access_control);
    EXPECT_FALSE(other_generation.tryGetCachedSettings(input, true));

    Settings query(*cached);
    query.set(Setting::log_comment, "query-local");
    EXPECT_EQ((*cached)[Setting::log_comment].value, String(256, 'p'));

    std::weak_ptr<const Settings> replaced = cached;
    Settings surviving(*cached);
    output.set(Setting::max_threads, UInt64(8));
    profile.cacheSettings(input, output, true);
    cached.reset();
    EXPECT_TRUE(replaced.expired());
    EXPECT_EQ(surviving[Setting::max_threads].value, 7);
    EXPECT_EQ((*profile.tryGetCachedSettings(input, true))[Setting::max_threads].value, 8);
}

GTEST_TEST(SettingsSnapshot, SetUserDoesNotCacheInheritedCustomSettings)
{
    auto global_context = getMutableContext().context;
    auto & access_control = global_context->getAccessControl();
    access_control.addMemoryStorage("gtest_settings_snapshot_memory", /*allow_backup_=*/ false);
    auto user = std::make_shared<User>();
    user->setName("gtest_settings_snapshot_user");
    const auto user_id = access_control.insert(user);
    SCOPE_EXIT({ access_control.remove(user_id); });
    const auto enabled_settings = access_control.getEnabledSettings(user_id, user->settings, {}, {});
    auto profiles = enabled_settings->getInfo();
    const auto type = global_context->getApplicationType();
    const bool sanity_clamp = type == Context::ApplicationType::LOCAL || type == Context::ApplicationType::SERVER;

    Settings inherited;
    ASSERT_TRUE(inherited.hasServerOwnedStorage());
    auto context = Context::createCopy(global_context);
    context->setSettings(inherited);
    context->setUser(user_id);
    auto cached = profiles->tryGetCachedSettings(inherited, sanity_clamp);
    ASSERT_TRUE(cached);

    /// Session-level `EXECUTE AS` and deferred executors can switch users on an existing context.
    /// Custom-only changes must not populate the server cache or evict the ordinary login entry.
    for (const Field & value : {Field(String(256, 'q')), Field(Map{Tuple{String("key"), String(256, 'v')}})})
    {
        Settings query_settings(inherited);
        query_settings.setCustom("custom_snapshot", value);
        EXPECT_FALSE(query_settings.hasServerOwnedStorage());
        context->setSettings(query_settings);
        context->setUser(user_id);
        EXPECT_EQ(context->getSettingsRef().get("custom_snapshot"), value);
        EXPECT_FALSE(profiles->tryGetCachedSettings(query_settings, sanity_clamp));
        EXPECT_EQ(profiles->tryGetCachedSettings(inherited, sanity_clamp).get(), cached.get());

        query_settings.setDefaultValue("custom_snapshot");
        EXPECT_TRUE(query_settings.hasServerOwnedStorage());
    }
}

GTEST_TEST(SettingsSnapshot, FailedProfileResolutionPreservesContextAndPrincipal)
{
    auto global_context = getMutableContext().context;
    auto & access_control = global_context->getAccessControl();
    access_control.addMemoryStorage("gtest_settings_snapshot_failure_memory", /*allow_backup_=*/ false);
    auto previous_user = std::make_shared<User>();
    previous_user->setName("gtest_settings_snapshot_previous_user");
    const auto previous_user_id = access_control.insert(previous_user);
    SCOPE_EXIT({ access_control.remove(previous_user_id); });
    auto user = std::make_shared<User>();
    user->setName("gtest_settings_snapshot_failure_user");
    auto & valid_change = user->settings.emplace_back();
    valid_change.setting_name = "log_comment";
    valid_change.value = String("partially applied profile");
    auto & invalid_change = user->settings.emplace_back();
    invalid_change.setting_name = "max_query_size";
    invalid_change.value = String("not a number");
    const auto user_id = access_control.insert(user);
    SCOPE_EXIT({ access_control.remove(user_id); });
    const auto enabled_settings = access_control.getEnabledSettings(user_id, user->settings, {}, {});
    const auto profiles = enabled_settings->getInfo();

    auto context = Context::createCopy(global_context);
    Settings inherited;
    auto previous_grants = std::make_shared<const AccessRightsElements>();
    context->setUser(previous_user_id, {}, previous_grants, 1234);
    context->setSettings(inherited);
    const auto previous_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto * wrapper = &context->getSettingsRef();
    ASSERT_TRUE(context->getSettingsRef().hasServerOwnedStorage());
    EXPECT_THROW(context->setUser(user_id), Exception);
    EXPECT_EQ(context->getUserID(), previous_user_id);
    EXPECT_EQ(context->getAuthenticationGrants(), previous_grants);
    EXPECT_EQ(context->getAuthenticationValidUntil(), 1234);
    EXPECT_EQ(context->getSettingsConstraintsAndCurrentProfiles(), previous_profiles);
    EXPECT_EQ(&context->getSettingsRef(), wrapper);
    EXPECT_TRUE(context->getSettingsRef().sharesSnapshotWith(inherited));
    EXPECT_FALSE(profiles->tryGetCachedSettings(inherited, true));
    EXPECT_FALSE(profiles->tryGetCachedSettings(inherited, false));
}

/// Like the allocation-interceptor tests, this requires tracked `new` and `delete`.
#if !defined(SANITIZER)
GTEST_TEST(SettingsSnapshot, FailedCachedProfileCopyPreservesContext)
{
    auto global_context = getMutableContext().context;
    SettingsProfilesInfo profile(global_context->getAccessControl());
    Settings inherited;
    constexpr size_t payload_size = 2 * 1024 * 1024;
    const auto type = global_context->getApplicationType();
    const bool sanity_clamp = type == Context::ApplicationType::LOCAL || type == Context::ApplicationType::SERVER;
    {
        MemoryTrackerBlockerInThread guard;
        Settings resolved(inherited);
        resolved.setCustom("custom_snapshot", String(payload_size, 'p'));
        profile.cacheSettings(inherited, resolved, sanity_clamp);
    }

    MemoryTracker query_tracker(&total_memory_tracker, VariableContext::Process, false);
    std::thread(
        [&]
        {
            ThreadStatus thread_status;
            thread_status.memory_tracker.setParent(&query_tracker);
            thread_status.untracked_memory_limit = 0;
            auto context = Context::createCopy(global_context);
            context->setSettings(inherited);
            const auto previous_profiles = context->getSettingsConstraintsAndCurrentProfiles();
            const auto * wrapper = &context->getSettingsRef();
            const auto previous_throw_threshold = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();
            SCOPE_EXIT({ CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(previous_throw_threshold); });
            CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(payload_size / 2);
            query_tracker.setHardLimit(query_tracker.get() + payload_size / 2);
            int exception_code = 0;
            try
            {
                context->setCurrentProfiles(profile, /*check_constraints=*/ false);
            }
            catch (const Exception & exception)
            {
                exception_code = exception.code();
            }
            query_tracker.setHardLimit(0);
            EXPECT_EQ(exception_code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
            EXPECT_EQ(context->getSettingsConstraintsAndCurrentProfiles(), previous_profiles);
            EXPECT_EQ(&context->getSettingsRef(), wrapper);
            EXPECT_TRUE(context->getSettingsRef().sharesSnapshotWith(inherited));

            /// A retry can use the same cache entry once the query can afford its own copy.
            context->setCurrentProfiles(profile, /*check_constraints=*/ false);
            EXPECT_EQ(&context->getSettingsRef(), wrapper);
            EXPECT_EQ(context->getSettingsRef().get("custom_snapshot"), Field(String(payload_size, 'p')));
            EXPECT_TRUE(profile.tryGetCachedSettings(inherited, sanity_clamp));
        })
        .join();
}

GTEST_TEST(SettingsSnapshot, CachedStorageAndQueryWritesUseSeparateMemoryDomains)
{
    /// Use the same real tracker hierarchy as the allocation-interceptor tests.
    MemoryTracker query_tracker(&total_memory_tracker, VariableContext::Process, false);
    std::thread(
        [&]
        {
            ThreadStatus thread_status;
            thread_status.memory_tracker.setParent(&query_tracker);
            thread_status.untracked_memory_limit = 0;
            constexpr size_t payload_size = 2 * 1024 * 1024;
            constexpr Int64 tolerance = 64 * 1024;
            EXPECT_FALSE(settingsAllocationIsServerOwned());

            std::shared_ptr<Settings> cached;
            const auto before_cache = query_tracker.get();
            {
                MemoryTrackerBlockerInThread guard;
                EXPECT_TRUE(settingsAllocationIsServerOwned());
                cached = std::allocate_shared<Settings>(SettingsSnapshotAllocator<Settings>{true});
                cached->set(Setting::log_comment, String(payload_size, 'p'));
                EXPECT_TRUE(cached->hasServerOwnedStorage());
            }
            CurrentThread::flushUntrackedMemory();
            EXPECT_LT(std::abs(query_tracker.get() - before_cache), tolerance);

            {
                Settings child(*cached);
                const auto before_write = query_tracker.get();
                child.set(Setting::log_comment, "query-local");
                EXPECT_FALSE(child.hasServerOwnedStorage());
                CurrentThread::flushUntrackedMemory();
                EXPECT_GT(query_tracker.get() - before_write, static_cast<Int64>(payload_size / 2));
                EXPECT_EQ((*cached)[Setting::log_comment].value.size(), payload_size);

                /// A globally allocated table can still refer to query-owned untouched chunks.
                {
                    MemoryTrackerBlockerInThread guard;
                    Settings mixed(child);
                    mixed.set(Setting::max_query_size, UInt64(100001));
                    EXPECT_FALSE(mixed.hasServerOwnedStorage());
                }
            }
            CurrentThread::flushUntrackedMemory();
            EXPECT_LT(std::abs(query_tracker.get() - before_cache), tolerance);

            {
                Settings child(*cached);
                const auto previous_throw_threshold = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();
                SCOPE_EXIT({ CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(previous_throw_threshold); });
                /// Ordinary `new` tracks but does not enforce limits until this option is enabled.
                CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(payload_size / 2);
                query_tracker.setHardLimit(query_tracker.get() + payload_size / 2);
                int exception_code = 0;
                try
                {
                    /// Isolate chunk detachment from the typed setter's input copy.
                    child.setChanged(Setting::log_comment, false);
                }
                catch (const Exception & exception)
                {
                    exception_code = exception.code();
                }
                query_tracker.setHardLimit(0);
                EXPECT_EQ(exception_code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
                EXPECT_EQ(child[Setting::log_comment].value.size(), payload_size);
                EXPECT_TRUE(child[Setting::log_comment].changed);
            }

            /// Releasing the last cached chunk inside a query must not debit unrelated query memory.
            String unrelated(payload_size, 'q');
            CurrentThread::flushUntrackedMemory();
            const auto before_release = query_tracker.get();
            cached.reset();
            CurrentThread::flushUntrackedMemory();
            EXPECT_LT(std::abs(query_tracker.get() - before_release), tolerance);
            EXPECT_EQ(unrelated.size(), payload_size);
        })
        .join();
}
#endif
