#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include <unistd.h>

namespace ProfileEvents
{
    extern const Event ZooKeeperSessionEstablishAttempts;
}

namespace DB::FailPoints
{
    extern const char context_establish_zookeeper_session[];
    extern const char context_join_zookeeper_establishment[];
    extern const char context_shutdown_zookeeper_drain[];
    extern const char context_shutdown_auxiliary_zookeeper_drain[];
    extern const char context_zookeeper_applied_server_started[];
    extern const char context_auxiliary_zookeeper_applied_server_started[];
    extern const char context_zookeeper_before_publish[];
    extern const char context_auxiliary_zookeeper_after_args[];
    extern const char context_zookeeper_used_initialized_log[];
    extern const char context_auxiliary_zookeeper_used_initialized_log[];
    extern const char context_auxiliary_zookeeper_before_publish[];
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::ErrorCodes
{
    extern const int UNFINISHED;
}

using namespace DB;

namespace
{

constexpr auto park_timeout = std::chrono::seconds(30);

ProfileEvents::Count establishAttempts()
{
    return ProfileEvents::global_counters[ProfileEvents::ZooKeeperSessionEstablishAttempts];
}

Context::ConfigurationPtr makeConfig(const std::string & xml)
{
    std::istringstream stream(xml);  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    return Context::ConfigurationPtr(new Poco::Util::XMLConfiguration(stream));
}

/// A fresh config object every call (distinct pointer), same in-memory `testkeeper` settings. Bringing
/// up a `testkeeper` session needs no network, so the success / expiry / reload paths are all real
/// here - unlike a context with no `<zookeeper>` section, which can only exercise failure.
Context::ConfigurationPtr testKeeperConfig()
{
    return makeConfig("<clickhouse><zookeeper><implementation>testkeeper</implementation></zookeeper></clickhouse>");
}

/// `implementation=zookeeper` with no nodes: `ZooKeeper::create` throws "No hosts passed". Used to
/// drive a reload whose `create` fails.
Context::ConfigurationPtr brokenKeeperConfig()
{
    return makeConfig("<clickhouse><zookeeper><implementation>zookeeper</implementation></zookeeper></clickhouse>");
}

/// `extra` is placed inside every named section, e.g. a `<session_timeout_ms>` to make settings differ.
Context::ConfigurationPtr auxConfig(const std::vector<std::string> & names, const std::string & extra = "")
{
    std::string xml = "<clickhouse><auxiliary_zookeepers>";
    for (const auto & name : names)
        xml += "<" + name + "><implementation>testkeeper</implementation>" + extra + "</" + name + ">";
    xml += "</auxiliary_zookeepers></clickhouse>";
    return makeConfig(xml);
}

/// True if `call` throws `UNFINISHED`. Used inside death-test children, where gtest macros are not.
template <typename Call>
bool refusedWithUnfinished(Call && call)
{
    try
    {
        call();
    }
    catch (const DB::Exception & e)
    {
        return e.code() == DB::ErrorCodes::UNFINISHED;
    }
    return false;
}

/// Runs `call` on its own thread and reports whether it was refused with `UNFINISHED` within
/// `park_timeout`. A broken implementation would make the call *block* (joining the parked attempt)
/// rather than fail, and a death-test child must then report that instead of hanging the suite. The
/// thread is detached on purpose: a `std::async` future would block in its destructor on the very call
/// that hung, and the child `_exit`s right after a failure anyway.
template <typename Call>
bool refusedPromptly(Call && call)
{
    auto refused = std::make_shared<std::promise<bool>>();
    auto result = refused->get_future();
    std::thread([refused, run = std::forward<Call>(call)]() mutable { refused->set_value(refusedWithUnfinished(run)); }).detach();
    if (result.wait_for(park_timeout) != std::future_status::ready)
        return false;
    return result.get();
}

/// Expects `error` to be a `DB::Exception` with code `UNFINISHED`, the "reloaded while connecting" signal.
void expectUnfinished(const std::exception_ptr & error)
{
    ASSERT_TRUE(error);
    try
    {
        std::rethrow_exception(error);
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNFINISHED) << e.message();
    }
}

/// Releases the failpoint and joins the threads however the test leaves the scope. Without this an
/// `ASSERT_*` returns with joinable threads and an enabled global failpoint, so the process
/// terminates instead of reporting the assertion, and every later test inherits the failpoint.
class ParkedEstablishment
{
public:
    ParkedEstablishment()
    {
        FailPointInjection::enableFailPoint(FailPoints::context_establish_zookeeper_session);
    }

    ~ParkedEstablishment()
    {
        release();
        joinAll();
    }

    void release()
    {
        if (!std::exchange(released, true))
            FailPointInjection::disableFailPoint(FailPoints::context_establish_zookeeper_session);
    }

    void add(std::thread thread) { threads.push_back(std::move(thread)); }

    void joinAll()
    {
        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    }

private:
    std::vector<std::thread> threads;
    bool released = false;
};

/// The tests share one global `Context` (a second global context is forbidden, and destroying a
/// throwaway `ContextSharedPart` would shut down process-global registries). So they cannot null the
/// published session between runs; instead each test brings the session to a known state through the
/// public API. The application type is moved away from `SERVER` for the duration so `ServerUUID::get`
/// - reached by `testkeeper`'s `initSession` - returns Nil instead of throwing.
class ContextZooKeeperSingleFlight : public testing::Test
{
protected:
    Context::ApplicationType saved_type{};

    void SetUp() override
    {
        auto & holder = getMutableContext();
        saved_type = holder.context->getApplicationType();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);
    }

    void TearDown() override
    {
        auto & holder = getMutableContext();
        FailPointInjection::disableFailPoint(FailPoints::context_establish_zookeeper_session);
        FailPointInjection::disableFailPoint(FailPoints::context_join_zookeeper_establishment);
        FailPointInjection::disableFailPoint(FailPoints::context_shutdown_zookeeper_drain);
        FailPointInjection::disableFailPoint(FailPoints::context_shutdown_auxiliary_zookeeper_drain);
        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_before_publish);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_before_publish);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_after_args);
        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_applied_server_started);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_applied_server_started);
        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_used_initialized_log);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_used_initialized_log);

        /// Finalize sessions so their `TestKeeper` processing threads are joined rather than left
        /// running until the binary exits.
        try { holder.context->reconnectZooKeeper("gtest teardown"); } catch (...) {}
        try
        {
            for (auto & [name, zk] : holder.context->getAuxiliaryZooKeepers())
                zk->finalize("gtest teardown");
        }
        catch (...) {}

        holder.context->setApplicationType(saved_type);
    }

    /// A live default session, whatever state a previous test left behind: a fresh reload builds one
    /// when none exists, and `getZooKeeper` renews it when it is expired.
    static zkutil::ZooKeeperPtr ensureLiveSession()
    {
        auto & holder = getMutableContext();
        holder.context->reloadZooKeeperIfChanged(testKeeperConfig());
        return holder.context->getZooKeeper();
    }
};

}

/// Single-flight and successful publication: N callers hit an expired session concurrently, one
/// becomes leader and is parked inside the establishment, and everybody else joins that one attempt.
/// With `testkeeper` the leader's attempt succeeds, so this also covers the success path and proves no
/// timeout downgrade - every follower waits for and receives the leader's session.
TEST_F(ContextZooKeeperSingleFlight, ConcurrentCallersShareOneAttempt)
{
    auto & holder = getMutableContext();

    ensureLiveSession();
    holder.context->reconnectZooKeeper("expire for test");

    constexpr size_t num_threads = 16;
    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    std::atomic<size_t> finished = 0;
    std::mutex results_mutex;
    std::vector<zkutil::ZooKeeperPtr> results;

    for (size_t i = 0; i < num_threads; ++i)
    {
        parked.add(std::thread([&]
        {
            zkutil::ZooKeeperPtr zk;
            try { zk = holder.context->getZooKeeper(); } catch (...) {}
            {
                std::lock_guard lock(results_mutex);
                results.push_back(zk);
            }
            ++finished;
        }));
    }

    /// Bounded, event-based gate: returns once a leader is parked inside the establishment, which is
    /// after election and after the attempt counter was incremented. Any caller arriving while the
    /// leader is parked finds the in-flight future, so a second attempt is impossible at this point.
    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
        << "no leader parked in the establishment";
    EXPECT_EQ(establishAttempts() - attempts_before, 1u) << "expected exactly one attempt in flight";
    EXPECT_EQ(finished.load(), 0u) << "a caller finished while the leader was still parked";

    parked.release();
    parked.joinAll();

    /// Still one attempt: the in-flight future absorbed the followers, and once the session was
    /// published every remaining caller took the fast path. No caller connects on its own.
    EXPECT_EQ(establishAttempts() - attempts_before, 1u);
    EXPECT_EQ(finished.load(), num_threads);

    std::lock_guard lock(results_mutex);
    for (const auto & zk : results)
    {
        ASSERT_TRUE(zk);
        EXPECT_FALSE(zk->expired());
        EXPECT_EQ(zk, results.front()) << "callers received different sessions";
    }
}

/// An expired session is renewed into a new, live one.
TEST_F(ContextZooKeeperSingleFlight, ExpiredSessionIsRenewed)
{
    auto & holder = getMutableContext();

    auto s0 = ensureLiveSession();
    ASSERT_TRUE(s0);
    ASSERT_FALSE(s0->expired());

    holder.context->reconnectZooKeeper("expire for test");
    ASSERT_TRUE(s0->expired());

    const auto attempts_before = establishAttempts();
    auto s1 = holder.context->getZooKeeper();

    EXPECT_TRUE(s1);
    EXPECT_FALSE(s1->expired());
    EXPECT_NE(s1, s0);
    EXPECT_EQ(establishAttempts() - attempts_before, 1u);
}

/// The reload race, mode 1: a `SYSTEM RELOAD CONFIG` with unchanged Keeper settings swaps the config
/// object but does not rebuild the session. A leader in flight must publish its own fresh session, not
/// treat itself as superseded and hand back the expired one. Comparing the config *pointer* captured
/// at election (the original bug) fails this: the pointer changed, so the leader returns the expired
/// session. The generation, which an unchanged-settings reload does not bump, keeps it correct.
TEST_F(ContextZooKeeperSingleFlight, ReloadWithUnchangedSettingsPublishesFreshSession)
{
    auto & holder = getMutableContext();

    auto s0 = ensureLiveSession();
    holder.context->reconnectZooKeeper("expire for test");
    ASSERT_TRUE(s0->expired());

    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    zkutil::ZooKeeperPtr result;
    std::exception_ptr error;
    parked.add(std::thread([&]
    {
        try { result = holder.context->getZooKeeper(); }
        catch (...) { error = std::current_exception(); }
    }));

    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
        << "no leader parked in the establishment";
    ASSERT_EQ(establishAttempts() - attempts_before, 1u);

    /// The leader has elected (capturing its generation and snapshot) and is parked. Reload now, with a new
    /// config object carrying the same settings.
    holder.context->reloadZooKeeperIfChanged(testKeeperConfig());

    parked.release();
    parked.joinAll();

    ASSERT_FALSE(error);
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->expired());
    EXPECT_EQ(result, holder.context->getZooKeeper()) << "the published session is not the current one";
}

/// The reload race, mode 2: a reload whose `create` fails must leave the previous session in place,
/// live and published, and propagate the error - never a finalized session for a getter to return.
TEST_F(ContextZooKeeperSingleFlight, FailedReloadKeepsPreviousSession)
{
    auto & holder = getMutableContext();

    auto s0 = ensureLiveSession();
    ASSERT_TRUE(s0);
    ASSERT_FALSE(s0->expired());

    EXPECT_THROW(holder.context->reloadZooKeeperIfChanged(brokenKeeperConfig()), DB::Exception);

    auto s1 = holder.context->getZooKeeper();
    EXPECT_EQ(s1, s0) << "the failed reload replaced the session";
    EXPECT_FALSE(s1->expired()) << "the failed reload left a finalized session published";
}

/// A failed attempt is not cached: the in-flight record is retired before the promise is fulfilled, so
/// the next caller starts its own attempt. Uses the auxiliary unknown-name path, whose failure is
/// independent of any default-session state.
TEST_F(ContextZooKeeperSingleFlight, FailureIsNotCached)
{
    auto & holder = getMutableContext();
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"known"}));

    const auto attempts_before = establishAttempts();

    for (size_t i = 0; i < 2; ++i)
        EXPECT_THROW(holder.context->getAuxiliaryZooKeeper("missing"), DB::Exception);

    EXPECT_EQ(establishAttempts() - attempts_before, 2u);
}

/// The incident scenario itself: N callers, one attempt, and that attempt FAILS. Every follower must
/// receive the leader's failure (one attempt, not one per caller), and the failure must not be cached -
/// the next call starts a fresh attempt. The auxiliary unknown-name path is the deterministic failure:
/// the check runs inside the establishment, after the counter and the failpoint.
TEST_F(ContextZooKeeperSingleFlight, ConcurrentCallersShareOneFailedAttempt)
{
    auto & holder = getMutableContext();
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"known"}));

    constexpr size_t num_threads = 8;
    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    FailPointInjection::enableFailPoint(FailPoints::context_join_zookeeper_establishment);

    std::mutex errors_mutex;
    std::vector<int> error_codes;
    std::atomic<size_t> got_session = 0;
    for (size_t i = 0; i < num_threads; ++i)
    {
        parked.add(std::thread([&]
        {
            try
            {
                holder.context->getAuxiliaryZooKeeper("missing");
                ++got_session;
            }
            catch (const DB::Exception & e)
            {
                std::lock_guard lock(errors_mutex);
                error_codes.push_back(e.code());
            }
        }));
    }

    /// Leader parked inside the one attempt, and every other caller parked right after joining it.
    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
        << "no leader parked in the establishment";
    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_join_zookeeper_establishment, park_timeout, num_threads - 1))
        << "not every follower joined the in-flight attempt";
    EXPECT_EQ(establishAttempts() - attempts_before, 1u);

    FailPointInjection::disableFailPoint(FailPoints::context_join_zookeeper_establishment);
    parked.release();
    parked.joinAll();

    EXPECT_EQ(got_session.load(), 0u);
    EXPECT_EQ(establishAttempts() - attempts_before, 1u) << "a follower connected on its own";
    {
        std::lock_guard lock(errors_mutex);
        ASSERT_EQ(error_codes.size(), num_threads);
        for (int code : error_codes)
            EXPECT_EQ(code, DB::ErrorCodes::BAD_ARGUMENTS);
    }

    /// Not cached: a later call pays for its own attempt.
    EXPECT_THROW(holder.context->getAuxiliaryZooKeeper("missing"), DB::Exception);
    EXPECT_EQ(establishAttempts() - attempts_before, 2u);
}

/// An invalid auxiliary name is rejected before anybody becomes a leader, so it leaves no in-flight
/// entry and starts no attempt.
TEST_F(ContextZooKeeperSingleFlight, InvalidAuxiliaryNameDoesNotStartAnAttempt)
{
    auto & holder = getMutableContext();

    const auto attempts_before = establishAttempts();

    EXPECT_THROW(holder.context->getAuxiliaryZooKeeper("bad/name"), DB::Exception);
    EXPECT_THROW(holder.context->getAuxiliaryZooKeeper("bad:name"), DB::Exception);

    EXPECT_EQ(establishAttempts() - attempts_before, 0u);
}

/// Auxiliary single-flight is per name: concurrent callers for one name share an attempt, and a name
/// parked in its establishment does not hold up a different name. Before the fix a single
/// `auxiliary_zookeepers_mutex` was held across the I/O, so the second name could not reach its own
/// establishment while the first was connecting.
TEST_F(ContextZooKeeperSingleFlight, AuxiliaryNamesAreIndependent)
{
    auto & holder = getMutableContext();
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"indep_a", "indep_b"}));

    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    constexpr size_t per_name = 4;
    for (const auto * name : {"indep_a", "indep_b"})
        for (size_t i = 0; i < per_name; ++i)
            parked.add(std::thread([&, name] { try { holder.context->getAuxiliaryZooKeeper(name); } catch (...) {} }));

    /// Both names must reach their own establishment concurrently - two parked leaders, not one
    /// blocking the other, and not one attempt per caller.
    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout, 2))
        << "the two names did not both park in the establishment";
    EXPECT_EQ(establishAttempts() - attempts_before, 2u) << "expected one attempt per name";

    parked.release();
    parked.joinAll();

    EXPECT_EQ(establishAttempts() - attempts_before, 2u);
}

/// Auxiliary removal during establishment - the generation-advancing reload racing a parked leader. The
/// leader renews from the snapshot it took at election, then must discard that session because the
/// reload advanced the name's generation, and must not resurrect the name.
TEST_F(ContextZooKeeperSingleFlight, AuxiliaryRemovalDuringEstablishmentDoesNotResurrect)
{
    auto & holder = getMutableContext();
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"removal_tmp"}));

    auto aux = holder.context->getAuxiliaryZooKeeper("removal_tmp");
    ASSERT_TRUE(aux);
    aux->finalize("expire for test");
    ASSERT_TRUE(aux->expired());

    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    std::exception_ptr error;
    zkutil::ZooKeeperPtr result;
    parked.add(std::thread([&]
    {
        try { result = holder.context->getAuxiliaryZooKeeper("removal_tmp"); }
        catch (...) { error = std::current_exception(); }
    }));

    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
        << "no leader parked in the establishment";
    ASSERT_EQ(establishAttempts() - attempts_before, 1u);

    /// Remove the name while the leader is parked; this advances the name's generation.
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({}));

    parked.release();
    parked.joinAll();

    /// The leader's renewal itself succeeds (`testkeeper`), so the only thing that can stop it from
    /// publishing is the generation check - which must report the reload rather than resurrect the
    /// name or return nothing silently.
    expectUnfinished(error);
    EXPECT_FALSE(result) << "the removed keeper was resurrected";
    EXPECT_EQ(holder.context->getAuxiliaryZooKeepers().count("removal_tmp"), 0u);
}

/// Cold auxiliary establishment racing a removal: no session exists yet, so the name is absent from the
/// published map and a reload that only walked that map would never supersede it - the leader would
/// publish a session built from a configuration that no longer has the name.
TEST_F(ContextZooKeeperSingleFlight, ColdAuxiliaryEstablishmentIsNotPublishedAfterRemoval)
{
    auto & holder = getMutableContext();
    /// Cold means "configured, nothing published": an empty reload first erases whatever an earlier
    /// run of this binary left in the published map under this name.
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({}));
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"cold_removed"}));
    ASSERT_EQ(holder.context->getAuxiliaryZooKeepers().count("cold_removed"), 0u);

    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    std::exception_ptr error;
    zkutil::ZooKeeperPtr result;
    parked.add(std::thread([&]
    {
        try { result = holder.context->getAuxiliaryZooKeeper("cold_removed"); }
        catch (...) { error = std::current_exception(); }
    }));

    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
        << "no leader parked in the establishment";
    ASSERT_EQ(establishAttempts() - attempts_before, 1u);

    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({}));

    parked.release();
    parked.joinAll();

    expectUnfinished(error);
    EXPECT_FALSE(result) << "a session for a removed keeper was published";
    EXPECT_EQ(holder.context->getAuxiliaryZooKeepers().count("cold_removed"), 0u);
    /// And a retry now fails the ordinary way: the name is simply not configured.
    EXPECT_THROW(holder.context->getAuxiliaryZooKeeper("cold_removed"), DB::Exception);
}

/// Cold auxiliary establishment racing a settings change: the session built against the old settings
/// is discarded, and the retry builds one against the new configuration.
TEST_F(ContextZooKeeperSingleFlight, ColdAuxiliaryEstablishmentIsRebuiltAfterSettingsChange)
{
    auto & holder = getMutableContext();
    /// The retry at the end publishes this name, so a repeated run must first make it cold again.
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({}));
    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"cold_changed"}, "<session_timeout_ms>10000</session_timeout_ms>"));
    ASSERT_EQ(holder.context->getAuxiliaryZooKeepers().count("cold_changed"), 0u);

    const auto attempts_before = establishAttempts();

    ParkedEstablishment parked;
    std::exception_ptr error;
    zkutil::ZooKeeperPtr result;
    parked.add(std::thread([&]
    {
        try { result = holder.context->getAuxiliaryZooKeeper("cold_changed"); }
        catch (...) { error = std::current_exception(); }
    }));

    ASSERT_TRUE(FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
        << "no leader parked in the establishment";

    holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig({"cold_changed"}, "<session_timeout_ms>20000</session_timeout_ms>"));

    parked.release();
    parked.joinAll();

    expectUnfinished(error);
    EXPECT_FALSE(result);
    EXPECT_EQ(holder.context->getAuxiliaryZooKeepers().count("cold_changed"), 0u) << "the session built against the old settings was published";

    auto retried = holder.context->getAuxiliaryZooKeeper("cold_changed");
    ASSERT_TRUE(retried);
    EXPECT_FALSE(retried->expired());
    EXPECT_EQ(establishAttempts() - attempts_before, 2u);
}

/// The shutdown drain. `Context::shutdown` cannot run on the singleton the whole binary shares, so this
/// is a death test in `threadsafe` style: the binary is re-executed for this one test and the child
/// gets its own singleton to shut down. The child prints a marker and `_exit`s, so no gtest assertion
/// macros are used inside it; every check is a plain condition reported through the marker.
///
/// Every step is event-gated through a failpoint, none is timed: the leader is parked inside its
/// establishment, the follower is parked right after joining the in-flight attempt, and shutdown is
/// parked inside its drain loop after it has observed that attempt. Then the leader is released.
/// Post-conditions: leader and follower hold the same session (shutdown waited for the attempt rather
/// than closing under it), shutdown completes, and afterwards both `getZooKeeper` and a configuration
/// reload are refused - a session published past the close would otherwise survive shutdown.
TEST_F(ContextZooKeeperSingleFlight, ShutdownDrainsInFlightEstablishment)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    EXPECT_EXIT(
    {
        auto & holder = getMutableContext();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);

        const auto fail = [](const char * what)
        {
            std::cerr << "SHUTDOWN_DRAIN_FAIL: " << what << std::endl;
            _exit(1);
        };

        holder.context->reloadZooKeeperIfChanged(testKeeperConfig());
        holder.context->getZooKeeper();
        holder.context->reconnectZooKeeper("expire for test");

        FailPointInjection::enableFailPoint(FailPoints::context_establish_zookeeper_session);
        FailPointInjection::enableFailPoint(FailPoints::context_join_zookeeper_establishment);
        FailPointInjection::enableFailPoint(FailPoints::context_shutdown_zookeeper_drain);

        zkutil::ZooKeeperPtr leader_result;
        zkutil::ZooKeeperPtr follower_result;
        std::exception_ptr leader_error;
        std::exception_ptr follower_error;
        std::thread leader([&]
        {
            try { leader_result = holder.context->getZooKeeper(); }
            catch (...) { leader_error = std::current_exception(); }
        });
        if (!FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout))
            fail("no leader parked in the establishment");

        std::thread follower([&]
        {
            try { follower_result = holder.context->getZooKeeper(); }
            catch (...) { follower_error = std::current_exception(); }
        });
        if (!FailPointInjection::waitForPause(FailPoints::context_join_zookeeper_establishment, park_timeout))
            fail("the follower did not join the in-flight attempt");
        FailPointInjection::disableFailPoint(FailPoints::context_join_zookeeper_establishment);

        std::thread shutdown([&] { holder.context->shutdown(); });
        if (!FailPointInjection::waitForPause(FailPoints::context_shutdown_zookeeper_drain, park_timeout))
            fail("shutdown did not observe the in-flight attempt in its drain");

        /// Shutdown is waiting for the parked leader. A retry arriving now must be refused at once, not
        /// join the attempt or become the next leader - otherwise a retry storm could keep shutdown
        /// draining forever. (With the leader parked, "admitted" means "blocked", hence the bounded check.)
        if (!refusedPromptly([&] { holder.context->getZooKeeper(); }))
            fail("a new caller was admitted while shutdown was draining");

        FailPointInjection::disableFailPoint(FailPoints::context_establish_zookeeper_session);
        leader.join();
        follower.join();
        FailPointInjection::disableFailPoint(FailPoints::context_shutdown_zookeeper_drain);
        shutdown.join();

        if (leader_error || !leader_result)
            fail("the leader did not get a session");
        if (follower_error || follower_result != leader_result)
            fail("the follower did not get the leader's session");

        if (!refusedWithUnfinished([&] { holder.context->getZooKeeper(); }))
            fail("getZooKeeper after shutdown did not refuse: a session was published past the close");
        if (!refusedWithUnfinished([&] { holder.context->reloadZooKeeperIfChanged(testKeeperConfig()); }))
            fail("a configuration reload after shutdown was not refused");
        if (!refusedWithUnfinished([&] { holder.context->getZooKeeper(); }))
            fail("getZooKeeper after a post-shutdown reload did not refuse");

        std::cerr << "SHUTDOWN_DRAIN_OK" << std::endl;
        _exit(0);
    },
    ::testing::ExitedWithCode(0),
    "SHUTDOWN_DRAIN_OK");
}

/// The auxiliary shutdown drain is a separate implementation over a multi-entry map, so it gets its own
/// death test: two names with establishments in flight, shutdown parked inside its auxiliary drain after
/// observing them, release, then both callers hold sessions and every auxiliary entry point is refused.
TEST_F(ContextZooKeeperSingleFlight, ShutdownDrainsInFlightAuxiliaryEstablishments)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    /// Outside the macro: a brace list with a comma would be split into macro arguments.
    const std::vector<std::string> drain_names{"drain_a", "drain_b"};

    EXPECT_EXIT(
    {
        auto & holder = getMutableContext();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);

        const auto fail = [](const char * what)
        {
            std::cerr << "AUX_SHUTDOWN_DRAIN_FAIL: " << what << std::endl;
            _exit(1);
        };

        holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig(drain_names));

        FailPointInjection::enableFailPoint(FailPoints::context_establish_zookeeper_session);
        FailPointInjection::enableFailPoint(FailPoints::context_shutdown_auxiliary_zookeeper_drain);

        std::vector<zkutil::ZooKeeperPtr> results(2);
        std::vector<std::exception_ptr> errors(2);
        std::vector<std::thread> leaders;
        for (size_t i = 0; i < 2; ++i)
            leaders.emplace_back([&, i]
            {
                try { results[i] = holder.context->getAuxiliaryZooKeeper(drain_names[i]); }
                catch (...) { errors[i] = std::current_exception(); }
            });
        if (!FailPointInjection::waitForPause(FailPoints::context_establish_zookeeper_session, park_timeout, 2))
            fail("the two leaders did not both park in the establishment");

        std::thread shutdown([&] { holder.context->shutdown(); });
        if (!FailPointInjection::waitForPause(FailPoints::context_shutdown_auxiliary_zookeeper_drain, park_timeout))
            fail("shutdown did not observe the in-flight auxiliary attempts in its drain");

        /// A retry while shutdown drains must be refused at once, not join the attempt or become a new
        /// leader.
        if (!refusedPromptly([&] { holder.context->getAuxiliaryZooKeeper(drain_names[0]); }))
            fail("a new auxiliary caller was admitted while shutdown was draining");

        FailPointInjection::disableFailPoint(FailPoints::context_shutdown_auxiliary_zookeeper_drain);
        FailPointInjection::disableFailPoint(FailPoints::context_establish_zookeeper_session);
        for (auto & leader : leaders)
            leader.join();
        shutdown.join();

        for (size_t i = 0; i < 2; ++i)
            if (errors[i] || !results[i])
                fail("a leader did not get its session");

        if (!refusedWithUnfinished([&] { holder.context->getAuxiliaryZooKeeper(drain_names[0]); }))
            fail("getAuxiliaryZooKeeper after shutdown did not refuse");
        if (!refusedWithUnfinished([&] { holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig(drain_names)); }))
            fail("an auxiliary configuration reload after shutdown was not refused");
        if (!refusedWithUnfinished([&] { holder.context->getAuxiliaryZooKeeper(drain_names[1]); }))
            fail("getAuxiliaryZooKeeper after a post-shutdown reload did not refuse");

        std::cerr << "AUX_SHUTDOWN_DRAIN_OK" << std::endl;
        _exit(0);
    },
    ::testing::ExitedWithCode(0),
    "AUX_SHUTDOWN_DRAIN_OK");
}

/// `setServerCompletelyStarted` used to be delivered to every session because the whole establishment
/// held the Keeper mutex, so its visit of the published session could not run while an attempt was in
/// flight. It no longer does, and the visit happens before the shared started flag is set, so a leader
/// that snapshotted the flag beforehand would publish a session the transition never reached. The
/// transition is therefore recorded under the Keeper mutexes and read at publication.
///
/// A death test because `setServerCompletelyStarted` may be called once per process (it asserts the
/// flag is still unset) and the tests share one global `Context`.
TEST_F(ContextZooKeeperSingleFlight, PublicationAppliesServerCompletelyStarted)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    static const std::vector<String> started_names{"started_aux"};

    EXPECT_EXIT(
    {
        auto & holder = getMutableContext();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);

        const auto fail = [](const char * what)
        {
            std::cerr << "SERVER_STARTED_FAIL: " << what << std::endl;
            _exit(1);
        };

        /// A live default session to renew, and the auxiliary keeper configured, both established
        /// before the transition so the attempts below are renewals in flight across it.
        holder.context->reloadZooKeeperIfChanged(testKeeperConfig());
        holder.context->getZooKeeper();
        holder.context->reconnectZooKeeper("expire for test");
        holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig(started_names));

        /// Parked past everything the attempts read outside the mutex and before they publish - the
        /// window in which a pre-publication snapshot of the transition is already stale.
        FailPointInjection::enableFailPoint(FailPoints::context_zookeeper_before_publish);
        FailPointInjection::enableFailPoint(FailPoints::context_auxiliary_zookeeper_before_publish);
        FailPointInjection::enableFailPoint(FailPoints::context_zookeeper_applied_server_started);
        FailPointInjection::enableFailPoint(FailPoints::context_auxiliary_zookeeper_applied_server_started);

        zkutil::ZooKeeperPtr default_result;
        zkutil::ZooKeeperPtr auxiliary_result;
        std::exception_ptr default_error;
        std::exception_ptr auxiliary_error;
        std::thread default_leader([&]
        {
            try { default_result = holder.context->getZooKeeper(); }
            catch (...) { default_error = std::current_exception(); }
        });
        std::thread auxiliary_leader([&]
        {
            try { auxiliary_result = holder.context->getAuxiliaryZooKeeper(started_names[0]); }
            catch (...) { auxiliary_error = std::current_exception(); }
        });

        /// Their sessions are built but unpublished, so neither is in the maps that
        /// `setServerCompletelyStarted` visits.
        if (!FailPointInjection::waitForPause(FailPoints::context_zookeeper_before_publish, park_timeout))
            fail("the default leader did not park before publishing");
        if (!FailPointInjection::waitForPause(FailPoints::context_auxiliary_zookeeper_before_publish, park_timeout))
            fail("the auxiliary leader did not park before publishing");

        /// The transition runs entirely while they are parked: its visit takes each Keeper mutex - both
        /// free - and finds neither session. `SERVER` only for this call, which asserts the application
        /// type; no establishment work runs in that window, so nothing reads `ServerUUID`.
        holder.context->setApplicationType(Context::ApplicationType::SERVER);
        holder.context->setServerCompletelyStarted();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);

        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_before_publish);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_before_publish);

        /// Each publisher must apply the transition itself: the visit above could not see it.
        if (!FailPointInjection::waitForPause(FailPoints::context_zookeeper_applied_server_started, park_timeout))
            fail("the default session was published without the startup transition");
        if (!FailPointInjection::waitForPause(FailPoints::context_auxiliary_zookeeper_applied_server_started, park_timeout))
            fail("the auxiliary session was published without the startup transition");

        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_applied_server_started);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_applied_server_started);
        default_leader.join();
        auxiliary_leader.join();

        if (default_error || !default_result)
            fail("the default leader did not get a session");
        if (auxiliary_error || !auxiliary_result)
            fail("the auxiliary leader did not get a session");

        /// A session established entirely after the transition takes the same branch, with no visit of
        /// the maps left to rely on.
        holder.context->reconnectZooKeeper("expire again for test");
        FailPointInjection::enableFailPoint(FailPoints::context_zookeeper_applied_server_started);
        std::thread later([&] { holder.context->getZooKeeper(); });
        if (!FailPointInjection::waitForPause(FailPoints::context_zookeeper_applied_server_started, park_timeout))
            fail("a session established after the transition was published without it");
        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_applied_server_started);
        later.join();

        std::cerr << "SERVER_STARTED_OK" << std::endl;
        _exit(0);
    },
    ::testing::ExitedWithCode(0),
    "SERVER_STARTED_OK");
}

/// The first auxiliary reload has no recorded configuration to compare an in-flight attempt against:
/// `auxiliary_zookeepers_config` is null until it assigns it. Comparing the *global* configuration
/// instead cannot work, because `getConfigRef` returns the live `Poco::Util::Application`
/// configuration whose contents the reloader has already replaced — so a leader that parsed the old
/// settings is measured against the new ones, the comparison reports no change, and its session is
/// published after a reload that was supposed to replace it. The reload now supersedes unconditionally
/// in that case.
///
/// A death test because it needs `auxiliary_zookeepers_config` still null, and any other test's
/// auxiliary reload in this process assigns it for good.
TEST_F(ContextZooKeeperSingleFlight, FirstAuxiliaryReloadSupersedesAnAttemptOnTheOldGlobalConfig)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    EXPECT_EXIT(
    {
        auto & holder = getMutableContext();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);

        const auto fail = [](const char * what)
        {
            std::cerr << "FIRST_AUX_RELOAD_FAIL: " << what << std::endl;
            _exit(1);
        };

        /// The name has to come from the global configuration: with no auxiliary configuration yet, the
        /// establishment resolves it through `getConfigRef`.
        holder.context->setConfig(auxConfig({"cold_first"}, "<session_timeout_ms>10000</session_timeout_ms>"));

        FailPointInjection::enableFailPoint(FailPoints::context_auxiliary_zookeeper_after_args);

        zkutil::ZooKeeperPtr result;
        std::exception_ptr error;
        std::thread leader([&]
        {
            try { result = holder.context->getAuxiliaryZooKeeper("cold_first"); }
            catch (...) { error = std::current_exception(); }
        });

        /// Parked with the old settings already parsed into its `ZooKeeperArgs`.
        if (!FailPointInjection::waitForPause(FailPoints::context_auxiliary_zookeeper_after_args, park_timeout))
            fail("no leader parked after parsing its arguments");

        /// The reloader replaces the global configuration before calling the reload, so the old
        /// settings the leader holds are no longer readable anywhere.
        auto new_config = auxConfig({"cold_first"}, "<session_timeout_ms>20000</session_timeout_ms>");
        holder.context->setConfig(new_config);
        holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(new_config);

        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_after_args);
        leader.join();

        if (holder.context->getAuxiliaryZooKeepers().count("cold_first") != 0)
            fail("the session built on the old global configuration was published by the first reload");
        if (result)
            fail("the superseded leader returned its own session");
        if (!error)
            fail("the superseded leader did not report that it could not publish");

        /// And the name is still usable afterwards, against the new settings.
        auto retried = holder.context->getAuxiliaryZooKeeper("cold_first");
        if (!retried || retried->expired())
            fail("the retry after the reload did not get a session");

        std::cerr << "FIRST_AUX_RELOAD_OK" << std::endl;
        _exit(0);
    },
    ::testing::ExitedWithCode(0),
    "FIRST_AUX_RELOAD_OK");
}

/// An attempt whose own fetch of `system.zookeeper_connection_log` predates system-log initialization
/// must still report its session, and must do it from inside the publication critical section so a
/// later reload cannot record its events first. The log the initialization handler leaves under the
/// Keeper mutex is what makes that possible; this pins that the publisher actually consults it.
///
/// A death test: it needs the process to start with no system logs, and `initializeSystemLogs` is
/// one-way.
TEST_F(ContextZooKeeperSingleFlight, PublicationUsesTheLogLeftByInitialization)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    static const std::vector<String> log_names{"log_aux"};

    EXPECT_EXIT(
    {
        auto & holder = getMutableContext();
        holder.context->setApplicationType(Context::ApplicationType::LOCAL);

        const auto fail = [](const char * what)
        {
            std::cerr << "INIT_LOG_FAIL: " << what << std::endl;
            _exit(1);
        };

        if (holder.context->getZooKeeperConnectionLog())
            fail("the test context already has a connection log, so the hand-off cannot be reached");

        holder.context->reloadZooKeeperIfChanged(testKeeperConfig());
        holder.context->getZooKeeper();
        holder.context->reconnectZooKeeper("expire for test");
        holder.context->reloadAuxiliaryZooKeepersConfigIfChanged(auxConfig(log_names));

        FailPointInjection::enableFailPoint(FailPoints::context_zookeeper_before_publish);
        FailPointInjection::enableFailPoint(FailPoints::context_auxiliary_zookeeper_before_publish);
        FailPointInjection::enableFailPoint(FailPoints::context_zookeeper_used_initialized_log);
        FailPointInjection::enableFailPoint(FailPoints::context_auxiliary_zookeeper_used_initialized_log);

        zkutil::ZooKeeperPtr default_result;
        zkutil::ZooKeeperPtr auxiliary_result;
        std::thread default_leader([&] { try { default_result = holder.context->getZooKeeper(); } catch (...) {} });
        std::thread auxiliary_leader([&] { try { auxiliary_result = holder.context->getAuxiliaryZooKeeper(log_names[0]); } catch (...) {} });

        /// Both are past their own fetch of the log - which found nothing - and have not published.
        if (!FailPointInjection::waitForPause(FailPoints::context_zookeeper_before_publish, park_timeout))
            fail("the default leader did not park before publishing");
        if (!FailPointInjection::waitForPause(FailPoints::context_auxiliary_zookeeper_before_publish, park_timeout))
            fail("the auxiliary leader did not park before publishing");

        /// The system logs come up entirely while they are parked.
        holder.context->setConfig(makeConfig("<clickhouse><zookeeper_connection_log></zookeeper_connection_log></clickhouse>"));
        try { holder.context->initializeSystemLogs(); }
        catch (...) { fail("initializeSystemLogs is not usable in the unit-test context"); }
        if (!holder.context->getZooKeeperConnectionLog())
            fail("the connection log was not created from the configuration");
        holder.context->handleSystemZooKeeperConnectionLogAfterInitializationIfNeeded();

        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_before_publish);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_before_publish);

        /// Each publisher must reach for the log the handler left, inside its critical section.
        if (!FailPointInjection::waitForPause(FailPoints::context_zookeeper_used_initialized_log, park_timeout))
            fail("the default publisher did not use the log left by initialization");
        if (!FailPointInjection::waitForPause(FailPoints::context_auxiliary_zookeeper_used_initialized_log, park_timeout))
            fail("the auxiliary publisher did not use the log left by initialization");

        FailPointInjection::disableFailPoint(FailPoints::context_zookeeper_used_initialized_log);
        FailPointInjection::disableFailPoint(FailPoints::context_auxiliary_zookeeper_used_initialized_log);
        default_leader.join();
        auxiliary_leader.join();

        if (!default_result || !auxiliary_result)
            fail("a leader did not get its session");

        std::cerr << "INIT_LOG_OK" << std::endl;
        _exit(0);
    },
    ::testing::ExitedWithCode(0),
    "INIT_LOG_OK");
}
