#include <gtest/gtest.h>

#include <Access/AccessControl.h>
#include <Access/Common/QuotaDefs.h>
#include <Access/EnabledQuota.h>
#include <Access/Quota.h>
#include <Access/User.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/Net/IPAddress.h>

#include <base/scope_guard.h>

#include <chrono>
#include <mutex>
#include <string>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int QUOTA_REQUIRES_CLIENT_KEY;
}

namespace
{

/// Records every logged message so a test can assert what was (not) logged.
class CapturingChannel : public Poco::Channel
{
public:
    void log(const Poco::Message & msg) override
    {
        std::lock_guard lock(mutex);
        messages.emplace_back(msg.getText());
    }

    bool contains(const std::string & needle) const
    {
        std::lock_guard lock(mutex);
        for (const auto & m : messages)
            if (m.contains(needle))
                return true;
        return false;
    }

private:
    mutable std::mutex mutex;
    std::vector<std::string> messages;
};

UUID addClientKeyQuota(AccessControl & access_control, const String & name, const UUID & user_id)
{
    auto quota = std::make_shared<Quota>();
    quota->setName(name);
    quota->key_type = QuotaKeyType::CLIENT_KEY;
    quota->to_roles.add(user_id);

    Quota::Limits limits;
    limits.duration = std::chrono::seconds{3600};
    limits.max[static_cast<size_t>(QuotaType::QUERIES)] = 1000;
    quota->all_limits.push_back(limits);

    return access_control.insert(quota);
}

}

/// Regression: a `KEYED BY client_key` quota leaves a persistent enabled set with an empty client key at
/// authentication time (the key is unknown during the handshake). Any later access-change notification runs
/// the QuotaCache recompute over that set. It used to raise `QUOTA_REQUIRES_CLIENT_KEY` there (caught and
/// logged by `AccessChangesNotifier::sendNotifications`), turning every quota/user DDL into a spurious error
/// line. The recompute must tolerate the empty key; requiring it is admission control, enforced elsewhere.
TEST(QuotaCacheClientKey, RecomputeDuringNotificationDoesNotError)
{
    AccessControl access_control;
    access_control.addMemoryStorage("gtest_quota_client_key", /*allow_backup_=*/ false);

    auto user = std::make_shared<User>();
    user->setName("gtest_quota_client_key_user");
    UUID user_id = access_control.insert(user);

    addClientKeyQuota(access_control, "gtest_quota_client_key_quota", user_id);

    /// Capture all loggers (root and existing), restored on scope exit.
    Poco::AutoPtr<CapturingChannel> capture(new CapturingChannel());
    Poco::AutoPtr<Poco::Channel> old_channel(Poco::Logger::root().getChannel(), /*shared=*/ true);
    int old_level = Poco::Logger::root().getLevel();
    Poco::Logger::setChannel("", capture.get());
    Poco::Logger::setLevel("", Poco::Message::PRIO_TRACE);
    SCOPE_EXIT({
        Poco::Logger::setChannel("", old_channel.get());
        Poco::Logger::setLevel("", old_level);
    });

    /// Authentication builds an enabled set with an empty client key; it must not throw and must persist
    /// (we hold the returned pointer so the recompute below iterates it).
    auto auth_quota = access_control.getAuthenticationQuota(user->getName(), Poco::Net::IPAddress{"127.0.0.1"}, "");
    ASSERT_TRUE(auth_quota);

    /// Fire an access-change notification while the empty-key set is alive: this drives the recompute.
    auto other_user = std::make_shared<User>();
    other_user->setName("gtest_quota_client_key_user2");
    UUID other_user_id = access_control.insert(other_user);
    addClientKeyQuota(access_control, "gtest_quota_client_key_quota2", other_user_id);

    EXPECT_FALSE(capture->contains("requires a client supplied key"));
    EXPECT_FALSE(capture->contains("QUOTA_REQUIRES_CLIENT_KEY"));
}

/// The flip side of the same design: admission control is preserved. A real request governed by a
/// `KEYED BY client_key` quota is rejected when it supplies no key, and admitted when it does.
TEST(QuotaCacheClientKey, RealRequestRequiresClientKey)
{
    AccessControl access_control;
    access_control.addMemoryStorage("gtest_quota_client_key_admission", /*allow_backup_=*/ false);

    auto user = std::make_shared<User>();
    user->setName("gtest_quota_client_key_admission_user");
    UUID user_id = access_control.insert(user);

    addClientKeyQuota(access_control, "gtest_quota_client_key_admission_quota", user_id);

    auto address = std::make_shared<Poco::Net::IPAddress>("127.0.0.1");

    bool threw_requires_key = false;
    try
    {
        access_control.getEnabledQuota(user_id, user->getName(), {}, address, "", /*custom_quota_key=*/ "");
    }
    catch (const Exception & e)
    {
        threw_requires_key = (e.code() == ErrorCodes::QUOTA_REQUIRES_CLIENT_KEY);
    }
    EXPECT_TRUE(threw_requires_key);

    EXPECT_NO_THROW(access_control.getEnabledQuota(user_id, user->getName(), {}, address, "", "tenant_a"));
}
