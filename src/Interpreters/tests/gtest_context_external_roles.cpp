#include <gtest/gtest.h>

#include <Access/AccessControl.h>
#include <Access/Role.h>
#include <Access/SettingsProfileElement.h>
#include <Access/User.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/tests/gtest_global_context.h>

#include <algorithm>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int SET_NON_GRANTED_ROLE;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

namespace
{

bool contains(const std::vector<UUID> & ids, const UUID & id)
{
    return std::find(ids.begin(), ids.end(), id) != ids.end();
}

}

/// Regression for the deferred executors (asynchronous insert flush and the `QueryRunner` invoker) that
/// re-create a context for the originating session. `getCurrentRoles` returns the session's *effective*
/// current roles, which already include the external (pushed) roles received over the interserver
/// protocol. Rebuilding the identity from `user_id` plus those effective roles and re-applying them with
/// the grant check throws `SET_NON_GRANTED_ROLE` for a role that exists only as an external role (it is
/// not locally granted to the user). The fix carries the external roles separately and replays them via
/// `setUser`, applying the effective current roles without the grant check.
TEST(ContextExternalRoles, DeferredReplayPreservesExternalRoles)
{
    auto context = getMutableContext().context;
    auto & access_control = context->getAccessControl();
    access_control.addMemoryStorage("gtest_external_roles_memory", /*allow_backup_=*/ false);

    /// A user with no locally granted roles.
    auto user = std::make_shared<User>();
    user->setName("gtest_external_roles_user");
    UUID user_id = access_control.insert(user);

    /// A role that is *not* granted to the user, standing in for a role pushed from another node.
    auto role = std::make_shared<Role>();
    role->setName("gtest_external_only_role");
    UUID role_id = access_control.insert(role);

    /// A session that received the role as an external (pushed) role.
    auto session_context = Context::createCopy(context);
    session_context->makeQueryContext();
    session_context->setUser(user_id, /*external_roles=*/ {role_id});

    /// The effective current roles include the external role even though it is not locally granted, and
    /// the external role is reported separately by the new getter used to carry it across the boundary.
    auto effective_roles = session_context->getCurrentRoles();
    EXPECT_TRUE(contains(effective_roles, role_id));
    EXPECT_EQ(session_context->getExternalRoles(), std::vector<UUID>{role_id});

    /// The old (buggy) deferred replay: rebuild from the user and the effective current roles only, with
    /// the grant check. The external-only role is not locally granted, so this throws.
    {
        auto job_context = Context::createCopy(context);
        job_context->makeQueryContext();
        job_context->setUser(user_id);
        bool threw_non_granted = false;
        try
        {
            job_context->setCurrentRoles(effective_roles);
        }
        catch (const Exception & e)
        {
            threw_non_granted = (e.code() == ErrorCodes::SET_NON_GRANTED_ROLE);
        }
        EXPECT_TRUE(threw_non_granted);
    }

    /// The new deferred replay: carry the external roles through `setUser` and re-apply the effective
    /// current roles without the grant check. It must not throw and must keep the external role.
    {
        auto job_context = Context::createCopy(context);
        job_context->makeQueryContext();
        job_context->setUser(user_id, session_context->getExternalRoles());
        EXPECT_NO_THROW(job_context->setCurrentRoles(effective_roles, /*check_grants=*/ false));
        EXPECT_TRUE(contains(job_context->getCurrentRoles(), role_id));
    }
}

/// Regression for switching principals on a context that already carries pushed (external) roles.
/// `ContextData`'s copy constructor now preserves `external_roles`, so `setUser` must clear them when the
/// new principal does not bring its own; otherwise the stale external role would remain enabled for the
/// target user (e.g. `EXECUTE AS target_user` reuses the session context via `impersonateSessionContext`,
/// calling `setUser(target_id)` with no external roles), silently widening the target's privileges.
TEST(ContextExternalRoles, SetUserClearsStaleExternalRolesOnUserSwitch)
{
    auto context = getMutableContext().context;
    auto & access_control = context->getAccessControl();
    access_control.addMemoryStorage("gtest_external_roles_switch_memory", /*allow_backup_=*/ false);

    /// The principal that authenticated with a pushed (external) role, and the one we switch to.
    auto source_user = std::make_shared<User>();
    source_user->setName("gtest_external_roles_source_user");
    UUID source_user_id = access_control.insert(source_user);

    auto target_user = std::make_shared<User>();
    target_user->setName("gtest_external_roles_target_user");
    UUID target_user_id = access_control.insert(target_user);

    /// A role that is not granted to either user, standing in for a role pushed from another node.
    auto role = std::make_shared<Role>();
    role->setName("gtest_external_roles_switch_role");
    UUID role_id = access_control.insert(role);

    /// A session authenticated as the source user with the role received as an external (pushed) role.
    auto session_context = Context::createCopy(context);
    session_context->makeQueryContext();
    session_context->setUser(source_user_id, /*external_roles=*/ {role_id});
    EXPECT_EQ(session_context->getExternalRoles(), std::vector<UUID>{role_id});
    EXPECT_TRUE(contains(session_context->getCurrentRoles(), role_id));

    /// Switch the same context to another principal without bringing any external roles.
    session_context->setUser(target_user_id);

    /// The stale external role must be gone; otherwise the target user would silently keep it enabled.
    EXPECT_TRUE(session_context->getExternalRoles().empty());
    EXPECT_FALSE(contains(session_context->getCurrentRoles(), role_id));
}

/// The provenance rule for external roles and settings profiles. An external role is always effective for
/// authorization (it is a current role of the context). Whether its attached settings profile - values and
/// constraints - is installed into the context's creation-time settings snapshot depends on how the role
/// arrived: only roles passed as `external_roles_for_settings_profiles_` (the authentication-time roles of a
/// freshly created session) initialize profiles. A role set that is merely propagated or replayed (interserver,
/// DDL worker, deferred executors) passes only `external_roles_` and must not rebuild profile state.
TEST(ContextExternalRoles, OnlyAuthenticationTimeExternalRolesInitializeProfiles)
{
    auto context = getMutableContext().context;
    auto & access_control = context->getAccessControl();
    access_control.addMemoryStorage("gtest_external_roles_profiles_memory", /*allow_backup_=*/ false);

    auto user = std::make_shared<User>();
    user->setName("gtest_external_roles_profiles_user");
    UUID user_id = access_control.insert(user);

    /// A role (not granted to the user) with an attached settings profile: a value and a constraint.
    auto role = std::make_shared<Role>();
    role->setName("gtest_external_roles_profiles_role");
    {
        SettingsProfileElement value_element;
        value_element.setting_name = "max_result_rows";
        value_element.value = Field(UInt64(555));
        role->settings.push_back(value_element);

        SettingsProfileElement constraint_element;
        constraint_element.setting_name = "max_threads";
        constraint_element.max_value = Field(UInt64(4));
        role->settings.push_back(constraint_element);
    }
    UUID role_id = access_control.insert(role);

    const SettingsChanges over_the_cap{{"max_threads", Field(UInt64(16))}};

    /// Case A: the role is propagated/replayed as an external role only. It is effective for authorization,
    /// but its profile is NOT installed: no value, no constraint.
    {
        auto replay_context = Context::createCopy(context);
        replay_context->makeQueryContext();
        replay_context->setUser(user_id, /*external_roles=*/ {role_id});
        EXPECT_TRUE(contains(replay_context->getCurrentRoles(), role_id));
        EXPECT_EQ(replay_context->getExternalRoles(), std::vector<UUID>{role_id});
        EXPECT_NE(replay_context->getSettingsRef().get("max_result_rows").safeGet<UInt64>(), 555u);
        EXPECT_NO_THROW(replay_context->checkSettingsConstraints(over_the_cap, SettingSource::QUERY));
    }

    /// Case B: the same role arrives as an authentication-time external role of a fresh session. It is
    /// effective for authorization AND its profile is installed: value applied, constraint enforced.
    {
        auto login_context = Context::createCopy(context);
        login_context->makeQueryContext();
        login_context->setUser(
            user_id,
            /*external_roles=*/ {role_id},
            /*authentication_grants=*/ nullptr,
            /*authentication_valid_until=*/ 0,
            /*external_roles_for_settings_profiles=*/ {role_id});
        EXPECT_TRUE(contains(login_context->getCurrentRoles(), role_id));
        EXPECT_EQ(login_context->getExternalRoles(), std::vector<UUID>{role_id});
        EXPECT_EQ(login_context->getSettingsRef().get("max_result_rows").safeGet<UInt64>(), 555u);
        bool threw_constraint_violation = false;
        try
        {
            login_context->checkSettingsConstraints(over_the_cap, SettingSource::QUERY);
        }
        catch (const Exception & e)
        {
            threw_constraint_violation = (e.code() == ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
        }
        EXPECT_TRUE(threw_constraint_violation);
    }
}
