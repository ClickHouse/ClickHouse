#pragma once

#include <Access/AccessRights.h>
#include <Access/RowPolicy.h>
#include <Interpreters/ClientInfo.h>
#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <ext/shared_ptr_helper.h>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <boost/container/flat_set.hpp>
#include <mutex>


namespace Poco { class Logger; }

namespace DB
{
struct User;
using UserPtr = std::shared_ptr<const User>;
struct EnabledRolesInfo;
class EnabledRoles;
class EnabledRowPolicies;
class EnabledQuota;
class EnabledSettings;
struct QuotaUsage;
struct Settings;
class SettingsConstraints;
class AccessControlManager;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class ContextAccess
{
public:
    struct Params
    {
        std::optional<UUID> user_id;
        boost::container::flat_set<UUID> current_roles;
        bool use_default_roles = false;
        UInt64 readonly = 0;
        bool allow_ddl = false;
        bool allow_introspection = false;
        String current_database;
        ClientInfo::Interface interface = ClientInfo::Interface::TCP;
        ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
        Poco::Net::IPAddress address;
        String quota_key;

        auto toTuple() const { return std::tie(user_id, current_roles, use_default_roles, readonly, allow_ddl, allow_introspection, current_database, interface, http_method, address, quota_key); }
        friend bool operator ==(const Params & lhs, const Params & rhs) { return lhs.toTuple() == rhs.toTuple(); }
        friend bool operator !=(const Params & lhs, const Params & rhs) { return !(lhs == rhs); }
        friend bool operator <(const Params & lhs, const Params & rhs) { return lhs.toTuple() < rhs.toTuple(); }
        friend bool operator >(const Params & lhs, const Params & rhs) { return rhs < lhs; }
        friend bool operator <=(const Params & lhs, const Params & rhs) { return !(rhs < lhs); }
        friend bool operator >=(const Params & lhs, const Params & rhs) { return !(lhs < rhs); }
    };

    const Params & getParams() const { return params; }

    /// Returns the current user. The function can return nullptr.
    UserPtr getUser() const;
    String getUserName() const;

    bool isCorrectPassword(const String & password) const;
    bool isClientHostAllowed() const;

    /// Returns information about current and enabled roles.
    /// The function can return nullptr.
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    /// Returns information about enabled row policies.
    /// The function can return nullptr.
    std::shared_ptr<const EnabledRowPolicies> getEnabledRowPolicies() const;

    /// Returns the row policy filter for a specified table.
    /// The function returns nullptr if there is no filter to apply.
    ASTPtr getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType index, const ASTPtr & extra_condition = nullptr) const;

    /// Returns the quota to track resource consumption.
    /// The function returns nullptr if no tracking or limitation is needed.
    std::shared_ptr<const EnabledQuota> getQuota() const;
    std::optional<QuotaUsage> getQuotaUsage() const;

    /// Returns the default settings, i.e. the settings to apply on user's login.
    /// The function returns nullptr if it's no need to apply settings.
    std::shared_ptr<const Settings> getDefaultSettings() const;

    /// Returns the settings' constraints.
    /// The function returns nullptr if there are no constraints.
    std::shared_ptr<const SettingsConstraints> getSettingsConstraints() const;

    /// Checks if a specified access is granted, and throws an exception if not.
    /// Empty database means the current database.
    void checkAccess(const AccessFlags & flags) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    void checkAccess(const AccessRightsElement & element) const;
    void checkAccess(const AccessRightsElements & elements) const;

    /// Checks if a specified access is granted.
    bool isGranted(const AccessFlags & flags) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & element) const;
    bool isGranted(const AccessRightsElements & elements) const;

    /// Checks if a specified access is granted, and logs a warning if not.
    bool isGranted(Poco::Logger * log_, const AccessFlags & flags) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(Poco::Logger * log_, const AccessRightsElement & element) const;
    bool isGranted(Poco::Logger * log_, const AccessRightsElements & elements) const;

    /// Checks if a specified access is granted with grant option, and throws an exception if not.
    void checkGrantOption(const AccessFlags & flags) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    void checkGrantOption(const AccessRightsElement & element) const;
    void checkGrantOption(const AccessRightsElements & elements) const;

    /// Checks if a specified role is granted with admin option, and throws an exception if not.
    void checkAdminOption(const UUID & role_id) const;

    /// Makes an instance of ContextAccess which provides full access to everything
    /// without any limitations. This is used for the global context.
    static std::shared_ptr<const ContextAccess> getFullAccess();

private:
    friend class AccessControlManager;
    ContextAccess() {}
    ContextAccess(const AccessControlManager & manager_, const Params & params_);

    void setUser(const UserPtr & user_) const;
    void setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & roles_info_) const;
    void setSettingsAndConstraints() const;

    template <int mode, bool grant_option>
    bool checkAccessImpl(Poco::Logger * log_, const AccessFlags & flags) const;

    template <int mode, bool grant_option, typename... Args>
    bool checkAccessImpl(Poco::Logger * log_, const AccessFlags & flags, const std::string_view & database, const Args &... args) const;

    template <int mode, bool grant_option>
    bool checkAccessImpl(Poco::Logger * log_, const AccessRightsElement & element) const;

    template <int mode, bool grant_option>
    bool checkAccessImpl(Poco::Logger * log_, const AccessRightsElements & elements) const;

    template <int mode, bool grant_option, typename... Args>
    bool calculateResultAccessAndCheck(Poco::Logger * log_, const AccessFlags & flags, const Args &... args) const;

    boost::shared_ptr<const AccessRights> calculateResultAccess(bool grant_option) const;
    boost::shared_ptr<const AccessRights> calculateResultAccess(bool grant_option, UInt64 readonly_, bool allow_ddl_, bool allow_introspection_) const;

    const AccessControlManager * manager = nullptr;
    const Params params;
    mutable Poco::Logger * trace_log = nullptr;
    mutable UserPtr user;
    mutable String user_name;
    mutable ext::scope_guard subscription_for_user_change;
    mutable std::shared_ptr<const EnabledRoles> enabled_roles;
    mutable ext::scope_guard subscription_for_roles_changes;
    mutable std::shared_ptr<const EnabledRolesInfo> roles_info;
    mutable boost::atomic_shared_ptr<const AccessRights> result_access[7];
    mutable std::shared_ptr<const EnabledRowPolicies> enabled_row_policies;
    mutable std::shared_ptr<const EnabledQuota> enabled_quota;
    mutable std::shared_ptr<const EnabledSettings> enabled_settings;
    mutable std::mutex mutex;
};

}
