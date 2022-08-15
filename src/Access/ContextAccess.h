#pragma once

#include <Access/AccessRights.h>
#include <Access/RowPolicy.h>
#include <Interpreters/ClientInfo.h>
#include <Core/UUID.h>
#include <common/scope_guard.h>
#include <common/shared_ptr_helper.h>
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
struct IAccessEntity;
using ASTPtr = std::shared_ptr<IAST>;


struct ContextAccessParams
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
    String forwarded_address;
    String quota_key;

    auto toTuple() const
    {
        return std::tie(
            user_id, current_roles, use_default_roles, readonly, allow_ddl, allow_introspection,
            current_database, interface, http_method, address, forwarded_address, quota_key);
    }

    friend bool operator ==(const ContextAccessParams & lhs, const ContextAccessParams & rhs) { return lhs.toTuple() == rhs.toTuple(); }
    friend bool operator !=(const ContextAccessParams & lhs, const ContextAccessParams & rhs) { return !(lhs == rhs); }
    friend bool operator <(const ContextAccessParams & lhs, const ContextAccessParams & rhs) { return lhs.toTuple() < rhs.toTuple(); }
    friend bool operator >(const ContextAccessParams & lhs, const ContextAccessParams & rhs) { return rhs < lhs; }
    friend bool operator <=(const ContextAccessParams & lhs, const ContextAccessParams & rhs) { return !(rhs < lhs); }
    friend bool operator >=(const ContextAccessParams & lhs, const ContextAccessParams & rhs) { return !(lhs < rhs); }
};


class ContextAccess
{
public:
    using Params = ContextAccessParams;
    const Params & getParams() const { return params; }

    /// Returns the current user. Throws if user is nullptr.
    UserPtr getUser() const;
    /// Same as above, but can return nullptr.
    UserPtr tryGetUser() const;
    String getUserName() const;

    /// Returns information about current and enabled roles.
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    /// Returns information about enabled row policies.
    std::shared_ptr<const EnabledRowPolicies> getEnabledRowPolicies() const;

    /// Returns the row policy filter for a specified table.
    /// The function returns nullptr if there is no filter to apply.
    ASTPtr getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType index, const ASTPtr & extra_condition = nullptr) const;

    /// Returns the quota to track resource consumption.
    std::shared_ptr<const EnabledQuota> getQuota() const;
    std::optional<QuotaUsage> getQuotaUsage() const;

    /// Returns the default settings, i.e. the settings to apply on user's login.
    std::shared_ptr<const Settings> getDefaultSettings() const;

    /// Returns the settings' constraints.
    std::shared_ptr<const SettingsConstraints> getSettingsConstraints() const;

    /// Returns the current access rights.
    std::shared_ptr<const AccessRights> getAccessRights() const;
    std::shared_ptr<const AccessRights> getAccessRightsWithImplicit() const;

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

    void checkGrantOption(const AccessFlags & flags) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    void checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    void checkGrantOption(const AccessRightsElement & element) const;
    void checkGrantOption(const AccessRightsElements & elements) const;

    /// Checks if a specified access is granted, and returns false if not.
    /// Empty database means the current database.
    bool isGranted(const AccessFlags & flags) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & element) const;
    bool isGranted(const AccessRightsElements & elements) const;

    bool hasGrantOption(const AccessFlags & flags) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool hasGrantOption(const AccessRightsElement & element) const;
    bool hasGrantOption(const AccessRightsElements & elements) const;

    /// Checks if a specified role is granted with admin option, and throws an exception if not.
    void checkAdminOption(const UUID & role_id) const;
    void checkAdminOption(const UUID & role_id, const String & role_name) const;
    void checkAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const;
    void checkAdminOption(const std::vector<UUID> & role_ids) const;
    void checkAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const;
    void checkAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const;

    /// Checks if a specified role is granted with admin option, and returns false if not.
    bool hasAdminOption(const UUID & role_id) const;
    bool hasAdminOption(const UUID & role_id, const String & role_name) const;
    bool hasAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const;
    bool hasAdminOption(const std::vector<UUID> & role_ids) const;
    bool hasAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const;
    bool hasAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const;

    /// Checks if a grantee is allowed for the current user, throws an exception if not.
    void checkGranteeIsAllowed(const UUID & grantee_id, const IAccessEntity & grantee) const;
    /// Checks if grantees are allowed for the current user, throws an exception if not.
    void checkGranteesAreAllowed(const std::vector<UUID> & grantee_ids) const;

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
    void calculateAccessRights() const;

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImpl(const AccessFlags & flags) const;

    template <bool throw_if_denied, bool grant_option, typename... Args>
    bool checkAccessImpl(const AccessFlags & flags, const std::string_view & database, const Args &... args) const;

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImpl(const AccessRightsElement & element) const;

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImpl(const AccessRightsElements & elements) const;

    template <bool throw_if_denied, bool grant_option, typename... Args>
    bool checkAccessImplHelper(const AccessFlags & flags, const Args &... args) const;

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImplHelper(const AccessRightsElement & element) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const UUID & role_id) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const UUID & role_id, const String & role_name) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const std::vector<UUID> & role_ids) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const;

    template <bool throw_if_denied, typename Container, typename GetNameFunction>
    bool checkAdminOptionImplHelper(const Container & role_ids, const GetNameFunction & get_name_function) const;

    const AccessControlManager * manager = nullptr;
    const Params params;
    bool is_full_access = false;
    mutable Poco::Logger * trace_log = nullptr;
    mutable UserPtr user;
    mutable String user_name;
    mutable bool user_was_dropped = false;
    mutable scope_guard subscription_for_user_change;
    mutable std::shared_ptr<const EnabledRoles> enabled_roles;
    mutable scope_guard subscription_for_roles_changes;
    mutable std::shared_ptr<const EnabledRolesInfo> roles_info;
    mutable std::shared_ptr<const AccessRights> access;
    mutable std::shared_ptr<const AccessRights> access_with_implicit;
    mutable std::shared_ptr<const EnabledRowPolicies> enabled_row_policies;
    mutable std::shared_ptr<const EnabledQuota> enabled_quota;
    mutable std::shared_ptr<const EnabledSettings> enabled_settings;
    mutable std::mutex mutex;
};

}
