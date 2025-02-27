#pragma once

#include <Access/AccessRights.h>
#include <Access/ContextAccessParams.h>
#include <Access/EnabledRowPolicies.h>
#include <Interpreters/ClientInfo.h>
#include <Core/UUID.h>
#include <base/scope_guard.h>
#include <boost/container/flat_set.hpp>
#include <mutex>
#include <optional>
#include <unordered_map>


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
struct SettingsProfilesInfo;
class SettingsChanges;
class AccessControl;
class IAST;
struct IAccessEntity;
using ASTPtr = std::shared_ptr<IAST>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;


class ContextAccess : public std::enable_shared_from_this<ContextAccess>
{
public:
    static std::shared_ptr<const ContextAccess> fromContext(const ContextPtr & context);

    using Params = ContextAccessParams;
    const Params & getParams() const { return params; }

    /// Returns the current user. Throws if user is nullptr.
    UserPtr getUser() const;
    /// Same as above, but can return nullptr.
    UserPtr tryGetUser() const;
    String getUserName() const;
    std::optional<UUID> getUserID() const { return getParams().user_id; }

    /// Returns information about current and enabled roles.
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    /// Returns the row policy filter for a specified table.
    /// The function returns nullptr if there is no filter to apply.
    RowPolicyFilterPtr getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const;

    /// Returns the quota to track resource consumption.
    std::shared_ptr<const EnabledQuota> getQuota() const;
    std::optional<QuotaUsage> getQuotaUsage() const;

    /// Returns the default settings, i.e. the settings which should be applied on user's login.
    SettingsChanges getDefaultSettings() const;
    std::shared_ptr<const SettingsProfilesInfo> getDefaultProfileInfo() const;

    /// Returns the current access rights.
    std::shared_ptr<const AccessRights> getAccessRights() const;
    std::shared_ptr<const AccessRights> getAccessRightsWithImplicit() const;

    /// Checks if a specified access is granted, and throws an exception if not.
    /// Empty database means the current database.
    void checkAccess(const AccessFlags & flags) const;
    void checkAccess(const AccessFlags & flags, std::string_view database) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    void checkAccess(const AccessRightsElement & element) const;
    void checkAccess(const AccessRightsElements & elements) const;

    void checkGrantOption(const AccessFlags & flags) const;
    void checkGrantOption(const AccessFlags & flags, std::string_view database) const;
    void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    void checkGrantOption(const AccessRightsElement & element) const;
    void checkGrantOption(const AccessRightsElements & elements) const;

    /// Checks if a specified access is granted, and returns false if not.
    /// Empty database means the current database.
    bool isGranted(const AccessFlags & flags) const;
    bool isGranted(const AccessFlags & flags, std::string_view database) const;
    bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & element) const;
    bool isGranted(const AccessRightsElements & elements) const;

    bool hasGrantOption(const AccessFlags & flags) const;
    bool hasGrantOption(const AccessFlags & flags, std::string_view database) const;
    bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
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

    ContextAccess(const AccessControl & access_control_, const Params & params_);
    ~ContextAccess();

private:
    friend class AccessControl;

    void initialize();
    void setUser(const UserPtr & user_) const TSA_REQUIRES(mutex);
    void setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & roles_info_) const TSA_REQUIRES(mutex);
    void calculateAccessRights() const TSA_REQUIRES(mutex);

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImpl(const AccessFlags & flags) const;

    template <bool throw_if_denied, bool grant_option, typename... Args>
    bool checkAccessImpl(const AccessFlags & flags, std::string_view database, const Args &... args) const;

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImpl(const AccessRightsElement & element) const;

    template <bool throw_if_denied, bool grant_option>
    bool checkAccessImpl(const AccessRightsElements & elements) const;

    template <bool throw_if_denied, bool grant_option, typename... Args>
    bool checkAccessImplHelper(AccessFlags flags, const Args &... args) const;

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

    const AccessControl * access_control = nullptr;
    const Params params;

    mutable std::atomic<bool> initialized = false; // can be removed after Bug 5504 is resolved
    mutable std::atomic<bool> user_was_dropped = false;

    mutable std::mutex mutex;
    /// TODO: Fix race
    mutable LoggerPtr trace_log;
    mutable UserPtr user TSA_GUARDED_BY(mutex);
    mutable String user_name TSA_GUARDED_BY(mutex);
    mutable scope_guard subscription_for_user_change TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const EnabledRoles> enabled_roles TSA_GUARDED_BY(mutex);
    mutable scope_guard subscription_for_roles_changes TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const EnabledRolesInfo> roles_info TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const AccessRights> access TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const AccessRights> access_with_implicit TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const EnabledRowPolicies> enabled_row_policies TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const EnabledRowPolicies> row_policies_of_initial_user TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const EnabledQuota> enabled_quota TSA_GUARDED_BY(mutex);
    mutable std::shared_ptr<const EnabledSettings> enabled_settings TSA_GUARDED_BY(mutex);
};

}
