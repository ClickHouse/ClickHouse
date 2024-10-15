#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <Access/AccessRights.h>
#include <Access/ContextAccessParams.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/QuotaUsage.h>
#include <Core/UUID.h>
#include <Interpreters/ClientInfo.h>
#include <base/scope_guard.h>
#include <Common/SettingsChanges.h>


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
    void checkAccess(const ContextPtr & context, const AccessFlags & flags) const;
    void checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const;
    void checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const;
    void checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    void checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    void checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    void checkAccess(const ContextPtr & context, const AccessRightsElement & element) const;
    void checkAccess(const ContextPtr & context, const AccessRightsElements & elements) const;

    void checkGrantOption(const ContextPtr & context, const AccessFlags & flags) const;
    void checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const;
    void checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const;
    void checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    void checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    void checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    void checkGrantOption(const ContextPtr & context, const AccessRightsElement & element) const;
    void checkGrantOption(const ContextPtr & context, const AccessRightsElements & elements) const;

    /// Checks if a specified access is granted, and returns false if not.
    /// Empty database means the current database.
    bool isGranted(const ContextPtr & context, const AccessFlags & flags) const;
    bool isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const;
    bool isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const;
    bool isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    bool isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    bool isGranted(const ContextPtr & context, const AccessRightsElement & element) const;
    bool isGranted(const ContextPtr & context, const AccessRightsElements & elements) const;

    bool hasGrantOption(const ContextPtr & context, const AccessFlags & flags) const;
    bool hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const;
    bool hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const;
    bool hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    bool hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    bool hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    bool hasGrantOption(const ContextPtr & context, const AccessRightsElement & element) const;
    bool hasGrantOption(const ContextPtr & context, const AccessRightsElements & elements) const;

    /// Checks if a specified role is granted with admin option, and throws an exception if not.
    void checkAdminOption(const ContextPtr & context, const UUID & role_id) const;
    void checkAdminOption(const ContextPtr & context, const UUID & role_id, const String & role_name) const;
    void checkAdminOption(const ContextPtr & context, const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const;
    void checkAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids) const;
    void checkAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const Strings & names_of_roles) const;
    void checkAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const;

    /// Checks if a specified role is granted with admin option, and returns false if not.
    bool hasAdminOption(const ContextPtr & context, const UUID & role_id) const;
    bool hasAdminOption(const ContextPtr & context, const UUID & role_id, const String & role_name) const;
    bool hasAdminOption(const ContextPtr & context, const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const;
    bool hasAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids) const;
    bool hasAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const Strings & names_of_roles) const;
    bool hasAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const;

    /// Checks if a grantee is allowed for the current user, throws an exception if not.
    void checkGranteeIsAllowed(const UUID & grantee_id, const IAccessEntity & grantee) const;
    /// Checks if grantees are allowed for the current user, throws an exception if not.
    void checkGranteesAreAllowed(const std::vector<UUID> & grantee_ids) const;

    static AccessRights addImplicitAccessRights(const AccessRights & access, const AccessControl & access_control);

    ContextAccess(const AccessControl & access_control_, const Params & params_);
    ~ContextAccess();

private:
    friend class AccessControl;

    void initialize();
    void setUser(const UserPtr & user_) const TSA_REQUIRES(mutex);
    void setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & roles_info_) const TSA_REQUIRES(mutex);
    void calculateAccessRights() const TSA_REQUIRES(mutex);

    template <bool throw_if_denied, bool grant_option, bool wildcard>
    bool checkAccessImpl(const ContextPtr & context, const AccessFlags & flags) const;

    template <bool throw_if_denied, bool grant_option, bool wildcard, typename... Args>
    bool checkAccessImpl(const ContextPtr & context, const AccessFlags & flags, std::string_view database, const Args &... args) const;

    template <bool throw_if_denied, bool grant_option, bool wildcard>
    bool checkAccessImpl(const ContextPtr & context, const AccessRightsElement & element) const;

    template <bool throw_if_denied, bool grant_option, bool wildcard>
    bool checkAccessImpl(const ContextPtr & context, const AccessRightsElements & elements) const;

    template <bool throw_if_denied, bool grant_option, bool wildcard, typename... Args>
    bool checkAccessImplHelper(const ContextPtr & context, AccessFlags flags, const Args &... args) const;

    template <bool throw_if_denied, bool grant_option, bool wildcard>
    bool checkAccessImplHelper(const ContextPtr & context, const AccessRightsElement & element) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const ContextPtr & context, const UUID & role_id) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const ContextPtr & context, const UUID & role_id, const String & role_name) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const ContextPtr & context, const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const ContextPtr & context, const std::vector<UUID> & role_ids) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const ContextPtr & context, const std::vector<UUID> & role_ids, const Strings & names_of_roles) const;

    template <bool throw_if_denied>
    bool checkAdminOptionImpl(const ContextPtr & context, const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const;

    template <bool throw_if_denied, typename Container, typename GetNameFunction>
    bool checkAdminOptionImplHelper(const ContextPtr & context, const Container & role_ids, const GetNameFunction & get_name_function) const;

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

/// This wrapper was added to be able to pass the current context to the access
/// without the need to change the signature and all calls to the ContextAccess itself.
/// Right now a context is used to store privileges that are checked for a query,
/// and might be useful for something else in the future as well.
class ContextAccessWrapper : public std::enable_shared_from_this<ContextAccessWrapper>
{
public:
    using ContextAccessPtr = std::shared_ptr<const ContextAccess>;

    ContextAccessWrapper(const ContextAccessPtr & access_, const ContextPtr & context_): access(access_), context(context_) {}
    ~ContextAccessWrapper() = default;

    static std::shared_ptr<const ContextAccessWrapper> fromContext(const ContextPtr & context);

    const ContextAccess::Params & getParams() const { return access->getParams(); }

    const ContextAccessPtr & getAccess() const { return access; }

    /// Returns the current user. Throws if user is nullptr.
    ALWAYS_INLINE UserPtr getUser() const { return access->getUser(); }
    /// Same as above, but can return nullptr.
    ALWAYS_INLINE UserPtr tryGetUser() const { return access->tryGetUser(); }
    ALWAYS_INLINE String getUserName() const { return access->getUserName(); }
    ALWAYS_INLINE std::optional<UUID> getUserID() const { return access->getUserID(); }

    /// Returns information about current and enabled roles.
    ALWAYS_INLINE std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const { return access->getRolesInfo(); }

    /// Returns the row policy filter for a specified table.
    /// The function returns nullptr if there is no filter to apply.
    ALWAYS_INLINE RowPolicyFilterPtr getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const { return access->getRowPolicyFilter(database, table_name, filter_type); }

    /// Returns the quota to track resource consumption.
    ALWAYS_INLINE std::shared_ptr<const EnabledQuota> getQuota() const { return access->getQuota(); }
    ALWAYS_INLINE std::optional<QuotaUsage> getQuotaUsage() const { return access->getQuotaUsage(); }

    /// Returns the default settings, i.e. the settings which should be applied on user's login.
    ALWAYS_INLINE SettingsChanges getDefaultSettings() const { return access->getDefaultSettings(); }
    ALWAYS_INLINE std::shared_ptr<const SettingsProfilesInfo> getDefaultProfileInfo() const { return access->getDefaultProfileInfo(); }

    /// Returns the current access rights.
    ALWAYS_INLINE std::shared_ptr<const AccessRights> getAccessRights() const { return access->getAccessRights(); }
    ALWAYS_INLINE std::shared_ptr<const AccessRights> getAccessRightsWithImplicit() const { return access->getAccessRightsWithImplicit(); }

    /// Checks if a specified access is granted, and throws an exception if not.
    /// Empty database means the current database.
    ALWAYS_INLINE void checkAccess(const AccessFlags & flags) const { access->checkAccess(context, flags); }
    ALWAYS_INLINE void checkAccess(const AccessFlags & flags, std::string_view database) const { access->checkAccess(context, flags, database); }
    ALWAYS_INLINE void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table) const { access->checkAccess(context, flags, database, table); }
    ALWAYS_INLINE void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { access->checkAccess(context, flags, database, table, column); }
    ALWAYS_INLINE void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { access->checkAccess(context, flags, database, table, columns); }
    ALWAYS_INLINE void checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { access->checkAccess(context, flags, database, table, columns); }
    ALWAYS_INLINE void checkAccess(const AccessRightsElement & element) const { access->checkAccess(context, element); }
    ALWAYS_INLINE void checkAccess(const AccessRightsElements & elements) const { access->checkAccess(context, elements); }

    ALWAYS_INLINE void checkGrantOption(const AccessFlags & flags) const { access->checkGrantOption(context, flags); }
    ALWAYS_INLINE void checkGrantOption(const AccessFlags & flags, std::string_view database) const { access->checkGrantOption(context, flags, database); }
    ALWAYS_INLINE void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) const { access->checkGrantOption(context, flags, database, table); }
    ALWAYS_INLINE void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { access->checkGrantOption(context, flags, database, table, column); }
    ALWAYS_INLINE void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { access->checkGrantOption(context, flags, database, table, columns); }
    ALWAYS_INLINE void checkGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { access->checkGrantOption(context, flags, database, table, columns); }
    ALWAYS_INLINE void checkGrantOption(const AccessRightsElement & element) const { access->checkGrantOption(context, element); }
    ALWAYS_INLINE void checkGrantOption(const AccessRightsElements & elements) const { access->checkGrantOption(context, elements); }

    /// Checks if a specified access is granted, and returns false if not.
    /// Empty database means the current database.
    ALWAYS_INLINE bool isGranted(const AccessFlags & flags) const { return access->isGranted(context, flags); }
    ALWAYS_INLINE bool isGranted(const AccessFlags & flags, std::string_view database) const { return access->isGranted(context, flags, database); }
    ALWAYS_INLINE bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table) const { return access->isGranted(context, flags, database, table); }
    ALWAYS_INLINE bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return access->isGranted(context, flags, database, table, column); }
    ALWAYS_INLINE bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return access->isGranted(context, flags, database, table, columns); }
    ALWAYS_INLINE bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return access->isGranted(context, flags, database, table, columns); }
    ALWAYS_INLINE bool isGranted(const AccessRightsElement & element) const { return access->isGranted(context, element); }
    ALWAYS_INLINE bool isGranted(const AccessRightsElements & elements) const { return access->isGranted(context, elements); }

    ALWAYS_INLINE bool hasGrantOption(const AccessFlags & flags) const { return access->hasGrantOption(context, flags); }
    ALWAYS_INLINE bool hasGrantOption(const AccessFlags & flags, std::string_view database) const { return access->hasGrantOption(context, flags, database); }
    ALWAYS_INLINE bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) const { return access->hasGrantOption(context, flags, database, table); }
    ALWAYS_INLINE bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return access->hasGrantOption(context, flags, database, table, column); }
    ALWAYS_INLINE bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return access->hasGrantOption(context, flags, database, table, columns); }
    ALWAYS_INLINE bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return access->hasGrantOption(context, flags, database, table, columns); }
    ALWAYS_INLINE bool hasGrantOption(const AccessRightsElement & element) const { return access->hasGrantOption(context, element); }
    ALWAYS_INLINE bool hasGrantOption(const AccessRightsElements & elements) const { return access->hasGrantOption(context, elements); }

    /// Checks if a specified role is granted with admin option, and throws an exception if not.
    ALWAYS_INLINE void checkAdminOption(const UUID & role_id) const { access->checkAdminOption(context, role_id); }
    ALWAYS_INLINE void checkAdminOption(const UUID & role_id, const String & role_name) const { access->checkAdminOption(context, role_id, role_name); }
    ALWAYS_INLINE void checkAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const { access->checkAdminOption(context, role_id, names_of_roles); }
    ALWAYS_INLINE void checkAdminOption(const std::vector<UUID> & role_ids) const { access->checkAdminOption(context, role_ids); }
    ALWAYS_INLINE void checkAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const { access->checkAdminOption(context, role_ids, names_of_roles); }
    ALWAYS_INLINE void checkAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const { access->checkAdminOption(context, role_ids, names_of_roles); }

    /// Checks if a specified role is granted with admin option, and returns false if not.
    ALWAYS_INLINE bool hasAdminOption(const UUID & role_id) const { return access->hasAdminOption(context, role_id); }
    ALWAYS_INLINE bool hasAdminOption(const UUID & role_id, const String & role_name) const { return access->hasAdminOption(context, role_id, role_name); }
    ALWAYS_INLINE bool hasAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const { return access->hasAdminOption(context, role_id, names_of_roles); }
    ALWAYS_INLINE bool hasAdminOption(const std::vector<UUID> & role_ids) const { return access->hasAdminOption(context, role_ids); }
    ALWAYS_INLINE bool hasAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const { return access->hasAdminOption(context, role_ids, names_of_roles); }
    ALWAYS_INLINE bool hasAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const { return access->hasAdminOption(context, role_ids, names_of_roles); }

    /// Checks if a grantee is allowed for the current user, throws an exception if not.
    ALWAYS_INLINE void checkGranteeIsAllowed(const UUID & grantee_id, const IAccessEntity & grantee) const { access->checkGranteeIsAllowed(grantee_id, grantee); }
    /// Checks if grantees are allowed for the current user, throws an exception if not.
    ALWAYS_INLINE void checkGranteesAreAllowed(const std::vector<UUID> & grantee_ids) const { access->checkGranteesAreAllowed(grantee_ids); }

private:
    ContextAccessPtr access;
    ContextPtr context;
};


}
