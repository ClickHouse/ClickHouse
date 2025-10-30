#include <Access/ContextAccess.h>
#include <Access/AccessControl.h>
#include <Access/EnabledRoles.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledSettings.h>
#include <Access/SettingsProfilesInfo.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <cassert>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int UNKNOWN_USER;
    extern const int LOGICAL_ERROR;
}


namespace
{
    const std::vector<std::tuple<AccessFlags, std::string>> source_and_table_engines = {
        {AccessType::FILE, "File"},
        {AccessType::URL, "URL"},
        {AccessType::REMOTE, "Distributed"},
        {AccessType::MONGO, "MongoDB"},
        {AccessType::REDIS, "Redis"},
        {AccessType::MYSQL, "MySQL"},
        {AccessType::POSTGRES, "PostgreSQL"},
        {AccessType::SQLITE, "SQLite"},
        {AccessType::ODBC, "ODBC"},
        {AccessType::JDBC, "JDBC"},
        {AccessType::HDFS, "HDFS"},
        {AccessType::S3, "S3"},
        {AccessType::HIVE, "Hive"},
        {AccessType::AZURE, "AzureBlobStorage"},
        {AccessType::KAFKA, "Kafka"},
        {AccessType::NATS, "NATS"},
        {AccessType::RABBITMQ, "RabbitMQ"}
    };


    AccessRights mixAccessRightsFromUserAndRoles(const User & user, const EnabledRolesInfo & roles_info)
    {
        AccessRights res = user.access;
        res.makeUnion(roles_info.access);
        return res;
    }


    std::array<UUID, 1> to_array(const UUID & id)
    {
        std::array<UUID, 1> ids;
        ids[0] = id;
        return ids;
    }

    /// Helper for using in templates.
    std::string_view getDatabase() { return {}; }

    template <typename... OtherArgs>
    std::string_view getDatabase(std::string_view arg1, const OtherArgs &...) { return arg1; }

    std::string_view getTableEngine() { return {}; }

    template <typename... OtherArgs>
    std::string_view getTableEngine(std::string_view arg1, const OtherArgs &...) { return arg1; }
}


AccessRights ContextAccess::addImplicitAccessRights(const AccessRights & access, const AccessControl & access_control)
{
    AccessFlags max_flags;

    auto modifier = [&](const AccessFlags & flags,
                        const AccessFlags & min_flags_with_children,
                        const AccessFlags & max_flags_with_children,
                        const size_t level,
                        bool /* grant_option */,
                        bool leaf_or_wildcard) -> AccessFlags
    {
        AccessFlags res = flags;

        /// CREATE_TABLE => CREATE_VIEW, DROP_TABLE => DROP_VIEW, ALTER_TABLE => ALTER_VIEW
        static const AccessFlags create_table = AccessType::CREATE_TABLE;
        static const AccessFlags create_view = AccessType::CREATE_VIEW;
        static const AccessFlags drop_table = AccessType::DROP_TABLE;
        static const AccessFlags drop_view = AccessType::DROP_VIEW;
        static const AccessFlags alter_table = AccessType::ALTER_TABLE;
        static const AccessFlags alter_view = AccessType::ALTER_VIEW;

        if (res & create_table)
            res |= create_view;

        if (res & drop_table)
            res |= drop_view;

        if (res & alter_table)
            res |= alter_view;

        /// CREATE TABLE (on any database/table) => CREATE_TEMPORARY_TABLE (global)
        static const AccessFlags create_temporary_table = AccessType::CREATE_TEMPORARY_TABLE;
        if ((level == 0) && (max_flags_with_children & create_table))
            res |= create_temporary_table;

        /// CREATE TABLE (on any database/table) => CREATE_ARBITRARY_TEMPORARY_TABLE (global)
        static const AccessFlags create_arbitrary_temporary_table = AccessType::CREATE_ARBITRARY_TEMPORARY_TABLE;
        if ((level == 0) && (max_flags_with_children & create_table))
            res |= create_arbitrary_temporary_table;

        /// ALTER_TTL => ALTER_MATERIALIZE_TTL
        static const AccessFlags alter_ttl = AccessType::ALTER_TTL;
        static const AccessFlags alter_materialize_ttl = AccessType::ALTER_MATERIALIZE_TTL;
        if (res & alter_ttl)
            res |= alter_materialize_ttl;

        /// RELOAD_DICTIONARY (global) => RELOAD_EMBEDDED_DICTIONARIES (global)
        static const AccessFlags reload_dictionary = AccessType::SYSTEM_RELOAD_DICTIONARY;
        static const AccessFlags reload_embedded_dictionaries = AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES;
        if ((level == 0) && (min_flags_with_children & reload_dictionary))
            res |= reload_embedded_dictionaries;

        /// any column flag => SHOW_COLUMNS => SHOW_TABLES => SHOW_DATABASES
        ///                  any table flag => SHOW_TABLES => SHOW_DATABASES
        ///       any dictionary flag => SHOW_DICTIONARIES => SHOW_DATABASES
        ///                              any database flag => SHOW_DATABASES
        static const AccessFlags show_columns = AccessType::SHOW_COLUMNS;
        static const AccessFlags show_tables = AccessType::SHOW_TABLES;
        static const AccessFlags show_dictionaries = AccessType::SHOW_DICTIONARIES;
        static const AccessFlags show_tables_or_dictionaries = show_tables | show_dictionaries;
        static const AccessFlags show_databases = AccessType::SHOW_DATABASES;

        if (res & AccessFlags::allColumnFlags())
            res |= show_columns;

        if ((res & AccessFlags::allTableFlags())
            || (level <= 2 && (res & show_columns))
            /// GRANT SELECT(x) ON y => GRANT SELECT(x) ON y, GRANT SHOW_TABLES ON y
            || (leaf_or_wildcard && level == 2 && (max_flags_with_children & show_columns)))
        {
            res |= show_tables;
        }

        if (res & AccessFlags::allDictionaryFlags())
            res |= show_dictionaries;

        if ((res & AccessFlags::allDatabaseFlags())
            || (level <= 1 && (res & show_tables_or_dictionaries))
            || (leaf_or_wildcard && level == 1 && (max_flags_with_children & show_tables_or_dictionaries)))
        {
            res |= show_databases;
        }

        static const AccessFlags alter_delete = AccessType::ALTER_DELETE;
        static const AccessFlags select = AccessType::SELECT;
        static const AccessFlags move_partition = AccessType::ALTER_MOVE_PARTITION;
        if ((res & alter_delete) && (res & select) && level <= 2)
            res |= move_partition;

        max_flags |= res;

        return res;
    };

    AccessRights res = access;
    res.modifyFlags(modifier);

    /// If "select_from_system_db_requires_grant" is enabled we provide implicit grants only for a few tables in the system database.
    if (access_control.doesSelectFromSystemDatabaseRequireGrant())
    {
        const char * always_accessible_tables[] = {
            /// Constant tables
            "one",

            /// "numbers", "numbers_mt", "zeros", "zeros_mt" were excluded because they can generate lots of values and
            /// that can decrease performance in some cases.

            "contributors",
            "licenses",
            "time_zones",
            "collations",

            "formats",
            "privileges",
            "data_type_families",
            "database_engines",
            "table_engines",
            "table_functions",
            "aggregate_function_combinators",

            "functions", /// Can contain user-defined functions

            /// The following tables hide some rows if the current user doesn't have corresponding SHOW privileges.
            "databases",
            "tables",
            "columns",

            /// Specific to the current session
            "settings",
            "current_roles",
            "enabled_roles",
            "quota_usage"
        };

        for (const auto * table_name : always_accessible_tables)
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, table_name);

        if (max_flags.contains(AccessType::SHOW_USERS))
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "users");

        if (max_flags.contains(AccessType::SHOW_ROLES))
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "roles");

        if (max_flags.contains(AccessType::SHOW_ROW_POLICIES))
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "row_policies");

        if (max_flags.contains(AccessType::SHOW_SETTINGS_PROFILES))
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "settings_profiles");

        if (max_flags.contains(AccessType::SHOW_QUOTAS))
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "quotas");
    }
    else
    {
        res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE);
    }

    /// If "select_from_information_schema_requires_grant" is enabled we don't provide implicit grants for the information_schema database.
    if (!access_control.doesSelectFromInformationSchemaRequireGrant())
    {
        res.grant(AccessType::SELECT, DatabaseCatalog::INFORMATION_SCHEMA);
        res.grant(AccessType::SELECT, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);
    }

    /// There is overlap between AccessType sources and table engines, so the following code avoids user granting twice.

    /// Sync SOURCE and TABLE_ENGINE, so only need to check TABLE_ENGINE later.
    if (access_control.doesTableEnginesRequireGrant())
    {
        for (const auto & source_and_table_engine : source_and_table_engines)
        {
            const auto & source = std::get<0>(source_and_table_engine);
            if (res.isGranted(source))
            {
                const auto & table_engine = std::get<1>(source_and_table_engine);
                res.grant(AccessType::TABLE_ENGINE, table_engine);
            }
        }
    }
    else
    {
        /// Add TABLE_ENGINE on * and then remove TABLE_ENGINE on particular engines.
        res.grant(AccessType::TABLE_ENGINE);
        for (const auto & source_and_table_engine : source_and_table_engines)
        {
            const auto & source = std::get<0>(source_and_table_engine);
            if (!res.isGranted(source))
            {
                const auto & table_engine = std::get<1>(source_and_table_engine);
                res.revoke(AccessType::TABLE_ENGINE, table_engine);
            }
        }
    }

    return res;
}


std::shared_ptr<const ContextAccess> ContextAccess::fromContext(const ContextPtr & context)
{
    return ContextAccessWrapper::fromContext(context)->getAccess();
}


ContextAccess::ContextAccess(const AccessControl & access_control_, const Params & params_)
    : access_control(&access_control_)
    , params(params_)
{
}


ContextAccess::~ContextAccess() = default;


void ContextAccess::initialize()
{
    std::lock_guard lock{mutex};

    if (params.full_access)
    {
        access = std::make_shared<AccessRights>(AccessRights::getFullAccess());
        access_with_implicit = access;
        return;
    }

    if (!params.user_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No user in current context, it's a bug");

    subscription_for_user_change = access_control->subscribeForChanges(
        *params.user_id,
        [weak_ptr = weak_from_this()](const UUID &, const AccessEntityPtr & entity)
        {
            auto ptr = weak_ptr.lock();
            if (!ptr)
                return;
            UserPtr changed_user = entity ? typeid_cast<UserPtr>(entity) : nullptr;
            std::lock_guard lock2{ptr->mutex};
            ptr->setUser(changed_user);
            if (!ptr->user && !ptr->user_was_dropped)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041, a)");
        });

    setUser(access_control->read<User>(*params.user_id));

    if (!user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041, b)");

    initialized = true;
}


void ContextAccess::setUser(const UserPtr & user_) const
{
    user = user_;

    if (!user_)
    {
        /// User has been dropped.
        user_was_dropped = true;
        subscription_for_user_change = {};
        subscription_for_roles_changes = {};
        access = nullptr;
        access_with_implicit = nullptr;
        enabled_roles = nullptr;
        roles_info = nullptr;
        enabled_row_policies = nullptr;
        row_policies_of_initial_user = nullptr;
        enabled_quota = nullptr;
        enabled_settings = nullptr;
        return;
    }

    user_name = user->getName();
    trace_log = getLogger("ContextAccess (" + user_name + ")");

    std::vector<UUID> current_roles;
    std::vector<UUID> current_roles_with_admin_option;
    if (params.use_default_roles)
    {
        current_roles = user->granted_roles.findGranted(user->default_roles);
        current_roles_with_admin_option = user->granted_roles.findGrantedWithAdminOption(user->default_roles);
    }
    else if (params.current_roles)
    {
        current_roles = user->granted_roles.findGranted(*params.current_roles);
        current_roles_with_admin_option = user->granted_roles.findGrantedWithAdminOption(*params.current_roles);
    }

    if (params.external_roles && !params.external_roles->empty())
    {
        current_roles.insert(current_roles.end(), params.external_roles->begin(), params.external_roles->end());
        auto new_granted_with_admin_option = user->granted_roles.findGrantedWithAdminOption(*params.external_roles);
        current_roles_with_admin_option.insert(current_roles_with_admin_option.end(), new_granted_with_admin_option.begin(), new_granted_with_admin_option.end());
    }

    subscription_for_roles_changes.reset();
    enabled_roles = access_control->getEnabledRoles(current_roles, current_roles_with_admin_option);
    subscription_for_roles_changes = enabled_roles->subscribeForChanges([weak_ptr = weak_from_this()](const std::shared_ptr<const EnabledRolesInfo> & roles_info_)
    {
        auto ptr = weak_ptr.lock();
        if (!ptr)
            return;
        std::lock_guard lock{ptr->mutex};
        ptr->setRolesInfo(roles_info_);
    });

    setRolesInfo(enabled_roles->getRolesInfo());

    std::optional<UUID> initial_user_id;
    if (!params.initial_user.empty())
        initial_user_id = access_control->find<User>(params.initial_user);
    row_policies_of_initial_user = initial_user_id ? access_control->tryGetDefaultRowPolicies(*initial_user_id) : nullptr;
}


void ContextAccess::setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & roles_info_) const
{
    assert(roles_info_);
    roles_info = roles_info_;

    enabled_row_policies = access_control->getEnabledRowPolicies(*params.user_id, roles_info->enabled_roles);

    enabled_settings = access_control->getEnabledSettings(
        *params.user_id, user->settings, roles_info->enabled_roles, roles_info->settings_from_enabled_roles);

    calculateAccessRights();
}


void ContextAccess::calculateAccessRights() const
{
    access = std::make_shared<AccessRights>(mixAccessRightsFromUserAndRoles(*user, *roles_info));
    access_with_implicit = std::make_shared<AccessRights>(addImplicitAccessRights(*access, *access_control));

    if (trace_log)
    {
        if (roles_info && !roles_info->getCurrentRolesNames().empty())
        {
            LOG_TRACE(trace_log, "Current_roles: {}, enabled_roles: {}",
                boost::algorithm::join(roles_info->getCurrentRolesNames(), ", "),
                boost::algorithm::join(roles_info->getEnabledRolesNames(), ", "));
        }
        LOG_TRACE(trace_log, "Settings: readonly = {}, allow_ddl = {}, allow_introspection_functions = {}", params.readonly, params.allow_ddl, params.allow_introspection);
        LOG_TRACE(trace_log, "List of all grants: {}", access->toString());
        LOG_TRACE(trace_log, "List of all grants including implicit: {}", access_with_implicit->toString());
    }
}


UserPtr ContextAccess::getUser() const
{
    auto res = tryGetUser();

    if (likely(res))
        return res;

    if (user_was_dropped)
        throw Exception(ErrorCodes::UNKNOWN_USER, "User has been dropped");

    throw Exception(ErrorCodes::LOGICAL_ERROR, "No user in current context, it's a bug");
}


UserPtr ContextAccess::tryGetUser() const
{
    std::lock_guard lock{mutex};
    return user;
}

String ContextAccess::getUserName() const
{
    std::lock_guard lock{mutex};
    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");
    return user_name;
}

std::shared_ptr<const EnabledRolesInfo> ContextAccess::getRolesInfo() const
{
    std::lock_guard lock{mutex};
    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");
    if (roles_info)
        return roles_info;
    static const auto no_roles = std::make_shared<EnabledRolesInfo>();
    return no_roles;
}

RowPolicyFilterPtr ContextAccess::getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    std::lock_guard lock{mutex};

    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");

    RowPolicyFilterPtr filter;
    if (enabled_row_policies)
        filter = enabled_row_policies->getFilter(database, table_name, filter_type);

    if (row_policies_of_initial_user)
    {
        /// Find and set extra row policies to be used based on `client_info.initial_user`, if the initial user exists.
        /// TODO: we need a better solution here. It seems we should pass the initial row policy
        /// because a shard is allowed to not have the initial user or it might be another user
        /// with the same name.
        filter = row_policies_of_initial_user->getFilter(database, table_name, filter_type, filter);
    }

    return filter;
}

std::shared_ptr<const EnabledQuota> ContextAccess::getQuota() const
{
    std::lock_guard lock{mutex};

    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");

    if (!enabled_quota)
    {
        if (roles_info)
        {
            enabled_quota = access_control->getEnabledQuota(*params.user_id,
                                                            user_name,
                                                            roles_info->enabled_roles,
                                                            params.address,
                                                            params.forwarded_address,
                                                            params.quota_key);
        }
        else
        {
            return nullptr;
        }
    }

    return enabled_quota;
}


std::optional<QuotaUsage> ContextAccess::getQuotaUsage() const
{
    return getQuota()->getUsage();
}

SettingsChanges ContextAccess::getDefaultSettings() const
{
    std::lock_guard lock{mutex};
    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");
    if (enabled_settings)
    {
        if (auto info = enabled_settings->getInfo())
            return info->settings;
    }
    return {};
}


std::shared_ptr<const SettingsProfilesInfo> ContextAccess::getDefaultProfileInfo() const
{
    std::lock_guard lock{mutex};
    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");
    if (enabled_settings)
        return enabled_settings->getInfo();
    static const auto everything_by_default = std::make_shared<SettingsProfilesInfo>(*access_control);
    return everything_by_default;
}


std::shared_ptr<const AccessRights> ContextAccess::getAccessRights() const
{
    std::lock_guard lock{mutex};
    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");
    if (access)
        return access;
    static const auto nothing_granted = std::make_shared<AccessRights>();
    return nothing_granted;
}


std::shared_ptr<const AccessRights> ContextAccess::getAccessRightsWithImplicit() const
{
    std::lock_guard lock{mutex};
    if (initialized && !user && !user_was_dropped)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ContextAccess is inconsistent (bug 55041)");
    if (access_with_implicit)
        return access_with_implicit;
    static const auto nothing_granted = std::make_shared<AccessRights>();
    return nothing_granted;
}


template <bool throw_if_denied, bool grant_option, bool wildcard, typename... Args>
bool ContextAccess::checkAccessImplHelper(const ContextPtr & context, AccessFlags flags, const Args &... args) const
{
    if (user_was_dropped)
    {
        /// If the current user has been dropped we always throw an exception (even if `throw_if_denied` is false)
        /// because dropping of the current user is considered as a situation which is exceptional enough to stop
        /// query execution.
        throw Exception(ErrorCodes::UNKNOWN_USER, "{}: User has been dropped", getUserName());
    }

    if (params.full_access)
        return true;

    auto access_granted = [&]
    {
        if constexpr (throw_if_denied)
            context->addQueryPrivilegesInfo(AccessRightsElement{flags, args...}.toStringWithoutOptions(), true);
        return true;
    };

    auto access_denied = [&]<typename... FmtArgs>(int error_code [[maybe_unused]],
                                               FormatStringHelper<String, FmtArgs...> fmt_string [[maybe_unused]],
                                               FmtArgs && ...fmt_args [[maybe_unused]])
    {
        if constexpr (throw_if_denied)
        {
            context->addQueryPrivilegesInfo(AccessRightsElement{flags, args...}.toStringWithoutOptions(), false);
            throw Exception(error_code, std::move(fmt_string), getUserName(), std::forward<FmtArgs>(fmt_args)...);
        }
        return false;
    };

    if (flags & AccessType::CLUSTER && !access_control->doesOnClusterQueriesRequireClusterGrant())
        flags &= ~AccessType::CLUSTER;

    if (!flags)
        return true;

    const auto parameter_type = flags.getParameterType();
    if (parameter_type == AccessFlags::NONE)
    {
        /// Access to temporary tables is controlled in an unusual way, not like normal tables.
        /// Creating of temporary tables is controlled by AccessType::CREATE_TEMPORARY_TABLES grant,
        /// and other grants are considered as always given.
        /// The DatabaseCatalog class won't resolve StorageID for temporary tables
        /// which shouldn't be accessed.
        if (getDatabase(args...) == DatabaseCatalog::TEMPORARY_DATABASE)
            return access_granted();
    }

    auto acs = getAccessRightsWithImplicit();
    bool granted;
    if constexpr (wildcard)
    {
        if constexpr (grant_option)
            granted = acs->hasGrantOptionWildcard(flags, args...);
        else
            granted = acs->isGrantedWildcard(flags, args...);
    }
    else
    {
        if constexpr (grant_option)
            granted = acs->hasGrantOption(flags, args...);
        else
            granted = acs->isGranted(flags, args...);
    }

    if (!granted)
    {
        auto access_denied_no_grant = [&]<typename... FmtArgs>(AccessFlags access_flags, FmtArgs && ...fmt_args)
        {
            if (grant_option && acs->isGranted(access_flags, fmt_args...))
            {
                return access_denied(ErrorCodes::ACCESS_DENIED,
                    "{}: Not enough privileges. "
                    "The required privileges have been granted, but without grant option. "
                    "To execute this query, it's necessary to have the grant {} WITH GRANT OPTION",
                    AccessRightsElement{access_flags, fmt_args...}.toStringWithoutOptions());
            }

            AccessRights difference;
            difference.grant(flags, fmt_args...);
            AccessRights original_rights = difference;
            difference.makeDifference(*getAccessRights());

            if (difference == original_rights)
            {
                return access_denied(ErrorCodes::ACCESS_DENIED,
                    "{}: Not enough privileges. To execute this query, it's necessary to have the grant {}",
                    AccessRightsElement{access_flags, fmt_args...}.toStringWithoutOptions() + (grant_option ? " WITH GRANT OPTION" : ""));
            }


            return access_denied(ErrorCodes::ACCESS_DENIED,
                "{}: Not enough privileges. To execute this query, it's necessary to have the grant {}. "
                "(Missing permissions: {}){}",
                AccessRightsElement{access_flags, fmt_args...}.toStringWithoutOptions() + (grant_option ? " WITH GRANT OPTION" : ""),
                difference.getElements().toStringWithoutOptions(),
                grant_option ? ". You can try to use the `GRANT CURRENT GRANTS(...)` statement" : "");
        };

        /// As we check the SOURCES from the Table Engine logic, direct prompt about Table Engine would be misleading
        /// since SOURCES is not granted actually. In order to solve this, turn the prompt logic back to Sources.
        if (flags & AccessType::TABLE_ENGINE && !access_control->doesTableEnginesRequireGrant())
        {
            AccessFlags new_flags;

            String table_engine_name{getTableEngine(args...)};
            for (const auto & source_and_table_engine : source_and_table_engines)
            {
                const auto & table_engine = std::get<1>(source_and_table_engine);
                if (table_engine != table_engine_name) continue;
                const auto & source = std::get<0>(source_and_table_engine);
                /// Set the flags from Table Engine to SOURCES so that prompts can be meaningful.
                new_flags = source;
                break;
            }

            /// Might happen in the case of grant Table Engine on A (but not source), then revoke A.
            if (new_flags.isEmpty())
                return access_denied_no_grant(flags, args...);

            return access_denied_no_grant(new_flags);
        }

        return access_denied_no_grant(flags, args...);
    }

    struct PrecalculatedFlags
    {
        const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
            | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
            | AccessType::TRUNCATE;

        const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY;
        const AccessFlags function_ddl = AccessType::CREATE_FUNCTION | AccessType::DROP_FUNCTION;
        const AccessFlags workload_ddl = AccessType::CREATE_WORKLOAD | AccessType::DROP_WORKLOAD;
        const AccessFlags resource_ddl = AccessType::CREATE_RESOURCE | AccessType::DROP_RESOURCE;
        const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
        const AccessFlags table_and_dictionary_and_function_ddl = table_ddl | dictionary_ddl | function_ddl;
        const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;
        const AccessFlags write_dcl_access = AccessType::ACCESS_MANAGEMENT - AccessType::SHOW_ACCESS;

        const AccessFlags not_readonly_flags = write_table_access | table_and_dictionary_and_function_ddl | workload_ddl | resource_ddl | write_dcl_access | AccessType::SYSTEM | AccessType::KILL_QUERY;
        const AccessFlags not_readonly_1_flags = AccessType::CREATE_TEMPORARY_TABLE;

        const AccessFlags ddl_flags = table_ddl | dictionary_ddl | function_ddl | workload_ddl | resource_ddl;
        const AccessFlags introspection_flags = AccessType::INTROSPECTION;
    };
    static const PrecalculatedFlags precalc;

    if (params.readonly)
    {
        if constexpr (grant_option)
            return access_denied(ErrorCodes::READONLY, "{}: Cannot change grants in readonly mode.");
        if ((flags & precalc.not_readonly_flags) ||
            ((params.readonly == 1) && (flags & precalc.not_readonly_1_flags)))
        {
            if (params.interface == ClientInfo::Interface::HTTP && params.http_method == ClientInfo::HTTPMethod::GET)
            {
                return access_denied(ErrorCodes::READONLY,
                    "{}: Cannot execute query in readonly mode. "
                    "For queries over HTTP, method GET implies readonly. "
                    "You should use method POST for modifying queries");
            }
            return access_denied(ErrorCodes::READONLY, "{}: Cannot execute query in readonly mode");
        }
    }

    if (!params.allow_ddl && !grant_option)
    {
        if (flags & precalc.ddl_flags)
            return access_denied(ErrorCodes::QUERY_IS_PROHIBITED,
                                 "Cannot execute query. DDL queries are prohibited for the user {}");
    }

    if (!params.allow_introspection && !grant_option)
    {
        if (flags & precalc.introspection_flags)
            return access_denied(ErrorCodes::FUNCTION_NOT_ALLOWED, "{}: Introspection functions are disabled, "
                                 "because setting 'allow_introspection_functions' is set to 0");
    }

    return access_granted();
}

template <bool throw_if_denied, bool grant_option, bool wildcard>
bool ContextAccess::checkAccessImpl(const ContextPtr & context, const AccessFlags & flags) const
{
    return checkAccessImplHelper<throw_if_denied, grant_option, wildcard>(context, flags);
}

template <bool throw_if_denied, bool grant_option, bool wildcard, typename... Args>
bool ContextAccess::checkAccessImpl(const ContextPtr & context, const AccessFlags & flags, std::string_view database, const Args &... args) const
{
    return checkAccessImplHelper<throw_if_denied, grant_option, wildcard>(context, flags, database.empty() ? params.current_database : database, args...);
}

template <bool throw_if_denied, bool grant_option, bool wildcard>
bool ContextAccess::checkAccessImplHelper(const ContextPtr & context, const AccessRightsElement & element) const
{
    assert(!element.grant_option || grant_option);
    if (element.isGlobalWithParameter())
    {
        if (element.anyParameter())
            return checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element.access_flags);

        return checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element.access_flags, element.parameter);
    }
    if (element.anyDatabase())
        return checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element.access_flags);
    if (element.anyTable())
        return checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element.access_flags, element.database);
    if (element.anyColumn())
        return checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element.access_flags, element.database, element.table);

    return checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element.access_flags, element.database, element.table, element.columns);
}

template <bool throw_if_denied, bool grant_option, bool wildcard>
bool ContextAccess::checkAccessImpl(const ContextPtr & context, const AccessRightsElement & element) const
{
    if (element.wildcard)
    {
        if (element.grant_option)
            return checkAccessImplHelper<throw_if_denied, true, true>(context, element);

        return checkAccessImplHelper<throw_if_denied, grant_option, true>(context, element);
    }

    if (element.grant_option)
        return checkAccessImplHelper<throw_if_denied, true, wildcard>(context, element);

    return checkAccessImplHelper<throw_if_denied, grant_option, wildcard>(context, element);
}

template <bool throw_if_denied, bool grant_option, bool wildcard>
bool ContextAccess::checkAccessImpl(const ContextPtr & context, const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
    {
        if (!checkAccessImpl<throw_if_denied, grant_option, wildcard>(context, element))
            return false;
    }
    return true;
}

bool ContextAccess::isGranted(const ContextPtr & context, const AccessFlags & flags) const { return checkAccessImpl<false, false, false>(context, flags); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const { return checkAccessImpl<false, false, false>(context, flags, database); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const { return checkAccessImpl<false, false, false>(context, flags, database, table); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return checkAccessImpl<false, false, false>(context, flags, database, table, column); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<false, false, false>(context, flags, database, table, columns); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return checkAccessImpl<false, false, false>(context, flags, database, table, columns); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessRightsElement & element) const { return checkAccessImpl<false, false, false>(context, element); }
bool ContextAccess::isGranted(const ContextPtr & context, const AccessRightsElements & elements) const { return checkAccessImpl<false, false, false>(context, elements); }

bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessFlags & flags) const { return checkAccessImpl<false, true, false>(context, flags); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const { return checkAccessImpl<false, true, false>(context, flags, database); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const { return checkAccessImpl<false, true, false>(context, flags, database, table); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return checkAccessImpl<false, true, false>(context, flags, database, table, column); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<false, true, false>(context, flags, database, table, columns); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return checkAccessImpl<false, true, false>(context, flags, database, table, columns); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessRightsElement & element) const { return checkAccessImpl<false, true, false>(context, element); }
bool ContextAccess::hasGrantOption(const ContextPtr & context, const AccessRightsElements & elements) const { return checkAccessImpl<false, true, false>(context, elements); }

void ContextAccess::checkAccess(const ContextPtr & context, const AccessFlags & flags) const { checkAccessImpl<true, false, false>(context, flags); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const { checkAccessImpl<true, false, false>(context, flags, database); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const { checkAccessImpl<true, false, false>(context, flags, database, table); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { checkAccessImpl<true, false, false>(context, flags, database, table, column); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { checkAccessImpl<true, false, false>(context, flags, database, table, columns); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { checkAccessImpl<true, false, false>(context, flags, database, table, columns); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessRightsElement & element) const { checkAccessImpl<true, false, false>(context, element); }
void ContextAccess::checkAccess(const ContextPtr & context, const AccessRightsElements & elements) const { checkAccessImpl<true, false, false>(context, elements); }

void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessFlags & flags) const { checkAccessImpl<true, true, false>(context, flags); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database) const { checkAccessImpl<true, true, false>(context, flags, database); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table) const { checkAccessImpl<true, true, false>(context, flags, database, table); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { checkAccessImpl<true, true, false>(context, flags, database, table, column); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { checkAccessImpl<true, true, false>(context, flags, database, table, columns); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { checkAccessImpl<true, true, false>(context, flags, database, table, columns); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessRightsElement & element) const { checkAccessImpl<true, true, false>(context, element); }
void ContextAccess::checkGrantOption(const ContextPtr & context, const AccessRightsElements & elements) const { checkAccessImpl<true, true, false>(context, elements); }


template <bool throw_if_denied, typename Container, typename GetNameFunction>
bool ContextAccess::checkAdminOptionImplHelper(const ContextPtr & context, const Container & role_ids, const GetNameFunction & get_name_function) const
{
    auto show_error = []<typename... FmtArgs>(int error_code [[maybe_unused]],
                                                  FormatStringHelper<FmtArgs...> fmt_string [[maybe_unused]],
                                                  FmtArgs && ...fmt_args [[maybe_unused]])
    {
        if constexpr (throw_if_denied)
            throw Exception(error_code, std::move(fmt_string), std::forward<FmtArgs>(fmt_args)...);
        return false;
    };

    if (params.full_access)
        return true;

    if (user_was_dropped)
    {
        show_error(ErrorCodes::UNKNOWN_USER, "User has been dropped");
        return false;
    }

    if (!std::size(role_ids))
        return true;

    if (isGranted(context, AccessType::ROLE_ADMIN))
        return true;

    auto info = getRolesInfo();
    size_t i = 0;
    for (auto it = std::begin(role_ids); it != std::end(role_ids); ++it, ++i)
    {
        const UUID & role_id = *it;
        if (info->enabled_roles_with_admin_option.count(role_id))
            continue;

        if (throw_if_denied)
        {
            auto role_name = get_name_function(role_id, i);
            if (!role_name)
                role_name = "ID {" + toString(role_id) + "}";

            if (info->enabled_roles.count(role_id))
                show_error(ErrorCodes::ACCESS_DENIED,
                           "Not enough privileges. "
                           "Role {} is granted, but without ADMIN option. "
                           "To execute this query, it's necessary to have the role {} granted with ADMIN option.",
                           backQuote(*role_name), backQuoteIfNeed(*role_name));
            else
                show_error(ErrorCodes::ACCESS_DENIED, "Not enough privileges. "
                           "To execute this query, it's necessary to have the role {} granted with ADMIN option.",
                           backQuoteIfNeed(*role_name));
        }

        return false;
    }

    return true;
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const ContextPtr & context, const UUID & role_id) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(context, to_array(role_id), [this](const UUID & id, size_t) { return access_control->tryReadName(id); });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const ContextPtr & context, const UUID & role_id, const String & role_name) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(context, to_array(role_id), [&role_name](const UUID &, size_t) { return std::optional<String>{role_name}; });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const ContextPtr & context, const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(context, to_array(role_id), [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const ContextPtr & context, const std::vector<UUID> & role_ids) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(context, role_ids, [this](const UUID & id, size_t) { return access_control->tryReadName(id); });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const ContextPtr & context, const std::vector<UUID> & role_ids, const Strings & names_of_roles) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(context, role_ids, [&names_of_roles](const UUID &, size_t i) { return std::optional<String>{names_of_roles[i]}; });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const ContextPtr & context, const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(context, role_ids, [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

bool ContextAccess::hasAdminOption(const ContextPtr & context, const UUID & role_id) const { return checkAdminOptionImpl<false>(context, role_id); }
bool ContextAccess::hasAdminOption(const ContextPtr & context, const UUID & role_id, const String & role_name) const { return checkAdminOptionImpl<false>(context, role_id, role_name); }
bool ContextAccess::hasAdminOption(const ContextPtr & context, const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const { return checkAdminOptionImpl<false>(context, role_id, names_of_roles); }
bool ContextAccess::hasAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids) const { return checkAdminOptionImpl<false>(context, role_ids); }
bool ContextAccess::hasAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const Strings & names_of_roles) const { return checkAdminOptionImpl<false>(context, role_ids, names_of_roles); }
bool ContextAccess::hasAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const { return checkAdminOptionImpl<false>(context, role_ids, names_of_roles); }

void ContextAccess::checkAdminOption(const ContextPtr & context, const UUID & role_id) const { checkAdminOptionImpl<true>(context, role_id); }
void ContextAccess::checkAdminOption(const ContextPtr & context, const UUID & role_id, const String & role_name) const { checkAdminOptionImpl<true>(context, role_id, role_name); }
void ContextAccess::checkAdminOption(const ContextPtr & context, const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const { checkAdminOptionImpl<true>(context, role_id, names_of_roles); }
void ContextAccess::checkAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids) const { checkAdminOptionImpl<true>(context, role_ids); }
void ContextAccess::checkAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const Strings & names_of_roles) const { checkAdminOptionImpl<true>(context, role_ids, names_of_roles); }
void ContextAccess::checkAdminOption(const ContextPtr & context, const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const { checkAdminOptionImpl<true>(context, role_ids, names_of_roles); }


void ContextAccess::checkGranteeIsAllowed(const UUID & grantee_id, const IAccessEntity & grantee) const
{
    if (params.full_access)
        return;

    auto current_user = getUser();
    if (!current_user->grantees.match(grantee_id))
        throw Exception(ErrorCodes::ACCESS_DENIED, "{} is not allowed as grantee", grantee.formatTypeWithName());
}

void ContextAccess::checkGranteesAreAllowed(const std::vector<UUID> & grantee_ids) const
{
    if (params.full_access)
        return;

    auto current_user = getUser();
    if (current_user->grantees == RolesOrUsersSet::AllTag{})
        return;

    for (const auto & id : grantee_ids)
    {
        auto entity = access_control->tryRead(id);
        if (auto role_entity = typeid_cast<RolePtr>(entity))
            checkGranteeIsAllowed(id, *role_entity);
        else if (auto user_entity = typeid_cast<UserPtr>(entity))
            checkGranteeIsAllowed(id, *user_entity);
    }
}

std::shared_ptr<const ContextAccessWrapper> ContextAccessWrapper::fromContext(const ContextPtr & context)
{
    return context->getAccess();
}


}
