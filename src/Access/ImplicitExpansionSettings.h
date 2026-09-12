#pragma once


namespace DB
{

/// The server settings the implicit expansion of access rights depends on
/// (see ContextAccess::addImplicitAccessRights). Read as one snapshot so that a calculation
/// and the cache entry describing it always use the same values.
struct ImplicitExpansionSettings
{
    bool select_from_system_db_requires_grant = false;
    bool select_from_information_schema_requires_grant = false;
    bool user_query_log_enabled = false;
    bool table_engines_require_grant = false;

    friend bool operator ==(const ImplicitExpansionSettings & lhs, const ImplicitExpansionSettings & rhs)
    {
        return (lhs.select_from_system_db_requires_grant == rhs.select_from_system_db_requires_grant)
            && (lhs.select_from_information_schema_requires_grant == rhs.select_from_information_schema_requires_grant)
            && (lhs.user_query_log_enabled == rhs.user_query_log_enabled)
            && (lhs.table_engines_require_grant == rhs.table_engines_require_grant);
    }
};

}
