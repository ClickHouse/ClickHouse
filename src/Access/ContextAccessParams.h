#pragma once

#include <Interpreters/ClientInfo.h>
#include <Core/UUID.h>
#include <optional>
#include <vector>


namespace DB
{
struct Settings;

/// Parameters which are used to calculate access rights and some related stuff like roles or constraints.
class ContextAccessParams
{
public:
    ContextAccessParams(
        std::optional<UUID> user_id_,
        bool full_access_,
        bool use_default_roles_,
        const std::shared_ptr<const std::vector<UUID>> & current_roles_,
        const Settings & settings_,
        const String & current_database_,
        const ClientInfo & client_info_);

    const std::optional<UUID> user_id;

    /// Full access to everything without any limitations.
    /// This is used for the global context.
    const bool full_access;

    const bool use_default_roles;
    const std::shared_ptr<const std::vector<UUID>> current_roles;

    const UInt64 readonly;
    const bool allow_ddl;
    const bool allow_introspection;

    const String current_database;

    const ClientInfo::Interface interface;
    const ClientInfo::HTTPMethod http_method;
    const Poco::Net::IPAddress address;

    /// The last entry from comma separated list of X-Forwarded-For addresses.
    /// Only the last proxy can be trusted (if any).
    const String forwarded_address;

    const String quota_key;

    /// Initial user is used to combine row policies with.
    const String initial_user;

    /// Outputs `ContextAccessParams` to string for logging.
    String toString() const;

    friend bool operator <(const ContextAccessParams & left, const ContextAccessParams & right);
    friend bool operator ==(const ContextAccessParams & left, const ContextAccessParams & right);
    friend bool operator !=(const ContextAccessParams & left, const ContextAccessParams & right) { return !(left == right); }
    friend bool operator >(const ContextAccessParams & left, const ContextAccessParams & right) { return right < left; }
    friend bool operator <=(const ContextAccessParams & left, const ContextAccessParams & right) { return !(right < left); }
    friend bool operator >=(const ContextAccessParams & left, const ContextAccessParams & right) { return !(left < right); }

    static bool dependsOnSettingName(std::string_view setting_name);
};

}
