#pragma once

#include <memory>
#include <optional>

#include <Access/AccessRights.h>
#include <Access/ContextAccessParams.h>


namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<const Context>;

class OpenPolicyAgentAccess : public std::enable_shared_from_this<OpenPolicyAgentAccess>
{
public:
    static constexpr size_t max_tries = 1;

    static std::shared_ptr<const OpenPolicyAgentAccess> create(ContextPtr context);

    explicit OpenPolicyAgentAccess(const String & role, const String & url_, const String & service_token_ = {});
    explicit OpenPolicyAgentAccess(const String & role, const String & url_, const std::pair<String, String> & basic_auth_);
    ~OpenPolicyAgentAccess();

    /// Checks if a specified access is granted, and throws an exception if not.
    /// Empty database means the current database.
    void checkAccess(ContextPtr context, const AccessRightsElement & element) const;
    void checkAccess(ContextPtr context, const AccessRightsElements & elements) const;

    // Helpers: convert arguments into AccessRightsElement
    void checkAccess(ContextPtr context, const AccessFlags & flags) const;
    void checkAccess(ContextPtr context, const AccessFlags & flags, std::string_view database) const;
    void checkAccess(ContextPtr context, const AccessFlags & flags, std::string_view database, std::string_view table) const;
    void checkAccess(
        ContextPtr context, const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    void checkAccess(
        ContextPtr context,
        const AccessFlags & flags,
        std::string_view database,
        std::string_view table,
        const std::vector<std::string_view> & columns) const;
    void checkAccess(
        ContextPtr context, const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;

    const String & getRole() const { return role; }

private:
    const String role;
    const String url;
    const String service_token;
    const std::optional<std::pair<String, String>> basic_auth;

    static AccessRightsElement normalizeRightsElement(const AccessRightsElement & element, const String & current_database);
};

}
