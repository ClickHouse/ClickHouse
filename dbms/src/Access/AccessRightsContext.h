#pragma once

#include <Access/AccessRights.h>
#include <Interpreters/ClientInfo.h>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <mutex>


namespace Poco { class Logger; }

namespace DB
{
struct Settings;


class AccessRightsContext
{
public:
    /// Default constructor creates access rights' context which allows everything.
    AccessRightsContext();

    AccessRightsContext(const ClientInfo & client_info_, const AccessRights & granted_to_user, const Settings & settings, const String & current_database_);

    /// Checks if a specified access granted, and throws an exception if not.
    /// Empty database means the current database.
    void check(const AccessFlags & access) const;
    void check(const AccessFlags & access, const std::string_view & database) const;
    void check(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const;
    void check(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    void check(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    void check(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    void check(const AccessRightsElement & access) const;
    void check(const AccessRightsElements & access) const;

    /// Checks if a specified access granted.
    bool isGranted(const AccessFlags & access) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & access) const;
    bool isGranted(const AccessRightsElements & access) const;

    /// Checks if a specified access granted, and logs a warning if not.
    bool isGranted(Poco::Logger * log_, const AccessFlags & access) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(Poco::Logger * log_, const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(Poco::Logger * log_, const AccessRightsElement & access) const;
    bool isGranted(Poco::Logger * log_, const AccessRightsElements & access) const;

private:
    template <int mode, typename... Args>
    bool checkImpl(Poco::Logger * log_, const AccessFlags & access, const Args &... args) const;

    template <int mode>
    bool checkImpl(Poco::Logger * log_, const AccessRightsElement & access) const;

    template <int mode>
    bool checkImpl(Poco::Logger * log_, const AccessRightsElements & access) const;

    boost::shared_ptr<const AccessRights> calculateResultAccess() const;
    boost::shared_ptr<const AccessRights> calculateResultAccess(UInt64 readonly_, bool allow_ddl_, bool allow_introspection_) const;

    const String user_name;
    const AccessRights granted_to_user;
    const UInt64 readonly = 0;
    const bool allow_ddl = true;
    const bool allow_introspection = true;
    const String current_database;
    const ClientInfo::Interface interface = ClientInfo::Interface::TCP;
    const ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    Poco::Logger * const trace_log = nullptr;
    mutable boost::atomic_shared_ptr<const AccessRights> result_access_cache[4];
    mutable std::mutex mutex;
};

}
