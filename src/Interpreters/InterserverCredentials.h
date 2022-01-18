#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <unordered_set>

namespace DB
{

/// InterserverCredentials implements authentication using a CurrentCredentials, which
/// is configured, e.g.
///    <interserver_http_credentials>
///        <user>admin</user>
///        <password>222</password>
///        <!-- To support mix of un/authenticated clients -->
///        <!-- <allow_empty>true</allow_empty> -->
///        <old>
///            <!-- Allow authentication using previous passwords during rotation -->
///            <user>admin</user>
///            <password>qqq</password>
///        </old>
///        <old>
///            <!-- Allow authentication using previous users during rotation -->
///            <user>johny</user>
///            <password>333</password>
///        </old>
///    </interserver_http_credentials>
class InterserverCredentials
{
public:
    using UserWithPassword = std::pair<std::string, std::string>;
    using CheckResult = std::pair<std::string, bool>;
    using CurrentCredentials = std::vector<UserWithPassword>;

    InterserverCredentials(const InterserverCredentials &) = delete;

    static std::unique_ptr<InterserverCredentials> make(const Poco::Util::AbstractConfiguration & config, const std::string & root_tag);

    InterserverCredentials(const std::string & current_user_, const std::string & current_password_, const CurrentCredentials & all_users_store_)
        : current_user(current_user_)
        , current_password(current_password_)
        , all_users_store(all_users_store_)
    {}

    CheckResult isValidUser(const UserWithPassword & credentials) const;
    CheckResult isValidUser(const std::string & user, const std::string & password) const;

    std::string getUser() const { return current_user; }

    std::string getPassword() const { return current_password; }


private:
    std::string current_user;
    std::string current_password;

    /// In common situation this store contains one record
    CurrentCredentials all_users_store;

    static CurrentCredentials parseCredentialsFromConfig(
        const std::string & current_user_,
        const std::string & current_password_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & root_tag);
};

using InterserverCredentialsPtr = std::shared_ptr<const InterserverCredentials>;

}
