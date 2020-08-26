#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
/// InterserverCredentials holds credentials for server (store) and client
/// credentials (current_*). The container is constructed through `make` and a
/// shared_ptr is captured inside Context.
class BaseInterserverCredentials
{
public:
    BaseInterserverCredentials(std::string current_user_, std::string current_password_)
        : current_user(current_user_), current_password(current_password_)
            { }

    virtual ~BaseInterserverCredentials() { }

    /// isValidUser returns true or throws WRONG_PASSWORD
    virtual bool isValidUser(const std::pair<std::string, std::string> credentials) = 0;

    std::string getUser() { return current_user; }

    std::string getPassword() { return current_password; }


protected:
    std::string current_user;
    std::string current_password;
};


/// NullInterserverCredentials are used when authentication is not configured
class NullInterserverCredentials : public virtual BaseInterserverCredentials
{
public:
    NullInterserverCredentials(const NullInterserverCredentials &) = delete;
    NullInterserverCredentials() : BaseInterserverCredentials("", "") { }

    ~NullInterserverCredentials() override { }

    static std::shared_ptr<NullInterserverCredentials> make() { return std::make_shared<NullInterserverCredentials>(); }

    bool isValidUser(const std::pair<std::string, std::string> credentials) override
    {
        std::ignore = credentials;
        return true;
    }
};


/// ConfigInterserverCredentials implements authentication using a Store, which
/// is configured, e.g.
///    <interserver_http_credentials>
///        <user>admin</user>
///        <password>222</password>
///        <!-- To support mix of un/authenticated clients -->
///        <!-- <allow_empty>true</allow_empty> -->
///        <users>
///            <!-- Allow authentication using previous passwords during rotation -->
///            <admin>111</admin>
///        </users>
///    </interserver_http_credentials>
class ConfigInterserverCredentials : public virtual BaseInterserverCredentials
{
public:
    using Store = std::map<std::pair<std::string, std::string>, bool>;

    ConfigInterserverCredentials(const ConfigInterserverCredentials &) = delete;

    static std::shared_ptr<ConfigInterserverCredentials> make(const Poco::Util::AbstractConfiguration & config, const std::string root_tag);

    ~ConfigInterserverCredentials() override { }

    ConfigInterserverCredentials(const std::string current_user_, const std::string current_password_, const Store & store_)
        : BaseInterserverCredentials(current_user_, current_password_), store(std::move(store_))
    {
    }

    bool isValidUser(const std::pair<std::string, std::string> credentials) override;

private:
    Store store;

    static Store makeCredentialStore(
        const std::string current_user_,
        const std::string current_password_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string root_tag);
};

}
