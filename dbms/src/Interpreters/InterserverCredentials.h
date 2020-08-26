#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

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

    /// isValidUser implements authentication for InterserverIOHandler
    virtual std::pair<std::string, bool> isValidUser(std::pair<std::string, std::string> pair) = 0;

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

    std::pair<std::string, bool> isValidUser(std::pair<std::string, std::string> pair) override
    {
        std::ignore = pair;
        return {"", true};
    }
};


/// SimpleInterserverCredentials implements authentication using a Store, which
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
class SimpleInterserverCredentials : public virtual BaseInterserverCredentials
{
public:
    using Store = std::vector<std::pair<std::string, std::string>>;

    SimpleInterserverCredentials(const SimpleInterserverCredentials &) = delete;

    static std::shared_ptr<SimpleInterserverCredentials> make(const Poco::Util::AbstractConfiguration & config, const std::string root_tag);

    ~SimpleInterserverCredentials() override { }

    SimpleInterserverCredentials(const std::string current_user_, const std::string current_password_, const Store & store_)
        : BaseInterserverCredentials(current_user_, current_password_), store(std::move(store_))
    {
    }

    std::pair<std::string, bool> isValidUser(std::pair<std::string, std::string> pair) override;

private:
    Store store;

    static Store makeCredentialStore(
        const std::string current_user,
        const std::string current_password,
        const Poco::Util::AbstractConfiguration & config,
        const std::string root_tag);
};

}
