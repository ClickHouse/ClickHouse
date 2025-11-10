#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <Poco/Net/Context.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Logger.h>
namespace DB
{

enum class HashiCorpVaultAuthMethod
{
    Token,
    Userpass,
    Cert
};

class HashiCorpVault
{
public:
    HashiCorpVault()
    {
        log = getLogger("HashiCorpVault");
        reset();
    }

    static HashiCorpVault & instance();

    /// Load data and throw exception if something went wrong.
    void load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context_);

    String readSecret(const String & secret, const String & key);

    bool isLoaded() const { return loaded; }

private:
    void reset()
    {
        url = "";
        token = "";
        username = "";
        password = "";
        client_token = "";
        cert_name = "";
        request_context = nullptr;
        loaded = false;
    }
    void initRequestContext(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    String makeRequest(const String & method, const String & path, const String & request_token, const String & body);
    String login();
    LoggerPtr log;
    bool loaded = false;
    String url;
    String token;
    String username;
    String password;
    String client_token;
    String cert_name;
    String scheme;
    String host;
    int port;
    HashiCorpVaultAuthMethod auth_method;
    ContextPtr context;
    Poco::Net::Context::Ptr request_context;
};

}
