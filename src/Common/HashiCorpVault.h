#pragma once

#include <base/types.h>

#if USE_SSL
#    include <Poco/Net/Context.h>
#endif

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
    void load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

    String readSecret(const String & secret, const String & key);

    bool isLoaded() const { return loaded; }

private:
    void reset()
    {
        loaded = false;
        url = "";
        token = "";
        username = "";
        password = "";
        cert_name = "";
        port = 0;
        secret_path = "secret";
        kv_api_version = 2;
        auth_method = HashiCorpVaultAuthMethod::Token;
#if USE_SSL
        request_context = nullptr;
#endif
    }
#if USE_SSL
    void initRequestContext(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
#endif
    String makeRequest(const String & method, const String & path, const String & request_token, const String & body);
    String login();
    LoggerPtr log;
    bool loaded = false;
    String url;
    String token;
    String username;
    String password;
    String cert_name;
    String scheme;
    String host;
    int port = 0;
    String secret_path = "secret";
    int kv_api_version = 2;
    HashiCorpVaultAuthMethod auth_method = HashiCorpVaultAuthMethod::Token;
#if USE_SSL
    Poco::Net::Context::Ptr request_context;
#endif
};

}
