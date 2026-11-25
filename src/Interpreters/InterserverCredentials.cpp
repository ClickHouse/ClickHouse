#include <Interpreters/InterserverCredentials.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/StringUtils.h>
#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#    include <openssl/ssl.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
std::pair<std::string, std::string> parseClientCertificateFiles(
    const Poco::Util::AbstractConfiguration & config, const std::string & root_tag)
{
    const auto certificate_path = root_tag + ".openSSL.client.certificateFile";
    const auto private_key_path = root_tag + ".openSSL.client.privateKeyFile";
    const bool has_certificate = config.has(certificate_path);
    const bool has_private_key = config.has(private_key_path);
    if (has_certificate != has_private_key)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Interserver client certificate and private key must be configured together");

    if (!has_certificate)
        return {};

    auto certificate_file = config.getString(certificate_path);
    auto private_key_file = config.getString(private_key_path);
    if (certificate_file.empty() || private_key_file.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interserver client certificate and private key paths must not be empty");
    return {std::move(certificate_file), std::move(private_key_file)};
}
}

std::unique_ptr<InterserverCredentials>
InterserverCredentials::make(const Poco::Util::AbstractConfiguration & config, const std::string & root_tag)
{
    /// Preserve validation behavior for configurations that predate certificate authentication.
    if (config.has("user") && !config.has("password"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Configuration parameter interserver_http_credentials.password can't be empty");

    if (!config.has("user") && config.has("password"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                        "Configuration parameter interserver_http_credentials.user can't be empty if user specified");

    /// They both can be empty
    auto user = config.getString(root_tag + ".user", "");
    SecretString password(config.getString(root_tag + ".password", ""));

    auto [client_certificate_file, client_private_key_file] = parseClientCertificateFiles(config, root_tag);
    const std::string subjects_path = root_tag + ".ssl_certificates";
#if USE_SSL
    X509Certificate::Subjects allowed_subjects;
    if (config.has(subjects_path))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(subjects_path, keys);
        for (const auto & key : keys)
        {
            const auto value = config.getString(subjects_path + "." + key);
            if (value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interserver certificate subjects must not be empty");
            if (key.starts_with("common_name"))
                allowed_subjects.insert(X509Certificate::Subjects::Type::CN, String(value));
            else if (key.starts_with("subject_alt_name"))
                allowed_subjects.insert(X509Certificate::Subjects::Type::SAN, String(value));
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown interserver certificate subject type: {}", key);
        }
        if (allowed_subjects.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "interserver_http_credentials.ssl_certificates requires at least one subject");
    }
#else
    if (config.has(subjects_path) || !client_certificate_file.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Interserver certificate authentication requires SSL support");
    const bool has_allowed_subjects = false;
#endif

    auto store = parseCredentialsFromConfig(user, password, config, root_tag,
#if USE_SSL
        !allowed_subjects.empty()
#else
        has_allowed_subjects
#endif
    );

    return std::make_unique<InterserverCredentials>(
        std::move(user), std::move(password), std::move(store)
#if USE_SSL
        , std::move(client_certificate_file), std::move(client_private_key_file), std::move(allowed_subjects)
#endif
    );
}

#if USE_SSL
void InterserverCredentials::initializeClientTLSContext(const Poco::Util::AbstractConfiguration & config, const std::string & root_tag)
{
    if (!usesClientCertificateAuthentication())
        return;

    using Poco::Net::Context;
    using Poco::Net::SSLManager;
    const std::string prefix = SSLManager::CFG_CLIENT_PREFIX;
    const auto setting = [&](const std::string & suffix)
    {
        const auto local = root_tag + "." + prefix + suffix;
        return config.hasProperty(local) ? local : prefix + suffix;
    };

    /// Initialize the SSL manager's certificate handler before creating a separate context.
    SSLManager::instance().defaultClientContext();

    Context::Params params;
    params.privateKeyFile = client_private_key_file;
    params.certificateFile = client_certificate_file;
    params.caLocation = config.getString(setting(SSLManager::CFG_CA_LOCATION), "");
    params.verificationMode = SSLManager::VAL_VER_MODE;
    if (config.hasProperty(setting(SSLManager::CFG_VER_MODE)))
        params.verificationMode = Poco::Net::Utility::convertVerificationMode(config.getString(setting(SSLManager::CFG_VER_MODE)));
    params.verificationDepth = config.getInt(setting(SSLManager::CFG_VER_DEPTH), SSLManager::VAL_VER_DEPTH);
    params.loadDefaultCAs = config.getBool(setting(SSLManager::CFG_ENABLE_DEFAULT_CA), SSLManager::VAL_ENABLE_DEFAULT_CA);
    params.cipherList = config.getString(setting(SSLManager::CFG_CIPHER_LIST), SSLManager::VAL_CIPHER_LIST);
    params.cipherList = config.getString(setting(SSLManager::CFG_CYPHER_LIST), params.cipherList);
    params.cipherSuites = config.getString(setting(SSLManager::CFG_CIPHER_SUITES), "");
    params.dhParamsFile = config.getString(setting(SSLManager::CFG_DH_PARAMS_FILE), "");
    params.ecdhCurve = config.getString(setting(SSLManager::CFG_ECDH_CURVE), "");

    Context::Usage usage = Context::CLIENT_USE;
    if (config.getBool(setting(SSLManager::CFG_REQUIRE_TLSV1_2), false))
        usage = Context::TLSV1_2_CLIENT_USE;
    else if (config.getBool(setting(SSLManager::CFG_REQUIRE_TLSV1_1), false))
        usage = Context::TLSV1_1_CLIENT_USE;
    else if (config.getBool(setting(SSLManager::CFG_REQUIRE_TLSV1), false))
        usage = Context::TLSV1_CLIENT_USE;

    auto context = Context::Ptr(new Context(usage, params));
    if (SSL_CTX_check_private_key(context->sslContext()) != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interserver client certificate does not match its private key");

    const auto disabled_list = config.getString(setting(SSLManager::CFG_DISABLE_PROTOCOLS), "");
    Poco::StringTokenizer disabled_protocols(
        disabled_list, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
    int disabled = 0;
    for (const auto & protocol : disabled_protocols)
    {
        if (protocol == "sslv2")
            disabled |= Context::PROTO_SSLV2;
        else if (protocol == "sslv3")
            disabled |= Context::PROTO_SSLV3;
        else if (protocol == "tlsv1")
            disabled |= Context::PROTO_TLSV1;
        else if (protocol == "tlsv1_1")
            disabled |= Context::PROTO_TLSV1_1;
        else if (protocol == "tlsv1_2")
            disabled |= Context::PROTO_TLSV1_2;
        else if (protocol == "tlsv1_3")
            disabled |= Context::PROTO_TLSV1_3;
    }
    context->disableProtocols(disabled);
    context->enableSessionCache(config.getBool(setting(SSLManager::CFG_CACHE_SESSIONS), false));
    context->enableExtendedCertificateVerification(config.getBool(setting(SSLManager::CFG_EXTENDED_VERIFICATION), true));
    if (config.getBool(setting(SSLManager::CFG_PREFER_SERVER_CIPHERS), false))
        context->preferServerCiphers();

    client_tls_context.swap(context);
}
#endif

InterserverCredentials::CurrentCredentials InterserverCredentials::parseCredentialsFromConfig(
    const std::string & current_user_,
    const SecretString & current_password_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & root_tag,
    bool has_allowed_subjects)
{
    auto log = getLogger("InterserverCredentials");
    CurrentCredentials store;
    if (config.has(root_tag + ".user") || config.has(root_tag + ".password") || !has_allowed_subjects)
        store.emplace_back(current_user_, current_password_);
    if (config.getBool(root_tag + ".allow_empty", false))
    {
        LOG_DEBUG(log, "Allowing empty credentials");
        /// Allow empty credential to support migrating from no auth
        store.emplace_back("", "");
    }

    Poco::Util::AbstractConfiguration::Keys old_users;
    config.keys(root_tag, old_users);

    for (const auto & user_key : old_users)
    {
        if (startsWith(user_key, "old"))
        {
            std::string full_prefix = root_tag + "." + user_key;
            std::string old_user_name = config.getString(full_prefix + ".user");
            LOG_DEBUG(log, "Adding credentials for old user {}", old_user_name);

            SecretString old_user_password(config.getString(full_prefix + ".password"));

            store.emplace_back(old_user_name, old_user_password);
        }
    }

    return store;
}

InterserverCredentials::CheckResult InterserverCredentials::isValidUser(const UserWithPassword & credentials) const
{
    auto itr = std::find(all_users_store.begin(), all_users_store.end(), credentials);

    if (itr == all_users_store.end())
    {
        if (all_users_store.empty())
            return {"Server requires a matching SSL certificate", false};

        if (credentials.first.empty())
            return {"Server requires HTTP Basic authentication, but client doesn't provide it", false};

        return {"Incorrect user or password in HTTP basic authentication: " + credentials.first, false};
    }

    return {"", true};
}

InterserverCredentials::CheckResult InterserverCredentials::isValidUser(std::string_view user, std::string_view password) const
{
    return isValidUser(UserWithPassword(std::string(user), SecretString(password)));
}

}
