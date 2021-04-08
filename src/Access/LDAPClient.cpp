#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <ext/scope_guard.h>

#include <mutex>

#include <cstring>

#include <sys/time.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int LDAP_ERROR;
}

LDAPClient::LDAPClient(const LDAPServerParams & params_)
    : params(params_)
{
}

LDAPClient::~LDAPClient()
{
    closeConnection();
}

#if USE_LDAP

namespace
{

    std::recursive_mutex ldap_global_mutex;

    auto escapeForLDAP(const String & src)
    {
        String dest;
        dest.reserve(src.size() * 2);

        for (auto ch : src)
        {
            switch (ch)
            {
                case ',':
                case '\\':
                case '#':
                case '+':
                case '<':
                case '>':
                case ';':
                case '"':
                case '=':
                    dest += '\\';
                    break;
            }
            dest += ch;
        }

        return dest;
    }

}

void LDAPClient::diag(const int rc)
{
    std::scoped_lock lock(ldap_global_mutex);

    if (rc != LDAP_SUCCESS)
    {
        String text;
        const char * raw_err_str = ldap_err2string(rc);

        if (raw_err_str)
            text = raw_err_str;

        if (handle)
        {
            String message;
            char * raw_message = nullptr;
            ldap_get_option(handle, LDAP_OPT_DIAGNOSTIC_MESSAGE, &raw_message);

            if (raw_message)
            {
                message = raw_message;
                ldap_memfree(raw_message);
                raw_message = nullptr;
            }

            if (!message.empty())
            {
                if (!text.empty())
                    text += ": ";
                text += message;
            }
        }

        throw Exception(text, ErrorCodes::LDAP_ERROR);
    }
}

void LDAPClient::openConnection()
{
    std::scoped_lock lock(ldap_global_mutex);

    closeConnection();

    {
        LDAPURLDesc url;
        std::memset(&url, 0, sizeof(url));

        url.lud_scheme = const_cast<char *>(params.enable_tls == LDAPServerParams::TLSEnable::YES ? "ldaps" : "ldap");
        url.lud_host = const_cast<char *>(params.host.c_str());
        url.lud_port = params.port;
        url.lud_scope = LDAP_SCOPE_DEFAULT;

        auto * uri = ldap_url_desc2str(&url);
        if (!uri)
            throw Exception("ldap_url_desc2str() failed", ErrorCodes::LDAP_ERROR);

        SCOPE_EXIT({ ldap_memfree(uri); });

        diag(ldap_initialize(&handle, uri));
        if (!handle)
            throw Exception("ldap_initialize() failed", ErrorCodes::LDAP_ERROR);
    }

    {
        int value = 0;
        switch (params.protocol_version)
        {
            case LDAPServerParams::ProtocolVersion::V2: value = LDAP_VERSION2; break;
            case LDAPServerParams::ProtocolVersion::V3: value = LDAP_VERSION3; break;
        }
        diag(ldap_set_option(handle, LDAP_OPT_PROTOCOL_VERSION, &value));
    }

    diag(ldap_set_option(handle, LDAP_OPT_RESTART, LDAP_OPT_ON));

#ifdef LDAP_OPT_KEEPCONN
    diag(ldap_set_option(handle, LDAP_OPT_KEEPCONN, LDAP_OPT_ON));
#endif

#ifdef LDAP_OPT_TIMEOUT
    {
        ::timeval operation_timeout;
        operation_timeout.tv_sec = params.operation_timeout.count();
        operation_timeout.tv_usec = 0;
        diag(ldap_set_option(handle, LDAP_OPT_TIMEOUT, &operation_timeout));
    }
#endif

#ifdef LDAP_OPT_NETWORK_TIMEOUT
    {
        ::timeval network_timeout;
        network_timeout.tv_sec = params.network_timeout.count();
        network_timeout.tv_usec = 0;
        diag(ldap_set_option(handle, LDAP_OPT_NETWORK_TIMEOUT, &network_timeout));
    }
#endif

    {
        const int search_timeout = params.search_timeout.count();
        diag(ldap_set_option(handle, LDAP_OPT_TIMELIMIT, &search_timeout));
    }

    {
        const int size_limit = params.search_limit;
        diag(ldap_set_option(handle, LDAP_OPT_SIZELIMIT, &size_limit));
    }

#ifdef LDAP_OPT_X_TLS_PROTOCOL_MIN
    {
        int value = 0;
        switch (params.tls_minimum_protocol_version)
        {
            case LDAPServerParams::TLSProtocolVersion::SSL2:   value = LDAP_OPT_X_TLS_PROTOCOL_SSL2;   break;
            case LDAPServerParams::TLSProtocolVersion::SSL3:   value = LDAP_OPT_X_TLS_PROTOCOL_SSL3;   break;
            case LDAPServerParams::TLSProtocolVersion::TLS1_0: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_0; break;
            case LDAPServerParams::TLSProtocolVersion::TLS1_1: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_1; break;
            case LDAPServerParams::TLSProtocolVersion::TLS1_2: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_2; break;
        }
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_PROTOCOL_MIN, &value));
    }
#endif

#ifdef LDAP_OPT_X_TLS_REQUIRE_CERT
    {
        int value = 0;
        switch (params.tls_require_cert)
        {
            case LDAPServerParams::TLSRequireCert::NEVER:  value = LDAP_OPT_X_TLS_NEVER;  break;
            case LDAPServerParams::TLSRequireCert::ALLOW:  value = LDAP_OPT_X_TLS_ALLOW;  break;
            case LDAPServerParams::TLSRequireCert::TRY:    value = LDAP_OPT_X_TLS_TRY;    break;
            case LDAPServerParams::TLSRequireCert::DEMAND: value = LDAP_OPT_X_TLS_DEMAND; break;
        }
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_REQUIRE_CERT, &value));
    }
#endif

#ifdef LDAP_OPT_X_TLS_CERTFILE
    if (!params.tls_cert_file.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_CERTFILE, params.tls_cert_file.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_KEYFILE
    if (!params.tls_key_file.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_KEYFILE, params.tls_key_file.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_CACERTFILE
    if (!params.tls_ca_cert_file.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTFILE, params.tls_ca_cert_file.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_CACERTDIR
    if (!params.tls_ca_cert_dir.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTDIR, params.tls_ca_cert_dir.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_CIPHER_SUITE
    if (!params.tls_cipher_suite.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_CIPHER_SUITE, params.tls_cipher_suite.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_NEWCTX
    {
        const int i_am_a_server = 0;
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_NEWCTX, &i_am_a_server));
    }
#endif

    if (params.enable_tls == LDAPServerParams::TLSEnable::YES_STARTTLS)
        diag(ldap_start_tls_s(handle, nullptr, nullptr));

    switch (params.sasl_mechanism)
    {
        case LDAPServerParams::SASLMechanism::SIMPLE:
        {
            const String dn = params.auth_dn_prefix + escapeForLDAP(params.user) + params.auth_dn_suffix;

            ::berval cred;
            cred.bv_val = const_cast<char *>(params.password.c_str());
            cred.bv_len = params.password.size();

            diag(ldap_sasl_bind_s(handle, dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr));

            break;
        }
        default:
        {
            throw Exception("Unknown SASL mechanism", ErrorCodes::LDAP_ERROR);
        }
    }
}

void LDAPClient::closeConnection() noexcept
{
    std::scoped_lock lock(ldap_global_mutex);

    if (!handle)
        return;

    ldap_unbind_ext_s(handle, nullptr, nullptr);
    handle = nullptr;
}

bool LDAPSimpleAuthClient::check()
{
    std::scoped_lock lock(ldap_global_mutex);

    if (params.user.empty())
        throw Exception("LDAP authentication of a user with empty name is not allowed", ErrorCodes::BAD_ARGUMENTS);

    // Silently reject authentication attempt if the password is empty as if it didn't match.
    if (params.password.empty())
        return false;

    SCOPE_EXIT({ closeConnection(); });

    // Will throw on any error, including invalid credentials.
    openConnection();

    return true;
}

#else // USE_LDAP

void LDAPClient::diag(const int)
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

void LDAPClient::openConnection()
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

void LDAPClient::closeConnection() noexcept
{
}

bool LDAPSimpleAuthClient::check()
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

#endif // USE_LDAP

}
