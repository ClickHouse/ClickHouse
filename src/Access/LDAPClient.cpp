#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <ext/scope_guard.h>
#include <common/logger_useful.h>

#include <Poco/Logger.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/container_hash/hash.hpp>

#include <mutex>
#include <utility>
#include <vector>

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

void LDAPClient::SearchParams::combineHash(std::size_t & seed) const
{
    boost::hash_combine(seed, base_dn);
    boost::hash_combine(seed, static_cast<int>(scope));
    boost::hash_combine(seed, search_filter);
    boost::hash_combine(seed, attribute);
    boost::hash_combine(seed, prefix);
}

void LDAPClient::Params::combineCoreHash(std::size_t & seed) const
{
    boost::hash_combine(seed, host);
    boost::hash_combine(seed, port);
    boost::hash_combine(seed, bind_dn);
    boost::hash_combine(seed, user);
    boost::hash_combine(seed, password);
}

LDAPClient::LDAPClient(const Params & params_)
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

    auto replacePlaceholders(const String & src, const std::vector<std::pair<String, String>> & pairs)
    {
        String dest = src;

        for (const auto & pair : pairs)
        {
            const auto & placeholder = pair.first;
            const auto & value = pair.second;
            for (
                 auto pos = dest.find(placeholder);
                 pos != std::string::npos;
                 pos = dest.find(placeholder, pos)
            )
            {
                dest.replace(pos, placeholder.size(), value);
                pos += value.size();
            }
        }

        return dest;
    }

}

void LDAPClient::diag(const int rc, String text)
{
    std::scoped_lock lock(ldap_global_mutex);

    if (rc != LDAP_SUCCESS)
    {
        const char * raw_err_str = ldap_err2string(rc);
        if (raw_err_str && *raw_err_str != '\0')
        {
            if (!text.empty())
                text += ": ";
            text += raw_err_str;
        }

        if (handle)
        {
            char * raw_message = nullptr;

            SCOPE_EXIT({
                if (raw_message)
                {
                    ldap_memfree(raw_message);
                    raw_message = nullptr;
                }
            });

            ldap_get_option(handle, LDAP_OPT_DIAGNOSTIC_MESSAGE, &raw_message);

            if (raw_message && *raw_message != '\0')
            {
                if (!text.empty())
                    text += ": ";
                text += raw_message;
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

        url.lud_scheme = const_cast<char *>(params.enable_tls == LDAPClient::Params::TLSEnable::YES ? "ldaps" : "ldap");
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
            case LDAPClient::Params::ProtocolVersion::V2: value = LDAP_VERSION2; break;
            case LDAPClient::Params::ProtocolVersion::V3: value = LDAP_VERSION3; break;
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
            case LDAPClient::Params::TLSProtocolVersion::SSL2:   value = LDAP_OPT_X_TLS_PROTOCOL_SSL2;   break;
            case LDAPClient::Params::TLSProtocolVersion::SSL3:   value = LDAP_OPT_X_TLS_PROTOCOL_SSL3;   break;
            case LDAPClient::Params::TLSProtocolVersion::TLS1_0: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_0; break;
            case LDAPClient::Params::TLSProtocolVersion::TLS1_1: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_1; break;
            case LDAPClient::Params::TLSProtocolVersion::TLS1_2: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_2; break;
        }
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_PROTOCOL_MIN, &value));
    }
#endif

#ifdef LDAP_OPT_X_TLS_REQUIRE_CERT
    {
        int value = 0;
        switch (params.tls_require_cert)
        {
            case LDAPClient::Params::TLSRequireCert::NEVER:  value = LDAP_OPT_X_TLS_NEVER;  break;
            case LDAPClient::Params::TLSRequireCert::ALLOW:  value = LDAP_OPT_X_TLS_ALLOW;  break;
            case LDAPClient::Params::TLSRequireCert::TRY:    value = LDAP_OPT_X_TLS_TRY;    break;
            case LDAPClient::Params::TLSRequireCert::DEMAND: value = LDAP_OPT_X_TLS_DEMAND; break;
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

    if (params.enable_tls == LDAPClient::Params::TLSEnable::YES_STARTTLS)
        diag(ldap_start_tls_s(handle, nullptr, nullptr));

    switch (params.sasl_mechanism)
    {
        case LDAPClient::Params::SASLMechanism::SIMPLE:
        {
            const auto escaped_user_name = escapeForLDAP(params.user);
            const auto bind_dn = replacePlaceholders(params.bind_dn, { {"{user_name}", escaped_user_name} });

            ::berval cred;
            cred.bv_val = const_cast<char *>(params.password.c_str());
            cred.bv_len = params.password.size();

            diag(ldap_sasl_bind_s(handle, bind_dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr));

            break;
        }

        default:
            throw Exception("Unknown SASL mechanism", ErrorCodes::LDAP_ERROR);
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

LDAPClient::SearchResults LDAPClient::search(const SearchParams & search_params)
{
    std::scoped_lock lock(ldap_global_mutex);

    SearchResults result;

    int scope = 0;
    switch (search_params.scope)
    {
        case SearchParams::Scope::BASE:      scope = LDAP_SCOPE_BASE;     break;
        case SearchParams::Scope::ONE_LEVEL: scope = LDAP_SCOPE_ONELEVEL; break;
        case SearchParams::Scope::SUBTREE:   scope = LDAP_SCOPE_SUBTREE;  break;
        case SearchParams::Scope::CHILDREN:  scope = LDAP_SCOPE_CHILDREN; break;
    }

    const auto escaped_user_name = escapeForLDAP(params.user);
    const auto bind_dn = replacePlaceholders(params.bind_dn, { {"{user_name}", escaped_user_name} });
    const auto base_dn = replacePlaceholders(search_params.base_dn, { {"{user_name}", escaped_user_name}, {"{bind_dn}", bind_dn} });
    const auto search_filter = replacePlaceholders(search_params.search_filter, { {"{user_name}", escaped_user_name}, {"{bind_dn}", bind_dn}, {"{base_dn}", base_dn} });
    char * attrs[] = { const_cast<char *>(search_params.attribute.c_str()), nullptr };
    ::timeval timeout = { params.search_timeout.count(), 0 };
    LDAPMessage* msgs = nullptr;

    SCOPE_EXIT({
        if (msgs)
        {
            ldap_msgfree(msgs);
            msgs = nullptr;
        }
    });

    diag(ldap_search_ext_s(handle, base_dn.c_str(), scope, search_filter.c_str(), attrs, 0, nullptr, nullptr, &timeout, params.search_limit, &msgs));

    for (
         auto * msg = ldap_first_message(handle, msgs);
         msg != nullptr;
         msg = ldap_next_message(handle, msg)
    )
    {
        switch (ldap_msgtype(msg))
        {
            case LDAP_RES_SEARCH_ENTRY:
            {
                BerElement * ber = nullptr;

                SCOPE_EXIT({
                    if (ber)
                    {
                        ber_free(ber, 0);
                        ber = nullptr;
                    }
                });

                for (
                     auto * attr = ldap_first_attribute(handle, msg, &ber);
                     attr != nullptr;
                     attr = ldap_next_attribute(handle, msg, ber)
                )
                {
                    SCOPE_EXIT({
                        ldap_memfree(attr);
                        attr = nullptr;
                    });

                    if (search_params.attribute.empty() || boost::iequals(attr, search_params.attribute))
                    {
                        auto ** vals = ldap_get_values_len(handle, msg, attr);
                        if (vals)
                        {
                            SCOPE_EXIT({
                                ldap_value_free_len(vals);
                                vals = nullptr;
                            });

                            for (std::size_t i = 0; vals[i]; i++)
                            {
                                if (vals[i]->bv_val && vals[i]->bv_len > 0)
                                    result.emplace(vals[i]->bv_val, vals[i]->bv_len);
                            }
                        }
                    }
                }

                break;
            }

            case LDAP_RES_SEARCH_REFERENCE:
            {
                char ** referrals = nullptr;
                diag(ldap_parse_reference(handle, msg, &referrals, nullptr, 0));

                if (referrals)
                {
                    SCOPE_EXIT({
//                      ldap_value_free(referrals);
                        ber_memvfree(reinterpret_cast<void **>(referrals));
                        referrals = nullptr;
                    });

                    for (std::size_t i = 0; referrals[i]; i++)
                    {
                        LOG_WARNING(&Poco::Logger::get("LDAPClient"), "Received reference during LDAP search but not following it: {}", referrals[i]);
                    }
                }

                break;
            }

            case LDAP_RES_SEARCH_RESULT:
            {
                int rc = LDAP_SUCCESS;
                char * matched_msg = nullptr;
                char * error_msg = nullptr;

                diag(ldap_parse_result(handle, msg, &rc, &matched_msg, &error_msg, nullptr, nullptr, 0));

                if (rc != LDAP_SUCCESS)
                {
                    String message = "LDAP search failed";

                    const char * raw_err_str = ldap_err2string(rc);
                    if (raw_err_str && *raw_err_str != '\0')
                    {
                        message += ": ";
                        message += raw_err_str;
                    }

                    if (error_msg && *error_msg != '\0')
                    {
                        message += ", ";
                        message += error_msg;
                    }

                    if (matched_msg && *matched_msg != '\0')
                    {
                        message += ", matching DN part: ";
                        message += matched_msg;
                    }

                    throw Exception(message, ErrorCodes::LDAP_ERROR);
                }

                break;
            }

            case -1:
                throw Exception("Failed to process LDAP search message", ErrorCodes::LDAP_ERROR);
        }
    }

    return result;
}

bool LDAPSimpleAuthClient::authenticate(const SearchParamsList * search_params, SearchResultsList * search_results)
{
    if (params.user.empty())
        throw Exception("LDAP authentication of a user with empty name is not allowed", ErrorCodes::BAD_ARGUMENTS);

    if (!search_params != !search_results)
        throw Exception("Cannot return LDAP search results", ErrorCodes::BAD_ARGUMENTS);

    // Silently reject authentication attempt if the password is empty as if it didn't match.
    if (params.password.empty())
        return false;

    SCOPE_EXIT({ closeConnection(); });

    // Will throw on any error, including invalid credentials.
    openConnection();

    // While connected, run search queries and save the results, if asked.
    if (search_params)
    {
        search_results->clear();
        search_results->reserve(search_params->size());

        try
        {
            for (const auto & single_search_params : *search_params)
            {
                search_results->emplace_back(search(single_search_params));
            }
        }
        catch (...)
        {
            search_results->clear();
            throw;
        }
    }

    return true;
}

#else // USE_LDAP

void LDAPClient::diag(const int, String)
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

LDAPClient::SearchResults LDAPClient::search(const SearchParams &)
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

bool LDAPSimpleAuthClient::authenticate(const SearchParamsList *, SearchResultsList *)
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

#endif // USE_LDAP

}
