#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>

#include <Poco/Logger.h>
#include <boost/algorithm/string/predicate.hpp>

#include <mutex>
#include <utility>
#include <vector>

#include <cstring>

#include <sys/time.h>

namespace
{

template <typename T>
requires std::is_fundamental_v<std::decay_t<T>>
void updateHash(SipHash & hash, const T & value)
{
    hash.update(value);
}

void updateHash(SipHash & hash, const std::string & value)
{
    hash.update(value.size());
    hash.update(value);
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int LDAP_ERROR;
}

void LDAPClient::SearchParams::updateHash(SipHash & hash) const
{
    ::updateHash(hash, base_dn);
    ::updateHash(hash, static_cast<int>(scope));
    ::updateHash(hash, search_filter);
    ::updateHash(hash, attribute);
}

void LDAPClient::RoleSearchParams::updateHash(SipHash & hash) const
{
    SearchParams::updateHash(hash);
    ::updateHash(hash, prefix);
}

void LDAPClient::Params::updateHash(SipHash & hash) const
{
    ::updateHash(hash, host);
    ::updateHash(hash, port);
    ::updateHash(hash, bind_dn);
    ::updateHash(hash, user);
    ::updateHash(hash, password);

    if (user_dn_detection)
        user_dn_detection->updateHash(hash);
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

    auto escapeForDN(const String & src)
    {
        String dest;
        dest.reserve(src.size() * 2);

        for (auto ch : src)
        {
            switch (ch) // NOLINT(bugprone-switch-missing-default-case)
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

    auto escapeForFilter(const String & src)
    {
        String dest;
        dest.reserve(src.size() * 3);

        for (auto ch : src)
        {
            switch (ch)
            {
                case '*':
                    dest += "\\2A";
                    break;
                case '(':
                    dest += "\\28";
                    break;
                case ')':
                    dest += "\\29";
                    break;
                case '\\':
                    dest += "\\5C";
                    break;
                case '\0':
                    dest += "\\00";
                    break;
                default:
                    dest += ch;
                    break;
            }
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

void LDAPClient::handleError(int result_code, String text)
{
    std::lock_guard lock(ldap_global_mutex);

    if (result_code != LDAP_SUCCESS)
    {
        const char * raw_err_str = ldap_err2string(result_code);
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

        throw Exception::createDeprecated(text, ErrorCodes::LDAP_ERROR);
    }
}

bool LDAPClient::openConnection()
{
    std::lock_guard lock(ldap_global_mutex);

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
            throw Exception(ErrorCodes::LDAP_ERROR, "ldap_url_desc2str() failed");

        SCOPE_EXIT({ ldap_memfree(uri); });

        handleError(ldap_initialize(&handle, uri));
        if (!handle)
            throw Exception(ErrorCodes::LDAP_ERROR, "ldap_initialize() failed");
    }

    {
        int value = 0;
        switch (params.protocol_version)
        {
            case LDAPClient::Params::ProtocolVersion::V2: value = LDAP_VERSION2; break;
            case LDAPClient::Params::ProtocolVersion::V3: value = LDAP_VERSION3; break;
        }
        handleError(ldap_set_option(handle, LDAP_OPT_PROTOCOL_VERSION, &value));
    }

    handleError(ldap_set_option(handle, LDAP_OPT_RESTART, LDAP_OPT_ON));

#ifdef LDAP_OPT_KEEPCONN
    handleError(ldap_set_option(handle, LDAP_OPT_KEEPCONN, LDAP_OPT_ON));
#endif

#ifdef LDAP_OPT_TIMEOUT
    {
        ::timeval operation_timeout;
        operation_timeout.tv_sec = params.operation_timeout.count();
        operation_timeout.tv_usec = 0;
        handleError(ldap_set_option(handle, LDAP_OPT_TIMEOUT, &operation_timeout));
    }
#endif

#ifdef LDAP_OPT_NETWORK_TIMEOUT
    {
        ::timeval network_timeout;
        network_timeout.tv_sec = params.network_timeout.count();
        network_timeout.tv_usec = 0;
        handleError(ldap_set_option(handle, LDAP_OPT_NETWORK_TIMEOUT, &network_timeout));
    }
#endif

    {
        const int search_timeout = static_cast<int>(params.search_timeout.count());
        handleError(ldap_set_option(handle, LDAP_OPT_TIMELIMIT, &search_timeout));
    }

    {
        const int size_limit = static_cast<int>(params.search_limit);
        handleError(ldap_set_option(handle, LDAP_OPT_SIZELIMIT, &size_limit));
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
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_PROTOCOL_MIN, &value));
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
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_REQUIRE_CERT, &value));
    }
#endif

#ifdef LDAP_OPT_X_TLS_CERTFILE
    if (!params.tls_cert_file.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CERTFILE, params.tls_cert_file.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_KEYFILE
    if (!params.tls_key_file.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_KEYFILE, params.tls_key_file.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_CACERTFILE
    if (!params.tls_ca_cert_file.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTFILE, params.tls_ca_cert_file.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_CACERTDIR
    if (!params.tls_ca_cert_dir.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTDIR, params.tls_ca_cert_dir.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_CIPHER_SUITE
    if (!params.tls_cipher_suite.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CIPHER_SUITE, params.tls_cipher_suite.c_str()));
#endif

#ifdef LDAP_OPT_X_TLS_NEWCTX
    {
        const int i_am_a_server = 0;
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_NEWCTX, &i_am_a_server));
    }
#endif

    if (params.enable_tls == LDAPClient::Params::TLSEnable::YES_STARTTLS)
        handleError(ldap_start_tls_s(handle, nullptr, nullptr));

    final_user_name = escapeForDN(params.user);
    final_bind_dn = replacePlaceholders(params.bind_dn, { {"{user_name}", final_user_name} });
    final_user_dn = final_bind_dn; // The default value... may be updated right after a successful bind.

    switch (params.sasl_mechanism)
    {
        case LDAPClient::Params::SASLMechanism::SIMPLE:
        {
            ::berval cred;
            cred.bv_val = const_cast<char *>(params.password.c_str());
            cred.bv_len = params.password.size();

            {
                const auto rc = ldap_sasl_bind_s(handle, final_bind_dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr);

                // Handle invalid credentials gracefully.
                if (rc == LDAP_INVALID_CREDENTIALS)
                    return false;

                handleError(rc);
            }

            // Once bound, run the user DN search query and update the default value, if asked.
            if (params.user_dn_detection)
            {
                const auto user_dn_search_results = search(*params.user_dn_detection);

                if (user_dn_search_results.empty())
                    throw Exception(ErrorCodes::LDAP_ERROR, "Failed to detect user DN: empty search results");

                if (user_dn_search_results.size() > 1)
                    throw Exception(ErrorCodes::LDAP_ERROR, "Failed to detect user DN: more than one entry in the search results");

                final_user_dn = *user_dn_search_results.begin();
            }

            return true;
        }

        default:
            throw Exception(ErrorCodes::LDAP_ERROR, "Unknown SASL mechanism");
    }
}

void LDAPClient::closeConnection() noexcept
{
    std::lock_guard lock(ldap_global_mutex);

    if (!handle)
        return;

    ldap_unbind_ext_s(handle, nullptr, nullptr);
    handle = nullptr;
    final_user_name.clear();
    final_bind_dn.clear();
    final_user_dn.clear();
}

LDAPClient::SearchResults LDAPClient::search(const SearchParams & search_params)
{
    std::lock_guard lock(ldap_global_mutex);

    SearchResults result;

    int scope = 0;
    switch (search_params.scope)
    {
        case SearchParams::Scope::BASE:      scope = LDAP_SCOPE_BASE;     break;
        case SearchParams::Scope::ONE_LEVEL: scope = LDAP_SCOPE_ONELEVEL; break;
        case SearchParams::Scope::SUBTREE:   scope = LDAP_SCOPE_SUBTREE;  break;
        case SearchParams::Scope::CHILDREN:  scope = LDAP_SCOPE_CHILDREN; break;
    }

    const auto final_base_dn = replacePlaceholders(search_params.base_dn, {
        {"{user_name}", final_user_name},
        {"{bind_dn}", final_bind_dn},
        {"{user_dn}", final_user_dn}
    });

    const auto final_search_filter = replacePlaceholders(search_params.search_filter, {
        {"{user_name}", escapeForFilter(final_user_name)},
        {"{bind_dn}", escapeForFilter(final_bind_dn)},
        {"{user_dn}", escapeForFilter(final_user_dn)},
        {"{base_dn}", escapeForFilter(final_base_dn)}
    });

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

    handleError(ldap_search_ext_s(handle, final_base_dn.c_str(), scope, final_search_filter.c_str(), attrs, 0, nullptr, nullptr, &timeout, params.search_limit, &msgs));

    for (
         auto * msg = ldap_first_message(handle, msgs);
         msg != nullptr;
         msg = ldap_next_message(handle, msg)
    )
    {
        switch (ldap_msgtype(msg)) // NOLINT(bugprone-switch-missing-default-case)
        {
            case LDAP_RES_SEARCH_ENTRY:
            {
                // Extract DN separately, if the requested attribute is DN.
                if (boost::iequals("dn", search_params.attribute))
                {
                    BerElement * ber = nullptr;

                    SCOPE_EXIT({
                        if (ber)
                        {
                            ber_free(ber, 0);
                            ber = nullptr;
                        }
                    });

                    ::berval bv;

                    handleError(ldap_get_dn_ber(handle, msg, &ber, &bv));

                    if (bv.bv_val && bv.bv_len > 0)
                        result.emplace(bv.bv_val, bv.bv_len);
                }

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

                            for (size_t i = 0; vals[i]; ++i)
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
                handleError(ldap_parse_reference(handle, msg, &referrals, nullptr, 0));

                if (referrals)
                {
                    SCOPE_EXIT({
                        ber_memvfree(reinterpret_cast<void **>(referrals));
                        referrals = nullptr;
                    });

                    for (size_t i = 0; referrals[i]; ++i)
                    {
                        LOG_WARNING(getLogger("LDAPClient"), "Received reference during LDAP search but not following it: {}", referrals[i]);
                    }
                }

                break;
            }

            case LDAP_RES_SEARCH_RESULT:
            {
                int rc = LDAP_SUCCESS;
                char * matched_msg = nullptr;
                char * error_msg = nullptr;

                handleError(ldap_parse_result(handle, msg, &rc, &matched_msg, &error_msg, nullptr, nullptr, 0));

                if (rc != LDAP_SUCCESS)
                {
                    String message;

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

                    throw Exception(ErrorCodes::LDAP_ERROR, "LDAP search failed{}", message);
                }

                break;
            }

            case -1:
                throw Exception(ErrorCodes::LDAP_ERROR, "Failed to process LDAP search message");
        }
    }

    return result;
}

bool LDAPSimpleAuthClient::authenticate(const RoleSearchParamsList * role_search_params, SearchResultsList * role_search_results)
{
    if (params.user.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP authentication of a user with empty name is not allowed");

    if (!role_search_params != !role_search_results)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot return LDAP search results");

    // Silently reject authentication attempt if the password is empty as if it didn't match.
    if (params.password.empty())
        return false;

    SCOPE_EXIT({ closeConnection(); });

    // Will return false on invalid credentials, will throw on any other error.
    if (!openConnection())
        return false;

    // While connected, run search queries and save the results, if asked.
    if (role_search_params)
    {
        role_search_results->clear();
        role_search_results->reserve(role_search_params->size());

        try
        {
            for (const auto & params_instance : *role_search_params)
            {
                role_search_results->emplace_back(search(params_instance));
            }
        }
        catch (...)
        {
            role_search_results->clear();
            throw;
        }
    }

    return true;
}

#else // USE_LDAP

void LDAPClient::handleError(const int, String)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

bool LDAPClient::openConnection()
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

void LDAPClient::closeConnection() noexcept
{
}

LDAPClient::SearchResults LDAPClient::search(const SearchParams &)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

bool LDAPSimpleAuthClient::authenticate(const RoleSearchParamsList *, SearchResultsList *)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

#endif // USE_LDAP

}
