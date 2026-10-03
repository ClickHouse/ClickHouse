#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>

#include <Poco/Logger.h>
#include <boost/algorithm/string/predicate.hpp>
#include <fmt/format.h>

#include <cctype>
#include <mutex>
#include <string_view>
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
    extern const int LOGICAL_ERROR;
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
    /// The lookup credentials are part of the key so that a login verified under rotated
    /// service credentials (i.e. one in flight during a reload) is not cached.
    ::updateHash(hash, lookup_bind_dn);
    ::updateHash(hash, lookup_password);
    ::updateHash(hash, static_cast<int>(follow_referrals)); // Include follow referral behavior

    if (user_dn_detection)
        user_dn_detection->updateHash(hash);
}

bool LDAPClient::Params::templateDependsOnUserName(const String & search_template) const
{
    if (search_template.contains("{user_name}"))
        return true;

    /// `{bind_dn}`, and `{user_dn}` (which equals the bind DN until the detection has run), are
    /// substituted from the `bind_dn` template, so they carry the login exactly when that
    /// template does. In search-and-bind `bind_dn` is `{user_dn}` itself and carries nothing.
    return bind_dn.contains("{user_name}")
        && (search_template.contains("{bind_dn}") || search_template.contains("{user_dn}"));
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

    /// Must be called under `ldap_global_mutex`.
    String getDiagnosticMessage(LDAP * handle)
    {
        if (!handle)
            return {};

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
            return raw_message;

        return {};
    }

    /// Active Directory explains a failed bind in the diagnostic message with a sub-code, e.g.
    /// `80090308: LdapErr: DSID-0C09042A, comment: AcceptSecurityContext error, data 52e, v3839`.
    /// Returns a human-readable rendering of the `data <hex>` part, if present.
    std::optional<String> describeActiveDirectorySubCode(const String & diagnostic_message)
    {
        static constexpr std::string_view marker = "data ";

        const auto pos = diagnostic_message.find(marker);
        if (pos == String::npos)
            return std::nullopt;

        const auto begin = pos + marker.size();
        auto end = begin;
        while (end < diagnostic_message.size() && std::isxdigit(static_cast<unsigned char>(diagnostic_message[end])))
            ++end;

        if (end == begin)
            return std::nullopt;

        const String code = diagnostic_message.substr(begin, end - begin);

        static constexpr std::pair<const char *, const char *> known_sub_codes[] =
        {
            {"525", "user not found"},
            {"52e", "invalid credentials"},
            {"530", "not permitted to logon at this time"},
            {"531", "not permitted to logon at this workstation"},
            {"532", "password expired"},
            {"533", "account disabled"},
            {"701", "account expired"},
            {"773", "user must reset password"},
            {"775", "account locked out"},
        };

        for (const auto & [sub_code, description] : known_sub_codes)
        {
            if (boost::iequals(code, sub_code))
                return fmt::format("data {} ({})", sub_code, description);
        }

        return fmt::format("data {} (unknown sub-code)", code);
    }

    const char * toString(LDAPClient::BindMode mode)
    {
        switch (mode)
        {
            case LDAPClient::BindMode::None:    return "nobody";
            case LDAPClient::BindMode::User:    return "the user";
            case LDAPClient::BindMode::Service: return "the lookup identity";
        }
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

        const auto diagnostic_message = getDiagnosticMessage(handle);
        if (!diagnostic_message.empty())
        {
            if (!text.empty())
                text += ": ";
            text += diagnostic_message;
        }

        throw Exception::createDeprecated(text, ErrorCodes::LDAP_ERROR);
    }
}

void LDAPClient::connect()
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

#ifdef LDAP_OPT_REFERRALS
    handleError(ldap_set_option(
        handle,
        LDAP_OPT_REFERRALS,
        params.follow_referrals ? LDAP_OPT_ON : LDAP_OPT_OFF));
#endif

    handleError(ldap_set_option(handle, LDAP_OPT_RESTART, LDAP_OPT_ON));

#ifdef LDAP_OPT_KEEPCONN
    handleError(ldap_set_option(handle, LDAP_OPT_KEEPCONN, LDAP_OPT_ON));
#endif

#ifdef LDAP_OPT_TIMEOUT
    {
        ::timeval operation_timeout{};
        operation_timeout.tv_sec = params.operation_timeout.count();
        operation_timeout.tv_usec = 0;
        handleError(ldap_set_option(handle, LDAP_OPT_TIMEOUT, &operation_timeout));
    }
#endif

#ifdef LDAP_OPT_NETWORK_TIMEOUT
    {
        ::timeval network_timeout{};
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

    bound_as = BindMode::None;

    /// The raw login is kept and escaped exactly once wherever it is substituted.
    placeholders.user_name = params.user;

    if (params.bindsAsDetectedUserDN())
    {
        /// The bind DN is only known after `user_dn_detection`. Leave the DN placeholders
        /// empty rather than substituting the literal `{user_dn}` into a template.
        placeholders.bind_dn.clear();
        placeholders.user_dn.clear();
    }
    else
    {
        placeholders.bind_dn = replacePlaceholders(params.bind_dn, { {"{user_name}", escapeForDN(params.user)} });
        placeholders.user_dn = placeholders.bind_dn; // The default value... may be updated by `user_dn_detection`.
    }
}

bool LDAPClient::bind(BindMode mode)
{
    std::lock_guard lock(ldap_global_mutex);

    if (!handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP bind attempted without an open connection");

    if (mode == BindMode::None)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP bind attempted without an identity");

    const bool as_service = (mode == BindMode::Service);

    if (as_service && !params.hasLookupIdentity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP lookup bind attempted while 'lookup_bind_dn' is not configured");

    if (!as_service && placeholders.bind_dn.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP user bind attempted before the bind DN is known");

    /// Whatever happens below, the previous identity is gone.
    bound_as = BindMode::None;

    const String & dn = as_service ? params.lookup_bind_dn : placeholders.bind_dn;
    const String & password = as_service ? params.lookup_password : params.password;

    switch (params.sasl_mechanism)
    {
        case LDAPClient::Params::SASLMechanism::SIMPLE:
        {
            ::berval cred{};
            cred.bv_val = const_cast<char *>(password.c_str());
            cred.bv_len = password.size();

            const auto rc = ldap_sasl_bind_s(handle, dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr);

            if (rc == LDAP_INVALID_CREDENTIALS)
            {
                /// The service credentials come from the server configuration, so a rejection
                /// means the lookup account is mistyped, rotated or revoked - an operator error,
                /// not a "user not found" or "wrong password" signal. Fail loudly.
                if (as_service)
                    throw Exception(ErrorCodes::LDAP_ERROR,
                        "LDAP lookup bind as '{}' failed for server '{}': invalid credentials; check 'lookup_bind_dn' and 'lookup_password'",
                        dn, params.name);

                /// The user supplied the password, so invalid credentials is the canonical
                /// authentication-failed outcome. Active Directory tells the reason apart in a
                /// sub-code which is useful in the log but must never reach the client.
                if (const auto sub_code = describeActiveDirectorySubCode(getDiagnosticMessage(handle)))
                    LOG_DEBUG(getLogger("LDAPClient"), "LDAP bind as '{}' failed with invalid credentials: {}", dn, *sub_code);

                return false;
            }

            handleError(rc);
            bound_as = mode;
            return true;
        }

        default:
            throw Exception(ErrorCodes::LDAP_ERROR, "Unknown SASL mechanism");
    }
}

std::optional<String> LDAPClient::detectUserDN(bool tolerate_missing_user)
{
    if (!params.user_dn_detection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP user DN detection requested while 'user_dn_detection' is not configured");

    /// A `base_dn` that depends on the login (`cn={user_name},ou=users,...`, or `{bind_dn}`
    /// with a `bind_dn` template carrying `{user_name}`) does not exist for an unknown user
    /// and the directory answers the search itself with `LDAP_NO_SUCH_OBJECT`; that is the
    /// same "user does not exist" signal as an empty result. A static `base_dn` (e.g.
    /// `dc=example,dc=org`) must exist, so the same code there means the configuration points
    /// at a wrong naming context; tolerating it would turn every login through this server
    /// into a silent "user not found" instead of an `LDAP_ERROR`. The rule is the one
    /// `parseLDAPServer` accepts the configuration with, so the two can never disagree.
    const bool base_dn_depends_on_user = params.templateDependsOnUserName(params.user_dn_detection->base_dn);
    const auto results = search(*params.user_dn_detection, /* tolerate_no_such_object = */ tolerate_missing_user && base_dn_depends_on_user);

    if (results.empty())
    {
        if (tolerate_missing_user)
            return std::nullopt;

        throw Exception(ErrorCodes::LDAP_ERROR, "Failed to detect user DN: empty search results");
    }

    if (results.size() > 1)
        throw Exception(ErrorCodes::LDAP_ERROR, "Failed to detect user DN: more than one entry in the search results");

    return *results.begin();
}

void LDAPClient::closeConnection() noexcept
{
    std::lock_guard lock(ldap_global_mutex);

    bound_as = BindMode::None;
    placeholders.user_name.clear();
    placeholders.bind_dn.clear();
    placeholders.user_dn.clear();

    if (!handle)
        return;

    ldap_unbind_ext_s(handle, nullptr, nullptr);
    handle = nullptr;
}

LDAPClient::SearchResults LDAPClient::search(const SearchParams & search_params, bool tolerate_no_such_object)
{
    std::lock_guard lock(ldap_global_mutex);

    if (!handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP search attempted without an open connection");

    /// Invariant: once a lookup identity is configured, no search may ever run as the user
    /// (users frequently cannot read group containers or their own `memberOf`, and a search
    /// as the user would silently return fewer roles). Without a lookup identity the legacy
    /// model applies and the connection must be bound as the user.
    const auto expected = params.hasLookupIdentity() ? BindMode::Service : BindMode::User;
    if (bound_as != expected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP search attempted while bound as {} instead of {}", toString(bound_as), toString(expected));

    SearchResults result;

    int scope = 0;
    switch (search_params.scope)
    {
        case SearchParams::Scope::BASE:      scope = LDAP_SCOPE_BASE;     break;
        case SearchParams::Scope::ONE_LEVEL: scope = LDAP_SCOPE_ONELEVEL; break;
        case SearchParams::Scope::SUBTREE:   scope = LDAP_SCOPE_SUBTREE;  break;
        case SearchParams::Scope::CHILDREN:  scope = LDAP_SCOPE_CHILDREN; break;
    }

    /// `{user_name}` is the raw login and is escaped for the context it lands in; the DN
    /// placeholders are already DNs and are only filter-escaped when used in a filter.
    const auto final_base_dn = replacePlaceholders(search_params.base_dn, {
        {"{user_name}", escapeForDN(placeholders.user_name)},
        {"{bind_dn}", placeholders.bind_dn},
        {"{user_dn}", placeholders.user_dn}
    });

    const auto final_search_filter = replacePlaceholders(search_params.search_filter, {
        {"{user_name}", escapeForFilter(placeholders.user_name)},
        {"{bind_dn}", escapeForFilter(placeholders.bind_dn)},
        {"{user_dn}", escapeForFilter(placeholders.user_dn)},
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

    const int search_rc = ldap_search_ext_s(handle, final_base_dn.c_str(), scope, final_search_filter.c_str(), attrs, 0, nullptr, nullptr, &timeout, params.search_limit, &msgs);
    if (tolerate_no_such_object && search_rc == LDAP_NO_SUCH_OBJECT)
        return result;
    handleError(search_rc);

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

                    ::berval bv{};

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
                        if (params.follow_referrals)
                            LOG_TRACE(getLogger("LDAPClient"), "Received LDAP search reference: {} (library referral chasing enabled)",
                            referrals[i]);
                        else
                            LOG_TRACE(getLogger("LDAPClient"), "Received LDAP search reference but not following it: {}",
                            referrals[i]);
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

bool LDAPSimpleAuthClient::find(const RoleSearchParamsList * role_search_params, SearchResultsList * role_search_results)
{
    if (params.user.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP lookup of a user with empty name is not allowed");

    if (!role_search_params != !role_search_results)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot return LDAP search results");

    /// The service-bind path requires lookup credentials AND `user_dn_detection`. The DN
    /// search is the only mechanism we have to confirm that the user actually exists in the
    /// directory; without it any non-empty name would be silently accepted, which would let
    /// an account holding `IMPERSONATE ON *` materialize arbitrary users.
    if (!params.hasLookupIdentity() || !params.user_dn_detection)
        return false;

    SCOPE_EXIT({ closeConnection(); });

    connect();
    bind(BindMode::Service);

    const auto user_dn = detectUserDN(/* tolerate_missing_user = */ true);
    if (!user_dn)
        return false;

    placeholders.user_dn = *user_dn;
    if (params.bindsAsDetectedUserDN())
        placeholders.bind_dn = *user_dn;

    if (role_search_params)
    {
        role_search_results->clear();
        role_search_results->reserve(role_search_params->size());

        try
        {
            for (const auto & params_instance : *role_search_params)
                role_search_results->emplace_back(search(params_instance));
        }
        catch (...)
        {
            role_search_results->clear();
            throw;
        }
    }

    return true;
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

    connect();

    if (params.bindsAsDetectedUserDN())
    {
        /// Search-and-bind: locate the user under the lookup identity, verify the password by
        /// binding as the DN that was found, then return to the lookup identity so that the
        /// role searches never run as the user. An unknown user is reported as `false` so the
        /// storages following this one still get their chance.
        bind(BindMode::Service);

        const auto user_dn = detectUserDN(/* tolerate_missing_user = */ true);
        if (!user_dn)
            return false;

        placeholders.bind_dn = *user_dn;
        placeholders.user_dn = *user_dn;

        if (!bind(BindMode::User))
            return false;

        bind(BindMode::Service);
    }
    else
    {
        /// Direct bind: the password is verified against the substituted `bind_dn` template.
        if (!bind(BindMode::User))
            return false;

        /// With a lookup identity configured neither the DN detection nor the role searches
        /// may run as the user.
        if (params.hasLookupIdentity())
            bind(BindMode::Service);

        if (params.user_dn_detection)
            placeholders.user_dn = detectUserDN(/* tolerate_missing_user = */ false).value();
    }

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

void LDAPClient::connect()
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

bool LDAPClient::bind(BindMode)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

std::optional<String> LDAPClient::detectUserDN(bool)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

void LDAPClient::closeConnection() noexcept
{
}

LDAPClient::SearchResults LDAPClient::search(const SearchParams &, bool)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

bool LDAPSimpleAuthClient::authenticate(const RoleSearchParamsList *, SearchResultsList *)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

bool LDAPSimpleAuthClient::find(const RoleSearchParamsList *, SearchResultsList *)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

#endif // USE_LDAP

}
