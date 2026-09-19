#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>

#include <Poco/Logger.h>
#include <boost/algorithm/string/predicate.hpp>
#include <fmt/format.h>

#include <algorithm>
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

bool LDAPClient::SearchParams::isSelfLookup(bool bind_dn_is_user_dn) const
{
    /// `{bind_dn}` denotes the user's own entry only when the login binds as the detected DN (search-and-bind);
    /// with a `bind_dn` template it is the template's result, which the detection may well differ from.
    const bool base_is_own_entry = (base_dn == Params::DETECTED_USER_DN_PLACEHOLDER) || (bind_dn_is_user_dn && base_dn == "{bind_dn}");
    return base_is_own_entry && scope == Scope::BASE && boost::iequals(search_filter, "(objectClass=*)");
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
    ::updateHash(hash, rdn_attribute);

    /// The order of the `groups` entries is irrelevant for the mapping, so hash them sorted:
    /// reordering the list must not invalidate cached authentication results.
    std::vector<String> sorted_groups = groups;
    std::sort(sorted_groups.begin(), sorted_groups.end());
    ::updateHash(hash, sorted_groups.size());
    for (const auto & group : sorted_groups)
        ::updateHash(hash, group);
}

void LDAPClient::Params::updateHash(SipHash & hash) const
{
    ::updateHash(hash, static_cast<int>(protocol_version));

    ::updateHash(hash, host);
    ::updateHash(hash, port);

    ::updateHash(hash, static_cast<int>(enable_tls));
    ::updateHash(hash, tls_minimum_protocol_version.has_value());
    if (tls_minimum_protocol_version)
        ::updateHash(hash, static_cast<int>(*tls_minimum_protocol_version));
    ::updateHash(hash, tls_maximum_protocol_version.has_value());
    if (tls_maximum_protocol_version)
        ::updateHash(hash, static_cast<int>(*tls_maximum_protocol_version));
    ::updateHash(hash, tls_require_cert.has_value());
    if (tls_require_cert)
        ::updateHash(hash, static_cast<int>(*tls_require_cert));
    ::updateHash(hash, tls_cert_file);
    ::updateHash(hash, tls_key_file);
    ::updateHash(hash, tls_ca_cert_file);
    ::updateHash(hash, tls_ca_cert_dir);
    ::updateHash(hash, tls_cipher_suite);

    ::updateHash(hash, static_cast<int>(sasl_mechanism));

    ::updateHash(hash, bind_dn);
    ::updateHash(hash, user);
    ::updateHash(hash, password);
    /// The lookup credentials are part of the key so that a login verified under rotated
    /// service credentials (i.e. one in flight during a reload) is not cached.
    ::updateHash(hash, lookup_bind_dn);
    ::updateHash(hash, lookup_password);

    ::updateHash(hash, operation_timeout.has_value());
    if (operation_timeout)
        ::updateHash(hash, operation_timeout->count());
    ::updateHash(hash, network_timeout.has_value());
    if (network_timeout)
        ::updateHash(hash, network_timeout->count());
    ::updateHash(hash, search_timeout.count());
    ::updateHash(hash, search_limit);

    ::updateHash(hash, static_cast<int>(follow_referrals));

    ::updateHash(hash, user_dn_detection.has_value());
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

#if defined(LDAP_OPT_X_TLS_PROTOCOL_MIN) || defined(LDAP_OPT_X_TLS_PROTOCOL_MAX)
    int toLDAPTLSProtocolVersion(LDAPClient::Params::TLSProtocolVersion version)
    {
        int value = 0;
        switch (version)
        {
            case LDAPClient::Params::TLSProtocolVersion::SSL2:   value = LDAP_OPT_X_TLS_PROTOCOL_SSL2;   break;
            case LDAPClient::Params::TLSProtocolVersion::SSL3:   value = LDAP_OPT_X_TLS_PROTOCOL_SSL3;   break;
            case LDAPClient::Params::TLSProtocolVersion::TLS1_0: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_0; break;
            case LDAPClient::Params::TLSProtocolVersion::TLS1_1: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_1; break;
            case LDAPClient::Params::TLSProtocolVersion::TLS1_2: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_2; break;
#ifdef LDAP_OPT_X_TLS_PROTOCOL_TLS1_3
            case LDAPClient::Params::TLSProtocolVersion::TLS1_3: value = LDAP_OPT_X_TLS_PROTOCOL_TLS1_3; break;
#else
            /// The constant appeared in OpenLDAP 2.4.47; older builds have `LDAP_OPT_X_TLS_PROTOCOL_MIN`/`MAX` but no way to name
            /// TLS 1.3 to them. Refuse the configured value instead of guessing, in line with the other extensions in `openConnection`.
            case LDAPClient::Params::TLSProtocolVersion::TLS1_3:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls1.3' is not supported by this build of libldap");
#endif
        }
        return value;
    }
#endif

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

    int toLDAPScope(LDAPClient::SearchParams::Scope scope)
    {
        switch (scope)
        {
            case LDAPClient::SearchParams::Scope::BASE:      return LDAP_SCOPE_BASE;
            case LDAPClient::SearchParams::Scope::ONE_LEVEL: return LDAP_SCOPE_ONELEVEL;
            case LDAPClient::SearchParams::Scope::SUBTREE:   return LDAP_SCOPE_SUBTREE;
            case LDAPClient::SearchParams::Scope::CHILDREN:  return LDAP_SCOPE_CHILDREN;
        }
    }

    /// An attribute description returned by `ldap_first_attribute` is the bare attribute name, optionally
    /// followed by `;`-separated options (`userCertificate;binary`, `memberOf;range=0-1499`).
    struct AttributeDescription
    {
        std::string_view name;
        bool ranged = false;
    };

    AttributeDescription parseAttributeDescription(std::string_view description)
    {
        const auto options_pos = description.find(';');
        const bool ranged = options_pos != std::string_view::npos && toLowerCopyASCII(description.substr(options_pos)).contains(";range=");
        return {description.substr(0, options_pos), ranged};
    }

    /// A range option means the directory returned only a slice of the values (Active Directory sends at most
    /// `MaxValRange` values, 1500 by default, of a multi-valued attribute). A login acting on the slice would see
    /// only part of the memberships and a synchronisation would revoke every role beyond it, so the incomplete
    /// entry is refused wherever it is read.
    [[noreturn]] void throwRangedAttribute(const String & entry_dn, const String & server_name, std::string_view description)
    {
        throw Exception(ErrorCodes::LDAP_ERROR,
            "LDAP entry '{}' on server '{}' returned attribute '{}' with a range option, i.e. only part of its values "
            "(Active Directory returns at most MaxValRange values of a multi-valued attribute); refusing to continue with an "
            "incomplete entry. Raise the limit on the directory or map roles with a search over the group entries instead",
            entry_dn, server_name, description);
    }

    /// Renders a failed search result: the result code and the optional diagnostic message and
    /// matched DN the directory attached to it.
    String describeSearchResultError(int rc, const char * error_msg, const char * matched_msg)
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

        return message;
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

/// The DN parsing routines below do not touch any `LDAP *` handle or library-global state
/// (they only allocate), so they are not serialized through `ldap_global_mutex`.

std::optional<String> LDAPClient::extractRDNValue(const String & dn, const String & rdn_attribute)
{
    LDAPDN parsed_dn = nullptr;

    SCOPE_EXIT({
        if (parsed_dn)
        {
            ldap_dnfree(parsed_dn);
            parsed_dn = nullptr;
        }
    });

    /// An empty string is a valid (root) DN for `ldap_str2dn` and yields a null `parsed_dn`.
    if (ldap_str2dn(dn.c_str(), &parsed_dn, LDAP_DN_FORMAT_LDAPV3) != LDAP_SUCCESS || !parsed_dn)
        return std::nullopt;

    /// RDNs are ordered from the most specific one; the first matching attribute type wins.
    for (size_t i = 0; parsed_dn[i]; ++i)
    {
        LDAPRDN rdn = parsed_dn[i];
        for (size_t j = 0; rdn[j]; ++j)
        {
            const LDAPAVA & ava = *rdn[j];

            /// `#hex`-encoded (BER) values are not names.
            if (ava.la_flags & LDAP_AVA_BINARY)
                continue;

            if (!ava.la_attr.bv_val || !ava.la_value.bv_val)
                continue;

            if (boost::iequals(std::string_view(ava.la_attr.bv_val, ava.la_attr.bv_len), rdn_attribute))
                return String(ava.la_value.bv_val, ava.la_value.bv_len);
        }
    }

    return std::nullopt;
}

std::optional<String> LDAPClient::normalizeDN(const String & dn)
{
    LDAPDN parsed_dn = nullptr;

    SCOPE_EXIT({
        if (parsed_dn)
        {
            ldap_dnfree(parsed_dn);
            parsed_dn = nullptr;
        }
    });

    if (ldap_str2dn(dn.c_str(), &parsed_dn, LDAP_DN_FORMAT_LDAPV3) != LDAP_SUCCESS || !parsed_dn)
        return std::nullopt;

    char * raw_str = nullptr;

    SCOPE_EXIT({
        if (raw_str)
        {
            ldap_memfree(raw_str);
            raw_str = nullptr;
        }
    });

    /// Serialization of a successfully parsed DN can only fail on allocation failure; do not degrade silently.
    const int rc = ldap_dn2str(parsed_dn, &raw_str, LDAP_DN_FORMAT_LDAPV3);
    if (rc != LDAP_SUCCESS || !raw_str)
        throw Exception(ErrorCodes::LDAP_ERROR, "ldap_dn2str() failed: {}", ldap_err2string(rc));

    String result(raw_str);
    toLowerASCII(result);
    return result;
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

    /// The options below are extensions that not every libldap provides. When one is missing, the default behaviour
    /// is left to the library as before, but a value that was configured explicitly is refused rather than ignored:
    /// the operator asked for a bound that this build cannot enforce.
#ifdef LDAP_OPT_TIMEOUT
    {
        ::timeval operation_timeout{};
        operation_timeout.tv_sec = params.operation_timeout.value_or(Params::default_operation_timeout).count();
        operation_timeout.tv_usec = 0;
        handleError(ldap_set_option(handle, LDAP_OPT_TIMEOUT, &operation_timeout));
    }
#else
    if (params.operation_timeout)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'operation_timeout' is not supported by this build of libldap");
#endif

#ifdef LDAP_OPT_NETWORK_TIMEOUT
    {
        ::timeval network_timeout{};
        network_timeout.tv_sec = params.network_timeout.value_or(Params::default_network_timeout).count();
        network_timeout.tv_usec = 0;
        handleError(ldap_set_option(handle, LDAP_OPT_NETWORK_TIMEOUT, &network_timeout));
    }
#else
    if (params.network_timeout)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'network_timeout' is not supported by this build of libldap");
#endif

    {
        const int search_timeout = static_cast<int>(params.search_timeout.count());
        handleError(ldap_set_option(handle, LDAP_OPT_TIMELIMIT, &search_timeout));
    }

    {
        const int size_limit = static_cast<int>(params.search_limit);
        handleError(ldap_set_option(handle, LDAP_OPT_SIZELIMIT, &size_limit));
    }

    /// Like every other TLS option here, the protocol bounds have to be set before `LDAP_OPT_X_TLS_NEWCTX` below:
    /// the new TLS context is built from the options accumulated on the handle at that moment.
#ifdef LDAP_OPT_X_TLS_PROTOCOL_MIN
    {
        int value = toLDAPTLSProtocolVersion(params.tls_minimum_protocol_version.value_or(Params::default_tls_minimum_protocol_version));
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_PROTOCOL_MIN, &value));
    }
#else
    if (params.tls_minimum_protocol_version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_minimum_protocol_version' is not supported by this build of libldap");

    /// Unlike the timeouts above, the default minimum is a security bound rather than a tuning knob, so an operator who
    /// relies on the documented `tls1.2` floor has to learn that this build leaves the protocol version to the library.
    /// There is nothing to enforce when TLS is off.
    if (params.enable_tls != Params::TLSEnable::NO)
        LOG_WARNING(getLogger("LDAPClient"),
            "This build of libldap lacks LDAP_OPT_X_TLS_PROTOCOL_MIN: the default minimum TLS protocol version (tls1.2) "
            "cannot be enforced for LDAP server {}:{}, the library negotiates any version it supports",
            params.host, params.port);
#endif

#ifdef LDAP_OPT_X_TLS_PROTOCOL_MAX
    if (params.tls_maximum_protocol_version)
    {
        int value = toLDAPTLSProtocolVersion(*params.tls_maximum_protocol_version);
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_PROTOCOL_MAX, &value));
    }
#else
    if (params.tls_maximum_protocol_version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_maximum_protocol_version' is not supported by this build of libldap");
#endif

#ifdef LDAP_OPT_X_TLS_REQUIRE_CERT
    {
        int value = 0;
        switch (params.tls_require_cert.value_or(Params::default_tls_require_cert))
        {
            case LDAPClient::Params::TLSRequireCert::NEVER:  value = LDAP_OPT_X_TLS_NEVER;  break;
            case LDAPClient::Params::TLSRequireCert::ALLOW:  value = LDAP_OPT_X_TLS_ALLOW;  break;
            case LDAPClient::Params::TLSRequireCert::TRY:    value = LDAP_OPT_X_TLS_TRY;    break;
            case LDAPClient::Params::TLSRequireCert::DEMAND: value = LDAP_OPT_X_TLS_DEMAND; break;
        }
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_REQUIRE_CERT, &value));
    }
#else
    if (params.tls_require_cert)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_require_cert' is not supported by this build of libldap");

    /// The default `demand` is the other security bound (see `tls_minimum_protocol_version` above).
    if (params.enable_tls != Params::TLSEnable::NO)
        LOG_WARNING(getLogger("LDAPClient"),
            "This build of libldap lacks LDAP_OPT_X_TLS_REQUIRE_CERT: the default certificate policy (demand) cannot be enforced "
            "for LDAP server {}:{}, the library applies its own",
            params.host, params.port);
#endif

#ifdef LDAP_OPT_X_TLS_CERTFILE
    if (!params.tls_cert_file.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CERTFILE, params.tls_cert_file.c_str()));
#else
    if (!params.tls_cert_file.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_cert_file' is not supported by this build of libldap");
#endif

#ifdef LDAP_OPT_X_TLS_KEYFILE
    if (!params.tls_key_file.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_KEYFILE, params.tls_key_file.c_str()));
#else
    if (!params.tls_key_file.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_key_file' is not supported by this build of libldap");
#endif

#ifdef LDAP_OPT_X_TLS_CACERTFILE
    if (!params.tls_ca_cert_file.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTFILE, params.tls_ca_cert_file.c_str()));
#else
    if (!params.tls_ca_cert_file.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_ca_cert_file' is not supported by this build of libldap");
#endif

#ifdef LDAP_OPT_X_TLS_CACERTDIR
    if (!params.tls_ca_cert_dir.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTDIR, params.tls_ca_cert_dir.c_str()));
#else
    if (!params.tls_ca_cert_dir.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_ca_cert_dir' is not supported by this build of libldap");
#endif

#ifdef LDAP_OPT_X_TLS_CIPHER_SUITE
    if (!params.tls_cipher_suite.empty())
        handleError(ldap_set_option(handle, LDAP_OPT_X_TLS_CIPHER_SUITE, params.tls_cipher_suite.c_str()));
#else
    if (!params.tls_cipher_suite.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'tls_cipher_suite' is not supported by this build of libldap");
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
    tls_logged = false;

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

            /// The first successful operation of the connection: with LDAPS the handshake has just happened, with
            /// StartTLS it happened in `openConnection`; either way the negotiated parameters are known now.
            if (params.enable_tls != Params::TLSEnable::NO && !tls_logged)
            {
                logNegotiatedTLS();
                tls_logged = true;
            }
            return true;
        }

        default:
            throw Exception(ErrorCodes::LDAP_ERROR, "Unknown SASL mechanism");
    }
}


void LDAPClient::logNegotiatedTLS()
{
#if defined(LDAP_OPT_X_TLS_VERSION) && defined(LDAP_OPT_X_TLS_CIPHER)
    /// Read-only options of libldap; both strings are allocated for the caller.
    char * version = nullptr;
    if (ldap_get_option(handle, LDAP_OPT_X_TLS_VERSION, &version) != LDAP_OPT_SUCCESS || !version)
        return;
    SCOPE_EXIT({ ldap_memfree(version); });

    String cipher = "unknown";
    char * cipher_raw = nullptr;
    if (ldap_get_option(handle, LDAP_OPT_X_TLS_CIPHER, &cipher_raw) == LDAP_OPT_SUCCESS && cipher_raw)
    {
        cipher = cipher_raw;
        ldap_memfree(cipher_raw);
    }

    /// The tests of the transport settings assert on this line: it is the only observable proof that StartTLS
    /// upgraded the connection and that the protocol bounds were applied.
    LOG_DEBUG(getLogger("LDAPClient"), "LDAP server '{}': the connection to {}:{} is protected by {} ({} with cipher {})",
        params.name, params.host, params.port,
        params.enable_tls == Params::TLSEnable::YES_STARTTLS ? "StartTLS" : "LDAPS", version, cipher);
#endif
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

void LDAPClient::assertBoundForSearch() const
{
    const auto expected = params.hasLookupIdentity() ? BindMode::Service : BindMode::User;
    if (bound_as != expected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP search attempted while bound as {} instead of {}", toString(bound_as), toString(expected));
}

std::pair<String, String> LDAPClient::resolveSearchTemplates(const SearchParams & search_params) const
{
    /// `{user_name}` is the raw login and is escaped for the context it lands in; the DN
    /// placeholders are already DNs and are only filter-escaped when used in a filter.
    auto final_base_dn = replacePlaceholders(search_params.base_dn, {
        {"{user_name}", escapeForDN(placeholders.user_name)},
        {"{bind_dn}", placeholders.bind_dn},
        {"{user_dn}", placeholders.user_dn}
    });

    auto final_search_filter = replacePlaceholders(search_params.search_filter, {
        {"{user_name}", escapeForFilter(placeholders.user_name)},
        {"{bind_dn}", escapeForFilter(placeholders.bind_dn)},
        {"{user_dn}", escapeForFilter(placeholders.user_dn)},
        {"{base_dn}", escapeForFilter(final_base_dn)}
    });

    return {std::move(final_base_dn), std::move(final_search_filter)};
}

LDAPClient::SearchResults LDAPClient::search(const SearchParams & search_params, bool tolerate_no_such_object)
{
    std::lock_guard lock(ldap_global_mutex);

    if (!handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP search attempted without an open connection");

    assertBoundForSearch();

    SearchResults result;

    const int scope = toLDAPScope(search_params.scope);
    const auto [final_base_dn, final_search_filter] = resolveSearchTemplates(search_params);

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

                    /// Match on the bare name: the requested attribute comes back as `memberOf;range=0-1499` when
                    /// the directory truncated it, and taking that for a missing attribute would let a login or an
                    /// `EXECUTE AS` lookup see no roles at all instead of failing.
                    const auto description = parseAttributeDescription(attr);
                    if (search_params.attribute.empty() || boost::iequals(description.name, search_params.attribute))
                    {
                        if (description.ranged)
                        {
                            char * dn = ldap_get_dn(handle, msg);
                            SCOPE_EXIT({
                                if (dn)
                                    ldap_memfree(dn);
                            });
                            throwRangedAttribute(dn ? String(dn) : String(), params.name, attr);
                        }

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

                /// Both strings are copies owned by the caller.
                SCOPE_EXIT({
                    if (matched_msg)
                    {
                        ldap_memfree(matched_msg);
                        matched_msg = nullptr;
                    }
                    if (error_msg)
                    {
                        ldap_memfree(error_msg);
                        error_msg = nullptr;
                    }
                });

                handleError(ldap_parse_result(handle, msg, &rc, &matched_msg, &error_msg, nullptr, nullptr, 0));

                if (rc != LDAP_SUCCESS)
                    throw Exception(ErrorCodes::LDAP_ERROR, "LDAP search failed{}", describeSearchResultError(rc, error_msg, matched_msg));

                break;
            }

            case -1:
                throw Exception(ErrorCodes::LDAP_ERROR, "Failed to process LDAP search message");
        }
    }

    return result;
}

std::vector<LDAPClient::Entry> LDAPClient::searchEntries(const SearchParams & search_params, const std::vector<String> & attributes, UInt32 page_size, size_t max_entries)
{
    /// libldap rejects any other page size with a bare `LDAP_PARAM_ERROR`; say what was wrong.
    if (page_size == 0 || page_size > static_cast<UInt32>(LDAP_MAXINT))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP page size must be between 1 and {}, got {}", LDAP_MAXINT, page_size);

    /// Neither depends on the connection; the placeholders are fixed for the whole enumeration.
    const int scope = toLDAPScope(search_params.scope);
    const auto [final_base_dn, final_search_filter] = resolveSearchTemplates(search_params);

    /// The strings behind `attrs` are owned by `attributes`, which outlives every page.
    std::vector<char *> attrs;
    attrs.reserve(attributes.size() + 1);
    for (const auto & attribute : attributes)
        attrs.push_back(const_cast<char *>(attribute.c_str()));
    attrs.push_back(nullptr);

    std::vector<Entry> result;

    /// The opaque position the directory hands back with every page; empty after the last one.
    String cookie;
    bool more_pages = true;
    size_t pages_received = 0;

    /// With `max_entries` == 0 nothing else bounds the loop: a directory that keeps answering with a
    /// non-empty cookie would be paged forever. The cookie itself cannot be checked for progress (some
    /// directories return the same opaque cookie for every page), so bound the number of pages instead;
    /// at the smallest page size this still allows a million entries.
    static constexpr size_t max_pages = 1'000'000;

    while (more_pages)
    {
        if (pages_received >= max_pages)
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP search under '{}' on server '{}' did not finish after {} pages ({} entries); the directory keeps returning a paging cookie, refusing to continue",
                final_base_dn, params.name, pages_received, result.size());

        /// One page per lock scope, so that logins on other connections proceed between pages.
        std::lock_guard lock(ldap_global_mutex);

        if (!handle)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LDAP search attempted without an open connection");

        assertBoundForSearch();

        const String & bound_dn = (bound_as == BindMode::Service) ? params.lookup_bind_dn : placeholders.bind_dn;

        /// A directory that stops the search at its own limit has returned an incomplete set; a
        /// synchronisation applying it would remove every user beyond the cut. Name the account the
        /// limit applies to, because that is where an operator has to raise it.
        auto throw_if_limit_exceeded = [&](int rc)
        {
            if (rc != LDAP_SIZELIMIT_EXCEEDED && rc != LDAP_ADMINLIMIT_EXCEEDED)
                return;

            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP search under '{}' on server '{}' as '{}' was cut off by the directory's {} after {} entries ({}); "
                "raise the limit for this identity on the directory (OpenLDAP: `olcLimits ... size.prtotal`) or narrow the search filter",
                final_base_dn, params.name, bound_dn,
                rc == LDAP_SIZELIMIT_EXCEEDED ? "size limit" : "administrative limit",
                result.size(), ldap_err2string(rc));
        };

        ::berval cookie_value{};
        cookie_value.bv_val = const_cast<char *>(cookie.data());
        cookie_value.bv_len = cookie.size();

        LDAPControl * page_control = nullptr;

        SCOPE_EXIT({
            if (page_control)
            {
                ldap_control_free(page_control);
                page_control = nullptr;
            }
        });

        /// Critical: a directory that cannot page must reject the request (`unavailableCriticalExtension`)
        /// rather than answer with a single, possibly truncated, page.
        handleError(ldap_create_page_control(handle, static_cast<ber_int_t>(page_size), cookie.empty() ? nullptr : &cookie_value, /* iscritical = */ 1, &page_control));

        LDAPControl * server_controls[] = { page_control, nullptr };

        ::timeval timeout = { params.search_timeout.count(), 0 };
        LDAPMessage * msgs = nullptr;

        SCOPE_EXIT({
            if (msgs)
            {
                ldap_msgfree(msgs);
                msgs = nullptr;
            }
        });

        /// No client-requested size limit: the page size bounds every response and `max_entries` bounds the total.
        const int search_rc = ldap_search_ext_s(handle, final_base_dn.c_str(), scope, final_search_filter.c_str(), attrs.data(), 0, server_controls, nullptr, &timeout, LDAP_NO_LIMIT, &msgs);
        throw_if_limit_exceeded(search_rc);
        if (search_rc == LDAP_UNAVAILABLE_CRITICAL_EXTENSION)
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP server '{}' does not support the paged results control (RFC 2696) that the user enumeration requires: {}",
                params.name, ldap_err2string(search_rc));
        handleError(search_rc);

        more_pages = false;

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
                    if (max_entries != 0 && result.size() >= max_entries)
                        throw Exception(ErrorCodes::LDAP_ERROR,
                            "LDAP search under '{}' on server '{}' returned more than {} entries; refusing to continue",
                            final_base_dn, params.name, max_entries);

                    Entry entry;

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
                            entry.dn.assign(bv.bv_val, bv.bv_len);
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

                        /// The key is the bare name, so that a lookup by the configured attribute finds it.
                        const auto description = parseAttributeDescription(attr);
                        if (description.ranged)
                            throwRangedAttribute(entry.dn, params.name, attr);
                        const auto attribute_name = toLowerCopyASCII(description.name);

                        auto ** vals = ldap_get_values_len(handle, msg, attr);
                        if (!vals)
                            continue;

                        SCOPE_EXIT({
                            ldap_value_free_len(vals);
                            vals = nullptr;
                        });

                        std::set<String> values;
                        for (size_t i = 0; vals[i]; ++i)
                        {
                            if (vals[i]->bv_val && vals[i]->bv_len > 0)
                                values.emplace(vals[i]->bv_val, vals[i]->bv_len);
                        }

                        if (!values.empty())
                            entry.attributes[attribute_name].insert(values.begin(), values.end());
                    }

                    result.push_back(std::move(entry));
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
                    LDAPControl ** response_controls = nullptr;

                    SCOPE_EXIT({
                        if (matched_msg)
                        {
                            ldap_memfree(matched_msg);
                            matched_msg = nullptr;
                        }
                        if (error_msg)
                        {
                            ldap_memfree(error_msg);
                            error_msg = nullptr;
                        }
                        if (response_controls)
                        {
                            ldap_controls_free(response_controls);
                            response_controls = nullptr;
                        }
                    });

                    handleError(ldap_parse_result(handle, msg, &rc, &matched_msg, &error_msg, nullptr, &response_controls, 0));

                    throw_if_limit_exceeded(rc);
                    if (rc != LDAP_SUCCESS)
                        throw Exception(ErrorCodes::LDAP_ERROR, "LDAP search failed{}", describeSearchResultError(rc, error_msg, matched_msg));

                    /// The directory echoes the control with the cookie of the next page, empty after the
                    /// last one. A directory that honoured the control always includes it (RFC 2696), and one
                    /// that could not has rejected the critical request above, so no control means the
                    /// whole result fitted into this response.
                    LDAPControl * page_response = response_controls ? ldap_control_find(LDAP_CONTROL_PAGEDRESULTS, response_controls, nullptr) : nullptr;
                    if (page_response)
                    {
                        ber_int_t estimated_total = 0;
                        ::berval next_cookie{};

                        SCOPE_EXIT({
                            if (next_cookie.bv_val)
                            {
                                ber_memfree(next_cookie.bv_val);
                                next_cookie.bv_val = nullptr;
                            }
                        });

                        handleError(ldap_parse_pageresponse_control(handle, page_response, &estimated_total, &next_cookie));

                        if (next_cookie.bv_val && next_cookie.bv_len > 0)
                            cookie.assign(next_cookie.bv_val, next_cookie.bv_len);
                        else
                            cookie.clear();

                        more_pages = !cookie.empty();
                    }

                    break;
                }

                case -1:
                    throw Exception(ErrorCodes::LDAP_ERROR, "Failed to process LDAP search message");
            }
        }

        ++pages_received;
        LOG_TRACE(getLogger("LDAPClient"), "Received page {} of the LDAP search under '{}' on server '{}': {} entries so far{}",
            pages_received, final_base_dn, params.name, result.size(), more_pages ? "" : ", no more pages");
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

        /// The detection and the re-bind as the lookup identity serve the role searches only (`{user_dn}` in
        /// their templates; with a lookup identity neither may run as the user). A login without role searches,
        /// none requested (a synchronised directory) or none configured (a directory without `role_mapping`),
        /// is complete here: the lookup identity is not involved, so a broken one cannot lock the users out.
        if (!role_search_params || role_search_params->empty())
        {
            if (role_search_results)
                role_search_results->clear();
            return true;
        }

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

std::vector<LDAPSyncClient::UserEntry> LDAPSyncClient::enumerate(const UserEnumerationParams & enumeration_params, const RoleSearchParamsList & role_search_params)
{
    if (!params.hasLookupIdentity())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LDAP user enumeration on server '{}' requires 'lookup_bind_dn' and 'lookup_password'", params.name);

    /// The DN is not an attribute and cannot serve as a ClickHouse user name anyway.
    if (enumeration_params.attribute.empty() || boost::iequals(enumeration_params.attribute, "dn"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "LDAP user enumeration on server '{}' requires 'attribute' to name the attribute that holds the user name, e.g. 'sAMAccountName' or 'uid'",
            params.name);

    /// Nothing could substitute a per-user placeholder before the users are known.
    for (const auto * placeholder : {"{user_name}", "{bind_dn}", "{user_dn}"})
    {
        if (enumeration_params.base_dn.contains(placeholder) || enumeration_params.search_filter.contains(placeholder))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "LDAP user enumeration on server '{}' cannot use '{}' in 'base_dn' or 'search_filter'", params.name, placeholder);
    }

    /// Fetch the user name and, in the same request, whatever the self-lookup mappings read. A
    /// self-lookup with an empty `attribute` would mean "every attribute" to `search`, which the
    /// shortcut does not reproduce, so such a mapping is searched like any other.
    std::vector<String> attributes{enumeration_params.attribute};
    std::vector<bool> is_self_lookup;
    is_self_lookup.reserve(role_search_params.size());
    for (const auto & mapping : role_search_params)
    {
        const bool self_lookup = mapping.isSelfLookup(params.bindsAsDetectedUserDN()) && !mapping.attribute.empty();
        is_self_lookup.push_back(self_lookup);

        if (!self_lookup || boost::iequals(mapping.attribute, "dn"))
            continue;

        const bool already_requested = std::any_of(attributes.begin(), attributes.end(),
            [&](const String & attribute) { return boost::iequals(attribute, mapping.attribute); });
        if (!already_requested)
            attributes.push_back(mapping.attribute);
    }

    SCOPE_EXIT({ closeConnection(); });

    connect();
    bind(BindMode::Service);

    const auto entries = searchEntries(enumeration_params, attributes, enumeration_params.page_size, enumeration_params.max_entries);

    const auto name_attribute = toLowerCopyASCII(enumeration_params.attribute);
    auto log = getLogger("LDAPSyncClient");

    std::vector<UserEntry> users;
    users.reserve(entries.size());

    /// A malformed entry fails the whole run rather than being skipped: a skipped entry is a user missing
    /// from the plan, and the next run would remove them from ClickHouse as if they had left the directory
    /// (within what `max_removed_fraction` allows). A failed run changes nothing.
    for (const auto & entry : entries)
    {
        if (entry.dn.empty())
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP user enumeration under '{}' on server '{}' returned an entry without a DN; refusing to synchronise",
                enumeration_params.base_dn, params.name);

        const auto name_it = entry.attributes.find(name_attribute);
        const size_t name_count = (name_it == entry.attributes.end()) ? 0 : name_it->second.size();
        if (name_count != 1)
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP entry '{}' returned by the user enumeration on server '{}' has {} values of the user name attribute '{}', expected exactly one; refusing to synchronise",
                entry.dn, params.name, name_count, enumeration_params.attribute);

        UserEntry user;
        user.name = *name_it->second.begin();
        user.dn = entry.dn;
        user.external_roles.reserve(role_search_params.size());

        /// The role searches see this entry as "the user", exactly as a login of that user would: `{user_dn}` is
        /// the entry's DN, and `{bind_dn}` is what that login binds as, the detected DN in search-and-bind mode and
        /// the `bind_dn` template with the name substituted otherwise (see `openConnection`; the detection changes
        /// `{user_dn}` only). A mapping over `{bind_dn}` thus grants during the run what it grants at a login.
        placeholders.user_name = user.name;
        placeholders.user_dn = user.dn;
        placeholders.bind_dn = params.bindsAsDetectedUserDN()
            ? user.dn
            : replacePlaceholders(params.bind_dn, { {"{user_name}", escapeForDN(user.name)} });

        for (size_t i = 0; i < role_search_params.size(); ++i)
        {
            const auto & mapping = role_search_params[i];

            if (!is_self_lookup[i])
            {
                user.external_roles.emplace_back(search(mapping));
                continue;
            }

            if (boost::iequals(mapping.attribute, "dn"))
            {
                user.external_roles.emplace_back(SearchResults{user.dn});
                continue;
            }

            const auto it = entry.attributes.find(toLowerCopyASCII(mapping.attribute));
            user.external_roles.emplace_back(it == entry.attributes.end() ? SearchResults{} : it->second);
        }

        users.push_back(std::move(user));
    }

    LOG_DEBUG(log, "Enumerated {} users out of {} entries under '{}' on server '{}'",
        users.size(), entries.size(), enumeration_params.base_dn, params.name);

    return users;
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

void LDAPClient::assertBoundForSearch() const
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

std::pair<String, String> LDAPClient::resolveSearchTemplates(const SearchParams &) const
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

LDAPClient::SearchResults LDAPClient::search(const SearchParams &, bool)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

std::optional<String> LDAPClient::extractRDNValue(const String &, const String &)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

std::optional<String> LDAPClient::normalizeDN(const String &)
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

std::vector<LDAPClient::Entry> LDAPClient::searchEntries(const SearchParams &, const std::vector<String> &, UInt32, size_t)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

std::vector<LDAPSyncClient::UserEntry> LDAPSyncClient::enumerate(const UserEnumerationParams &, const RoleSearchParamsList &)
{
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "ClickHouse was built without LDAP support");
}

#endif // USE_LDAP

}
