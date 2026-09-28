#include <Common/StringUtils.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/SettingsAuthResponseParser.h>
#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ClientInfo.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/predicate.hpp>

#include <limits>
#include <optional>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LDAP_ERROR;
}

namespace
{

void parseLDAPSearchParams(LDAPClient::SearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    const bool has_base_dn = config.has(prefix + ".base_dn");
    const bool has_search_filter = config.has(prefix + ".search_filter");
    const bool has_attribute = config.has(prefix + ".attribute");
    const bool has_scope = config.has(prefix + ".scope");

    if (has_base_dn)
        params.base_dn = config.getString(prefix + ".base_dn");

    if (has_search_filter)
        params.search_filter = config.getString(prefix + ".search_filter");

    if (has_attribute)
        params.attribute = config.getString(prefix + ".attribute");

    if (has_scope)
    {
        auto scope = config.getString(prefix + ".scope");
        toLowerASCII(scope);

        if (scope == "base")           params.scope = LDAPClient::SearchParams::Scope::BASE;
        else if (scope == "one_level") params.scope = LDAPClient::SearchParams::Scope::ONE_LEVEL;
        else if (scope == "subtree")   params.scope = LDAPClient::SearchParams::Scope::SUBTREE;
        else if (scope == "children")  params.scope = LDAPClient::SearchParams::Scope::CHILDREN;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid value for 'scope' field of LDAP search parameters "
                            "in '{}' section, must be one of 'base', 'one_level', 'subtree', or 'children'", prefix);
    }
}

LDAPClient::Params::TLSProtocolVersion parseLDAPTLSProtocolVersion(
    const Poco::Util::AbstractConfiguration & config, const String & ldap_server_config, const String & entry_name)
{
    String value = config.getString(ldap_server_config + "." + entry_name);
    toLowerASCII(value);

    if (value == "ssl2")   return LDAPClient::Params::TLSProtocolVersion::SSL2;
    if (value == "ssl3")   return LDAPClient::Params::TLSProtocolVersion::SSL3;
    if (value == "tls1.0") return LDAPClient::Params::TLSProtocolVersion::TLS1_0;
    if (value == "tls1.1") return LDAPClient::Params::TLSProtocolVersion::TLS1_1;
    if (value == "tls1.2") return LDAPClient::Params::TLSProtocolVersion::TLS1_2;
    if (value == "tls1.3") return LDAPClient::Params::TLSProtocolVersion::TLS1_3;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Bad value for '{}' entry, allowed values are: "
                    "'ssl2', 'ssl3', 'tls1.0', 'tls1.1', 'tls1.2', 'tls1.3'", entry_name);
}

/// The timeouts are handed to libldap as `int` seconds (`LDAP_OPT_TIMELIMIT`) or `timeval`,
/// and a zero timeout would make every connect or operation fail immediately, so only
/// positive values that fit into an `int` are accepted.
std::chrono::seconds parseLDAPTimeout(
    const Poco::Util::AbstractConfiguration & config, const String & ldap_server_config, const String & entry_name)
{
    const UInt64 value = config.getUInt64(ldap_server_config + "." + entry_name);
    if (value == 0 || value > static_cast<UInt64>(std::numeric_limits<int>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Bad value for '{}' entry, must be a number of seconds between 1 and {}",
                        entry_name, std::numeric_limits<int>::max());

    return std::chrono::seconds{value};
}

void parseLDAPServer(LDAPClient::Params & params, const Poco::Util::AbstractConfiguration & config, const String & name)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP server name cannot be empty");

    params.name = name;

    const String ldap_server_config = "ldap_servers." + name;

    const bool has_host = config.has(ldap_server_config + ".host");
    const bool has_port = config.has(ldap_server_config + ".port");
    const bool has_bind_dn = config.has(ldap_server_config + ".bind_dn");
    const bool has_auth_dn_prefix = config.has(ldap_server_config + ".auth_dn_prefix");
    const bool has_auth_dn_suffix = config.has(ldap_server_config + ".auth_dn_suffix");
    const bool has_user_dn_detection = config.has(ldap_server_config + ".user_dn_detection");
    const bool has_lookup_bind_dn = config.has(ldap_server_config + ".lookup_bind_dn");
    const bool has_lookup_password = config.has(ldap_server_config + ".lookup_password");
    const bool has_verification_cooldown = config.has(ldap_server_config + ".verification_cooldown");
    const bool has_enable_tls = config.has(ldap_server_config + ".enable_tls");
    const bool has_tls_minimum_protocol_version = config.has(ldap_server_config + ".tls_minimum_protocol_version");
    const bool has_tls_maximum_protocol_version = config.has(ldap_server_config + ".tls_maximum_protocol_version");
    const bool has_tls_require_cert = config.has(ldap_server_config + ".tls_require_cert");
    const bool has_tls_cert_file = config.has(ldap_server_config + ".tls_cert_file");
    const bool has_tls_key_file = config.has(ldap_server_config + ".tls_key_file");
    const bool has_tls_ca_cert_file = config.has(ldap_server_config + ".tls_ca_cert_file");
    const bool has_tls_ca_cert_dir = config.has(ldap_server_config + ".tls_ca_cert_dir");
    const bool has_tls_cipher_suite = config.has(ldap_server_config + ".tls_cipher_suite");
    const bool has_search_limit = config.has(ldap_server_config + ".search_limit");
    const bool has_follow_referrals = config.has(ldap_server_config + ".follow_referrals");
    const bool has_operation_timeout = config.has(ldap_server_config + ".operation_timeout");
    const bool has_network_timeout = config.has(ldap_server_config + ".network_timeout");
    const bool has_search_timeout = config.has(ldap_server_config + ".search_timeout");

    if (!has_host)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'host' entry");

    params.host = config.getString(ldap_server_config + ".host");

    if (params.host.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'host' entry");

    if (has_bind_dn)
    {
        if (has_auth_dn_prefix || has_auth_dn_suffix)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deprecated 'auth_dn_prefix' and 'auth_dn_suffix' entries cannot be used with 'bind_dn' entry");

        params.bind_dn = config.getString(ldap_server_config + ".bind_dn");
    }
    else if (has_auth_dn_prefix || has_auth_dn_suffix)
    {
        std::string auth_dn_prefix = config.getString(ldap_server_config + ".auth_dn_prefix");
        std::string auth_dn_suffix = config.getString(ldap_server_config + ".auth_dn_suffix");
        params.bind_dn = auth_dn_prefix + "{user_name}" + auth_dn_suffix;
    }
    else if (has_lookup_bind_dn)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'bind_dn' is required; use '{}' to bind as the detected user DN", LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER);
    }
    else
    {
        /// Without any bind DN the client would issue a simple bind with an empty name, i.e. an
        /// anonymous or unauthenticated bind, and consider every password "verified".
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Either 'bind_dn' or 'lookup_bind_dn' must be specified");
    }

    if (params.bind_dn.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'bind_dn' entry");

    /// `{user_dn}` in `bind_dn` is only meaningful as the whole value: it selects search-and-bind,
    /// where the DN to bind as is the one `user_dn_detection` finds under the lookup identity.
    const bool binds_as_detected_user_dn = params.bindsAsDetectedUserDN();
    if (!binds_as_detected_user_dn && params.bind_dn.contains(LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'bind_dn' containing '{}' must be exactly '{}'",
            LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER, LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER);

    if (has_user_dn_detection)
    {
        if (!params.user_dn_detection)
        {
            params.user_dn_detection.emplace();
            params.user_dn_detection->attribute = "dn";
        }

        parseLDAPSearchParams(*params.user_dn_detection, config, ldap_server_config + ".user_dn_detection");
    }

    /// Optional service-account credentials used by
    /// `IAccessStorage::find(..., force_external_lookup=true)` to resolve a user name
    /// without the user's own password. Both must be provided together, and the lookup
    /// path also requires `user_dn_detection` to confirm the user exists.
    if (has_lookup_bind_dn != has_lookup_password)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Both 'lookup_bind_dn' and 'lookup_password' must be specified together");

    if (binds_as_detected_user_dn)
    {
        /// Search-and-bind: the user is located by `user_dn_detection` under the lookup identity
        /// before any DN is known, so the detection templates can only depend on `{user_name}`
        /// and the search must yield a DN to bind as. These checks run before the generic
        /// `lookup_bind_dn` ones below so that a search-and-bind configuration gets the
        /// mode-specific message (the generic hint to use `{bind_dn}`/`{user_dn}` does not
        /// apply here).
        if (!has_lookup_bind_dn)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'bind_dn' = '{}' requires 'lookup_bind_dn' and 'lookup_password'", LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER);

        if (!params.user_dn_detection)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'bind_dn' = '{}' requires 'user_dn_detection'", LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER);

        const String & udd_base_dn = params.user_dn_detection->base_dn;
        const String & udd_search_filter = params.user_dn_detection->search_filter;

        /// With `bind_dn` = `{user_dn}` the helper reduces to the literal `{user_name}` check.
        if (!params.templateDependsOnUserName(udd_base_dn) && !params.templateDependsOnUserName(udd_search_filter))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'bind_dn' = '{}' requires 'user_dn_detection.base_dn' or 'user_dn_detection.search_filter' to contain '{{user_name}}'",
                LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER);

        for (const auto * placeholder : {"{bind_dn}", "{user_dn}"})
        {
            if (udd_base_dn.contains(placeholder) || udd_search_filter.contains(placeholder))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "'user_dn_detection' cannot reference '{}' when 'bind_dn' = '{}': the user DN is not known before the detection",
                    placeholder, LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER);
        }

        /// Whatever `attribute` returns is what the user's password is verified against, so it
        /// must be the entry DN; any other attribute would be bound as a DN and every login would fail.
        if (!boost::iequals(params.user_dn_detection->attribute, "dn"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'user_dn_detection.attribute' must be 'dn' when 'bind_dn' = '{}', got '{}'",
                LDAPClient::Params::DETECTED_USER_DN_PLACEHOLDER, params.user_dn_detection->attribute);
    }

    if (has_lookup_bind_dn)
    {
        params.lookup_bind_dn = config.getString(ldap_server_config + ".lookup_bind_dn");
        params.lookup_password = config.getString(ldap_server_config + ".lookup_password");

        if (params.lookup_bind_dn.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'lookup_bind_dn' entry");

        /// Fail closed: an empty `lookup_password` with a non-empty `lookup_bind_dn` would
        /// issue an LDAP unauthenticated simple bind, which directories may accept as an
        /// anonymous bind. That would let the service-bind path resolve users without
        /// actually authenticating the lookup service account, defeating the purpose of
        /// the service credentials and silently widening who can be impersonated. Mirror
        /// the same fail-closed check that the user-mode bind already applies to
        /// `params.password`.
        if (params.lookup_password.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'lookup_password' entry");

        /// The lookup identity is fixed by configuration and must not vary with the login.
        if (params.lookup_bind_dn.contains("{user_name}"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'lookup_bind_dn' must not contain '{{user_name}}'");

        if (!params.user_dn_detection)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'lookup_bind_dn' requires 'user_dn_detection' to be configured");

        /// `user_dn_detection` must depend on the requested user name; otherwise a static
        /// query (e.g. `search_filter=(cn=janedoe)`) returning a single entry would let
        /// `EXECUTE AS some_other_name` resolve to that entry's DN. The same rule decides in
        /// `detectUserDN` whether `LDAP_NO_SUCH_OBJECT` for the base means "user not found".
        const String & udd_base_dn = params.user_dn_detection->base_dn;
        const String & udd_search_filter = params.user_dn_detection->search_filter;
        if (!params.templateDependsOnUserName(udd_base_dn) && !params.templateDependsOnUserName(udd_search_filter))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'lookup_bind_dn' requires 'user_dn_detection' to depend on the requested user name; "
                "use '{{user_name}}' in 'user_dn_detection.base_dn' or '.search_filter', "
                "or use '{{bind_dn}}'/'{{user_dn}}' with a 'bind_dn' template that contains '{{user_name}}'");

        /// Whatever `attribute` returns is stored as the user DN and consumed as such wherever `{user_dn}` appears
        /// (role mappings, the synchronisation, `EXECUTE AS`); any other attribute would silently turn a mapping such
        /// as `(member={user_dn})` into a search over a plain value and strip the mapped roles. Search-and-bind
        /// reports the same for the DN that is bound (below); this covers the `bind_dn` template with a lookup identity.
        if (!params.bindsAsDetectedUserDN() && !boost::iequals(params.user_dn_detection->attribute, "dn"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'user_dn_detection.attribute' must be 'dn' when 'lookup_bind_dn' is set, got '{}'",
                params.user_dn_detection->attribute);
    }

    if (has_verification_cooldown)
        params.verification_cooldown = std::chrono::seconds{config.getUInt64(ldap_server_config + ".verification_cooldown")};

    if (has_operation_timeout)
        params.operation_timeout = parseLDAPTimeout(config, ldap_server_config, "operation_timeout");

    if (has_network_timeout)
        params.network_timeout = parseLDAPTimeout(config, ldap_server_config, "network_timeout");

    if (has_search_timeout)
        params.search_timeout = parseLDAPTimeout(config, ldap_server_config, "search_timeout");

    if (has_enable_tls)
    {
        String enable_tls_lc_str = config.getString(ldap_server_config + ".enable_tls");
        toLowerASCII(enable_tls_lc_str);

        if (enable_tls_lc_str == "starttls")
            params.enable_tls = LDAPClient::Params::TLSEnable::YES_STARTTLS;
        else if (config.getBool(ldap_server_config + ".enable_tls"))
            params.enable_tls = LDAPClient::Params::TLSEnable::YES;
        else
            params.enable_tls = LDAPClient::Params::TLSEnable::NO;
    }

    if (has_tls_minimum_protocol_version)
        params.tls_minimum_protocol_version = parseLDAPTLSProtocolVersion(config, ldap_server_config, "tls_minimum_protocol_version");

    if (has_tls_maximum_protocol_version)
    {
        params.tls_maximum_protocol_version = parseLDAPTLSProtocolVersion(config, ldap_server_config, "tls_maximum_protocol_version");

        /// The enumerators are ordered by protocol age, see `TLSProtocolVersion`. The default minimum bounds the maximum
        /// as well; comparing against the unset `std::optional` directly would rank it below every version and skip the check.
        if (*params.tls_maximum_protocol_version
            < params.tls_minimum_protocol_version.value_or(LDAPClient::Params::default_tls_minimum_protocol_version))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Bad value for 'tls_maximum_protocol_version' entry, "
                            "must not be lower than 'tls_minimum_protocol_version'");
    }

    if (has_tls_require_cert)
    {
        String tls_require_cert_lc_str = config.getString(ldap_server_config + ".tls_require_cert");
        toLowerASCII(tls_require_cert_lc_str);

        if (tls_require_cert_lc_str == "never")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::NEVER;
        else if (tls_require_cert_lc_str == "allow")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::ALLOW;
        else if (tls_require_cert_lc_str == "try")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::TRY;
        else if (tls_require_cert_lc_str == "demand")
            params.tls_require_cert = LDAPClient::Params::TLSRequireCert::DEMAND;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Bad value for 'tls_require_cert' entry, allowed values are: "
                            "'never', 'allow', 'try', 'demand'");
    }

    if (has_tls_cert_file)
        params.tls_cert_file = config.getString(ldap_server_config + ".tls_cert_file");

    if (has_tls_key_file)
        params.tls_key_file = config.getString(ldap_server_config + ".tls_key_file");

    if (has_tls_ca_cert_file)
        params.tls_ca_cert_file = config.getString(ldap_server_config + ".tls_ca_cert_file");

    if (has_tls_ca_cert_dir)
        params.tls_ca_cert_dir = config.getString(ldap_server_config + ".tls_ca_cert_dir");

    if (has_tls_cipher_suite)
        params.tls_cipher_suite = config.getString(ldap_server_config + ".tls_cipher_suite");

    if (has_port)
    {
        UInt32 port = config.getUInt(ldap_server_config + ".port");
        if (port > 65535)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad value for 'port' entry");

        params.port = static_cast<UInt16>(port);
    }
    else
        params.port = (params.enable_tls == LDAPClient::Params::TLSEnable::YES ? 636 : 389);

    if (has_search_limit)
        params.search_limit = static_cast<UInt32>(config.getUInt64(ldap_server_config + ".search_limit"));

    if (has_follow_referrals)
        params.follow_referrals = config.getBool(ldap_server_config + ".follow_referrals");
}

void parseKerberosParams(GSSAcceptorContext::Params & params, const Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("kerberos", keys);

    std::size_t reealm_key_count = 0;
    std::size_t principal_keys_count = 0;

    for (auto key : keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            key.resize(bracket_pos);

        toLowerASCII(key);

        reealm_key_count += (key == "realm");
        principal_keys_count += (key == "principal");
    }

    if (reealm_key_count > 0 && principal_keys_count > 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Realm and principal name cannot be specified simultaneously");

    if (reealm_key_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple realm sections are not allowed");

    if (principal_keys_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple principal sections are not allowed");

    params.realm = config.getString("kerberos.realm", "");
    params.principal = config.getString("kerberos.principal", "");
    params.keytab = config.getString("kerberos.keytab", "");
}

HTTPAuthClientParams parseHTTPAuthParams(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    HTTPAuthClientParams http_auth_params;

    http_auth_params.uri = config.getString(prefix + ".uri");

    size_t connection_timeout_ms = config.getInt(prefix + ".connection_timeout_ms", 1000);
    size_t receive_timeout_ms = config.getInt(prefix + ".receive_timeout_ms", 1000);
    size_t send_timeout_ms = config.getInt(prefix + ".send_timeout_ms", 1000);
    http_auth_params.timeouts = ConnectionTimeouts()
        .withConnectionTimeout(Poco::Timespan(connection_timeout_ms * 1000))
        .withReceiveTimeout(Poco::Timespan(receive_timeout_ms * 1000))
        .withSendTimeout(Poco::Timespan(send_timeout_ms * 1000));

    http_auth_params.max_tries = config.getInt(prefix + ".max_tries", 3);
    http_auth_params.retry_initial_backoff_ms = config.getInt(prefix + ".retry_initial_backoff_ms", 50);
    http_auth_params.retry_max_backoff_ms = config.getInt(prefix + ".retry_max_backoff_ms", 1000);

    Strings forward_headers;
    config.keys(prefix + ".forward_headers", forward_headers);
    for (const auto & header : forward_headers)
    {
        String name = config.getString(prefix + ".forward_headers." + header);
        http_auth_params.forward_headers.push_back(name);
    }

    return http_auth_params;
}

}

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    parseLDAPSearchParams(params, config, prefix);

    const bool has_prefix = config.has(prefix + ".prefix");
    const bool has_rdn_attribute = config.has(prefix + ".rdn_attribute");
    const bool has_groups = config.has(prefix + ".groups");

    if (has_prefix)
        params.prefix = config.getString(prefix + ".prefix");

    if (has_rdn_attribute)
    {
        params.rdn_attribute = config.getString(prefix + ".rdn_attribute");
        if (params.rdn_attribute.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'rdn_attribute' entry in '{}' section", prefix);
    }

    if (has_groups)
    {
        Poco::Util::AbstractConfiguration::Keys group_keys;
        config.keys(prefix + ".groups", group_keys);

        for (const auto & key : group_keys)
        {
            if (key != "group" && !key.starts_with("group["))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown entry '{}' in '{}.groups' section, only 'group' entries are allowed", key, prefix);

            const auto group = config.getString(prefix + ".groups." + key);
            if (group.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'group' entry in '{}.groups' section", prefix);

            /// The role name candidate `prefix` is stripped from: the plain name itself, or the `rdn_attribute`
            /// value of a DN-form entry. The lookup maps are filled here, where the normalized forms are computed;
            /// duplicates surface as failed insertions (the two maps cannot collide because only DNs contain `=`).
            String candidate;
            bool inserted = false;
            if (LDAPClient::RoleSearchParams::isGroupDN(group))
            {
                if (params.rdn_attribute.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Group '{}' in '{}.groups' section contains '=' and is therefore treated as a DN, "
                                    "which requires 'rdn_attribute' to be set", group, prefix);

                const auto normalized_dn = LDAPClient::normalizeDN(group);
                if (!normalized_dn)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Group '{}' in '{}.groups' section contains '=' and is therefore treated as a DN, "
                                    "but it is not a valid DN", group, prefix);

                const auto rdn_value = LDAPClient::extractRDNValue(group, params.rdn_attribute);
                if (!rdn_value)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Group '{}' in '{}.groups' section contains '=' and is therefore treated as a DN, "
                                    "but it has no '{}' RDN", group, prefix, params.rdn_attribute);

                candidate = *rdn_value;
                inserted = params.dn_groups.emplace(*normalized_dn, candidate).second;
            }
            else
            {
                candidate = group;
                inserted = params.plain_groups.emplace(toLowerCopyASCII(group), group).second;
            }

            if (!inserted)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate group '{}' in '{}.groups' section", group, prefix);

            /// The role name is the candidate with `prefix` removed (see `LDAPAccessStorage::mapExternalRolesNoLock`),
            /// so an entry that does not start with the prefix, or has nothing left after it, can never grant a role.
            /// Such an entry is a dead configuration; reject it instead of silently ignoring it at every login.
            if (!(candidate.size() > params.prefix.size() && candidate.starts_with(params.prefix)))
            {
                if (params.prefix.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Group '{}' in '{}.groups' section has an empty '{}' RDN value and can never be mapped to a role",
                                    group, prefix, params.rdn_attribute);

                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Group '{}' in '{}.groups' section does not start with the configured prefix '{}' or is equal to it, "
                                "so it can never be mapped to a role", group, prefix, params.prefix);
            }

            params.groups.push_back(group);
        }
    }

    /// Without an allow-list or a prefix every RDN value of every group the user belongs to would be
    /// tried as a role name, which makes any LDAP group with a matching name grant a ClickHouse role.
    if (!params.rdn_attribute.empty() && params.groups.empty() && params.prefix.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "'rdn_attribute' in '{}' section requires a non-empty 'groups' list or a non-empty 'prefix'", prefix);
}

void parseLDAPUserEnumerationParams(LDAPClient::UserEnumerationParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    for (const auto * key : {"base_dn", "search_filter", "attribute"})
    {
        if (!config.has(prefix + "." + key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing '{}' entry in '{}' section", key, prefix);
    }

    parseLDAPSearchParams(params, config, prefix);

    if (params.base_dn.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'base_dn' entry in '{}' section", prefix);

    if (params.search_filter.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'search_filter' entry in '{}' section", prefix);

    /// The DN is not an attribute and cannot serve as a ClickHouse user name.
    if (params.attribute.empty() || boost::iequals(params.attribute, "dn"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "'attribute' in '{}' section must name the attribute that holds the user name, e.g. 'sAMAccountName' or 'uid'", prefix);

    /// The enumeration runs once for the whole directory, so nothing could substitute a per-user placeholder.
    for (const auto * placeholder : {"{user_name}", "{bind_dn}", "{user_dn}"})
    {
        if (params.base_dn.contains(placeholder) || params.search_filter.contains(placeholder))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "'base_dn' and 'search_filter' in '{}' section must not contain '{}': the enumeration is not performed on behalf of a user",
                            prefix, placeholder);
    }

    if (config.has(prefix + ".page_size"))
    {
        const UInt64 page_size = config.getUInt64(prefix + ".page_size");
        if (page_size < 1 || page_size > 1000)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'page_size' in '{}' section must be between 1 and 1000, got {}", prefix, page_size);
        params.page_size = static_cast<UInt32>(page_size);
    }
}

void ExternalAuthenticators::resetImpl()
{
    ldap_client_params_blueprint.clear();
    ldap_server_parse_errors.clear();
    ldap_caches.clear();
    kerberos_params.reset();
}

void ExternalAuthenticators::reset()
{
    std::lock_guard lock(mutex);
    resetImpl();
}

void ExternalAuthenticators::setConfiguration(const Poco::Util::AbstractConfiguration & config, LoggerPtr log)
{
    std::lock_guard lock(mutex);
    resetImpl();

    Poco::Util::AbstractConfiguration::Keys all_keys;
    config.keys("", all_keys);

    std::size_t ldap_servers_key_count = 0;
    std::size_t kerberos_keys_count = 0;
    std::size_t http_auth_server_keys_count = 0;

    const String http_auth_servers_config = "http_authentication_servers";

    for (auto key : all_keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            key.resize(bracket_pos);

        toLowerASCII(key);

        ldap_servers_key_count += (key == "ldap_servers");
        kerberos_keys_count += (key == "kerberos");
        http_auth_server_keys_count += (key == http_auth_servers_config);
    }

    if (ldap_servers_key_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple ldap_servers sections are not allowed");

    if (kerberos_keys_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple kerberos sections are not allowed");

    if (http_auth_server_keys_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple http_authentication_servers sections are not allowed");

    Poco::Util::AbstractConfiguration::Keys http_auth_server_names;
    config.keys(http_auth_servers_config, http_auth_server_names);
    http_auth_servers.clear();
    for (const auto & http_auth_server_name : http_auth_server_names)
    {
        String prefix = fmt::format("{}.{}", http_auth_servers_config, http_auth_server_name);
        try
        {
            http_auth_servers[http_auth_server_name] = parseHTTPAuthParams(config, prefix);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Could not parse HTTP auth server" + backQuote(http_auth_server_name));
        }
    }

    /// Parse every server into local maps and publish both at once, so a reload never
    /// leaves a half-applied blueprint behind.
    Poco::Util::AbstractConfiguration::Keys ldap_server_names;
    config.keys("ldap_servers", ldap_server_names);
    LDAPParams new_blueprint;
    LDAPParseErrors new_parse_errors;
    for (auto ldap_server_name : ldap_server_names)
    {
        const auto bracket_pos = ldap_server_name.find('[');
        if (bracket_pos != std::string::npos)
            ldap_server_name.resize(bracket_pos);

        /// Remember the error so that `checkLDAPCredentials` and `findLDAPUser` can surface
        /// it at use. Without this the parsed-out server is dropped silently: every login
        /// through it fails as "no such user" and `EXECUTE AS` collapses to `UNKNOWN_USER`,
        /// hiding a real operator misconfiguration. The name is also removed from the
        /// blueprint: with two entries sharing a name the first one has already been parsed
        /// when the second fails, and it must not stay usable through the first entry.
        const auto record_parse_error = [&](String message)
        {
            tryLogCurrentException(log, "Could not parse LDAP server " + backQuote(ldap_server_name));
            new_blueprint.erase(ldap_server_name);
            new_parse_errors[ldap_server_name] = std::move(message);
        };

        try
        {
            if (new_blueprint.contains(ldap_server_name) || new_parse_errors.contains(ldap_server_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple LDAP servers with the same name are not allowed");

            LDAPClient::Params ldap_client_params_tmp;
            parseLDAPServer(ldap_client_params_tmp, config, ldap_server_name);
            new_blueprint.emplace(ldap_server_name, std::move(ldap_client_params_tmp));
        }
        catch (const Exception & e)
        {
            record_parse_error(e.message());
        }
        catch (...)
        {
            record_parse_error(getCurrentExceptionMessage(/* with_stacktrace = */ false));
        }
    }
    ldap_client_params_blueprint.swap(new_blueprint);
    ldap_server_parse_errors.swap(new_parse_errors);

    kerberos_params.reset();
    try
    {
        if (kerberos_keys_count > 0)
        {
            GSSAcceptorContext::Params kerberos_params_tmp;
            parseKerberosParams(kerberos_params_tmp, config);
            kerberos_params = std::move(kerberos_params_tmp);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Could not parse Kerberos section");
    }
}

static UInt128 computeParamsHash(const LDAPClient::Params & params, const LDAPClient::RoleSearchParamsList * role_search_params)
{
    SipHash hash;
    params.updateHash(hash);
    if (role_search_params)
    {
        for (const auto & params_instance : *role_search_params)
        {
            params_instance.updateHash(hash);
        }
    }

    return hash.get128();
}

LDAPClient::Params ExternalAuthenticators::getLDAPServerParams(const String & server) const
{
    /// A recorded parse error wins over anything in the blueprint: a server that failed to
    /// parse must fail closed with the original reason, never be served from an entry that
    /// happened to parse under the same name (`setConfiguration` also drops such names from
    /// the blueprint; checking the errors first keeps this true regardless of that).
    const auto eit = ldap_server_parse_errors.find(server);
    if (eit != ldap_server_parse_errors.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP server '{}' is misconfigured: {}", server, eit->second);

    const auto pit = ldap_client_params_blueprint.find(server);
    if (pit != ldap_client_params_blueprint.end())
        return pit->second;

    /// The directory references a name with no `<ldap_servers>` block at all (e.g. a typo).
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP server '{}' is not configured", server);
}

bool ExternalAuthenticators::checkLDAPCredentials(const String & server, const BasicCredentials & credentials,
    const LDAPClient::RoleSearchParamsList * role_search_params, LDAPClient::SearchResultsList * role_search_results) const
{
    std::optional<LDAPClient::Params> params;
    UInt128 params_hash = 0;

    {
        std::lock_guard lock(mutex);

        // Retrieve the server parameters.
        params = getLDAPServerParams(server);
        params->user = credentials.getUserName();
        params->password = credentials.getPassword();

        params_hash = computeParamsHash(*params, role_search_params);

        // Check the cache, but only if the caching is enabled at all.
        if (params->verification_cooldown > std::chrono::seconds{0})
        {
            const auto cit = ldap_caches.find(server);
            if (cit != ldap_caches.end())
            {
                auto & cache = cit->second;

                const auto eit = cache.find(credentials.getUserName());
                if (eit != cache.end())
                {
                    const auto & entry = eit->second;
                    const auto last_check_period = std::chrono::steady_clock::now() - entry.last_successful_authentication_timestamp;

                    if (
                        // Forbid the initial values explicitly.
                        entry.last_successful_params_hash != 0 &&
                        entry.last_successful_authentication_timestamp != std::chrono::steady_clock::time_point{} &&

                        // Check if we can safely "reuse" the result of the previous successful password verification.
                        entry.last_successful_params_hash == params_hash &&
                        last_check_period >= std::chrono::seconds{0} &&
                        last_check_period <= params->verification_cooldown &&

                        // Ensure that search_params are compatible.
                        (
                            role_search_params == nullptr ?
                            entry.last_successful_role_search_results.empty() :
                            role_search_params->size() == entry.last_successful_role_search_results.size()
                        )
                    )
                    {
                        if (role_search_results)
                            *role_search_results = entry.last_successful_role_search_results;

                        return true;
                    }

                    // Erase the entry, if expired.
                    if (last_check_period > params->verification_cooldown)
                        cache.erase(eit);
                }

                // Erase the cache, if empty.
                if (cache.empty())
                    ldap_caches.erase(cit);
            }
        }
    }

    LDAPSimpleAuthClient client(params.value());
    const auto result = client.authenticate(role_search_params, role_search_results);
    const auto current_check_timestamp = std::chrono::steady_clock::now();

    std::lock_guard lock(mutex);

    /// `SYSTEM RELOAD CONFIG` can replace or remove the server definition while the password is being verified.
    /// Neither outcome obtained under the old definition counts for the new one: a success must not be cached for
    /// it, and a failure must not be reported as one either, since `MultipleAccessStorage` would then let the
    /// storages that follow serve the same name. Both are an error, like the same post-check of `findLDAPUser` and
    /// `enumerateLDAPUsers`.
    const auto pit = ldap_client_params_blueprint.find(server);
    if (pit == ldap_client_params_blueprint.end())
        throw Exception(ErrorCodes::LDAP_ERROR,
            "LDAP server '{}' was removed from the configuration while the password of user '{}' was being verified; refusing to use the result",
            server, credentials.getUserName());

    auto new_params = pit->second;
    new_params.user = credentials.getUserName();
    new_params.password = credentials.getPassword();

    if (params_hash != computeParamsHash(new_params, role_search_params))
        throw Exception(ErrorCodes::LDAP_ERROR,
            "The definition of LDAP server '{}' changed while the password of user '{}' was being verified; refusing to use the result",
            server, credentials.getUserName());

    if (!result)
        return false;

    // Update the cache, but only if this is the latest check.
    auto & entry = ldap_caches[server][credentials.getUserName()];
    if (entry.last_successful_authentication_timestamp < current_check_timestamp)
    {
        entry.last_successful_params_hash = params_hash;
        entry.last_successful_authentication_timestamp = current_check_timestamp;

        if (role_search_results)
            entry.last_successful_role_search_results = *role_search_results;
        else
            entry.last_successful_role_search_results.clear();
    }
    else if (
        entry.last_successful_params_hash != params_hash ||
        (
            role_search_params == nullptr ?
            !entry.last_successful_role_search_results.empty() :
            role_search_params->size() != entry.last_successful_role_search_results.size()
        )
    )
    {
        // Somehow a newer check with different params/password succeeded, so the current result is obsolete and we discard it.
        return false;
    }

    return true;
}

bool ExternalAuthenticators::findLDAPUser(const String & server, const String & user_name,
    const LDAPClient::RoleSearchParamsList * role_search_params, LDAPClient::SearchResultsList * role_search_results) const
{
    if (user_name.empty())
        return false;

    std::optional<LDAPClient::Params> params;
    UInt128 params_hash = 0;

    {
        std::lock_guard lock(mutex);

        params = getLDAPServerParams(server);

        /// The service-bind path is opt-in: a server without `lookup_bind_dn` configured does
        /// not participate in forced lookups. Returning false here lets the caller fall through
        /// to other access storages.
        if (!params->hasLookupIdentity())
            return false;

        params->user = user_name;
        /// The user's own password is not used in service-bind mode; clear it so it cannot
        /// accidentally bleed into the LDAP exchange via cached state.
        params->password.clear();

        params_hash = computeParamsHash(*params, role_search_params);
    }

    LDAPSimpleAuthClient client(params.value());
    const auto result = client.find(role_search_params, role_search_results);

    /// `SYSTEM RELOAD CONFIG` can mutate `ldap_client_params_blueprint` between the snapshot above and the
    /// bind/search round-trip. Neither outcome obtained under the old definition counts for the new one: a user
    /// found must not be materialised for it, and a miss must not be reported as one either, since
    /// `LDAPAccessStorage::findImpl` would then let the storages that follow serve the same name. Both are an
    /// error, like the same post-check of `checkLDAPCredentials` and `enumerateLDAPUsers`.
    {
        std::lock_guard lock(mutex);

        const auto pit = ldap_client_params_blueprint.find(server);
        if (pit == ldap_client_params_blueprint.end())
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP server '{}' was removed from the configuration while user '{}' was being looked up; refusing to use the result",
                server, user_name);

        auto new_params = pit->second;
        new_params.user = user_name;
        new_params.password.clear();

        if (params_hash != computeParamsHash(new_params, role_search_params))
            throw Exception(ErrorCodes::LDAP_ERROR,
                "The definition of LDAP server '{}' changed while user '{}' was being looked up; refusing to use the result",
                server, user_name);
    }

    return result;
}

bool ExternalAuthenticators::hasLDAPLookupIdentity(const String & server) const
{
    std::lock_guard lock(mutex);
    const auto it = ldap_client_params_blueprint.find(server);
    return it != ldap_client_params_blueprint.end() && it->second.hasLookupIdentity();
}

void ExternalAuthenticators::checkLDAPServerCanEnumerate(const String & server) const
{
    std::lock_guard lock(mutex);

    /// Not configured, or not parsed (a duplicated name included): refused with the same reason a login gets.
    const auto params = getLDAPServerParams(server);

    if (!params.hasLookupIdentity())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP sync requires 'lookup_bind_dn' on server '{}'", server);
}


std::vector<LDAPSyncClient::UserEntry> ExternalAuthenticators::enumerateLDAPUsers(const String & server,
    const LDAPClient::UserEnumerationParams & enumeration_params, const LDAPClient::RoleSearchParamsList & role_search_params) const
{
    std::optional<LDAPClient::Params> params;

    {
        std::lock_guard lock(mutex);
        params = getLDAPServerParams(server);
    }

    /// The enumeration reads the directory on behalf of nobody in particular, so only a service
    /// account can run it; without one there is no identity to bind as. `checkLDAPServerCanEnumerate`
    /// refuses such a configuration before it is applied; this is the backstop.
    if (!params->hasLookupIdentity())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LDAP sync requires 'lookup_bind_dn' on server '{}'", server);

    /// `user`/`password` stay empty: the client never binds as a user here.
    const auto params_hash = computeParamsHash(*params, &role_search_params);
    LDAPSyncClient client(params.value());
    auto entries = client.enumerate(enumeration_params, role_search_params);

    {
        /// `SYSTEM RELOAD CONFIG` can replace or remove the server definition while the enumeration is
        /// running. Entries read from the old host or under the old lookup identity must not become
        /// an authoritative snapshot of the new definition, so the run fails and is retried; the same
        /// post-check protects `checkLDAPCredentials` and `findLDAPUser`.
        std::lock_guard lock(mutex);

        const auto pit = ldap_client_params_blueprint.find(server);
        if (pit == ldap_client_params_blueprint.end())
            throw Exception(ErrorCodes::LDAP_ERROR,
                "LDAP server '{}' was removed from the configuration while it was being enumerated; refusing to apply the result",
                server);

        if (params_hash != computeParamsHash(pit->second, &role_search_params))
            throw Exception(ErrorCodes::LDAP_ERROR,
                "The definition of LDAP server '{}' changed while it was being enumerated; refusing to apply the result",
                server);
    }

    return entries;
}

bool ExternalAuthenticators::checkKerberosCredentials(const String & realm, const GSSAcceptorContext & credentials) const
{
    std::lock_guard lock(mutex);

    if (!kerberos_params.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kerberos is not enabled");

    if (!credentials.isReady())
        return false;

    if (credentials.isFailed())
        return false;

    if (!realm.empty() && realm != credentials.getRealm())
        return false;

    return true;
}

GSSAcceptorContext::Params ExternalAuthenticators::getKerberosParams() const
{
    std::lock_guard lock(mutex);

    if (!kerberos_params.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kerberos is not enabled");

    return kerberos_params.value();
}

HTTPAuthClientParams ExternalAuthenticators::getHTTPAuthenticationParams(const String& server) const
{
    std::lock_guard lock{mutex};

    const auto it = http_auth_servers.find(server);
    if (it == http_auth_servers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP server '{}' is not configured", server);
    return it->second;
}

bool ExternalAuthenticators::checkHTTPBasicCredentials(
    const String & server, const BasicCredentials & credentials, const ClientInfo & client_info, SettingsChanges & settings) const
{
    auto params = getHTTPAuthenticationParams(server);
    HTTPBasicAuthClient<SettingsAuthResponseParser> client(params);

    auto [is_ok, settings_from_auth_server] = client.authenticate(credentials.getUserName(), credentials.getPassword(), client_info.http_headers);

    if (is_ok)
        std::ranges::move(settings_from_auth_server, std::back_inserter(settings));

    return is_ok;
}
}
