#include <Access/AccessControl.h>
#include <Access/AuthenticationData.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Common/OpenSSLHelpers.h>
#include <Poco/SHA1Engine.h>
#include <base/types.h>
#include <base/hex.h>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string/case_conv.hpp>

#include "config.h"

#if USE_SSL
#     include <openssl/crypto.h>
#     include <openssl/rand.h>
#     include <openssl/err.h>
#endif

#if USE_BCRYPT
#     include <bcrypt.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int OPENSSL_ERROR;
}

using Digest = IPasswordAuthData::Digest;

String IPasswordAuthData::Util::digestToString(const Digest & text) { return String(text.data(), text.data() + text.size()); }
Digest IPasswordAuthData::Util::stringToDigest(std::string_view text) { return Digest(text.data(), text.data() + text.size()); }
Digest IPasswordAuthData::Util::encodeSHA1(const Digest & text) { return encodeSHA1(std::string_view{reinterpret_cast<const char *>(text.data()), text.size()}); }
Digest IPasswordAuthData::Util::encodeDoubleSHA1(std::string_view text) { return encodeSHA1(encodeSHA1(text)); }
Digest IPasswordAuthData::Util::encodeDoubleSHA1(const Digest & text) { return encodeSHA1(encodeSHA1(text)); }

Digest IPasswordAuthData::Util::encodeSHA256(std::string_view text [[maybe_unused]])
{
#if USE_SSL
    Digest hash;
    hash.resize(32);
    ::DB::encodeSHA256(text, hash.data());
    return hash;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SHA256 passwords support is disabled, because ClickHouse was built without SSL library");
#endif
}

Digest IPasswordAuthData::Util::encodeSHA1(std::string_view text)
{
    Poco::SHA1Engine engine;
    engine.update(text.data(), text.size());
    return engine.digest();
}

IPasswordAuthData::Digest IPasswordAuthData::Util::encodeBcrypt(std::string_view text [[maybe_unused]], int workfactor [[maybe_unused]])
{
#if USE_BCRYPT
    if (text.size() > 72)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "bcrypt does not support passwords with a length of more than 72 bytes");

    char salt[BCRYPT_HASHSIZE];
    Digest hash;
    hash.resize(64);

    int ret = bcrypt_gensalt(workfactor, salt);
    if (ret != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BCrypt library failed: bcrypt_gensalt returned {}", ret);

    ret = bcrypt_hashpw(text.data(), salt, reinterpret_cast<char *>(hash.data()));
    if (ret != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BCrypt library failed: bcrypt_hashpw returned {}", ret);

    return hash;
#else
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "bcrypt passwords support is disabled, because ClickHouse was built without bcrypt library");
#endif
}

bool IPasswordAuthData::Util::checkPasswordBcrypt(std::string_view password [[maybe_unused]], const Digest & password_bcrypt [[maybe_unused]])
{
#if USE_BCRYPT
    int ret = bcrypt_checkpw(password.data(), reinterpret_cast<const char *>(password_bcrypt.data()));
    if (ret == -1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BCrypt library failed: bcrypt_checkpw returned {}", ret);
    return (ret == 0);
#else
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "bcrypt passwords support is disabled, because ClickHouse was built without bcrypt library");
#endif
}


bool operator ==(const IAuthenticationData & lhs, const IAuthenticationData & rhs)
{
    return lhs.equal(rhs);
}

bool IAuthenticationData::equal(const IAuthenticationData& other) const
{
    return getType() == other.getType();
}

String IPasswordAuthData::getPassword() const
{
    return String(hash.data(), hash.data() + hash.size());
}


void IPasswordAuthData::setPasswordHash(const String & hash_)
{
    setPasswordHashHex(hash_);
}

void IPasswordAuthData::setPasswordHashHex(const String & hash_)
{
    Digest digest;
    digest.resize(hash_.size() / 2);

    try
    {
        boost::algorithm::unhex(hash_.begin(), hash_.end(), digest.data());
    }
    catch (const std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read password hash in hex, check for valid characters [0-9a-fA-F] and length");
    }
    setPasswordHashBinary(digest);
}

String IPasswordAuthData::getPasswordHashHex() const
{
    String hex;
    hex.resize(hash.size() * 2);
    boost::algorithm::hex(hash.begin(), hash.end(), hex.data());
    return hex;
}

const Digest & IPasswordAuthData::getPasswordHashBinary() const
{
    return hash;
}

void IPasswordAuthData::setPasswordHashBinary(const Digest & hash_)
{
    hash = hash_;
}

bool IPasswordAuthData::equal(const IAuthenticationData& other) const
{
    if (!IAuthenticationData::equal(other))
        return false;
    const auto & other_password = typeid_cast<const IPasswordAuthData &>(other);
    return hash == other_password.hash;
}


std::shared_ptr<ASTAuthenticationData> NoPasswordAuthData::toAST() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "AST: Unexpected authentication type {}", toString(getType()));
}

void PlainTextPasswordAuthData::setPassword(const String & password_)
{
    setPasswordHashBinary(Util::stringToDigest(password_));
}


std::shared_ptr<ASTAuthenticationData> PlainTextPasswordAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();

    node->contains_password = true;
    node->children.push_back(std::make_shared<ASTLiteral>(getPassword()));
    return node;
}


void SHA256PasswordAuthData::setPassword(const String & password)
{
    setPasswordHashBinary(Util::encodeSHA256(password + salt));
}

void SHA256PasswordAuthData::setPasswordHashBinary(const Digest & hash)
{
    if (hash.size() != 32)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Password hash for the 'SHA256_PASSWORD' authentication type has length {} "
            "but must be exactly 32 bytes.", hash.size());
    }
    IPasswordAuthData::setPasswordHashBinary(hash);
}


void SHA256PasswordAuthData::setSalt(String salt_)
{
    salt = salt_;
}

String SHA256PasswordAuthData::getSalt() const
{
    return salt;
}

bool SHA256PasswordAuthData::equal(const IAuthenticationData& other) const
{
    if (!IPasswordAuthData::equal(other))
        return false;
    const auto & other_password = typeid_cast<const SHA256PasswordAuthData &>(other);
    return salt == other_password.salt;
}

std::shared_ptr<ASTAuthenticationData> SHA256PasswordAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();
    node->contains_hash = true;
    node->children.push_back(std::make_shared<ASTLiteral>(getPasswordHashHex()));

    if (!getSalt().empty())
        node->children.push_back(std::make_shared<ASTLiteral>(getSalt()));
    return node;
}


void DoubleSHA1PasswordAuthData::setPassword(const String & password)
{
    setPasswordHashBinary(Util::encodeDoubleSHA1(password));
}

void DoubleSHA1PasswordAuthData::setPasswordHashBinary(const Digest & hash)
{
    if (hash.size() != 20)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Password hash for the 'DOUBLE_SHA1_PASSWORD' authentication type has length {} "
            "but must be exactly 20 bytes.", hash.size());
    }
    IPasswordAuthData::setPasswordHashBinary(hash);
}

std::shared_ptr<ASTAuthenticationData> DoubleSHA1PasswordAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();
    node->contains_hash = true;
    node->children.push_back(std::make_shared<ASTLiteral>(getPasswordHashHex()));
    return node;
}

void BcryptPasswordAuthData::setWorkfactor(int workfactor_)
{
    workfactor = workfactor_;
}

void BcryptPasswordAuthData::setPassword(const String & password)
{
    if (!workfactor.has_value())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "For setting password 'BCRYPT_PASSWORD' authentication type workfactor must be provided.");
    }
    setPasswordHashBinary(Util::encodeBcrypt(password, workfactor.value()));
}

void BcryptPasswordAuthData::setPasswordHash(const String & hash_)
{
    setPasswordHashBinary(IPasswordAuthData::Util::stringToDigest(hash_));
}

void BcryptPasswordAuthData::setPasswordHashBinary(const Digest & hash)
{
    /// Depending on the workfactor the resulting hash can be 59 or 60 characters long.
    /// However the library we use to encode it requires hash string to be 64 characters long,
    ///  so we also allow the hash of this length.

    if (hash.size() != 59 && hash.size() != 60 && hash.size() != 64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Password hash for the 'BCRYPT_PASSWORD' authentication type has length {} "
                        "but must be 59 or 60 bytes.", hash.size());
    auto hash_ = hash;
    hash_.resize(64);
    IPasswordAuthData::setPasswordHashBinary(hash_);
}

std::shared_ptr<ASTAuthenticationData> BcryptPasswordAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();

    node->contains_hash = true;
    node->children.push_back(std::make_shared<ASTLiteral>(Util::digestToString(getPasswordHashBinary())));

    return node;
}


bool LDAPAuthData::equal(const IAuthenticationData & other) const
{
     if (!IAuthenticationData::equal(other))
        return false;
    const auto & other_ldap_data = typeid_cast<const LDAPAuthData &>(other);
    return ldap_server_name == other_ldap_data.ldap_server_name;
}

std::shared_ptr<ASTAuthenticationData> LDAPAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();
    node->children.push_back(std::make_shared<ASTLiteral>(getLDAPServerName()));
    return node;
}


bool KerberosAuthData::equal(const IAuthenticationData & other) const
{
    if (!IAuthenticationData::equal(other))
        return false;
    const auto & other_kerberos_data = typeid_cast<const KerberosAuthData &>(other);
    return kerberos_realm == other_kerberos_data.kerberos_realm;
}

std::shared_ptr<ASTAuthenticationData> KerberosAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();

    const auto & realm = getKerberosRealm();
    if (!realm.empty())
        node->children.push_back(std::make_shared<ASTLiteral>(realm));
    return node;
}


SSLCertificateAuthData::SSLCertificateAuthData(NamesContainer common_names_)
{
    setSSLCertificateCommonNames(std::move(common_names_));
}

bool SSLCertificateAuthData::equal(const IAuthenticationData & other) const
{
    if (!IAuthenticationData::equal(other))
        return false;
    const auto & other_cert_data = typeid_cast<const SSLCertificateAuthData &>(other);
    return ssl_certificate_common_names == other_cert_data.ssl_certificate_common_names;
}

std::shared_ptr<ASTAuthenticationData> SSLCertificateAuthData::toAST() const
{
    auto node = std::make_shared<ASTAuthenticationData>();
    node->type = getType();

    for (const auto & name : getSSLCertificateCommonNames())
        node->children.push_back(std::make_shared<ASTLiteral>(name));
    return node;
}

void SSLCertificateAuthData::setSSLCertificateCommonNames(NamesContainer common_names_)
{
    if (common_names_.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'SSL CERTIFICATE' authentication type requires a non-empty list of common names.");
    ssl_certificate_common_names = std::move(common_names_);
}

String generateSalt()
{
#if USE_SSL
    ///random generator FIPS complaint
    uint8_t key[32];
    if (RAND_bytes(key, sizeof(key)) != 1)
    {
        char buf[512] = {0};
        ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot generate salt for password. OpenSSL {}", buf);
    }

    String salt;
    salt.resize(sizeof(key) * 2);
    char * buf_pos = salt.data();
    for (uint8_t k : key)
    {
        writeHexByteUppercase(k, buf_pos);
        buf_pos += 2;
    }
    return salt;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "SHA256 passwords support is disabled, because ClickHouse was built without SSL library");
#endif
}

IAuthenticationDataPtr makePasswordAuthenticationData(const ASTAuthenticationData & query, const ASTs & args, ContextPtr context, bool check_password_rules)
{
    if (!query.type && !context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get default password type without context");

    if (check_password_rules && !context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot check password complexity rules without context");

    if (query.type == AuthenticationType::BCRYPT_PASSWORD && !context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get bcrypt work factor without context");

    String password = checkAndGetLiteralArgument<String>(args[0], "password");
    if (check_password_rules)
        context->getAccessControl().checkPasswordComplexityRules(password);

    auto auth_type = query.type.value_or(context->getAccessControl().getDefaultPasswordType());

    IPasswordAuthDataPtr auth_data;

    if (auth_type == AuthenticationType::PLAINTEXT_PASSWORD)
    {
        auth_data = std::make_shared<PlainTextPasswordAuthData>();
    }
    else if (auth_type == AuthenticationType::SHA256_PASSWORD)
    {
        auth_data = std::make_shared<SHA256PasswordAuthData>(generateSalt());
    }
    else if (auth_type == AuthenticationType::DOUBLE_SHA1_PASSWORD)
    {
        auth_data = std::make_shared<DoubleSHA1PasswordAuthData>();
    }
    else if (auth_type == AuthenticationType::BCRYPT_PASSWORD)
    {
        int workfactor = context->getAccessControl().getBcryptWorkfactor();
        auth_data = std::make_shared<BcryptPasswordAuthData>(workfactor);
    }
    auth_data->setPassword(password);

    return auth_data;
}

IAuthenticationDataPtr makePasswordAuthenticationData(const ASTAuthenticationData & query, const ASTs & args)
{
    AuthenticationType auth_type = *query.type;
    String hash = checkAndGetLiteralArgument<String>(args[0], "hash");

    IPasswordAuthDataPtr auth_data;

    if (auth_type == AuthenticationType::PLAINTEXT_PASSWORD)
    {
        auth_data = std::make_shared<PlainTextPasswordAuthData>();
    }
    else if (auth_type == AuthenticationType::SHA256_PASSWORD)
    {
        String parsed_salt;
        if (args.size() == 2)
            parsed_salt = checkAndGetLiteralArgument<String>(args[1], "salt");
        auth_data = std::make_shared<SHA256PasswordAuthData>(parsed_salt);
    }
    else if (auth_type == AuthenticationType::DOUBLE_SHA1_PASSWORD)
    {
        auth_data = std::make_shared<DoubleSHA1PasswordAuthData>();
    }
    else if (auth_type == AuthenticationType::BCRYPT_PASSWORD)
    {
        auth_data = std::make_shared<BcryptPasswordAuthData>();
    }
    auth_data->setPasswordHash(hash);

    return auth_data;
}

IAuthenticationDataPtr IAuthenticationData::fromAST(const ASTAuthenticationData & query, ContextPtr context, bool check_password_rules)
{
    if (query.type && query.type == AuthenticationType::NO_PASSWORD)
        return std::make_shared<NoPasswordAuthData>();

    size_t args_size = query.children.size();
    ASTs args(args_size);
    for (size_t i = 0; i < args_size; ++i)
        args[i] = evaluateConstantExpressionAsLiteral(query.children[i], context);

    if (query.contains_password)
    {
        return makePasswordAuthenticationData(query, args, context, check_password_rules);
    }
    else if (query.contains_hash)
    {
        return makePasswordAuthenticationData(query, args);
    }
    else if (query.type == AuthenticationType::LDAP)
    {
        String ldap_server_name = checkAndGetLiteralArgument<String>(args[0], "ldap_server_name");
        return std::make_shared<LDAPAuthData>(std::move(ldap_server_name));
    }
    else if (query.type == AuthenticationType::KERBEROS)
    {
        String realm;
        if (!args.empty())
        {
            realm = checkAndGetLiteralArgument<String>(args[0], "kerberos_realm");
        }
        return std::make_shared<KerberosAuthData>(realm);
    }
    else if (query.type == AuthenticationType::SSL_CERTIFICATE)
    {
        SSLCertificateAuthData::NamesContainer common_names;
        for (const auto & arg : args)
            common_names.insert(checkAndGetLiteralArgument<String>(arg, "common_name"));

        return std::make_shared<SSLCertificateAuthData>(std::move(common_names));
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected ASTAuthenticationData structure");
}

}
