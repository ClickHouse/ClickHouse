#pragma once

#include <Access/Common/AuthenticationType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/Access/ASTAuthenticationData.h>

#include <variant>
#include <vector>
#include <base/TypeList.h>
#include <base/types.h>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/container/flat_set.hpp>

namespace DB
{

/// Stores data for checking password when a user logins.
class IAuthenticationData
{
public:
    virtual AuthenticationType getType() const = 0;
    virtual std::shared_ptr<ASTAuthenticationData> toAST() const = 0;

    friend bool operator==(const IAuthenticationData & lhs, const IAuthenticationData & rhs);
    virtual ~IAuthenticationData() = default;
    virtual String getExternalAuthServer() const { return ""; }

    /// Factory method
    static std::shared_ptr<IAuthenticationData> fromAST(const ASTAuthenticationData & query, ContextPtr context, bool check_password_rules);

protected:
    virtual bool equal(const IAuthenticationData & other) const;
};

using IAuthenticationDataPtr = std::shared_ptr<IAuthenticationData>;


class NoPasswordAuthData : public IAuthenticationData
{
public:
    AuthenticationType getType() const override { return AuthenticationType::NO_PASSWORD; }
    std::shared_ptr<ASTAuthenticationData> toAST() const override;
};


class IPasswordAuthData : public IAuthenticationData
{
public:
    using Digest = std::vector<uint8_t>;
    using Ptr = std::shared_ptr<IPasswordAuthData>;

    virtual String getPassword() const;
    virtual String getPasswordHashHex() const;
    const Digest & getPasswordHashBinary() const;

    virtual void setPassword(const String & password) = 0;
    virtual void setPasswordHash(const String & hash);
    virtual void setPasswordHashHex(const String & hash);
    virtual void setPasswordHashBinary(const Digest & hash);

    struct Util
    {
        static String digestToString(const Digest & text);
        static Digest stringToDigest(std::string_view text);
        static Digest encodeSHA256(std::string_view text);
        static Digest encodeSHA1(std::string_view text);
        static Digest encodeSHA1(const Digest & text);
        static Digest encodeDoubleSHA1(std::string_view text);
        static Digest encodeDoubleSHA1(const Digest & text);
        static Digest encodeBcrypt(std::string_view text, int workfactor);
        static bool checkPasswordBcrypt(std::string_view password, const Digest & password_bcrypt);
    };

protected:
    bool equal(const IAuthenticationData & other) const override;

private:
    Digest hash;
};
using IPasswordAuthDataPtr = std::shared_ptr<IPasswordAuthData>;

class PlainTextPasswordAuthData : public IPasswordAuthData
{
public:
    AuthenticationType getType() const override { return AuthenticationType::PLAINTEXT_PASSWORD; }
    void setPassword(const String & password_) override;
    std::shared_ptr<ASTAuthenticationData> toAST() const override;
};


class SHA256PasswordAuthData : public IPasswordAuthData
{
public:
    AuthenticationType getType() const override { return AuthenticationType::SHA256_PASSWORD; }
    SHA256PasswordAuthData(String salt_ = "") : salt(std::move(salt_)) { }

    void setPassword(const String & password_) override;
    void setPasswordHashBinary(const Digest & hash) override;
    void setSalt(String salt_);
    String getSalt() const;

    std::shared_ptr<ASTAuthenticationData> toAST() const override;

protected:
    bool equal(const IAuthenticationData & other) const override;

private:
    String salt;
};


class DoubleSHA1PasswordAuthData : public IPasswordAuthData
{
    static constexpr auto TYPE = AuthenticationType::DOUBLE_SHA1_PASSWORD;

public:
    AuthenticationType getType() const override { return AuthenticationType::DOUBLE_SHA1_PASSWORD; }

    void setPassword(const String & password) override;
    void setPasswordHashBinary(const Digest & hash) override;

    std::shared_ptr<ASTAuthenticationData> toAST() const override;
};


class BcryptPasswordAuthData : public IPasswordAuthData
{
public:
    BcryptPasswordAuthData() = default;
    BcryptPasswordAuthData(int workfactor_) : workfactor(workfactor_) { }

    AuthenticationType getType() const override { return AuthenticationType::BCRYPT_PASSWORD; }

    void setPassword(const String & password) override;
    void setPasswordHashBinary(const Digest & hash) override;
    void setPasswordHash(const String & hash) override;
    void setWorkfactor(int workfactor_);

    std::shared_ptr<ASTAuthenticationData> toAST() const override;

private:
    std::optional<int> workfactor;
};


class LDAPAuthData : public IAuthenticationData
{
public:
    LDAPAuthData(String ldap_server_name_) : ldap_server_name(std::move(ldap_server_name_)) { }

    AuthenticationType getType() const override { return AuthenticationType::LDAP; }
    const String & getLDAPServerName() const { return ldap_server_name; }
    void setLDAPServerName(const String & name) { ldap_server_name = name; }
    String getExternalAuthServer() const override { return getLDAPServerName(); }

    std::shared_ptr<ASTAuthenticationData> toAST() const override;

protected:
    bool equal(const IAuthenticationData & other) const override;

private:
    String ldap_server_name;
};


class KerberosAuthData : public IAuthenticationData
{
public:
    KerberosAuthData() = default;
    KerberosAuthData(String kerberos_realm_) : kerberos_realm(std::move(kerberos_realm_)) { }

    AuthenticationType getType() const override { return AuthenticationType::KERBEROS; }

    const String & getKerberosRealm() const { return kerberos_realm; }
    void setKerberosRealm(const String & realm) { kerberos_realm = realm; }

    std::shared_ptr<ASTAuthenticationData> toAST() const override;

protected:
    bool equal(const IAuthenticationData & other) const override;

private:
    String kerberos_realm;
};


class SSLCertificateAuthData : public IAuthenticationData
{
public:
    using NamesContainer = boost::container::flat_set<String>;

    SSLCertificateAuthData(NamesContainer common_names_);

    AuthenticationType getType() const override { return AuthenticationType::SSL_CERTIFICATE; }
    const NamesContainer & getSSLCertificateCommonNames() const { return ssl_certificate_common_names; }
    void setSSLCertificateCommonNames(NamesContainer common_names_);

    std::shared_ptr<ASTAuthenticationData> toAST() const override;

protected:
    bool equal(const IAuthenticationData & other) const override;

private:
    NamesContainer ssl_certificate_common_names;
};

}
