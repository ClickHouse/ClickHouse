#pragma once

#include <memory>
#include <Access/Common/SSLCertificateSubjects.h>
#include <base/types.h>

#include "config.h"

namespace DB
{

class Credentials
{
public:
    explicit Credentials() = default;
    explicit Credentials(const String & user_name_);

    Credentials(const Credentials &) = default;
    Credentials(Credentials &&) = default;

    virtual ~Credentials() = default;

    const String & getUserName() const;
    bool isReady() const;

protected:
    [[noreturn]] static void throwNotReady();

    bool is_ready = false;
    String user_name;
};

/// Does not check the password/credentials and that the specified host is allowed.
/// (Used only internally in cluster, if the secret matches)
class AlwaysAllowCredentials
    : public Credentials
{
public:
    explicit AlwaysAllowCredentials();
    explicit AlwaysAllowCredentials(const String & user_name_);

    void setUserName(const String & user_name_);
};

class SSLCertificateCredentials
    : public Credentials
{
public:
    explicit SSLCertificateCredentials(const String & user_name_, SSLCertificateSubjects && subjects_);
    const SSLCertificateSubjects & getSSLCertificateSubjects() const;

private:
    SSLCertificateSubjects certificate_subjects;
};

class BasicCredentials
    : public Credentials
{
public:
    explicit BasicCredentials();
    explicit BasicCredentials(const String & user_name_);
    explicit BasicCredentials(const String & user_name_, const String & password_);

    void setUserName(const String & user_name_);
    void setPassword(const String & password_);
    const String & getPassword() const;

private:
    String password;
};

class CredentialsWithScramble : public Credentials
{
public:
    explicit CredentialsWithScramble(const String & user_name_, const String & scramble_, const String & scrambled_password_)
        : Credentials(user_name_), scramble(scramble_), scrambled_password(scrambled_password_)
    {
        is_ready = true;
    }

    const String & getScramble() const { return scramble; }
    const String & getScrambledPassword() const { return scrambled_password; }

private:
    String scramble;
    String scrambled_password;
};

class MySQLNative41Credentials : public CredentialsWithScramble
{
    using CredentialsWithScramble::CredentialsWithScramble;
};

#if USE_SSH
class SshCredentials : public Credentials
{
public:
    SshCredentials(const String & user_name_, const String & signature_, const String & original_)
        : Credentials(user_name_), signature(signature_), original(original_)
    {
        is_ready = true;
    }

    const String & getSignature() const
    {
        if (!isReady())
        {
            throwNotReady();
        }
        return signature;
    }

    const String & getOriginal() const
    {
        if (!isReady())
        {
            throwNotReady();
        }
        return original;
    }

private:
    String signature;
    String original;
};
#endif

}
