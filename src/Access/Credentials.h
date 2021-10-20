#pragma once

#include <common/types.h>
#include <memory>


namespace DB
{

class Credentials
{
public:
    explicit Credentials() = default;
    explicit Credentials(const String & user_name_);

    virtual ~Credentials() = default;

    const String & getUserName() const;
    bool isReady() const;

protected:
    [[noreturn]] static void throwNotReady();

protected:
    bool is_ready = false;
    String user_name;
};

class AlwaysAllowCredentials
    : public Credentials
{
public:
    explicit AlwaysAllowCredentials();
    explicit AlwaysAllowCredentials(const String & user_name_);

    void setUserName(const String & user_name_);
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

}
