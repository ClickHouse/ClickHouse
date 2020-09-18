#pragma once

#include <Core/Types.h>
#include <memory>


namespace DB
{

class Credentials
{
public:
    virtual ~Credentials() = default;

    const String & getUserName() const;
    bool isReady() const;

protected:
    bool is_ready = false;
    String user_name;
};

class BasicCredentials
    : public Credentials
{
public:
    explicit BasicCredentials();

    void setUserName(const String & user_name_);
    void setPassword(const String & password_);
    const String & getPassword() const;

private:
    String password;
};

}
