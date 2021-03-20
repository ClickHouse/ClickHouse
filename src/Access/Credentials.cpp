#include <Access/Credentials.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Credentials::Credentials(const String & user_name_)
    : user_name(user_name_)
{
}

const String & Credentials::getUserName() const
{
    if (!isReady())
        throwNotReady();
    return user_name;
}

bool Credentials::isReady() const
{
    return is_ready;
}

void Credentials::throwNotReady()
{
    throw Exception("Credentials are not ready", ErrorCodes::LOGICAL_ERROR);
}

AlwaysAllowCredentials::AlwaysAllowCredentials()
{
    is_ready = true;
}

AlwaysAllowCredentials::AlwaysAllowCredentials(const String & user_name_)
    : Credentials(user_name_)
{
    is_ready = true;
}

void AlwaysAllowCredentials::setUserName(const String & user_name_)
{
    user_name = user_name_;
}

BasicCredentials::BasicCredentials()
{
    is_ready = true;
}

BasicCredentials::BasicCredentials(const String & user_name_)
    : Credentials(user_name_)
{
    is_ready = true;
}

BasicCredentials::BasicCredentials(const String & user_name_, const String & password_)
    : Credentials(user_name_)
    , password(password_)
{
    is_ready = true;
}

void BasicCredentials::setUserName(const String & user_name_)
{
    user_name = user_name_;
}

void BasicCredentials::setPassword(const String & password_)
{
    password = password_;
}

const String & BasicCredentials::getPassword() const
{
    if (!isReady())
        throwNotReady();
    return password;
}

}
