#include <Access/Credentials.h>


namespace DB
{

const String & Credentials::getUserName() const
{
    return user_name;
}

bool Credentials::isReady() const
{
    return is_ready;
}

BasicCredentials::BasicCredentials()
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
    return password;
}

}
