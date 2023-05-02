#include <Access/Common/AuthenticationType.h>
#include <Common/Exception.h>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


const AuthenticationTypeInfo & AuthenticationTypeInfo::get(AuthenticationType type_)
{
    static constexpr auto make_info = [](const char * raw_name_, bool is_password_ = false)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        return AuthenticationTypeInfo{raw_name_, std::move(init_name), is_password_};
    };

    switch (type_)
    {
        case AuthenticationType::NO_PASSWORD:
        {
            static const auto info = make_info("NO_PASSWORD");
            return info;
        }
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            static const auto info = make_info("PLAINTEXT_PASSWORD", true);
            return info;
        }
        case AuthenticationType::SHA256_PASSWORD:
        {
            static const auto info = make_info("SHA256_PASSWORD", true);
            return info;
        }
        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            static const auto info = make_info("DOUBLE_SHA1_PASSWORD", true);
            return info;
        }
        case AuthenticationType::LDAP:
        {
            static const auto info = make_info("LDAP");
            return info;
        }
        case AuthenticationType::KERBEROS:
        {
            static const auto info = make_info("KERBEROS");
            return info;
        }
        case AuthenticationType::SSL_CERTIFICATE:
        {
            static const auto info = make_info("SSL_CERTIFICATE");
            return info;
        }
        case AuthenticationType::BCRYPT_PASSWORD:
        {
            static const auto info = make_info("BCRYPT_PASSWORD", true);
            return info;
        }
        case AuthenticationType::MAX:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown authentication type: {}", static_cast<int>(type_));
}

}
