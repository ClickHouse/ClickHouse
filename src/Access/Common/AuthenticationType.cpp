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
    static constexpr auto make_info = [](Keyword keyword_, bool is_password_ = false)
    {
        String init_name = String(toStringView(keyword_));
        boost::to_lower(init_name);
        return AuthenticationTypeInfo{keyword_, std::move(init_name), is_password_};
    };

    switch (type_)
    {
        case AuthenticationType::NO_PASSWORD:
        {
            static const auto info = make_info(Keyword::NO_PASSWORD);
            return info;
        }
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            static const auto info = make_info(Keyword::PLAINTEXT_PASSWORD, true);
            return info;
        }
        case AuthenticationType::SHA256_PASSWORD:
        {
            static const auto info = make_info(Keyword::SHA256_PASSWORD, true);
            return info;
        }
        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            static const auto info = make_info(Keyword::DOUBLE_SHA1_PASSWORD, true);
            return info;
        }
        case AuthenticationType::LDAP:
        {
            static const auto info = make_info(Keyword::LDAP);
            return info;
        }
        case AuthenticationType::KERBEROS:
        {
            static const auto info = make_info(Keyword::KERBEROS);
            return info;
        }
        case AuthenticationType::SSL_CERTIFICATE:
        {
            static const auto info = make_info(Keyword::SSL_CERTIFICATE);
            return info;
        }
        case AuthenticationType::BCRYPT_PASSWORD:
        {
            static const auto info = make_info(Keyword::BCRYPT_PASSWORD, true);
            return info;
        }
        case AuthenticationType::SSH_KEY:
        {
            static const auto info = make_info(Keyword::SSH_KEY);
            return info;
        }
        case AuthenticationType::HTTP:
        {
            static const auto info = make_info(Keyword::HTTP);
            return info;
        }
        case AuthenticationType::JWT:
        {
            static const auto info = make_info(Keyword::JWT);
            return info;
        }
        case AuthenticationType::MAX:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown authentication type: {}", static_cast<int>(type_));
}

}
