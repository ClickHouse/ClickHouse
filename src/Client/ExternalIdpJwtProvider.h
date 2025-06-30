#pragma once

#include <Client/JwtProvider.h>
#include <string>
#include <iosfwd>

namespace DB
{

/// JWT Provider for external IdPs, where the IdP token is used directly.
class ExternalIdpJwtProvider : public JwtProvider
{
public:
    ExternalIdpJwtProvider(
        std::string auth_url,
        std::string client_id,
        std::ostream & out,
        std::ostream & err);

    std::string getJWT() override;

private:
};

}
