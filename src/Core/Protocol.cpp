#include <Core/Protocol.h>
#include <Common/Base64.h>


namespace DB
{

namespace EncodedUserInfo
{

String SSHKeyAuthenticationData::encodeBase64() const
{
    return base64Encode(user);
}

void SSHKeyAuthenticationData::decodeBase64(const String & source)
{
    user = base64Decode(source);
}


}

}
