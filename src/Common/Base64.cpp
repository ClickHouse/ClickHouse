#include <Common/Base64.h>

#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>

#include <sstream>

namespace DB
{

std::string base64Encode(const std::string & decoded, bool url_encoding)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr, url_encoding ? Poco::BASE64_URL_ENCODING : 0);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

std::string base64Decode(const std::string & encoded, bool url_encoding)
{
    std::string decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr, url_encoding ? Poco::BASE64_URL_ENCODING : 0);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

}
