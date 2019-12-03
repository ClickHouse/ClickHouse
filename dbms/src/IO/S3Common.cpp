#include <IO/S3Common.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <iterator>
#include <sstream>

#include <Poco/Base64Encoder.h>
#include <Poco/HMACEngine.h>
#include <Poco/SHA1Engine.h>
#include <Poco/URI.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_FORMAT_DATETIME;
}

void S3Helper::authenticateRequest(
    Poco::Net::HTTPRequest & request,
    const String & access_key_id,
    const String & secret_access_key)
{
    /// See https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html

    if (access_key_id.empty())
        return;

    /// Limitations:
    /// 1. Virtual hosted-style requests are not supported (e.g. `http://johnsmith.net.s3.amazonaws.com/homepage.html`).
    /// 2. AMZ headers are not supported (TODO).

    if (!request.has("Date"))
    {
        WriteBufferFromOwnString out;
        writeDateTimeTextRFC1123(time(nullptr), out, DateLUT::instance("UTC"));
        request.set("Date", out.str());
    }

    String string_to_sign = request.getMethod() + "\n"
        + request.get("Content-MD5", "") + "\n"
        + request.get("Content-Type", "") + "\n"
        + request.get("Date") + "\n"
        + Poco::URI(request.getURI()).getPathAndQuery();

    Poco::HMACEngine<Poco::SHA1Engine> engine(secret_access_key);
    engine.update(string_to_sign);
    auto digest = engine.digest();
    std::ostringstream signature;
    Poco::Base64Encoder encoder(signature);
    std::copy(digest.begin(), digest.end(), std::ostream_iterator<char>(encoder));
    encoder.close();

    request.set("Authorization", "AWS " + access_key_id + ":" + signature.str());
}

}
