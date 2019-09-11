#include <IO/ReadWriteBufferFromHTTP.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_REDIRECTS;
}


std::unique_ptr<DB::ReadWriteBufferFromHTTP> makeReadWriteBufferFromHTTP(const Poco::URI & uri,
    const std::string & method,
    std::function<void(std::ostream &)> callback,
    const DB::ConnectionTimeouts & timeouts,
    const DB::SettingUInt64 max_redirects)
    {
        auto actual_uri =uri;
        UInt64 redirects = 0;

        do
        {
            try
            {
                return std::make_unique<DB::ReadWriteBufferFromHTTP>(actual_uri, method, callback, timeouts);
            }
            catch (Poco::URIRedirection & exc)
            {
                redirects++;
                actual_uri = exc.uri();
            }
        } while(max_redirects>redirects);

        // too many redirects....
        std::stringstream error_message;
        error_message << "Too many redirects while trying to access " << uri.toString() ;

        throw Exception(error_message.str(), ErrorCodes::TOO_MANY_REDIRECTS);
    }
}