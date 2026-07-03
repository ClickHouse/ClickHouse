#include <Poco/Net/DNS.h>
#include <base/getFQDNOrHostName.h>


namespace
{
    std::string getFQDNOrHostNameImpl()
    {
#if defined(OS_DARWIN)
        return Poco::Net::DNS::hostName();
#else
        try
        {
            return Poco::Net::DNS::thisHost().name();
        }
        catch (...)
        {
            return Poco::Net::DNS::hostName();
        }
#endif
    }
}


const std::string & getFQDNOrHostName()
{
    static std::string result = getFQDNOrHostNameImpl();
    return result;
}
