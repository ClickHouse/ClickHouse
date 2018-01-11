#include <Poco/Net/DNS.h>
#include <Common/getFQDNOrHostName.h>


namespace
{
    std::string getFQDNOrHostNameImpl()
    {
        try
        {
            return Poco::Net::DNS::thisHost().name();
        }
        catch (...)
        {
            return Poco::Net::DNS::hostName();
        }
    }
}


const std::string & getFQDNOrHostName()
{
    static std::string result = getFQDNOrHostNameImpl();
    return result;

}
