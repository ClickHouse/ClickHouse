#include <iostream>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Common/Exception.h>
#include <Access/KerberosInit.h>

/** The example demonstrates using of kerberosInit function to obtain and cache Kerberos ticket-granting ticket.
  * The first argument specifies keytab file. The second argument specifies principal name.
  * The third argument is optional. It specifies credentials cache location.
  * After successful run of kerberos_init it is useful to call klist command to list cached Kerberos tickets.
  * It is also useful to run kdestroy to destroy Kerberos tickets if needed.
  */

using namespace DB;

int main(int argc, char ** argv)
{
    std::cout << "Kerberos Init" << "\n";

    if (argc < 3)
    {
        std::cout << "kerberos_init obtains and caches an initial ticket-granting ticket for principal." << "\n\n";
        std::cout << "Usage:" << "\n" << "    kerberos_init keytab principal [cache]" << "\n";
        return 0;
    }

    const char * cache_name = "";
    if (argc == 4)
        cache_name = argv[3];

    Poco::AutoPtr<Poco::ConsoleChannel> app_channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(app_channel);
    Poco::Logger::root().setLevel("trace");

    try
    {
        kerberosInit(argv[1], argv[2], cache_name);
    }
    catch (const Exception & e)
    {
        std::cout << "KerberosInit failure: " << getExceptionMessage(e, false) << "\n";
        return -1;
    }
    std::cout << "Done" << "\n";
    return 0;
}
