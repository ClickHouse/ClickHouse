#include <Common/getServerUUID.h>
#include <daemon/BaseDaemon.h>
#include <Poco/Util/Application.h>

DB::UUID getServerUUID()
{
    const auto * daemon = dynamic_cast<const BaseDaemon *>(&Poco::Util::Application::instance());
    if (daemon)
        return daemon->getServerUUID();
    else
        return DB::UUIDHelpers::Nil;
}
