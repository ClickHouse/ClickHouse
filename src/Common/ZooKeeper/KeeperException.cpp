#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/KeeperException.h>

namespace DB::ErrorCodes
{
extern const int KEEPER_EXCEPTION;
}

namespace ProfileEvents
{
extern const Event ZooKeeperUserExceptions;
extern const Event ZooKeeperHardwareExceptions;
extern const Event ZooKeeperOtherExceptions;
}

namespace Coordination
{

Exception::Exception(const std::string & msg, Error code_, int)
    : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION)
    , code(code_)
{
    incrementErrorMetrics(code);
}

Exception::Exception(PreformattedMessage && msg, Error code_)
    : DB::Exception(std::move(msg), DB::ErrorCodes::KEEPER_EXCEPTION)
    , code(code_)
{
    extendedMessage(errorMessage(code));
    incrementErrorMetrics(code);
}

Exception::Exception(Error code_)
    : Exception(code_, "Coordination error: {}", errorMessage(code_))
{
}

Exception::Exception(const Exception & exc) = default;

void Exception::incrementErrorMetrics(Error code_)
{
    if (Coordination::isUserError(code_))
        ProfileEvents::increment(ProfileEvents::ZooKeeperUserExceptions);
    else if (Coordination::isHardwareError(code_))
        ProfileEvents::increment(ProfileEvents::ZooKeeperHardwareExceptions);
    else
        ProfileEvents::increment(ProfileEvents::ZooKeeperOtherExceptions);
}

}
