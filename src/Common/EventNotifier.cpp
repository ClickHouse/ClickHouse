#include <Common/EventNotifier.h>
#include <Common/Exception.h>

#include <boost/functional/hash.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::unique_ptr<EventNotifier> EventNotifier::event_notifier;

EventNotifier & EventNotifier::init()
{
    if (event_notifier)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "EventNotifier is initialized twice. This is a bug.");

    event_notifier = std::make_unique<EventNotifier>();

    return *event_notifier;
}

EventNotifier & EventNotifier::instance()
{
    if (!event_notifier)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "EventNotifier is not initialized. This is a bug.");

    return *event_notifier;
}

void EventNotifier::shutdown()
{
    if (event_notifier)
      event_notifier.reset();
}

size_t EventNotifier::calculateIdentifier(size_t a, size_t b)
{
    size_t result = 0;
    boost::hash_combine(result, a);
    boost::hash_combine(result, b);
    return result;
}

}
