#include <Common/EventNotifier.h>
#include <Common/Exception.h>


namespace DB
{

std::unique_ptr<EventNotifier> EventNotifier::event_notifier;

EventNotifier & EventNotifier::instance()
{
    if (!event_notifier)
        throw Exception("EventNotifier is not initialized. This is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    return *event_notifier;
}

}
