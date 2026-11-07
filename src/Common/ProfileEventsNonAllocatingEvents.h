#pragma once

#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsNonAllocatingEventList.h>

namespace ProfileEvents
{
#define M(NAME) extern const Event NAME; /// NOLINT: used by the generated trait specializations below.
APPLY_FOR_NON_ALLOCATING_PROFILE_EVENTS(M)
#undef M

/// Classification uses the named event object's identity, not its numeric value.
template <const Event & event>
inline constexpr bool is_nonallocating_event = false;

#define M(NAME) template <> inline constexpr bool is_nonallocating_event<NAME> = true;
APPLY_FOR_NON_ALLOCATING_PROFILE_EVENTS(M)
#undef M

class NonAllocatingEvent
{
public:
    Event value() const noexcept
    {
        return stored;
    }

private:
    template <const Event & event>
    friend NonAllocatingEvent nonAllocatingEvent() noexcept;

    explicit NonAllocatingEvent(Event event) noexcept : stored(event)
    {}

    Event stored;
};

/// Only catalogued named events can request the nonallocating update contract.
template <const Event & event>
NonAllocatingEvent nonAllocatingEvent() noexcept
{
    static_assert(is_nonallocating_event<event>, "Event is not in the nonallocating profile-event catalogue");
    return NonAllocatingEvent(event);
}
}
