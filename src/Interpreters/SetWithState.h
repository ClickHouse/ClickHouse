#pragma once

#include <Interpreters/Set.h>

#include <atomic>

namespace DB
{

struct SetWithState : public Set
{
    using Set::Set;

    /// Flow: Creating -> Finished or Suspended
    enum class State : uint8_t
    {
        /// Set is not yet created,
        /// Creating processor continues to build set.
        /// Filtering bypasses data.
        Creating,

        /// Set is finished.
        /// Creating processor is finished.
        /// Filtering filter stream with this set.
        Finished,

        /// Set building is canceled (due to limit exceeded).
        /// Creating and filtering processors bypass data.
        Suspended,
    };

    std::atomic<State> state = State::Creating;

    /// Track number of processors that are currently working on this set.
    /// Last one finalizes set.
    std::atomic_size_t finished_count = 0;
};

}
