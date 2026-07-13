#pragma once

#include <string_view>


namespace DB
{

/// Describes how resource is shared among concurrent consumers.
/// See comments in ISchedulerNode for details.
enum class SharingMode
{
    TimeShared,
    SpaceShared
};

inline std::string_view sharingModeToString(SharingMode mode)
{
    switch (mode)
    {
        case SharingMode::TimeShared: return "TimeShared";
        case SharingMode::SpaceShared: return "SpaceShared";
    }
}

}
