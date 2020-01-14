#pragma once

#include <Core/Types.h>

namespace DB
{
enum WINDOW_VIEW_FIRE_STATUS
{
    WAITING,
    READY,
    RETIRED
};

// struct WindowViewBlocksMetadata
// {
//     // String hash;
//     // std::set<UInt64> window_ids;
//     UInt64 max_window_id = 0;
//     WINDOW_VIEW_FIRE_STATUS fired_status = WINDOW_VIEW_FIRE_STATUS::WAITING;
//     // bool retired;
// };

}