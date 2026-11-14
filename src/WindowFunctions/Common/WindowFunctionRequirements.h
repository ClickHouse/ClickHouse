#pragma once

namespace DB
{

struct WindowFunctionRequirements
{
    bool frame_start = false;
    bool frame_end = false;
    bool peers_start = false;
    bool peers_end = false;
    bool partition_end = false;
    bool order_by = false;
};

WindowFunctionRequirements unionOf(const WindowFunctionRequirements & lhs, const WindowFunctionRequirements & rhs);

}
