#pragma once

namespace DB
{

struct WindowFunctionRequirements
{
    bool frame = true;
    bool peers = false;
    bool partition = false;
    bool order_by = false;
};

WindowFunctionRequirements unionOf(const WindowFunctionRequirements & lhs, const WindowFunctionRequirements & rhs);

}
