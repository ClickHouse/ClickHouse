#pragma once

namespace DB
{
enum VisitorStatus
{
    Ok,
    Exhausted,
    Error,
    Ignore
};

}
