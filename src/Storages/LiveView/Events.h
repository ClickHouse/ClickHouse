#pragma once

namespace DB
{
    /// events processed by the events thread
    enum LiveViewEvent
    {
        NONE = 0,
        LAST_USER = 1,
        NEW_USER = 2,
        NEW_BLOCKS = 4,
        SHUTDOWN = 8
    };
}
