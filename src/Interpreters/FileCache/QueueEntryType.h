#pragma once

#include <base/types.h>

namespace DB
{
enum class QueueEntryType : uint8_t
{
    None,
    LRU,
    SLRU_Protected,
    SLRU_Probationary,
    SplitCache_Data,
    SplitCache_System,
};

String toString(QueueEntryType queue_type);
}
