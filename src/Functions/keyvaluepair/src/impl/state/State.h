#pragma once

#include <cstddef>

namespace DB
{

enum State
{
    // Skip characters until it finds a valid first key character. Might jump to READING_KEY, READING_QUOTED_KEY or END.
    WAITING_KEY,
    // Tries to read a key. Might jump to WAITING_KEY, WAITING_VALUE or END.
    READING_KEY,
    // Tries to read an quoted/ quoted key. Might jump to WAITING_KEY, READING_KV_DELIMITER or END.
    READING_QUOTED_KEY,
    // Tries to read the key value pair delimiter. Might jump to WAITING_KEY, WAITING_VALUE or END.
    READING_KV_DELIMITER,
    // Skip characters until it finds a valid first value character. Might jump to READING_QUOTED_VALUE, READING_EMPTY_VALUE or READING_VALUE.
    WAITING_VALUE,
    // Tries to read a value. Jumps to FLUSH_PAIR.
    READING_VALUE,
    // Tries to read an quoted/ quoted value. Might jump to FLUSH_PAIR or END.
    READING_QUOTED_VALUE,
    // "Reads" an empty value. Jumps to FLUSH_PAIR.
    READING_EMPTY_VALUE,
    // In this state, both key and value have already been collected and should be flushed. Might jump to WAITING_KEY or END.
    FLUSH_PAIR,
    END
};

struct NextState
{
    std::size_t position_in_string;
    State state;
};

}
