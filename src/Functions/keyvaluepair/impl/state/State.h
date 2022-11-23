#pragma once

#include <cstddef>

namespace DB
{

enum State {
    WAITING_KEY,
    READING_KEY,
    READING_ENCLOSED_KEY,
    READING_KV_DELIMITER,
    WAITING_VALUE,
    READING_VALUE,
    READING_ENCLOSED_VALUE,
    READING_EMPTY_VALUE,
    FLUSH_PAIR,
    END
};

struct NextState {
    std::size_t pos;
    State state;
};

struct NextStateWithElement {
    NextState state;
    std::string_view element;
};

}
