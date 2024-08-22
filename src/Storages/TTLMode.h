#pragma once


namespace DB
{

enum class TTLMode : uint8_t
{
    DELETE,
    MOVE,
    GROUP_BY,
    RECOMPRESS,
};

}
