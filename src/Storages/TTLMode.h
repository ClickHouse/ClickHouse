#pragma once


namespace DB
{

enum class TTLMode
{
    DELETE,
    MOVE,
    GROUP_BY,
    RECOMPRESS,
};

}
