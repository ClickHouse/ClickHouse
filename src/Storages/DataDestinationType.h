#pragma once


namespace DB
{

enum class DataDestinationType : uint8_t
{
    DISK,
    VOLUME,
    TABLE,
    DELETE,
    SHARD,
};

}
