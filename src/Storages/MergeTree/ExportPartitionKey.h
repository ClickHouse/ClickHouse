#pragma once

#include <Common/escapeForFileName.h>
#include <base/types.h>

namespace DB
{

namespace ExportPartitionUtils
{

inline String compositeKey(
    const String & partition_id, const String & destination_database, const String & destination_table)
{
    return escapeForFileName(partition_id) + "."
        + escapeForFileName(destination_database) + "."
        + escapeForFileName(destination_table);
}

}

}
