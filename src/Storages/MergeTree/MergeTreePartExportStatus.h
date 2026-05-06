#pragma once

#include <base/types.h>
#include <ctime>


namespace DB
{

struct MergeTreeExportStatus
{
    String source_database;
    String source_table;
    String destination_database;
    String destination_table;
    time_t create_time = 0;
    std::string part_name;
};

}
