#pragma once
#include <Core/Types.h>

namespace DB
{

struct SerializationInfoSettings
{
    double ratio_of_defaults_for_sparse = 1.0;
    size_t type_map_num_shards = 1;
    bool choose_kind = false;

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0 && type_map_num_shards == 1;}
};

}
