#pragma once

#include <base/types.h>

namespace DB
{

struct ElasticsearchConfiguration
{
    String url;
    String index;
    String keep_alive = "1m";
    UInt64 page_size = 1000;
};

}
