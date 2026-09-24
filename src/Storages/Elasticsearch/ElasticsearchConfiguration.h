#pragma once

#include <base/types.h>

namespace DB 
{

struct ElasticsearchConfiguration
{
    String url = "http://localhost:9200";
    String index;
};

}
