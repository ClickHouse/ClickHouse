#pragma once

#include <base/types.h>

namespace DB
{

struct ElasticsearchConfiguration
{
    String url;
    String index;
};

}
