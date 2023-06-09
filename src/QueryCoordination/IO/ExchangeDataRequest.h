#pragma once

#include <base/types.h>

namespace DB
{

struct ExchangeDataRequest
{
    String query_id;
    UInt32 fragment_id;
    UInt32 exchange_id;
};

}
