#pragma once

#include <base/types.h>

namespace DB
{

class ExchangeDataRequest
{

private:
    String query_id;
    UInt32 fragment_id;
    UInt32 exchange_id;
};

}
