#pragma once

#include <memory>

namespace DB
{

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

}
