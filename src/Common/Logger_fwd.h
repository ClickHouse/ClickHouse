#pragma once

#include <memory>

namespace Poco
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;
