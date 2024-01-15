#pragma once

#include <Poco/Logger.h>

namespace Poco
{

class Logger;

}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

LoggerPtr getLogger(const std::string & name);
