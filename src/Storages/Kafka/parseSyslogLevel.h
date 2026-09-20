#pragma once

#include <utility>
#include <Core/LogsLevel.h>
#include <Poco/Message.h>

std::pair<Poco::Message::Priority, DB::LogsLevel> parseSyslogLevel(int level);
