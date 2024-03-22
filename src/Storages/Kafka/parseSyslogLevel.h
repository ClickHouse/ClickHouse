#pragma once

#include <utility>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>

std::pair<Poco::Message::Priority, DB::LogsLevel> parseSyslogLevel(const int level);
