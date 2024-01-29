#pragma once

#include <memory>

#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>

using LoggerPtr = Poco::LoggerPtr;

using LoggerRawPtr = Poco::Logger *;

/** RAII wrappers around Poco/Logger.h.
  *
  * You should use this functions in case Logger instance lifetime needs to be properly
  * managed, because otherwise it will leak memory.
  *
  * For example when Logger is created when table is created and Logger contains table name.
  * Then it must be destroyed when underlying table is destroyed.
  */

/** Get Logger with specified name. If the Logger does not exists, it is created.
  * Logger is destroyed, when last shared ptr that refers to Logger with specified name is destroyed.
  */
LoggerPtr getLogger(const std::string & name);

/** Create Logger with specified name, channel and logging level.
  * If Logger already exists, throws exception.
  * Logger is destroyed, when last shared ptr that refers to Logger with specified name is destroyed.
  */
LoggerPtr createLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level = Poco::Message::PRIO_INFORMATION);

/** Create raw Poco::Logger that will not be destroyed before program termination.
  * This can be used in cases when specific Logger instance can be singletone.
  *
  * For example you need to pass Logger into low-level libraries as raw pointer, and using
  * RAII wrapper is inconvenient.
  *
  * Generally you should always use getLogger functions.
  */

LoggerRawPtr getRawLogger(const std::string & name);

LoggerRawPtr createRawLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level = Poco::Message::PRIO_INFORMATION);


/** Returns true, if currently Logger with specified name is created.
  * Otherwise, returns false.
  */
bool hasLogger(const std::string & name);
