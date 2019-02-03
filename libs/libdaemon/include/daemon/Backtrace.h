#pragma once

#include <daemon/BaseDaemon.h>
#include <daemon/OwnFormattingChannel.h>
#include <daemon/OwnPatternFormatter.h>

#include <Common/Config/ConfigProcessor.h>
#include <daemon/OwnSplitChannel.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <cxxabi.h>
#include <execinfo.h>
#include <unistd.h>

#if USE_UNWIND
#define UNW_LOCAL_ONLY
    #include <libunwind.h>
#endif

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>

#include <typeinfo>
#include <common/logger_useful.h>
#include <common/ErrorHandlers.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
#include <Poco/Observer.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/PatternFormatter.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/TaskManager.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/MapConfiguration.h>
#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/ErrorHandler.h>
#include <Poco/Condition.h>
#include <Poco/SyslogChannel.h>
#include <Poco/DirectoryIterator.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/ClickHouseRevision.h>
#include <Common/config_version.h>
#include <daemon/OwnPatternFormatter.h>
#include <Common/CurrentThread.h>
#include <Poco/Net/RemoteSyslogChannel.h>


std::string signalToErrorMessage(int sig, siginfo_t & info, ucontext_t & context);

void * getCallerAddress(ucontext_t & context);

std::vector<void *> getBacktraceFrames(ucontext_t & context);

std::string backtraceFramesToString(const std::vector<void *> & frames);
