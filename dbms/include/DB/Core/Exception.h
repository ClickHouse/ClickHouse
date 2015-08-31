#pragma once

#include <cerrno>
#include <vector>

#include <statdaemons/Exception.h>
#include <Poco/SharedPtr.h>

namespace Poco { class Logger; }

namespace DB
{

using Poco::SharedPtr;

typedef SharedPtr<Poco::Exception> ExceptionPtr;
typedef std::vector<ExceptionPtr> Exceptions;


void throwFromErrno(const std::string & s, int code = 0, int the_errno = errno);


/** Для использования в блоке catch (...).
  * Преобразует Exception, Poco::Exception, std::exception или неизвестный exception в ExceptionPtr.
  */
ExceptionPtr cloneCurrentException();

/** Попробовать записать исключение в лог (и забыть про него).
  * Можно использовать в деструкторах в блоке catch (...).
  */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");

std::string getCurrentExceptionMessage(bool with_stacktrace);


void rethrowFirstException(Exceptions & exceptions);


}
