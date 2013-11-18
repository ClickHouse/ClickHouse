#pragma once

#include <cerrno>
#include <vector>

#include <Poco/Exception.h>
#include <Poco/SharedPtr.h>

#include <DB/Core/StackTrace.h>


namespace DB
{

class Exception : public Poco::Exception
{
public:
	Exception(int code = 0);
	Exception(const std::string & msg, int code = 0);
	Exception(const std::string & msg, const std::string & arg, int code = 0);
	Exception(const std::string & msg, const Exception & exc, int code = 0);
	Exception(const Exception & exc);
	explicit Exception(const Poco::Exception & exc);
	~Exception() throw();
	Exception & operator = (const Exception & exc);
	const char * name() const throw();
	const char * className() const throw();
	Exception * clone() const;
	void rethrow() const;

	/// Дописать к существующему сообщению что-нибудь ещё.
	void addMessage(const std::string & arg);

	const StackTrace & getStackTrace() const { return trace; }

private:
	StackTrace trace;
};

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
void tryLogCurrentException(const char * log_name);


void rethrowFirstException(Exceptions & exceptions);


}
