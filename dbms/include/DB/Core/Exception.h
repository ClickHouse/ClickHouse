#pragma once

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

	const StackTrace & getStackTrace() const { return trace; }

private:
	StackTrace trace;
};

using Poco::SharedPtr;

typedef SharedPtr<Poco::Exception> ExceptionPtr;
typedef std::vector<ExceptionPtr> Exceptions;

}
