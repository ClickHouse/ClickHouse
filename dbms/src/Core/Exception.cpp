#include <errno.h>
#include <string.h>

#include <DB/IO/WriteHelpers.h>

#include <DB/Core/Exception.h>


namespace DB
{

Exception::Exception(int code): Poco::Exception(code) {}
Exception::Exception(const std::string & msg, int code) : Poco::Exception(msg, code) {}
Exception::Exception(const std::string & msg, const std::string & arg, int code): Poco::Exception(msg, arg, code) {}
Exception::Exception(const std::string & msg, const Exception & exc, int code): Poco::Exception(msg, exc, code), trace(exc.trace) {}
Exception::Exception(const Exception & exc) : Poco::Exception(exc), trace(exc.trace) {}
Exception::Exception(const Poco::Exception & exc) : Poco::Exception(exc.displayText()) {}
Exception::~Exception() throw() {}

Exception & Exception::operator=(const Exception& exc)
{
	Poco::Exception::operator=(exc);
	trace = exc.trace;	
	return *this;
}

const char* Exception::name() const throw()
{
	return "DB::Exception";
}

const char* Exception::className() const throw()
{
	return "DB::Exception";
}

Exception * Exception::clone() const
{
	return new Exception(*this);
}

void Exception::rethrow() const
{
	throw *this;
}

void Exception::addMessage(const std::string & arg)
{
	extendedMessage(arg);
}


void throwFromErrno(const std::string & s, int code, int e)
{
	char buf[128];
	throw Exception(s + ", errno: " + toString(e) + ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf))), code);
}


ExceptionPtr cloneCurrentException()
{
	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		return e.clone();
	}
	catch (const Poco::Exception & e)
	{
		return e.clone();
	}
	catch (const std::exception & e)
	{
		return new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
	}
	catch (...)
	{
		return new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
	}
}


void rethrowFirstException(Exceptions & exceptions)
{
	for (size_t i = 0, size = exceptions.size(); i < size; ++i)
		if (exceptions[i])
			exceptions[i]->rethrow();
}


}
