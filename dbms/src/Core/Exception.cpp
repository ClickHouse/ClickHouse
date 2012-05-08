#include <DB/Core/Exception.h>


namespace DB
{

Exception::Exception(int code): Poco::Exception(code) {}
Exception::Exception(const std::string & msg, int code) : Poco::Exception(msg, code) {}
Exception::Exception(const std::string & msg, const std::string & arg, int code): Poco::Exception(msg, arg, code) {}
Exception::Exception(const std::string & msg, const Exception & exc, int code): Poco::Exception(msg, exc, code), trace(exc.trace) {}
Exception::Exception(const Exception & exc) : Poco::Exception(exc), trace(exc.trace) {}
Exception::Exception(const Poco::Exception & exc) : Poco::Exception(exc) {}
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

}
