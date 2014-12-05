#include <errno.h>
#include <string.h>
#include <cxxabi.h>

#include <Yandex/logger_useful.h>

#include <DB/IO/WriteHelpers.h>

#include <DB/Core/Exception.h>


namespace DB
{

void throwFromErrno(const std::string & s, int code, int e)
{
	char buf[128];
	throw ErrnoException(s + ", errno: " + toString(e) + ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf))), code, e);
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


void tryLogCurrentException(const char * log_name)
{
	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		try
		{
			LOG_ERROR(&Logger::get(log_name), "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what()
				<< ", Stack trace:\n\n" << e.getStackTrace().toString());
		}
		catch (...) {}
	}
	catch (const Poco::Exception & e)
	{
		try
		{
			LOG_ERROR(&Logger::get(log_name), "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
				<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
		}
		catch (...) {}
	}
	catch (const std::exception & e)
	{
		try
		{
			int status;
			char * realname = abi::__cxa_demangle(typeid(e).name(), 0, 0, &status);
			std::string name = realname;
			free(realname);

			if (status)
				name += " (demangling status: " + toString(status) + ")";

			LOG_ERROR(&Logger::get(log_name), "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what());
		}
		catch (...) {}
	}
	catch (...)
	{
		try
		{
			LOG_ERROR(&Logger::get(log_name), "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION);
		}
		catch (...) {}
	}
}


void rethrowFirstException(Exceptions & exceptions)
{
	for (size_t i = 0, size = exceptions.size(); i < size; ++i)
		if (exceptions[i])
			exceptions[i]->rethrow();
}


}
