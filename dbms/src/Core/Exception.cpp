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

inline std::string demangle(const char * const mangled, int & status)
{
	const auto demangled_str = abi::__cxa_demangle(mangled, 0, 0, &status);
	std::string demangled{demangled_str};
	free(demangled_str);

	return demangled;
}

void tryLogCurrentException(const char * log_name)
{
	tryLogCurrentException(&Logger::get(log_name));
}

void tryLogCurrentException(Poco::Logger * logger)
{
	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		try
		{
			LOG_ERROR(logger, "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what()
				<< ", Stack trace:\n\n" << e.getStackTrace().toString());
		}
		catch (...) {}
	}
	catch (const Poco::Exception & e)
	{
		try
		{
			LOG_ERROR(logger, "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
				<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
		}
		catch (...) {}
	}
	catch (const std::exception & e)
	{
		try
		{
			int status = 0;
			auto name = demangle(typeid(e).name(), status);

			if (status)
				name += " (demangling status: " + toString(status) + ")";

			LOG_ERROR(logger, "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what());
		}
		catch (...) {}
	}
	catch (...)
	{
		try
		{
			int status = 0;
			auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

			if (status)
				name += " (demangling status: " + toString(status) + ")";

			LOG_ERROR(logger, "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ", type: " << name);
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
