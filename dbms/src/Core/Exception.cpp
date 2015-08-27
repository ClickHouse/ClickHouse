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

void tryLogCurrentException(const char * log_name, const std::string & start_of_message)
{
	tryLogCurrentException(&Logger::get(log_name), start_of_message);
}

void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message)
{
	try
	{
		LOG_ERROR(logger, start_of_message << (start_of_message.empty() ? "" : ": ") << getCurrentExceptionMessage(true));
	}
	catch (...)
	{
	}
}

std::string getCurrentExceptionMessage(bool with_stacktrace)
{
	std::stringstream stream;

	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		try
		{
			stream << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();

			if (with_stacktrace)
				stream << ", Stack trace:\n\n" << e.getStackTrace().toString();
		}
		catch (...) {}
	}
	catch (const Poco::Exception & e)
	{
		try
		{
			stream << "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
				<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
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

			stream << "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what();
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

			stream << "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ", type: " << name;
		}
		catch (...) {}
	}

	return stream.str();
}


void rethrowFirstException(Exceptions & exceptions)
{
	for (size_t i = 0, size = exceptions.size(); i < size; ++i)
		if (exceptions[i])
			exceptions[i]->rethrow();
}


}
