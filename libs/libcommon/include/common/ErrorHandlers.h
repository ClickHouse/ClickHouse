#pragma once

#include <Poco/ErrorHandler.h>
#include <common/logger_useful.h>
#include <DB/Common/Exception.h>


/** ErrorHandler для потоков, который в случае неперехваченного исключения,
  * выводит ошибку в лог и завершает демон.
  */
class KillingErrorHandler : public Poco::ErrorHandler
{
public:
	void exception(const Poco::Exception & e) 	{ std::terminate(); }
	void exception(const std::exception & e)	{ std::terminate(); }
	void exception()							{ std::terminate(); }
};


/** Выводит информацию об исключении в лог.
  */
class ServerErrorHandler : public Poco::ErrorHandler
{
public:
	void exception(const Poco::Exception & e) 	{ logException(); }
	void exception(const std::exception & e)	{ logException(); }
	void exception()							{ logException(); }

private:
	Logger * log = &Logger::get("ServerErrorHandler");

	void logException()
	{
		DB::tryLogCurrentException(log);
	}
};
