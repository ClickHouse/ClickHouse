#pragma once

#include <Poco/ErrorHandler.h>
#include <Poco/Net/SocketDefs.h>
#include <common/logger_useful.h>


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


/** То же самое, но не завершает работу в случае эксепшена типа Socket is not connected.
  * Этот эксепшен возникает внутри реализаций Poco::Net::HTTPServer, Poco::Net::TCPServer,
  *  и иначе его не удаётся перехватить, и сервер завершает работу.
  */
class ServerErrorHandler : public KillingErrorHandler
{
public:
	void exception(const Poco::Exception & e)
	{
		if (e.code() == POCO_ENOTCONN)
			LOG_WARNING(log, "Client has gone away.");
		else
			std::terminate();
	}

private:
	Logger * log = &Logger::get("ServerErrorHandler");
};
