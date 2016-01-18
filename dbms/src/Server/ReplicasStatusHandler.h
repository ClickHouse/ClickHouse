#pragma once

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

class Context;

/// Отвечает "Ok.\n", если все реплики на этом сервере не слишком сильно отстают. Иначе выводит информацию об отставании. TODO Вынести в отдельный файл.
class ReplicasStatusHandler : public Poco::Net::HTTPRequestHandler
{
private:
	Context & context;

public:
	ReplicasStatusHandler(Context & context_);

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};


}
