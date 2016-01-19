#include "ReplicasStatusHandler.h"

#include <DB/Interpreters/Context.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/HTMLForm.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>


namespace DB
{


ReplicasStatusHandler::ReplicasStatusHandler(Context & context_)
	: context(context_)
{
}


void ReplicasStatusHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	try
	{
		HTMLForm params(request);

		/// Даже в случае, когда отставание небольшое, выводить подробную информацию об отставании.
		bool verbose = params.get("verbose", "") == "1";

		/// Собираем набор реплицируемых таблиц.
		Databases replicated_tables;
		{
			Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

			for (const auto & db : context.getDatabases())
				for (const auto & table : db.second)
					if (typeid_cast<const StorageReplicatedMergeTree *>(table.second.get()))
						replicated_tables[db.first][table.first] = table.second;
		}

		const MergeTreeSettings & settings = context.getMergeTreeSettings();

		bool ok = true;
		std::stringstream message;

		for (const auto & db : replicated_tables)
		{
			for (auto & table : db.second)
			{
				time_t absolute_delay = 0;
				time_t relative_delay = 0;

				static_cast<StorageReplicatedMergeTree &>(*table.second).getReplicaDelays(absolute_delay, relative_delay);

				if ((settings.min_absolute_delay_to_close && absolute_delay >= static_cast<time_t>(settings.min_absolute_delay_to_close))
					|| (settings.min_relative_delay_to_close && relative_delay >= static_cast<time_t>(settings.min_relative_delay_to_close)))
					ok = false;

				message << backQuoteIfNeed(db.first) << "." << backQuoteIfNeed(table.first)
					<< ":\tAbsolute delay: " << absolute_delay << ". Relative delay: " << relative_delay << ".\n";
			}
		}

		if (ok && !verbose)
		{
			const char * data = "Ok.\n";
			response.sendBuffer(data, strlen(data));
		}
		else
		{
			response.send() << message.rdbuf();
		}
	}
	catch (...)
	{
		tryLogCurrentException("ReplicasStatusHandler");

		try
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

			if (!response.sent())
			{
				/// Ещё ничего не отправляли, и даже не знаем, нужно ли сжимать ответ.
				response.send() << getCurrentExceptionMessage(false) << std::endl;
			}
		}
		catch (...)
		{
			LOG_ERROR((&Logger::get("ReplicasStatusHandler")), "Cannot send exception to client");
		}
	}
}


}
