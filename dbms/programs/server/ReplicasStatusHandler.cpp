#include "ReplicasStatusHandler.h"

#include <Interpreters/Context.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/HTMLForm.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>
#include <IO/HTTPCommon.h>

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

        /// Even if lag is small, output detailed information about the lag.
        bool verbose = params.get("verbose", "") == "1";

        const MergeTreeSettings & settings = context.getMergeTreeSettings();

        bool ok = true;
        std::stringstream message;

        auto databases = context.getDatabases();

        /// Iterate through all the replicated tables.
        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getIterator(context); iterator->isValid(); iterator->next())
            {
                auto & table = iterator->table();
                StorageReplicatedMergeTree * table_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());

                if (!table_replicated)
                    continue;

                time_t absolute_delay = 0;
                time_t relative_delay = 0;

                table_replicated->getReplicaDelays(absolute_delay, relative_delay);

                if ((settings.min_absolute_delay_to_close && absolute_delay >= static_cast<time_t>(settings.min_absolute_delay_to_close))
                    || (settings.min_relative_delay_to_close && relative_delay >= static_cast<time_t>(settings.min_relative_delay_to_close)))
                    ok = false;

                message << backQuoteIfNeed(db.first) << "." << backQuoteIfNeed(iterator->name())
                    << ":\tAbsolute delay: " << absolute_delay << ". Relative delay: " << relative_delay << ".\n";
            }
        }

        const auto & config = context.getConfigRef();
        setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));

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
        /// We have not sent anything yet and we don't even know if we need to compress response.
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
