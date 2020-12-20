#include "ReplicasStatusHandler.h"

#include <Interpreters/Context.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/HTMLForm.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>
#include <IO/HTTPCommon.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>


namespace DB
{


ReplicasStatusHandler::ReplicasStatusHandler(IServer & server)
    : context(server.context())
{
}


void ReplicasStatusHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    try
    {
        HTMLForm params(request);

        /// Even if lag is small, output detailed information about the lag.
        bool verbose = params.get("verbose", "") == "1";

        const MergeTreeSettings & settings = context.getReplicatedMergeTreeSettings();

        bool ok = true;
        WriteBufferFromOwnString message;

        auto databases = DatabaseCatalog::instance().getDatabases();

        /// Iterate through all the replicated tables.
        for (const auto & db : databases)
        {
            /// Check if database can contain replicated tables
            if (!db.second->canContainMergeTreeTables())
                continue;

            for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                const auto & table = iterator->table();
                if (!table)
                    continue;

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

        if (!ok)
        {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
            verbose = true;
        }

        if (verbose)
            response.send() << message.str();
        else
        {
            const char * data = "Ok.\n";
            response.sendBuffer(data, strlen(data));
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
            LOG_ERROR((&Poco::Logger::get("ReplicasStatusHandler")), "Cannot send exception to client");
        }
    }
}

Poco::Net::HTTPRequestHandlerFactory * createReplicasStatusHandlerFactory(IServer & server, const std::string & config_prefix)
{
    return addFiltersFromConfig(new HandlingRuleHTTPHandlerFactory<ReplicasStatusHandler>(server), server.config(), config_prefix);
}

}
