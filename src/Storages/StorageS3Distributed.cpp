#include "Storages/StorageS3Distributed.h"

#include <Common/Throttler.h>
#include "Client/Connection.h"
#include "DataStreams/RemoteBlockInputStream.h"
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/queryToString.h>

#include <memory>
#include <string>
#include <thread>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}



StorageS3Distributed::StorageS3Distributed(
    const StorageID & table_id_, 
    std::string cluster_name_,
    const Context & context)
    : IStorage(table_id_)
    , cluster_name(cluster_name_)
    , cluster(context.getCluster(cluster_name)->getClusterWithReplicasAsShards(context.getSettings()))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({{"dummy", std::make_shared<DataTypeUInt8>()}}));
    setInMemoryMetadata(storage_metadata);
}



Pipe StorageS3Distributed::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    /// Secondary query, need to read from S3
    if (context.getCurrentQueryId() != context.getInitialQueryId()) {
        std::cout << "Secondary query" << std::endl;
        auto initial_host = context.getClientInfo().initial_address.host().toString();
        auto initial_port = std::to_string(context.getClientInfo().initial_address.port());
        // auto client_info = context.getClientInfo();

        std::cout << initial_host << ' ' << initial_port << std::endl;


        String password;
        String cluster_anime;
        String cluster_secret;

        // auto connection = std::make_shared<Connection>(
        //         /*host=*/initial_host,
        //         /*port=*/initial_port,
        //         /*default_database=*/context.getGlobalContext().getCurrentDatabase(),
        //         /*user=*/client_info.initial_user,
        //         /*password=*/password,
        //         /*cluster=*/cluster_anime,
        //         /*cluster_secret=*/cluster_secret
        //     );

        // connection->sendNextTaskRequest(context.getInitialQueryId());
        // auto packet = connection->receivePacket();


        std::this_thread::sleep_for(std::chrono::seconds(1));


        Block header{ColumnWithTypeAndName(
                DataTypeUInt8().createColumn(),
                std::make_shared<DataTypeUInt8>(),
                "dummy")};

        auto column = DataTypeUInt8().createColumnConst(1, 0u)->convertToFullColumnIfConst();
        Chunk chunk({ std::move(column) }, 1);

        return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
    }


    Pipes pipes;
    connections.reserve(cluster->getShardCount());

    std::cout << "StorageS3Distributed::read" << std::endl;

    for (const auto & replicas : cluster->getShardsAddresses()) {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            connections.emplace_back(std::make_shared<Connection>(
                /*host=*/node.host_name,
                /*port=*/node.port,
                /*default_database=*/context.getGlobalContext().getCurrentDatabase(),
                /*user=*/node.user,
                /*password=*/node.password,
                /*cluster=*/node.cluster,
                /*cluster_secret=*/node.cluster_secret
            ));
            auto stream = std::make_shared<RemoteBlockInputStream>(
                /*connection=*/*connections.back(),
                /*query=*/queryToString(query_info.query),
                /*header=*/metadata_snapshot->getSampleBlock(),
                /*context=*/context
            );
            pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));
        }
    }


    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());


    return Pipe::unitePipes(std::move(pipes));
}






}

