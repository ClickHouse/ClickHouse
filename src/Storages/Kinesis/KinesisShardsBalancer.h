#include <IO/ReadBufferFromMemory.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Types.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/Record.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>

namespace DB
{

class StorageKinesis;

class KinesisShardsBalancer
{
public:

    KinesisShardsBalancer(
        StorageKinesis & storage,
        const String & stream_name,
        Aws::Kinesis::KinesisClient & client);

    bool isStreamChanged();
    void balanceShards(bool load_from_storage);
    std::vector<Aws::Kinesis::Model::Shard> getActualShards();

private:
    StorageKinesis & storage;
    String stream_name;
    Aws::Kinesis::KinesisClient & client;
    std::vector<Aws::Kinesis::Model::Shard> shards;
};

}

#endif // USE_AWS_KINESIS
