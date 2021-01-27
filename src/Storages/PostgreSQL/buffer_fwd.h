#pragma once

namespace DB
{

class PostgreSQLReplicaConsumerBuffer;
using ConsumerBufferPtr = std::shared_ptr<PostgreSQLReplicaConsumerBuffer>;

}
