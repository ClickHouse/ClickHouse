#include "PostgreSQLReplicaConsumerBuffer.h"


namespace DB
{

PostgreSQLReplicaConsumerBuffer::PostgreSQLReplicaConsumerBuffer(
        uint64_t max_block_size_)
        : ReadBuffer(nullptr, 0)
        , rows_data(max_block_size_)
{
}


PostgreSQLReplicaConsumerBuffer::~PostgreSQLReplicaConsumerBuffer()
{
    BufferBase::set(nullptr, 0, 0);
}


bool PostgreSQLReplicaConsumerBuffer::nextImpl()
{
    if (!allowed)
        return false;

    if (rows_data.tryPop(current_row_data))
    {
        auto * new_position = const_cast<char *>(current_row_data.data.data());
        BufferBase::set(new_position, current_row_data.data.size(), 0);
        allowed = false;

        return true;
    }

    return false;
}

}
