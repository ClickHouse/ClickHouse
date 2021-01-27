#pragma once

#include <Core/Names.h>
#include <common/types.h>
#include <IO/ReadBuffer.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <common/logger_useful.h>
#include "buffer_fwd.h"


namespace DB
{

class PostgreSQLReplicaConsumerBuffer : public ReadBuffer
{

public:
    PostgreSQLReplicaConsumerBuffer(
            uint64_t max_block_size_);

    ~PostgreSQLReplicaConsumerBuffer() override;

    void allowNext() { allowed = true; }

private:
    bool nextImpl() override;

    struct RowData
    {
        String data;
        RowData() : data("") {}
    };

    RowData current_row_data;
    ConcurrentBoundedQueue<RowData> rows_data;
    bool allowed = true;
};

}
