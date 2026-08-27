#include <Storages/MergeTree/Streaming/ReadProgress.h>

namespace DB
{

void StreamReadProgress::accountChunk(const Chunk & data)
{
    read_rows += data.getNumRows();
    read_bytes += data.bytes();
    round_read_rows += data.getNumRows();
    round_read_bytes += data.bytes();
}

void StreamReadProgress::accountRound()
{
    finished_rounds += 1;
    round_read_rows = 0;
    round_read_bytes = 0;
}

}
