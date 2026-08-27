#include <Storages/MergeTree/Streaming/ReadProgress.h>

namespace DB
{

void StreamReadProgress::accountChunk(const Chunk & data)
{
    overall_read_rows += data.getNumRows();
    overall_read_bytes += data.bytes();
    current_round_read_rows += data.getNumRows();
    current_round_read_bytes += data.bytes();
}

void StreamReadProgress::accountRound()
{
    finished_rounds += 1;
    current_round_read_rows = 0;
    current_round_read_bytes = 0;
}

}
