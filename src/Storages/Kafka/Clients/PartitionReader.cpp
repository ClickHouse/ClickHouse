#include <Storages/Kafka/Clients/PartitionReader.h>

namespace Kafka
{

PartitionReader::PartitionReader(Client & client_, TopicPartition tp_, Int64 start_offset_)
    : client(client_)
    , tp(std::move(tp_))
    , pos(start_offset_)
{
}

PartitionReader::PartitionReader(Client & client_, TopicPartition tp_, OffsetSpec start, std::chrono::milliseconds timeout)
    : client(client_)
    , tp(std::move(tp_))
    , pos(client_.listOffset(tp, start, timeout))
{
}

void PartitionReader::seek(Int64 offset)
{
    pos = offset;
    buffer.clear();
    buffer_pos = 0;
    fetched_next_offset = -1;
}

void PartitionReader::serve(std::vector<Record> & out, size_t max_records)
{
    while (buffer_pos < buffer.size() && out.size() < max_records)
    {
        out.push_back(std::move(buffer[buffer_pos]));
        ++buffer_pos;
        pos = out.back().offset + 1;
    }
    if (buffer_pos >= buffer.size())
    {
        buffer.clear();
        buffer_pos = 0;
        /// The last fetch may end past the last record (control batches,
        /// compaction gaps) — skip the dead range on the next fetch.
        if (fetched_next_offset > pos)
            pos = fetched_next_offset;
    }
}

std::vector<Record> PartitionReader::poll(std::chrono::milliseconds max_wait, size_t max_records)
{
    std::vector<Record> out;
    if (max_records == 0)
        return out;
    serve(out, max_records);
    if (!out.empty())
        return out; // never block while data is already buffered

    FetchSpec spec;
    spec.topic = tp.topic;
    spec.partition = tp.partition;
    spec.offset = pos;
    spec.max_wait_ms = static_cast<Int32>(max_wait.count());
    spec.max_bytes = fetch_max_bytes;
    /// Margin on top of the broker-side long poll for the round trip itself.
    auto result = client.fetch(spec, max_wait + std::chrono::milliseconds{5'000});

    high_watermark = result.high_watermark;
    fetched_next_offset = result.next_offset;
    buffer = std::move(result.records);
    buffer_pos = 0;
    serve(out, max_records);
    return out;
}

}
