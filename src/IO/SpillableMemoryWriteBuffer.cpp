#include <IO/SpillableMemoryWriteBuffer.h>

#include <Common/Exception.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SpillableMemoryWriteBuffer::SpillableMemoryWriteBuffer(
    std::shared_ptr<SpillConfig> spill_config_,
    String filename_,
    size_t initial_chunk_size_,
    double growth_rate_,
    size_t max_chunk_size_)
    : MemoryWriteBuffer(0, initial_chunk_size_, growth_rate_, max_chunk_size_)
    , spill_config(std::move(spill_config_))
    , filename(std::move(filename_))
{
    if (spill_config)
        spill_config->checker.alloc(static_cast<Int64>(initial_chunk_size));
}

SpillableMemoryWriteBuffer::~SpillableMemoryWriteBuffer()
{
    if (spill_config)
    {
        for (const auto & range : chunk_list)
            spill_config->checker.dealloc(range.size());
    }
}

void SpillableMemoryWriteBuffer::nextImpl()
{
    if (unlikely(hasPendingData()))
    {
        /// Manual flush: keep the unwritten part of the current chunk.
        buffer() = Buffer(pos, buffer().end());
        return;
    }

    /// Estimate the size of the next chunk to decide whether spilling is needed.
    size_t next_chunk_size = chunk_list.empty()
        ? initial_chunk_size
        : std::min(std::max(size_t(1), static_cast<size_t>(static_cast<double>(chunk_tail->size()) * growth_rate)), max_chunk_size);

    if (spill_config && spill_config->checker.isSpillable(static_cast<Int64>(next_chunk_size)))
        spillImpl();

    addChunkAndTrack();
}

void SpillableMemoryWriteBuffer::addChunkAndTrack()
{
    addChunk();
    if (spill_config)
        spill_config->checker.alloc(static_cast<Int64>(chunk_tail->size()));
}

void SpillableMemoryWriteBuffer::spill()
{
    spillImpl();
    addChunkAndTrack();
}

void SpillableMemoryWriteBuffer::spillImpl()
{
    if (!spill_config || chunk_list.empty())
        return;

    if (!spill_buffer)
    {
        if (!spill_config->write_creator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "spill_write_buffer_creator is not set");
        if (!spill_dir_created)
        {
            if (!spill_config->create_spill_temp_dir)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "create_spill_temp_dir is not set");
            spill_config->create_spill_temp_dir();
            spill_dir_created = true;
        }
        spill_buffer = spill_config->write_creator(filename);
    }

    /// Write all chunks to the spill target; the last chunk is truncated to the current position.
    Position end_pos = position();
    auto it = chunk_list.begin();
    while (true)
    {
        auto next_it = std::next(it);
        size_t len = (next_it == chunk_list.end()) ? static_cast<size_t>(end_pos - it->begin()) : it->size();
        if (len)
            spill_buffer->write(it->begin(), len);
        if (next_it == chunk_list.end())
            break;
        it = next_it;
    }

    for (const auto & range : chunk_list)
        spill_config->checker.dealloc(range.size());

    /// Free the spilled chunks instead of only dropping their handles.
    freeChunks();
}

void SpillableMemoryWriteBuffer::finalizeImpl()
{
    if (spill_buffer)
        spill_buffer->finalize();
}

void SpillableMemoryWriteBuffer::cancelImpl() noexcept
{
    if (spill_buffer)
        spill_buffer->cancel();

    if (spill_config)
    {
        for (const auto & range : chunk_list)
            spill_config->checker.dealloc(range.size());
    }

    freeChunks();
}

std::unique_ptr<ReadBuffer> SpillableMemoryWriteBuffer::getReadBufferImpl()
{
    finalize();

    ConcatReadBuffer::Buffers buffers;

    if (spill_buffer)
    {
        if (!spill_config->read_creator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "spill_file_reader_creator is not set");

        auto read_buf = spill_config->read_creator(filename);
        if (!read_buf)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "spill_file_reader_creator returned an empty buffer");

        buffers.push_back(std::move(read_buf));
    }

    /// Unregister the in-memory chunks from the checker; they are released by the
    /// read buffer returned from MemoryWriteBuffer.
    if (spill_config)
    {
        for (const auto & range : chunk_list)
            spill_config->checker.dealloc(range.size());
    }

    if (auto memory_buf = MemoryWriteBuffer::getReadBufferImpl())
        buffers.push_back(std::move(memory_buf));

    if (buffers.size() == 1)
        return std::unique_ptr<ReadBuffer>(buffers.front().release());
    return std::make_unique<ConcatReadBuffer>(std::move(buffers));
}

}
