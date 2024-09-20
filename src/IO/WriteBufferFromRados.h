#pragma once
#include <config.h>

#if USE_CEPH

#include <Disks/ObjectStorages/Ceph/RadosObjectStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/Ceph/RadosIOContext.h>
#include <IO/WriteSettings.h>
#include <Common/BufferAllocationPolicy.h>
#include <Common/ThreadPoolTaskTracker.h>

namespace DB
{

/// Write data to Rados, if data exceeds max_chunk_size, it will be split into multiple chunk
/// Each chunk is a physical object in Rados. The HEAD chunk contains metadata about the ClickHouse
/// object. Method names are similar to WriteBufferFromS3 (using `part` instead of `chunk`).
/// TODO: use async IO from RadosContext. Not to implement async write int this class.
/// The only concern is in async write we need to keep the data buffer alive until the write is finished.
/// But it's not obvious how to do it now.
class WriteBufferFromRados : public WriteBufferFromFileBase
{
public:
    WriteBufferFromRados(
        std::shared_ptr<RadosIOContext> io_ctx_,
        const String & object_id_,
        size_t buf_size_,
        const OSDSettings & osd_settings_,
        const WriteSettings & write_settings_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt);

    ~WriteBufferFromRados() override;
    void nextImpl() override;
    std::string getFileName() const override { return object_id; }
    void sync() override { next(); }

private:
    void finalizeImpl() override;
    void startNewChunk();
    void tryAbortWrittenChunks();

    std::shared_ptr<RadosIOContext> io_ctx;
    String object_id;
    OSDSettings osd_settings;
    WriteSettings write_settings;
    const std::optional<std::map<String, String>> object_metadata;
    LoggerPtr log = getLogger("WriteBufferFromRados");

    size_t chunk_count = 0;

    String current_chunk_name;
    size_t current_chunk_size_bytes = INT64_MAX;
    bool first_write = true;

    bool all_chunks_finished = true;
    size_t writeImpl(const char * begin, size_t len);

    size_t total_size = 0;
};

}

#endif
