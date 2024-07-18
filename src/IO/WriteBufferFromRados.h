#pragma once
#include <config.h>

#if USE_CEPH

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
class WriteBufferFromRados : public WriteBufferFromFileBase
{
public:
    WriteBufferFromRados(
        std::shared_ptr<RadosIOContext> io_ctx_,
        const String & object_id_,
        size_t buf_size_,
        size_t max_chunk_size_,
        const WriteSettings & write_settings_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {});

    ~WriteBufferFromRados() override;
    void nextImpl() override;
    void preFinalize() override;
    std::string getFileName() const override { return object_id; }
    void sync() override { next(); }

private:
    void finalizeImpl() override;

    String getVerboseLogDetails() const;
    String getShortLogDetails() const;

    struct PartData;
    void hidePartialData();
    void reallocateFirstBuffer();
    void detachBuffer();
    void allocateBuffer();
    void allocateFirstBuffer();
    void setFakeBufferWhenPreFinalized();

    void writePart(PartData && data);
    void writeMultipartUpload();

    std::shared_ptr<RadosIOContext> io_ctx;
    String object_id;
    size_t chunk_count = 0;
    size_t max_chunk_size;
    WriteSettings write_settings;
    const std::optional<std::map<String, String>> object_metadata;
    LoggerPtr log = getLogger("StripperWriteBufferFromRados");
    LogSeriesLimiterPtr limitedLog = std::make_shared<LogSeriesLimiter>(log, 1, 5);

    BufferAllocationPolicyPtr buffer_allocation_policy;

    /// Track that prefinalize() is called only once
    bool is_prefinalized = false;

    /// First fully filled buffer has to be delayed
    /// There are two ways after:
    /// First is to call prefinalize/finalize, which leads to single part upload
    /// Second is to write more data, which leads to multi part upload
    std::deque<PartData> detached_part_data;
    char fake_buffer_when_prefinalized[1] = {};

    /// offset() and count() are unstable inside nextImpl
    /// For example nextImpl changes position hence offset() and count() is changed
    /// This vars are dedicated to store information about sizes when offset() and count() are unstable
    size_t total_size = 0;
    size_t hidden_size = 0;

    std::unique_ptr<TaskTracker> task_tracker;
};

}

#endif
