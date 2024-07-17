#pragma once

#include "Common/BufferAllocationPolicy.h"
#include "Common/ThreadPoolTaskTracker.h"
#include "Disks/ObjectStorages/Ceph/CephObjectStorage.h"
#include "Disks/ObjectStorages/IObjectStorage.h"
#include "IO/Ceph/RadosIO.h"
#include "IO/WriteBufferFromFileBase.h"
#include "IO/WriteSettings.h"
namespace DB
{

/// Write buffer to multiple objects in Rados
class StriperWriteBufferFromRados : public WriteBufferFromFileBase
{
public:
    StriperWriteBufferFromRados(
        std::shared_ptr<Ceph::RadosIO> impl_,
        const String & object_id_,
        size_t buf_size_,
        size_t max_object_size_,
        const WriteSettings & write_settings_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {});

    ~StriperWriteBufferFromRados() override;
    void nextImpl() override;
    void preFinalize() override;
    std::string getFileName() const override { return object_id; }
    void sync() override { next(); }

private:
    /// Receives response from the server after sending all data.
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

    std::shared_ptr<Ceph::RadosIO> impl;
    String object_id;
    size_t object_seq = 0;
    size_t max_object_size;
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
