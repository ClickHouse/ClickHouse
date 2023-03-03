#pragma once

#include <Compression/ICompressionCodec.h>
#include <map>
#include <random>
#include <qpl/qpl.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/// DeflateQplJobHWPool is resource pool to provide the job objects.
/// Job object is used for storing context information during offloading compression job to HW Accelerator.
class DeflateQplJobHWPool
{
public:
    DeflateQplJobHWPool();
    ~DeflateQplJobHWPool();

    static DeflateQplJobHWPool & instance();

    qpl_job * acquireJob(UInt32 & job_id);
    static void releaseJob(UInt32 job_id);
    static const bool & isJobPoolReady() { return job_pool_ready; }

private:
    static bool tryLockJob(UInt32 index);
    static void unLockJob(UInt32 index);

    /// Maximum jobs running in parallel supported by IAA hardware
    static constexpr auto MAX_HW_JOB_NUMBER = 1024;
    /// Entire buffer for storing all job objects
    static std::unique_ptr<uint8_t[]> hw_jobs_buffer;
    /// Job pool for storing all job object pointers
    static std::array<qpl_job *, MAX_HW_JOB_NUMBER> hw_job_ptr_pool;
    /// Locks for accessing each job object pointers
    static std::array<std::atomic_bool, MAX_HW_JOB_NUMBER> hw_job_ptr_locks;
    static bool job_pool_ready;
    std::mt19937 random_engine;
    std::uniform_int_distribution<int> distribution;
};

class SoftwareCodecDeflateQpl
{
public:
    ~SoftwareCodecDeflateQpl();
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size);
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);

private:
    qpl_job * sw_job = nullptr;
    std::unique_ptr<uint8_t[]> sw_buffer;

    qpl_job * getJobCodecPtr();
};

class HardwareCodecDeflateQpl
{
public:
    /// RET_ERROR stands for hardware codec fail, needs fallback to software codec.
    static constexpr Int32 RET_ERROR = -1;

    HardwareCodecDeflateQpl();
    ~HardwareCodecDeflateQpl();

    Int32 doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size) const;

    /// Submit job request to the IAA hardware and then busy waiting till it complete.
    Int32 doDecompressDataSynchronous(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);

    /// Submit job request to the IAA hardware and return immediately. IAA hardware will process decompression jobs automatically.
    Int32 doDecompressDataAsynchronous(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);

    /// Flush result for all previous requests which means busy waiting till all the jobs in "decomp_async_job_map" are finished.
    /// Must be called subsequently after several calls of doDecompressDataReq.
    void flushAsynchronousDecompressRequests();

private:
    /// Asynchronous job map for decompression: job ID - job object.
    /// For each submission, push job ID && job object into this map;
    /// For flush, pop out job ID && job object from this map. Use job ID to release job lock and use job object to check job status till complete.
    std::map<UInt32, qpl_job *> decomp_async_job_map;
    Poco::Logger * log;
};

class CompressionCodecDeflateQpl : public ICompressionCodec
{
public:
    CompressionCodecDeflateQpl();
    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

protected:
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    /// Flush result for previous asynchronous decompression requests on asynchronous mode.
    void flushAsynchronousDecompressRequests() override;

private:
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    std::unique_ptr<HardwareCodecDeflateQpl> hw_codec;
    std::unique_ptr<SoftwareCodecDeflateQpl> sw_codec;
};

}
