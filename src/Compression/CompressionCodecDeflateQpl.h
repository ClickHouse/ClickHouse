#pragma once

#include <Compression/ICompressionCodec.h>
#include <Common/Logger.h>
#include <atomic>
#include <map>
#include <random>
#include <pcg_random.hpp>

#include "config.h"

#if USE_QPL

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
    void releaseJob(UInt32 job_id);
    const bool & isJobPoolReady() const { return job_pool_ready; }

private:
    bool tryLockJob(UInt32 index);
    void unLockJob(UInt32 index);

    /// size of each job objects
    UInt32 per_job_size;
    /// Maximum jobs running in parallel supported by IAA hardware
    UInt32 max_hw_jobs;
    /// Entire buffer for storing all job objects
    std::unique_ptr<uint8_t[]> hw_jobs_buffer;
    /// Locks for accessing each job object pointers
    std::unique_ptr<std::atomic_bool[]> hw_job_ptr_locks;

    bool job_pool_ready;
    pcg64_fast random_engine;
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

    explicit HardwareCodecDeflateQpl(SoftwareCodecDeflateQpl & sw_codec_);
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
    LoggerPtr log;
    /// Provides a fallback in case of errors.
    SoftwareCodecDeflateQpl & sw_codec;
};

class CompressionCodecDeflateQpl final : public ICompressionCodec
{
public:
    CompressionCodecDeflateQpl();
    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

protected:
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isDeflateQpl() const override { return true; }

    std::string getDescription() const override
    {
        return "Requires hardware support for Intel’s QuickAssist Technology for DEFLATE compression; enhanced performance for specific hardware.";
    }


    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    /// Flush result for previous asynchronous decompression requests on asynchronous mode.
    void flushAsynchronousDecompressRequests() override;

private:
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    std::unique_ptr<SoftwareCodecDeflateQpl> sw_codec;
    std::unique_ptr<HardwareCodecDeflateQpl> hw_codec;
};

}
#endif
