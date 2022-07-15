#pragma once

#include <Compression/ICompressionCodec.h>
#include <qpl/qpl.h>
#include <random>

namespace Poco
{
class Logger;
}

namespace DB
{

/// DeflateQplJobHWPool is resource pool for provide the job objects which is required to save context infomation during offload asynchronous compression to IAA.
class DeflateQplJobHWPool
{
public:
    DeflateQplJobHWPool();
    ~DeflateQplJobHWPool();

    qpl_job * acquireJob(UInt32 * job_id);

    static qpl_job * releaseJob(UInt32 job_id);

    static const bool & isJobPoolReady() { return job_pool_ready; }

    static DeflateQplJobHWPool & instance();

    static constexpr auto MAX_HW_JOB_NUMBER = 1024;

private:
    static bool tryLockJob(UInt32 index);

    static void unLockJob(UInt32 index);

    class ReleaseJobObjectGuard
    {
        UInt32 index;
        ReleaseJobObjectGuard() = delete;

    public:
        ReleaseJobObjectGuard(const UInt32 index_) : index(index_){}

        ~ReleaseJobObjectGuard(){ hw_job_ptr_locks[index].store(false); }
    };

    /// Entire buffer for storing all job objects
    static std::unique_ptr<uint8_t[]> hw_jobs_buffer;
    /// Job pool for storing all job object pointers
    static std::array<qpl_job *, DeflateQplJobHWPool::MAX_HW_JOB_NUMBER> hw_job_ptr_pool;
    /// Locks for accessing each job object pointers
    static std::array<std::atomic_bool, DeflateQplJobHWPool::MAX_HW_JOB_NUMBER> hw_job_ptr_locks;
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
    /// RET_ERROR stands for hardware codec fail,need fallback to software codec.
    static constexpr Int32 RET_ERROR = -1;

    HardwareCodecDeflateQpl();
    ~HardwareCodecDeflateQpl();
    Int32 doCompressData(const char * source, UInt32 source_size, char * dest, UInt32 dest_size) const;
    Int32 doDecompressDataAsynchronous(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);
    /// Flush result for previous asynchronous decompression requests.Must be used following with several calls of doDecompressDataReq.
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
    bool isCompression() const override
    {
        return true;
    }

    bool isGenericCompression() const override
    {
        return true;
    }

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    ///Flush result for previous asynchronous decompression requests on asynchronous mode.
    void flushAsynchronousDecompressRequests() override;

private:
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;
    std::unique_ptr<HardwareCodecDeflateQpl> hw_codec;
    std::unique_ptr<SoftwareCodecDeflateQpl> sw_codec;
};

}
