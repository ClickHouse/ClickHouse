#pragma once

#include <Compression/ICompressionCodec.h>
#include <qpl/qpl.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/// DeflateJobHWPool is resource pool for provide the job objects which is required to save context infomation during offload asynchronous compression to IAA.
class DeflateJobHWPool
{
public:
    DeflateJobHWPool();
    ~DeflateJobHWPool();
    static DeflateJobHWPool & instance();
    static constexpr auto JOB_POOL_SIZE = 1024;
    static constexpr qpl_path_t PATH = qpl_path_hardware;
    static qpl_job * jobPool[JOB_POOL_SIZE];
    static std::atomic_bool jobLocks[JOB_POOL_SIZE];
    bool job_pool_ready = false;

    bool jobPoolReady() const
    {
        return job_pool_ready;
    }

    qpl_job * acquireJob(uint32_t * job_id)
    {
        if (jobPoolReady())
        {
            uint32_t retry = 0;
            auto index = random(JOB_POOL_SIZE);
            while (tryLockJob(index) == false)
            {
                index = random(JOB_POOL_SIZE);
                retry++;
                if (retry > JOB_POOL_SIZE)
                {
                    return nullptr;
                }
            }
            *job_id = JOB_POOL_SIZE - index;
            return jobPool[index];
        }
        else
        {
            return nullptr;
        }
    }

    qpl_job * releaseJob(uint32_t job_id)
    {
        if (jobPoolReady())
        {
            uint32_t index = JOB_POOL_SIZE - job_id;
            ReleaseJobObjectGuard _(index);
            return jobPool[index];
        }
        else
        {
            return nullptr;
        }
    }

private:
    /// Returns true if Job pool initialization succeeded, otherwise false
    bool initJobPool();

    size_t random(uint32_t pool_size) const
    {
        size_t tsc = 0;
        unsigned lo, hi;
        __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi) : :);
        tsc = (((static_cast<uint64_t>(hi)) << 32) | (static_cast<uint64_t>(lo)));
        return (static_cast<size_t>((tsc * 44485709377909ULL) >> 4)) % pool_size;
    }

    bool tryLockJob(size_t index)
    {
        bool expected = false;
        return jobLocks[index].compare_exchange_strong(expected, true);
    }

    void destroyJobPool()
    {
        uint32_t size = 0;
        qpl_get_job_size(PATH, &size);
        for (uint32_t i = 0; i < JOB_POOL_SIZE && size > 0; ++i)
        {
            while (tryLockJob(i) == false);
            if (jobPool[i])
            {
                qpl_fini_job(jobPool[i]);
                delete[] jobPool[i];
            }
            jobPool[i] = nullptr;
            jobLocks[i].store(false);
        }
    }

    struct ReleaseJobObjectGuard
    {
        uint32_t index;
        ReleaseJobObjectGuard() = delete;

    public:
        ReleaseJobObjectGuard(const uint32_t i) : index(i)
        {
        }

        ~ReleaseJobObjectGuard()
        {
            jobLocks[index].store(false);
        }
    };
    Poco::Logger * log;
};

class SoftwareCodecDeflate
{
public:
    SoftwareCodecDeflate();
    ~SoftwareCodecDeflate();
    uint32_t doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size);
    void doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size);

private:
    qpl_job * jobSWPtr;
    std::unique_ptr<uint8_t[]> jobSWbuffer;
    qpl_job * getJobCodecPtr();
};

class HardwareCodecDeflate
{
public:
    bool hwEnabled;
    HardwareCodecDeflate();
    ~HardwareCodecDeflate();
    uint32_t doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size) const;
    uint32_t doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const;
    uint32_t doDecompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size);
    void flushAsynchronousDecompressRequests();

private:
    std::map<uint32_t, qpl_job *> jobDecompAsyncMap;
    Poco::Logger * log;
};
class CompressionCodecDeflate : public ICompressionCodec
{
public:
    CompressionCodecDeflate();
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

    uint32_t doCompressData(const char * source, uint32_t source_size, char * dest) const override;
    void doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const override;
    void flushAsynchronousDecompressRequests() override;

private:
    uint32_t getMaxCompressedDataSize(uint32_t uncompressed_size) const override;
    std::unique_ptr<HardwareCodecDeflate> hw_codec;
    std::unique_ptr<SoftwareCodecDeflate> sw_codec;
};

}
