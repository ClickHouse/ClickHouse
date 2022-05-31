#pragma once

#include <map>
#include <vector>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <Parsers/StringRange.h>
#include <qpl/qpl.h>
#include <x86intrin.h>
namespace Poco
{
class Logger;
}

namespace DB
{

class DeflateJobHWPool
{
public:
    DeflateJobHWPool();
    ~DeflateJobHWPool();
    static DeflateJobHWPool & instance();
    static constexpr auto jobPoolSize = 1024;
    static constexpr qpl_path_t PATH = qpl_path_hardware;
    static qpl_job * jobPool[jobPoolSize];
    static std::atomic_bool jobLock[jobPoolSize];

    qpl_job * ALWAYS_INLINE acquireJob(uint32_t * job_id)
    {
        uint32_t retry = 0;
        auto index = random(jobPoolSize);
        while (tryLockJob(index) == false)
        {
            index = random(jobPoolSize);
            retry++;
            if (retry > jobPoolSize)
            {
                return nullptr;
            }
        }
        *job_id = jobPoolSize - index;
        return jobPool[index];
    }
    qpl_job * ALWAYS_INLINE releaseJob(uint32_t job_id)
    {
        uint32_t index = jobPoolSize - job_id;
        ReleaseJobObjectGuard _(index);
        return jobPool[index];
    }
    qpl_job * ALWAYS_INLINE getJobPtr(uint32_t job_id)
    {
        uint32_t index = jobPoolSize - job_id;
        return jobPool[index];
    }

private:
    size_t ALWAYS_INLINE random(uint32_t pool_size)
    {
        size_t tsc = 0;
        unsigned lo, hi;
        __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi) : :);
        tsc = (((static_cast<uint64_t>(hi)) << 32) | (static_cast<uint64_t>(lo)));
        return (static_cast<size_t>((tsc * 44485709377909ULL) >> 4)) % pool_size;
    }

    int32_t ALWAYS_INLINE get_job_size_helper()
    {
        static uint32_t size = 0;
        if (size == 0)
        {
            const auto status = qpl_get_job_size(PATH, &size);
            if (status != QPL_STS_OK)
            {
                return -1;
            }
        }
        return size;
    }

    int32_t ALWAYS_INLINE init_job_helper(qpl_job * qpl_job_ptr)
    {
        if (qpl_job_ptr == nullptr)
        {
            return -1;
        }
        auto status = qpl_init_job(PATH, qpl_job_ptr);
        if (status != QPL_STS_OK)
        {
            return -1;
        }
        return 0;
    }

    int32_t ALWAYS_INLINE initJobPool()
    {
        static bool initialized = false;

        if (initialized == false)
        {
            const int32_t size = get_job_size_helper();
            if (size < 0)
                return -1;
            for (int i = 0; i < jobPoolSize; ++i)
            {
                jobPool[i] = nullptr;
                qpl_job * qpl_job_ptr = reinterpret_cast<qpl_job *>(new uint8_t[size]);
                if (init_job_helper(qpl_job_ptr) < 0)
                    return -1;
                jobPool[i] = qpl_job_ptr;
                jobLock[i].store(false);
            }
            initialized = true;
        }
        return 0;
    }

    bool ALWAYS_INLINE tryLockJob(size_t index)
    {
        bool expected = false;
        return jobLock[index].compare_exchange_strong(expected, true);
    }

    void ALWAYS_INLINE destroyJobPool()
    {
        const uint32_t size = get_job_size_helper();
        for (uint32_t i = 0; i < jobPoolSize && size > 0; ++i)
        {
            while (tryLockJob(i) == false)
            {
            }
            if (jobPool[i])
            {
                qpl_fini_job(jobPool[i]);
                delete[] jobPool[i];
            }
            jobPool[i] = nullptr;
            jobLock[i].store(false);
        }
    }

    struct ReleaseJobObjectGuard
    {
        uint32_t index;
        ReleaseJobObjectGuard() = delete;

    public:
        ALWAYS_INLINE ReleaseJobObjectGuard(const uint32_t i) : index(i)
        {
        }
        ALWAYS_INLINE ~ReleaseJobObjectGuard()
        {
            jobLock[index].store(false);
        }
    };
};

class DeflateJobSWPool
{
public:
    DeflateJobSWPool();
    ~DeflateJobSWPool();
    static DeflateJobSWPool & instance();

    static constexpr auto jobSWPoolSize = 128;
    static constexpr qpl_path_t SW_PATH = qpl_path_software;
    static qpl_job * jobSWPool[jobSWPoolSize];
    static std::atomic_bool jobSWLock[jobSWPoolSize];

    qpl_job * ALWAYS_INLINE acquireJob(uint32_t * job_id)
    {
        uint32_t retry = 0;
        auto index = random(jobSWPoolSize);
        while (tryLockJob(index) == false)
        {
            index = random(jobSWPoolSize);
            retry++;
            if (retry > jobSWPoolSize)
            {
                return nullptr;
            }
        }
        *job_id = jobSWPoolSize - index;
        return jobSWPool[index];
    }
    qpl_job * ALWAYS_INLINE releaseJob(uint32_t job_id)
    {
        uint32_t index = jobSWPoolSize - job_id;
        ReleaseJobObjectGuard _(index);
        return jobSWPool[index];
    }

private:
    size_t ALWAYS_INLINE random(uint32_t pool_size)
    {
        size_t tsc = 0;
        unsigned lo, hi;
        __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi) : :);
        tsc = (((static_cast<uint64_t>(hi)) << 32) | (static_cast<uint64_t>(lo)));
        return (static_cast<size_t>((tsc * 44485709377909ULL) >> 4)) % pool_size;
    }

    int32_t ALWAYS_INLINE get_job_size_helper()
    {
        static uint32_t size = 0;
        if (size == 0)
        {
            const auto status = qpl_get_job_size(SW_PATH, &size);
            if (status != QPL_STS_OK)
            {
                return -1;
            }
        }
        return size;
    }

    int32_t ALWAYS_INLINE init_job_helper(qpl_job * qpl_job_ptr)
    {
        if (qpl_job_ptr == nullptr)
        {
            return -1;
        }
        auto status = qpl_init_job(SW_PATH, qpl_job_ptr);
        if (status != QPL_STS_OK)
        {
            return -1;
        }
        return 0;
    }

    int32_t ALWAYS_INLINE initJobPool()
    {
        static bool initialized = false;

        if (initialized == false)
        {
            const int32_t size = get_job_size_helper();
            if (size < 0)
                return -1;
            for (int i = 0; i < jobSWPoolSize; ++i)
            {
                jobSWPool[i] = nullptr;
                qpl_job * qpl_job_ptr = reinterpret_cast<qpl_job *>(new uint8_t[size]);
                if (init_job_helper(qpl_job_ptr) < 0)
                    return -1;
                jobSWPool[i] = qpl_job_ptr;
                jobSWLock[i].store(false);
            }
            initialized = true;
        }
        return 0;
    }

    bool ALWAYS_INLINE tryLockJob(size_t index)
    {
        bool expected = false;
        return jobSWLock[index].compare_exchange_strong(expected, true);
    }

    void ALWAYS_INLINE destroyJobPool()
    {
        const uint32_t size = get_job_size_helper();
        for (uint32_t i = 0; i < jobSWPoolSize && size > 0; ++i)
        {
            while (tryLockJob(i) == false)
            {
            }
            if (jobSWPool[i])
            {
                qpl_fini_job(jobSWPool[i]);
                delete[] jobSWPool[i];
            }
            jobSWPool[i] = nullptr;
            jobSWLock[i].store(false);
        }
    }

    struct ReleaseJobObjectGuard
    {
        uint32_t index;
        ReleaseJobObjectGuard() = delete;

    public:
        ALWAYS_INLINE ReleaseJobObjectGuard(const uint32_t i) : index(i)
        {
        }
        ALWAYS_INLINE ~ReleaseJobObjectGuard()
        {
            jobSWLock[index].store(false);
        }
    };
};
class CompressionCodecDeflate : public ICompressionCodec
{
public:
    CompressionCodecDeflate();
    ~CompressionCodecDeflate() override;
    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;
    bool isAsyncSupported() const override;

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
    uint32_t doCompressDataSW(const char * source, uint32_t source_size, char * dest) const;
    uint32_t doCompressDataSWNative(const char * source, uint32_t source_size, char * dest) const;
    UInt32 doCompressDataReq(const char * source, UInt32 source_size, char * dest, uint32_t & req_id) override;
    uint32_t doCompressDataFlush(uint32_t req_id) override;

    void doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const override;
    void doDecompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) override;
    void doDecompressDataSW(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const override;
    static void doDecompressDataSWNative(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size);
    void doDecompressDataFlush() override;

private:
    uint32_t getMaxCompressedDataSize(uint32_t uncompressed_size) const override;
    std::map<uint32_t, qpl_job *> jobDecompAsyncMap;
    std::vector<uint32_t> jobCompAsyncList;
    Poco::Logger * log;
};

}
