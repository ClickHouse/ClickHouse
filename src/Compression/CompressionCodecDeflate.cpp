#ifdef ENABLE_QPL_COMPRESSION
#include <cstdio>
#include <thread>
#include <Compression/CompressionCodecDeflate.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}
qpl_job * DeflateJobHWPool::hw_job_pool[JOB_POOL_SIZE];
std::atomic_bool DeflateJobHWPool::hw_job_locks[JOB_POOL_SIZE];
bool DeflateJobHWPool::job_pool_ready;

DeflateJobHWPool & DeflateJobHWPool::instance()
{
    static DeflateJobHWPool ret;
    return ret;
}

DeflateJobHWPool::DeflateJobHWPool():log(&Poco::Logger::get("DeflateJobHWPool"))
{
    uint32_t size = 0;
    uint32_t index = 0;
    /// get total size required for saving qpl job context
    qpl_get_job_size(PATH, &size);
    /// allocate buffer for storing all job objects
    hw_job_pool_buffer = std::make_unique<uint8_t[]>(size * JOB_POOL_SIZE);
    memset(hw_job_pool, 0, JOB_POOL_SIZE*sizeof(qpl_job *));
    for (index = 0; index < JOB_POOL_SIZE; ++index)
    {
        qpl_job * qpl_job_ptr = reinterpret_cast<qpl_job *>(hw_job_pool_buffer.get() + index*JOB_POOL_SIZE);
        if ((nullptr == qpl_job_ptr) || (qpl_init_job(PATH, qpl_job_ptr) != QPL_STS_OK))
            break;
        hw_job_pool[index] = qpl_job_ptr;
        unLockJob(index);
    }

    const char * qpl_version = qpl_get_library_version();
    if(JOB_POOL_SIZE == index)
    {
        jobPoolReady() = true;
        LOG_DEBUG(log, "QPL deflate HW codec is ready! QPL Version:{}",qpl_version);
    }
    else
    {
        jobPoolReady() = false;
        LOG_WARNING(log, "QPL deflate HW codec is not ready! Please check if IAA hardware support. Will fallback to software codec here. QPL Version:{}.",qpl_version);
    }
}

DeflateJobHWPool::~DeflateJobHWPool()
{
    for (uint32_t i = 0; i < JOB_POOL_SIZE; ++i)
    {
        if (hw_job_pool[i] != nullptr)
        {
            while (!tryLockJob(i));
            qpl_fini_job(hw_job_pool[i]);
        }
        hw_job_pool[i] = nullptr;
        unLockJob(i);
    }
    jobPoolReady() = false;
}

//HardwareCodecDeflate
HardwareCodecDeflate::HardwareCodecDeflate():
    hw_enabled(DeflateJobHWPool::instance().jobPoolReady()),
    log(&Poco::Logger::get("HardwareCodecDeflate"))
{
}

HardwareCodecDeflate::~HardwareCodecDeflate()
{
    if (!decomp_async_job_map.empty())
    {
        LOG_WARNING(log, "Find un-released job when HardwareCodecDeflate destroy");
        for (auto it : decomp_async_job_map)
        {
            DeflateJobHWPool::instance().releaseJob(it.first);
        }
        decomp_async_job_map.clear();
    }
}
uint32_t HardwareCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size) const
{
    uint32_t job_id = 0;
    qpl_job* job_ptr = nullptr;
    uint32_t compressed_size = 0;
    if (job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id); nullptr == job_ptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doCompressData acquireJob fail!");
        return 0;
    }

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = dest_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    if (auto status = qpl_execute_job(job_ptr); QPL_STS_OK == status)
        compressed_size = job_ptr->total_out;
    else
        LOG_WARNING(log, "HardwareCodecDeflate::doCompressData fail ->status: '{}' ", static_cast<size_t>(status));

    DeflateJobHWPool::instance().releaseJob(job_id);
    return compressed_size;
}

uint32_t HardwareCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    uint32_t job_id = 0;
    qpl_job* job_ptr = nullptr;
    if (job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id); nullptr == job_ptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressData acquireJob fail!");
        return 0;
    }
    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_execute_job(job_ptr); QPL_STS_OK == status)
    {
        DeflateJobHWPool::instance().releaseJob(job_id);
        return job_ptr->total_out;
    }
    else
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressData fail ->status: '{}' ", static_cast<size_t>(status));
        DeflateJobHWPool::instance().releaseJob(job_id);
        return 0;
    }
}

uint32_t HardwareCodecDeflate::doDecompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = nullptr;
    if (job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id); nullptr == job_ptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressDataReq acquireJob fail!");
        return 0;
    }

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_submit_job(job_ptr); QPL_STS_OK == status)
    {
        decomp_async_job_map.insert(std::make_pair(job_id, job_ptr));
        return job_id;
    }
    else
    {
        DeflateJobHWPool::instance().releaseJob(job_id);
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressDataReq fail ->status: '{}' ", static_cast<size_t>(status));
        return 0;
    }
}

void HardwareCodecDeflate::flushAsynchronousDecompressRequests()
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = nullptr;

    std::map<uint32_t, qpl_job *>::iterator it;
    uint32_t n_jobs_processing = decomp_async_job_map.size();
    it = decomp_async_job_map.begin();

    while (n_jobs_processing)
    {
        job_id = it->first;
        job_ptr = it->second;

        if (QPL_STS_BEING_PROCESSED == qpl_check_job(job_ptr))
        {
            it++;
        }
        else
        {
            DeflateJobHWPool::instance().releaseJob(job_id);
            it = decomp_async_job_map.erase(it);
            n_jobs_processing--;
            if (n_jobs_processing <= 0)
                break;
        }
        if (it == decomp_async_job_map.end())
        {
            it = decomp_async_job_map.begin();
            _tpause(1, __rdtsc() + 1000);
        }
    }
}

SoftwareCodecDeflate::SoftwareCodecDeflate():sw_job(nullptr)
{
}

SoftwareCodecDeflate::~SoftwareCodecDeflate()
{
    if (nullptr != sw_job)
        qpl_fini_job(sw_job);
}

qpl_job * SoftwareCodecDeflate::getJobCodecPtr()
{
    if (nullptr == sw_job)
    {
        uint32_t size = 0;
        qpl_get_job_size(qpl_path_software, &size);

        sw_job = reinterpret_cast<qpl_job *>((std::make_unique<uint8_t[]>(size)).get());
        // Job initialization
        if (auto status = qpl_init_job(qpl_path_software, sw_job); QPL_STS_OK != status)
            throw Exception(
                "SoftwareCodecDeflate::getJobCodecPtr -> qpl_init_job fail:" + std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
    }
    return sw_job;
}

uint32_t SoftwareCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size)
{
    qpl_job * job_ptr = getJobCodecPtr();
    // Performing a compression operation
    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = dest_size;
    job_ptr->level = qpl_default_level;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    if (auto status = qpl_execute_job(job_ptr); QPL_STS_OK != status)
        throw Exception(
            "SoftwareCodecDeflate::doCompressData -> qpl_execute_job fail:" + std::to_string(status), ErrorCodes::CANNOT_COMPRESS);

    return job_ptr->total_out;
}

void SoftwareCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    qpl_job * job_ptr = getJobCodecPtr();

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_execute_job(job_ptr); QPL_STS_OK != status)
        throw Exception(
            "SoftwareCodecDeflate::doDecompressData -> qpl_execute_job fail:" + std::to_string(status), ErrorCodes::CANNOT_DECOMPRESS);
}

//CompressionCodecDeflate
CompressionCodecDeflate::CompressionCodecDeflate():
    hw_codec(std::make_unique<HardwareCodecDeflate>()),
    sw_codec(std::make_unique<SoftwareCodecDeflate>())
{
    setCodecDescription("DEFLATE");
}

uint8_t CompressionCodecDeflate::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Deflate);
}

void CompressionCodecDeflate::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

uint32_t CompressionCodecDeflate::getMaxCompressedDataSize(uint32_t uncompressed_size) const
{
    /// Aligned with ZLIB
    return ((uncompressed_size) + ((uncompressed_size) >> 12) + ((uncompressed_size) >> 14) + ((uncompressed_size) >> 25) + 13);
}

uint32_t CompressionCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest) const
{
    uint32_t res = 0;
    if (hw_codec->hw_enabled)
        res = hw_codec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    if (0 == res)
        res = sw_codec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    return res;
}

void CompressionCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    switch (getDecompressMode())
    {
        case CodecMode::Synchronous:
        {
            uint32_t res = 0;
            if (hw_codec->hw_enabled)
                res = hw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            if (0 == res)
                sw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            break;
        }
        case CodecMode::Asynchronous:
        {
            uint32_t res = 0;
            if (hw_codec->hw_enabled)
                res = hw_codec->doDecompressDataReq(source, source_size, dest, uncompressed_size);
            if (0 == res)
                sw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            break;
        }
        case CodecMode::SoftwareFallback:
            sw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            break;
    }
}

void CompressionCodecDeflate::flushAsynchronousDecompressRequests()
{
    if (hw_codec->hw_enabled)
        hw_codec->flushAsynchronousDecompressRequests();
    setDecompressMode(CodecMode::Synchronous);
}
void registerCodecDeflate(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec(
        "DEFLATE", static_cast<char>(CompressionMethodByte::Deflate), [&]() { return std::make_shared<CompressionCodecDeflate>(); });
}
}
#endif
