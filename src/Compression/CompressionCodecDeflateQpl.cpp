#ifdef ENABLE_QPL_COMPRESSION
#include <cstdio>
#include <thread>
#include <Compression/CompressionCodecDeflateQpl.h>
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
qpl_job * DeflateQplJobHWPool::hw_job_pool[JOB_POOL_SIZE];
std::atomic_bool DeflateQplJobHWPool::hw_job_locks[JOB_POOL_SIZE];
bool DeflateQplJobHWPool::job_pool_ready;

DeflateQplJobHWPool & DeflateQplJobHWPool::instance()
{
    static DeflateQplJobHWPool ret;
    return ret;
}

DeflateQplJobHWPool::DeflateQplJobHWPool():
    log(&Poco::Logger::get("DeflateQplJobHWPool")),
    random_engine(std::random_device()()),
    distribution(0, JOB_POOL_SIZE-1)
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
        if ((!qpl_job_ptr) || (qpl_init_job(PATH, qpl_job_ptr) != QPL_STS_OK))
            break;
        hw_job_pool[index] = qpl_job_ptr;
        unLockJob(index);
    }

    const char * qpl_version = qpl_get_library_version();
    if(JOB_POOL_SIZE == index)
    {
        jobPoolReady() = true;
        LOG_DEBUG(log, "Hardware-assisted DeflateQpl codec is ready! QPL Version:{}",qpl_version);
    }
    else
    {
        jobPoolReady() = false;
        LOG_WARNING(log, "Initialization of hardware-assisted DeflateQpl codec failed, falling back to software DeflateQpl codec. Please check if Intel In-Memory Analytics Accelerator (IAA) is properly set up. QPL Version:{}.",qpl_version);
    }
}

DeflateQplJobHWPool::~DeflateQplJobHWPool()
{
    for (uint32_t i = 0; i < JOB_POOL_SIZE; ++i)
    {
        if (hw_job_pool[i])
        {
            while (!tryLockJob(i));
            qpl_fini_job(hw_job_pool[i]);
            unLockJob(i);
            hw_job_pool[i] = nullptr;
        }
    }
    jobPoolReady() = false;
}

qpl_job * DeflateQplJobHWPool::acquireJob(uint32_t * job_id)
{
    if (jobPoolReady())
    {
        uint32_t retry = 0;
        auto index = distribution(random_engine);
        while (tryLockJob(index) == false)
        {
            index = distribution(random_engine);
            retry++;
            if (retry > JOB_POOL_SIZE)
            {
                return nullptr;
            }
        }
        *job_id = JOB_POOL_SIZE - index;
        return hw_job_pool[index];
    }
    else
        return nullptr;
}

qpl_job * DeflateQplJobHWPool::releaseJob(uint32_t job_id)
{
    if (jobPoolReady())
    {
        uint32_t index = JOB_POOL_SIZE - job_id;
        ReleaseJobObjectGuard _(index);
        return hw_job_pool[index];
    }
    else
        return nullptr;
}

bool DeflateQplJobHWPool::tryLockJob(size_t index)
{
    bool expected = false;
    assert(index < JOB_POOL_SIZE);
    return hw_job_locks[index].compare_exchange_strong(expected, true);
}

//HardwareCodecDeflateQpl
HardwareCodecDeflateQpl::HardwareCodecDeflateQpl():
    log(&Poco::Logger::get("HardwareCodecDeflateQpl"))
{
}

HardwareCodecDeflateQpl::~HardwareCodecDeflateQpl()
{
    if (!decomp_async_job_map.empty())
    {
        LOG_WARNING(log, "Find un-released job when HardwareCodecDeflateQpl destroy");
        for (auto it : decomp_async_job_map)
        {
            DeflateQplJobHWPool::instance().releaseJob(it.first);
        }
        decomp_async_job_map.clear();
    }
}

int32_t HardwareCodecDeflateQpl::doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size) const
{
    uint32_t job_id = 0;
    qpl_job* job_ptr = nullptr;
    uint32_t compressed_size = 0;
    if (!(job_ptr = DeflateQplJobHWPool::instance().acquireJob(&job_id)))
    {
        LOG_WARNING(log, "DeflateQpl HW codec failed, falling back to SW codec.(Details: doCompressData->acquireJob fail, probably job pool exhausted)");
        return RET_ERROR;
    }

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = dest_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    if (auto status = qpl_execute_job(job_ptr); status == QPL_STS_OK)
        compressed_size = job_ptr->total_out;
    else
        LOG_WARNING(log, "DeflateQpl HW codec failed, falling back to SW codec.(Details: doCompressData->qpl_execute_job with error code:{} - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)", status);
    DeflateQplJobHWPool::instance().releaseJob(job_id);
    return compressed_size;
}

int32_t HardwareCodecDeflateQpl::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    uint32_t job_id = 0;
    qpl_job* job_ptr = nullptr;
    if (!(job_ptr = DeflateQplJobHWPool::instance().acquireJob(&job_id)))
    {
        LOG_WARNING(log, "DeflateQpl HW codec failed, falling back to SW codec.(Details: doDecompressData->acquireJob fail, probably job pool exhausted)");
        return RET_ERROR;
    }
    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_execute_job(job_ptr); status == QPL_STS_OK)
    {
        DeflateQplJobHWPool::instance().releaseJob(job_id);
        return job_ptr->total_out;
    }
    else
    {
        LOG_WARNING(log, "DeflateQpl HW codec failed, falling back to SW codec.(Details: doDecompressData->qpl_execute_job with error code:{} - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)", status);
        DeflateQplJobHWPool::instance().releaseJob(job_id);
        return RET_ERROR;
    }
}

int32_t HardwareCodecDeflateQpl::doDecompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = nullptr;
    if (!(job_ptr = DeflateQplJobHWPool::instance().acquireJob(&job_id)))
    {
        LOG_WARNING(log, "DeflateQpl HW codec failed, falling back to SW codec.(Details: doDecompressDataReq->acquireJob fail, probably job pool exhausted)");
        return RET_ERROR;
    }

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_submit_job(job_ptr); status == QPL_STS_OK)
    {
        decomp_async_job_map.insert({job_id, job_ptr});
        return job_id;
    }
    else
    {
        DeflateQplJobHWPool::instance().releaseJob(job_id);
        LOG_WARNING(log, "DeflateQpl HW codec failed, falling back to SW codec.(Details: doDecompressDataReq->qpl_execute_job with error code:{} - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)", status);
        return RET_ERROR;
    }
}

void HardwareCodecDeflateQpl::flushAsynchronousDecompressRequests()
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
            DeflateQplJobHWPool::instance().releaseJob(job_id);
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

SoftwareCodecDeflateQpl::~SoftwareCodecDeflateQpl()
{
    if (nullptr != sw_job)
        qpl_fini_job(sw_job);
}

qpl_job * SoftwareCodecDeflateQpl::getJobCodecPtr()
{
    if (!sw_job)
    {
        uint32_t size = 0;
        qpl_get_job_size(qpl_path_software, &size);

        sw_job = reinterpret_cast<qpl_job *>((std::make_unique<uint8_t[]>(size)).get());
        // Job initialization
        if (auto status = qpl_init_job(qpl_path_software, sw_job); status != QPL_STS_OK)
            throw Exception(ErrorCodes::CANNOT_COMPRESS,
                "Initialization of DeflateQpl software fallback codec failed. (Details: qpl_init_job with error code {} - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)", status);
    }
    return sw_job;
}

uint32_t SoftwareCodecDeflateQpl::doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size)
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

    if (auto status = qpl_execute_job(job_ptr); status != QPL_STS_OK)
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Execution of DeflateQpl software fallback codec failed. (Details: qpl_execute_job with error code {} - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)", status);

    return job_ptr->total_out;
}

void SoftwareCodecDeflateQpl::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    qpl_job * job_ptr = getJobCodecPtr();

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    if (auto status = qpl_execute_job(job_ptr); status != QPL_STS_OK)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Execution of DeflateQpl software fallback codec failed. (Details: qpl_execute_job with error code {} - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h)", status);
}

//CompressionCodecDeflateQpl
CompressionCodecDeflateQpl::CompressionCodecDeflateQpl():
    hw_codec(std::make_unique<HardwareCodecDeflateQpl>()),
    sw_codec(std::make_unique<SoftwareCodecDeflateQpl>())
{
    setCodecDescription("DEFLATE_QPL");
}

uint8_t CompressionCodecDeflateQpl::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::DeflateQpl);
}

void CompressionCodecDeflateQpl::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

uint32_t CompressionCodecDeflateQpl::getMaxCompressedDataSize(uint32_t uncompressed_size) const
{
    /// Aligned with ZLIB
    return ((uncompressed_size) + ((uncompressed_size) >> 12) + ((uncompressed_size) >> 14) + ((uncompressed_size) >> 25) + 13);
}

uint32_t CompressionCodecDeflateQpl::doCompressData(const char * source, uint32_t source_size, char * dest) const
{
    int32_t res = HardwareCodecDeflateQpl::RET_ERROR;
    if (DeflateQplJobHWPool::instance().jobPoolReady())
        res = hw_codec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    if (res == HardwareCodecDeflateQpl::RET_ERROR)
        res = sw_codec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    return res;
}

void CompressionCodecDeflateQpl::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    switch (getDecompressMode())
    {
        case CodecMode::Synchronous:
        {
            int32_t res = HardwareCodecDeflateQpl::RET_ERROR;
            if (DeflateQplJobHWPool::instance().jobPoolReady())
                res = hw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            if (res == HardwareCodecDeflateQpl::RET_ERROR)
                sw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            break;
        }
        case CodecMode::Asynchronous:
        {
            int32_t res = HardwareCodecDeflateQpl::RET_ERROR;
            if (DeflateQplJobHWPool::instance().jobPoolReady())
                res = hw_codec->doDecompressDataReq(source, source_size, dest, uncompressed_size);
            if (res == HardwareCodecDeflateQpl::RET_ERROR)
                sw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            break;
        }
        case CodecMode::SoftwareFallback:
            sw_codec->doDecompressData(source, source_size, dest, uncompressed_size);
            break;
    }
}

void CompressionCodecDeflateQpl::flushAsynchronousDecompressRequests()
{
    if (DeflateQplJobHWPool::instance().jobPoolReady())
        hw_codec->flushAsynchronousDecompressRequests();
    setDecompressMode(CodecMode::Synchronous);
}
void registerCodecDeflateQpl(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec(
        "DEFLATE_QPL", static_cast<char>(CompressionMethodByte::DeflateQpl), [&]() { return std::make_shared<CompressionCodecDeflateQpl>(); });
}
}
#endif
