#include <thread>
#include <cstdio>
#include <Compression/CompressionCodecDeflate.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}

qpl_job * DeflateJobHWPool::jobPool[jobPoolSize];
std::atomic_bool DeflateJobHWPool::jobLock[jobPoolSize];

DeflateJobHWPool & DeflateJobHWPool::instance()
{
    static DeflateJobHWPool ret;
    return ret;
}

DeflateJobHWPool::DeflateJobHWPool()
{
    log = &Poco::Logger::get("DeflateJobHWPool");
    if (initJobPool() < 0)
    {
        jobPoolAvailable = false;
        LOG_WARNING(log, "DeflateJobHWPool initializing fail! Please check if IAA hardware support.");
    }
    else
    {
        jobPoolAvailable = true;
    }
}
DeflateJobHWPool::~DeflateJobHWPool()
{
    destroyJobPool();
}

CompressionCodecDeflate::CompressionCodecDeflate()
{
    log = &Poco::Logger::get("CompressionCodecDeflate");
    setCodecDescription("DEFLATE");
    jobSWPtr = initSoftwareJobCodecPtr();
}

CompressionCodecDeflate::~CompressionCodecDeflate()
{
    if (!jobDecompAsyncMap.empty())
    {
        LOG_ERROR(log, "Exception -> find un-released job when CompressionCodecDeflate destroy");
        for (auto it : jobDecompAsyncMap)
        {
            DeflateJobHWPool::instance().releaseJob(it.first);
        }
        jobDecompAsyncMap.clear();
    }
    if (!jobCompAsyncList.empty())
    {
        for (auto id : jobCompAsyncList)
        {
            DeflateJobHWPool::instance().releaseJob(id);
        }
        jobCompAsyncList.clear();
    }
}

uint8_t CompressionCodecDeflate::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Deflate);
}

bool CompressionCodecDeflate::isAsyncSupported() const
{
    return true;
}

void CompressionCodecDeflate::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

#define DEFLATE_COMPRESSBOUND(isize) ((isize) + ((isize) >> 12) + ((isize) >> 14) + ((isize) >> 25) + 13) //Aligned with ZLIB
uint32_t CompressionCodecDeflate::getMaxCompressedDataSize(uint32_t uncompressed_size) const
{
    return DEFLATE_COMPRESSBOUND(uncompressed_size);
}

qpl_job * CompressionCodecDeflate::initSoftwareJobCodecPtr()
{
    qpl_job * job_ptr;
    qpl_status status;
    uint32_t size = 0;
    std::unique_ptr<uint8_t[]> job_buffer;

    // Job initialization
    status = qpl_get_job_size(qpl_path_software, &size);
    if (status != QPL_STS_OK)
    {
        throw Exception("initSoftwareJobCodecPtr: qpl_get_job_size fail:"+ std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
    }

    job_buffer = std::make_unique<uint8_t[]>(size);
    job_ptr = reinterpret_cast<qpl_job *>(job_buffer.get());

    status = qpl_init_job(qpl_path_software, job_ptr);
    if (status != QPL_STS_OK)
    {
        throw Exception("initSoftwareJobCodecPtr: qpl_init_job fail:"+ std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
    }
    return job_ptr;
}

uint32_t CompressionCodecDeflate::doCompressDataSW(const char * source, uint32_t source_size, char * dest)const
{
    qpl_status status;
    qpl_job * job_ptr = jobSWPtr;
    // Performing a compression operation
    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = getMaxCompressedDataSize(source_size);
    job_ptr->level = qpl_high_level;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    // Compression
    status = qpl_execute_job(job_ptr);
    if (status != QPL_STS_OK)
    {
        throw Exception("doCompressDataSW cannot compress: qpl_execute_job fail:" + std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
    }

    return job_ptr->total_out;
}

uint32_t CompressionCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest) const
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "doCompressData HW acquireJob fail! switch to SW compress...");
        return doCompressDataSW(source, source_size, dest);
    }
    qpl_status status;
    uint32_t compressed_size = 0;

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = getMaxCompressedDataSize(source_size);
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
    // Compression
    status = qpl_execute_job(job_ptr);
    if (QPL_STS_OK == status)
    {
        compressed_size = job_ptr->total_out;
    }
    else
    {
        LOG_WARNING(log, "doCompressData HW fail! switch to SW compress ->status: '{}' ", static_cast<size_t>(status));
        compressed_size = doCompressDataSW(source, source_size, dest);
    }
    DeflateJobHWPool::instance().releaseJob(job_id);
    return compressed_size;
}

UInt32 CompressionCodecDeflate::doCompressDataReq(const char * source, UInt32 source_size, char * dest, UInt32 & req_id)
{
    uint32_t job_id = 0;
    req_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "doCompressDataReq HW acquireJob fail! switch to SW compress...");
        return doCompressDataSW(source, source_size, dest);
    }
    qpl_status status;

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = getMaxCompressedDataSize(source_size);
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
    // Compression
    status = qpl_submit_job(job_ptr);
    if (QPL_STS_OK != status)
    {
        LOG_WARNING(log, "doCompressDataReq HW fail! switch to SW compress ->status: '{}' ", static_cast<size_t>(status));
        DeflateJobHWPool::instance().releaseJob(job_id);
        return doCompressDataSW(source, source_size, dest);
    }
    //LOG_WARNING(log, "doCompressDataReq ->job_id:{}, source_size:{}",job_id, source_size);
    jobCompAsyncList.push_back(job_id);
    req_id = job_id;
    return 0;
}

uint32_t CompressionCodecDeflate::doCompressDataFlush(uint32_t req_id)
{
    uint32_t compressed_size = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().getJobPtr(req_id);
    if(nullptr != job_ptr)
    {
        while (QPL_STS_BEING_PROCESSED == qpl_check_job(job_ptr))
        {
            _tpause(1, __rdtsc() + 1000);
        }
        compressed_size = job_ptr->total_out;
        DeflateJobHWPool::instance().releaseJob(req_id);
    }
    return compressed_size;
}

void CompressionCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "doDecompressData HW acquireJob fail! switch to SW decompress");
        return doDecompressDataSW(source, source_size, dest, uncompressed_size);
    }
    qpl_status status;

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    // Decompression
    status = qpl_execute_job(job_ptr);
    if (status != QPL_STS_OK)
    {
        LOG_WARNING(
            log,
            "doDecompressData HW fail! switch to SW decompress ->status: '{}' ,source_size: '{}' ,uncompressed_size: '{}'  ",
            static_cast<size_t>(status),
            source_size,
            uncompressed_size);
        doDecompressDataSW(source, source_size, dest, uncompressed_size);
    }
    DeflateJobHWPool::instance().releaseJob(job_id);
}

void CompressionCodecDeflate::doDecompressDataSW(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)const
{
    qpl_status status;
    qpl_job * job_ptr = jobSWPtr;

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    // Decompression
    status = qpl_execute_job(job_ptr);
    if (status != QPL_STS_OK)
    {
        throw Exception("doDecompressDataSW cannot decompress: qpl_execute_job fail:"+ std::to_string(status), ErrorCodes::CANNOT_DECOMPRESS);
    }
}

void CompressionCodecDeflate::doDecompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "doDecompressDataReq acquireJob fail! switch to SW decompress");
        doDecompressDataSW(source, source_size, dest, uncompressed_size);
        return;
    }
    qpl_status status;

    // Performing a decompression operation
    job_ptr->op = qpl_op_decompress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = uncompressed_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

    // Decompression
    status = qpl_submit_job(job_ptr);
    if (QPL_STS_OK == status)
    {
        jobDecompAsyncMap.insert(std::make_pair(job_id, job_ptr));
    }
    else
    {
        DeflateJobHWPool::instance().releaseJob(job_id);
        LOG_WARNING(log, "doDecompressDataReq HW fail! switch to SW decompress... ->status: '{}' ", static_cast<size_t>(status));
        doDecompressDataSW(source, source_size, dest, uncompressed_size);
    }
}

void CompressionCodecDeflate::doDecompressDataFlush()
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = nullptr;


    std::map<uint32_t, qpl_job *>::iterator it;
    uint32_t n_jobs_processing = jobDecompAsyncMap.size();
    it = jobDecompAsyncMap.begin();

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
            it = jobDecompAsyncMap.erase(it);
            n_jobs_processing--;
        }
        if (it == jobDecompAsyncMap.end())
        {
            it = jobDecompAsyncMap.begin();
            _tpause(1, __rdtsc() + 1000);
        }
    }
}

void registerCodecDeflate(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec(
        "DEFLATE", static_cast<char>(CompressionMethodByte::Deflate), [&]() { return std::make_shared<CompressionCodecDeflate>(); });
}

}
