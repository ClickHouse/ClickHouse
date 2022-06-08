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
//DeflateJobHWPool
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
        jobPoolEnabled = false;
        LOG_WARNING(log, "DeflateJobHWPool is not ready. Please check if IAA hardware support.Auto switch to deflate software codec here");
    }
    else
        jobPoolEnabled = true;
}
DeflateJobHWPool::~DeflateJobHWPool()
{
    destroyJobPool();
}
//HardwareCodecDeflate
HardwareCodecDeflate::HardwareCodecDeflate()
{
    log = &Poco::Logger::get("HardwareCodecDeflate");
    hwEnabled = DeflateJobHWPool::instance().jobPoolReady();
}

HardwareCodecDeflate::~HardwareCodecDeflate()
{
    if (!jobDecompAsyncMap.empty())
    {
        LOG_WARNING(log, "Find un-released job when HardwareCodecDeflate destroy");
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
uint32_t HardwareCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size) const
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doCompressData acquireJob fail!");
        return 0;
    }
    qpl_status status;
    uint32_t compressed_size = 0;

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = dest_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
    status = qpl_execute_job(job_ptr);
    if (QPL_STS_OK == status)
    {
        compressed_size = job_ptr->total_out;
    }
    else
        LOG_WARNING(log, "HardwareCodecDeflate::doCompressData fail ->status: '{}' ", static_cast<size_t>(status));

    DeflateJobHWPool::instance().releaseJob(job_id);
    return compressed_size;
}

uint32_t HardwareCodecDeflate::doCompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t dest_size)
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doCompressDataReq acquireJob fail!");
        return 0;
    }
    qpl_status status;

    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->level = qpl_default_level;
    job_ptr->available_out = dest_size;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
    status = qpl_submit_job(job_ptr);
    if (QPL_STS_OK == status)
    {
        jobCompAsyncList.push_back(job_id);
        return job_id;
    }
    else
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doCompressDataReq fail ->status: '{}' ", static_cast<size_t>(status));
        DeflateJobHWPool::instance().releaseJob(job_id);
        return 0;
    }
}

uint32_t HardwareCodecDeflate::doCompressDataFlush(uint32_t req_id)
{
    uint32_t compressed_size = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().getJobPtr(req_id);
    if (nullptr != job_ptr)
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

uint32_t HardwareCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    uint32_t job_id = 0;
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressData acquireJob fail!");
        return 0;
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

    if (status == QPL_STS_OK)
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
    qpl_job * job_ptr = DeflateJobHWPool::instance().acquireJob(&job_id);
    if (job_ptr == nullptr)
    {
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressDataReq acquireJob fail!");
        return 0;
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
        return job_id;
    }
    else
    {
        DeflateJobHWPool::instance().releaseJob(job_id);
        LOG_WARNING(log, "HardwareCodecDeflate::doDecompressDataReq fail ->status: '{}' ", static_cast<size_t>(status));
        return 0;
    }
}

void HardwareCodecDeflate::doDecompressDataFlush()
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
            if (n_jobs_processing <= 0)
                break;
        }
        if (it == jobDecompAsyncMap.end())
        {
            it = jobDecompAsyncMap.begin();
            _tpause(1, __rdtsc() + 1000);
        }
    }
}
//SoftwareCodecDeflate
SoftwareCodecDeflate::SoftwareCodecDeflate()
{
    jobSWPtr = nullptr;
}

SoftwareCodecDeflate::~SoftwareCodecDeflate()
{
    if (nullptr != jobSWPtr)
        qpl_fini_job(jobSWPtr);
}

qpl_job * SoftwareCodecDeflate::getJobCodecPtr()
{
    if (nullptr == jobSWPtr)
    {
        qpl_status status;
        uint32_t size = 0;
        // Job initialization
        status = qpl_get_job_size(qpl_path_software, &size);
        if (status != QPL_STS_OK)
        {
            throw Exception(
                "SoftwareCodecDeflate::getJobCodecPtr -> qpl_get_job_size fail:" + std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
        }

        jobSWbuffer = std::make_unique<uint8_t[]>(size);
        jobSWPtr = reinterpret_cast<qpl_job *>(jobSWbuffer.get());

        status = qpl_init_job(qpl_path_software, jobSWPtr);
        if (status != QPL_STS_OK)
        {
            throw Exception(
                "SoftwareCodecDeflate::getJobCodecPtr -> qpl_init_job fail:" + std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
        }
    }
    return jobSWPtr;
}

uint32_t SoftwareCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest, uint32_t dest_size)
{
    qpl_status status;
    qpl_job * job_ptr = getJobCodecPtr();
    // Performing a compression operation
    job_ptr->op = qpl_op_compress;
    job_ptr->next_in_ptr = reinterpret_cast<uint8_t *>(const_cast<char *>(source));
    job_ptr->next_out_ptr = reinterpret_cast<uint8_t *>(dest);
    job_ptr->available_in = source_size;
    job_ptr->available_out = dest_size;
    job_ptr->level = qpl_default_level;
    job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;

    // Compression
    status = qpl_execute_job(job_ptr);
    if (status != QPL_STS_OK)
    {
        throw Exception(
            "SoftwareCodecDeflate::doCompressData -> qpl_execute_job fail:" + std::to_string(status), ErrorCodes::CANNOT_COMPRESS);
    }

    return job_ptr->total_out;
}

void SoftwareCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    qpl_status status;
    qpl_job * job_ptr = getJobCodecPtr();

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
        throw Exception(
            "SoftwareCodecDeflate::doDecompressData -> qpl_execute_job fail:" + std::to_string(status), ErrorCodes::CANNOT_DECOMPRESS);
    }
}

//CompressionCodecDeflate
CompressionCodecDeflate::CompressionCodecDeflate()
{
    setCodecDescription("DEFLATE");
    hwCodec = std::make_unique<HardwareCodecDeflate>();
    swCodec = std::make_unique<SoftwareCodecDeflate>();
}

uint8_t CompressionCodecDeflate::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Deflate);
}

bool CompressionCodecDeflate::isAsyncSupported() const
{
    return hwCodec->hwEnabled;
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

uint32_t CompressionCodecDeflate::doCompressDataSW(const char * source, uint32_t source_size, char * dest) const
{
    return swCodec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
}

uint32_t CompressionCodecDeflate::doCompressData(const char * source, uint32_t source_size, char * dest) const
{
    uint32_t res = 0;
    if (hwCodec->hwEnabled)
        res = hwCodec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    if (0 == res)
        res = swCodec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    return res;
}

uint32_t CompressionCodecDeflate::doCompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t & req_id)
{
    if (hwCodec->hwEnabled)
        req_id = hwCodec->doCompressDataReq(source, source_size, dest, getMaxCompressedDataSize(source_size));
    else
        req_id = 0;

    if (0 == req_id)
        return swCodec->doCompressData(source, source_size, dest, getMaxCompressedDataSize(source_size));
    else
        return 0;
}

uint32_t CompressionCodecDeflate::doCompressDataFlush(uint32_t req_id)
{
    if (hwCodec->hwEnabled)
        return hwCodec->doCompressDataFlush(req_id);
    else
        return 0;
}

void CompressionCodecDeflate::doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    uint32_t res = 0;
    if (hwCodec->hwEnabled)
        res = hwCodec->doDecompressData(source, source_size, dest, uncompressed_size);
    if (0 == res)
        swCodec->doDecompressData(source, source_size, dest, uncompressed_size);
}

void CompressionCodecDeflate::doDecompressDataSW(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const
{
    return swCodec->doDecompressData(source, source_size, dest, uncompressed_size);
}

void CompressionCodecDeflate::doDecompressDataReq(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size)
{
    uint32_t res = 0;
    if (hwCodec->hwEnabled)
        res = hwCodec->doDecompressDataReq(source, source_size, dest, uncompressed_size);
    if (0 == res)
        swCodec->doDecompressData(source, source_size, dest, uncompressed_size);
}

void CompressionCodecDeflate::doDecompressDataFlush()
{
    if (hwCodec->hwEnabled)
        hwCodec->doDecompressDataFlush();
}
void registerCodecDeflate(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec(
        "DEFLATE", static_cast<char>(CompressionMethodByte::Deflate), [&]() { return std::make_shared<CompressionCodecDeflate>(); });
}
}
