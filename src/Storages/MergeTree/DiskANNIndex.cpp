#include <Storages/MergeTree/DiskANNIndex.h>

#if USE_DISKANN

#include <diskann_ffi.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

::DiskANNMetric toFFIMetric(DiskANNMetric metric)
{
    switch (metric)
    {
        case DiskANNMetric::L2:
            return DISKANN_METRIC_L2;
        case DiskANNMetric::Cosine:
            return DISKANN_METRIC_COSINE;
    }
    UNREACHABLE();
}

std::string getLastFFIError()
{
    char buf[512];
    int64_t len = diskann_last_error(buf, sizeof(buf));
    if (len <= 0)
        return "unknown error";
    return std::string(buf, static_cast<size_t>(len));
}

}

DiskANNIndexWithSerialization::DiskANNIndexWithSerialization(
    size_t dimensions,
    DiskANNMetric metric_,
    DiskANNParams params)
    : dim(dimensions)
    , metric(metric_)
{
    handle = diskann_create_index(
        static_cast<uint32_t>(dim),
        toFFIMetric(metric),
        params.pruned_degree,
        params.max_degree,
        params.l_build,
        params.alpha);

    if (handle < 0)
        throwFromFFIError("DiskANN create_index failed");
}

DiskANNIndexWithSerialization::DiskANNIndexWithSerialization(
    int64_t handle_,
    size_t dimensions,
    DiskANNMetric metric_)
    : handle(handle_)
    , dim(dimensions)
    , metric(metric_)
{
}

DiskANNIndexWithSerialization::~DiskANNIndexWithSerialization()
{
    if (handle >= 0)
        diskann_drop_index(handle);
}

DiskANNIndexWithSerialization::DiskANNIndexWithSerialization(DiskANNIndexWithSerialization && other) noexcept
    : handle(other.handle)
    , dim(other.dim)
    , metric(other.metric)
{
    other.handle = -1;
}

DiskANNIndexWithSerialization & DiskANNIndexWithSerialization::operator=(DiskANNIndexWithSerialization && other) noexcept
{
    if (this != &other)
    {
        if (handle >= 0)
            diskann_drop_index(handle);

        handle = other.handle;
        dim = other.dim;
        metric = other.metric;
        other.handle = -1;
    }
    return *this;
}

void DiskANNIndexWithSerialization::build(const float * vectors, size_t count)
{
    auto rc = diskann_insert_batch(handle, vectors, count, static_cast<uint32_t>(dim));
    if (rc < 0)
        throwFromFFIError("DiskANN insert_batch failed");
}

size_t DiskANNIndexWithSerialization::search(const float * query, size_t k, uint64_t * ids, float * distances) const
{
    auto rc = diskann_search(handle, query, static_cast<uint32_t>(dim), k, ids, distances);
    if (rc < 0)
        throwFromFFIError("DiskANN search failed");
    return static_cast<size_t>(rc);
}

void DiskANNIndexWithSerialization::serialize(WriteBuffer & ostr) const
{
    uint8_t * out_ptr = nullptr;
    uint64_t out_size = 0;

    auto rc = diskann_serialize(handle, &out_ptr, &out_size);
    if (rc < 0)
        throwFromFFIError("DiskANN serialize failed");

    try
    {
        ostr.write(reinterpret_cast<const char *>(out_ptr), out_size);
    }
    catch (...)
    {
        diskann_free_buffer(out_ptr);
        throw;
    }

    diskann_free_buffer(out_ptr);
}

DiskANNIndexWithSerialization DiskANNIndexWithSerialization::deserialize(
    ReadBuffer & istr,
    size_t dimensions,
    DiskANNMetric metric_)
{
    /// Read entire buffer into a string
    std::string buf;
    {
        constexpr size_t chunk_size = 65536;
        while (!istr.eof())
        {
            size_t old_size = buf.size();
            buf.resize(old_size + chunk_size);
            size_t bytes_read = istr.read(buf.data() + old_size, chunk_size);
            buf.resize(old_size + bytes_read);
        }
    }

    int64_t new_handle = diskann_deserialize(
        reinterpret_cast<const uint8_t *>(buf.data()),
        buf.size());

    if (new_handle < 0)
        throwFromFFIError("DiskANN deserialize failed");

    return DiskANNIndexWithSerialization(new_handle, dimensions, metric_);
}

size_t DiskANNIndexWithSerialization::size() const
{
    if (handle < 0)
        return 0;

    auto rc = diskann_index_size(handle);
    if (rc < 0)
        throwFromFFIError("DiskANN index_size failed");
    return static_cast<size_t>(rc);
}

[[noreturn]] void DiskANNIndexWithSerialization::throwFromFFIError(const std::string & context)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "{}: {}", context, getLastFFIError());
}

}

#endif
