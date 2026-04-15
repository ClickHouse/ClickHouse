#pragma once
#include "config.h"
#if USE_DISKANN

#include <cstdint>
#include <memory>
#include <string>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

enum class DiskANNMetric : uint8_t
{
    L2 = 0,
    Cosine = 1,
};

struct DiskANNParams
{
    uint32_t pruned_degree = 32;
    uint32_t max_degree = 64;
    uint32_t l_build = 128;
    float alpha = 1.2f;
};

class DiskANNIndexWithSerialization
{
public:
    DiskANNIndexWithSerialization(
        size_t dimensions,
        DiskANNMetric metric,
        DiskANNParams params = {});

    explicit DiskANNIndexWithSerialization(int64_t handle, size_t dimensions, DiskANNMetric metric);

    ~DiskANNIndexWithSerialization();

    DiskANNIndexWithSerialization(const DiskANNIndexWithSerialization &) = delete;
    DiskANNIndexWithSerialization & operator=(const DiskANNIndexWithSerialization &) = delete;
    DiskANNIndexWithSerialization(DiskANNIndexWithSerialization && other) noexcept;
    DiskANNIndexWithSerialization & operator=(DiskANNIndexWithSerialization && other) noexcept;

    void build(const float * vectors, size_t count);
    size_t search(const float * query, size_t k, uint64_t * ids, float * distances) const;
    void serialize(WriteBuffer & ostr) const;
    static DiskANNIndexWithSerialization deserialize(ReadBuffer & istr, size_t dimensions, DiskANNMetric metric);

    size_t size() const;
    bool empty() const { return size() == 0; }
    size_t getDimensions() const { return dim; }
    DiskANNMetric getMetric() const { return metric; }

private:
    int64_t handle = -1;
    size_t dim;
    DiskANNMetric metric;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

using DiskANNIndexWithSerializationPtr = std::shared_ptr<DiskANNIndexWithSerialization>;

}

#endif
