#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <limits>
#include <memory>
#include <numeric>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace PDX
{

static constexpr size_t PDX_MAX_DIMS = 16384;

enum class DistanceMetric
{
    L2SQ,
    COSINE,
    IP,
};

struct KNNCandidate
{
    uint32_t index;
    float distance;
};

struct PDXIndexConfig
{
    uint32_t num_dimensions = 0;
    DistanceMetric distance_metric = DistanceMetric::L2SQ;
    uint32_t seed = 42;
    uint32_t num_clusters = 0;
    uint32_t num_meso_clusters = 0;
    bool normalize = false;
    float sampling_fraction = 0.0f;
    uint32_t kmeans_iters = 10;
    bool hierarchical_indexing = false;
    uint32_t n_threads = 0;

    void Validate() const
    {
        if (num_dimensions == 0 || num_dimensions > PDX_MAX_DIMS)
        {
            throw std::invalid_argument(
                "num_dimensions must be in [1, " + std::to_string(PDX_MAX_DIMS) + "]");
        }
    }

    void ValidateNumEmbeddings(size_t num_embeddings_value) const
    {
        if (num_embeddings_value > std::numeric_limits<uint32_t>::max())
            throw std::invalid_argument("num_embeddings must be <= UInt32::max() for MVP PDX index");

        if (num_clusters > 0 && num_clusters > num_embeddings_value)
        {
            throw std::invalid_argument(
                "num_clusters must be <= num_embeddings (got num_clusters=" + std::to_string(num_clusters)
                    + ", num_embeddings=" + std::to_string(num_embeddings_value) + ")");
        }
    }
};

class IPDXIndex
{
public:
    virtual ~IPDXIndex() = default;

    virtual std::vector<KNNCandidate> Search(const float * query_embedding, size_t knn) const = 0;
    virtual std::vector<KNNCandidate> FilteredSearch(
        const float * query_embedding,
        size_t knn,
        const std::vector<size_t> & passing_row_ids) const = 0;

    virtual void BuildIndex(const float * embeddings, size_t num_embeddings) = 0;
    virtual void SetNProbe(uint32_t n_probe) = 0;

    virtual void Save(const std::string & path) = 0;
    virtual void Restore(const std::string & path) = 0;

    virtual uint32_t GetNumDimensions() const = 0;
    virtual uint32_t GetNumClusters() const = 0;
    virtual uint32_t GetClusterSize(uint32_t cluster_id) const = 0;
    virtual std::vector<uint32_t> GetClusterRowIds(uint32_t cluster_id) const = 0;
    virtual size_t GetInMemorySizeInBytes() const = 0;

    virtual void Append(size_t /*row_id*/, const float * /*embedding*/)
    {
        throw std::runtime_error("Append is not supported by this index type");
    }

    virtual void Delete(size_t /*row_id*/)
    {
        throw std::runtime_error("Delete is not supported by this index type");
    }
};

class PDXIndexF32 final : public IPDXIndex
{
public:
    explicit PDXIndexF32(PDXIndexConfig config_)
        : config(std::move(config_))
    {
        config.Validate();
        if (config.num_clusters == 0)
            config.num_clusters = 1;
    }

    std::vector<KNNCandidate> Search(const float * query_embedding, size_t knn) const override
    {
        if (!query_embedding)
            throw std::invalid_argument("query_embedding is null");
        if (knn == 0 || num_embeddings == 0)
            return {};

        const size_t effective_knn = std::min(knn, num_embeddings);
        const float query_sq_norm = computeSquaredNorm(query_embedding);

        using CandidateHeap = std::priority_queue<KNNCandidate, std::vector<KNNCandidate>, FarthestFirst>;
        CandidateHeap heap;

        for (size_t row = 0; row < num_embeddings; ++row)
            addCandidate(row, query_embedding, query_sq_norm, effective_knn, heap);

        return drainHeapToSortedResult(heap);
    }

    std::vector<KNNCandidate> FilteredSearch(
        const float * query_embedding,
        size_t knn,
        const std::vector<size_t> & passing_row_ids) const override
    {
        if (!query_embedding)
            throw std::invalid_argument("query_embedding is null");
        if (knn == 0 || num_embeddings == 0 || passing_row_ids.empty())
            return {};

        const size_t effective_knn = std::min(knn, passing_row_ids.size());
        const float query_sq_norm = computeSquaredNorm(query_embedding);

        using CandidateHeap = std::priority_queue<KNNCandidate, std::vector<KNNCandidate>, FarthestFirst>;
        CandidateHeap heap;

        for (size_t row : passing_row_ids)
        {
            if (row >= num_embeddings)
                continue;
            addCandidate(row, query_embedding, query_sq_norm, effective_knn, heap);
        }

        return drainHeapToSortedResult(heap);
    }

    void BuildIndex(const float * embeddings, size_t num_embeddings_) override
    {
        config.Validate();
        config.ValidateNumEmbeddings(num_embeddings_);

        if (num_embeddings_ > 0 && !embeddings)
            throw std::invalid_argument("embeddings is null");

        num_embeddings = num_embeddings_;
        const size_t dims = config.num_dimensions;

        dim_major_embeddings.assign(dims * num_embeddings, 0.0f);
        squared_norms.assign(num_embeddings, 0.0f);

        if (num_embeddings == 0)
            return;

        const bool normalize_rows = config.normalize && (config.distance_metric == DistanceMetric::COSINE || config.distance_metric == DistanceMetric::IP);

        for (size_t row = 0; row < num_embeddings; ++row)
        {
            float row_sq_norm = 0.0f;
            const size_t row_offset = row * dims;

            if (normalize_rows)
            {
                for (size_t dim = 0; dim < dims; ++dim)
                {
                    const float value = embeddings[row_offset + dim];
                    row_sq_norm += value * value;
                }
            }

            const float row_norm = std::sqrt(row_sq_norm);
            const float row_norm_inv = (row_norm > 0.0f ? 1.0f / row_norm : 1.0f);

            float stored_sq_norm = 0.0f;
            for (size_t dim = 0; dim < dims; ++dim)
            {
                float value = embeddings[row_offset + dim];
                if (normalize_rows)
                    value *= row_norm_inv;

                dim_major_embeddings[dim * num_embeddings + row] = value;
                stored_sq_norm += value * value;
            }

            squared_norms[row] = stored_sq_norm;
        }
    }

    void SetNProbe(uint32_t n_probe_) override
    {
        n_probe = std::max<uint32_t>(1, n_probe_);
    }

    void Save(const std::string & path) override
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out)
            throw std::runtime_error("Cannot open PDX file for writing: " + path);

        static constexpr uint32_t MAGIC = 0x31584450; // 'PDX1'
        static constexpr uint32_t VERSION = 1;

        const uint32_t metric = static_cast<uint32_t>(config.distance_metric);
        const uint64_t rows = static_cast<uint64_t>(num_embeddings);
        const uint64_t dim_major_size = static_cast<uint64_t>(dim_major_embeddings.size());
        const uint64_t norms_size = static_cast<uint64_t>(squared_norms.size());

        writeBinary(out, MAGIC);
        writeBinary(out, VERSION);
        writeBinary(out, config.num_dimensions);
        writeBinary(out, metric);
        writeBinary(out, n_probe);
        writeBinary(out, rows);
        writeBinary(out, dim_major_size);
        writeBinary(out, norms_size);

        if (!dim_major_embeddings.empty())
            out.write(reinterpret_cast<const char *>(dim_major_embeddings.data()), static_cast<std::streamsize>(dim_major_embeddings.size() * sizeof(float)));
        if (!squared_norms.empty())
            out.write(reinterpret_cast<const char *>(squared_norms.data()), static_cast<std::streamsize>(squared_norms.size() * sizeof(float)));

        if (!out)
            throw std::runtime_error("Failed to write PDX file: " + path);
    }

    void Restore(const std::string & path) override
    {
        std::ifstream in(path, std::ios::binary);
        if (!in)
            throw std::runtime_error("Cannot open PDX file for reading: " + path);

        static constexpr uint32_t MAGIC = 0x31584450; // 'PDX1'
        static constexpr uint32_t VERSION = 1;

        uint32_t magic = 0;
        uint32_t version = 0;
        uint32_t dims = 0;
        uint32_t metric = 0;
        uint32_t restored_n_probe = 1;
        uint64_t rows = 0;
        uint64_t dim_major_size = 0;
        uint64_t norms_size = 0;

        readBinary(in, magic);
        readBinary(in, version);
        readBinary(in, dims);
        readBinary(in, metric);
        readBinary(in, restored_n_probe);
        readBinary(in, rows);
        readBinary(in, dim_major_size);
        readBinary(in, norms_size);

        if (magic != MAGIC)
            throw std::runtime_error("Invalid PDX file magic: " + path);
        if (version != VERSION)
            throw std::runtime_error("Unsupported PDX file version: " + std::to_string(version));
        if (dims == 0 || dims > PDX_MAX_DIMS)
            throw std::runtime_error("Invalid number of dimensions in PDX file: " + std::to_string(dims));
        if (metric > static_cast<uint32_t>(DistanceMetric::IP))
            throw std::runtime_error("Invalid distance metric in PDX file: " + std::to_string(metric));

        const uint64_t expected_dim_major_size = rows * static_cast<uint64_t>(dims);
        if (dim_major_size != expected_dim_major_size)
        {
            throw std::runtime_error(
                "Invalid PDX payload size: expected " + std::to_string(expected_dim_major_size)
                + ", got " + std::to_string(dim_major_size));
        }
        if (norms_size != rows)
            throw std::runtime_error("Invalid PDX norms payload size");

        config.num_dimensions = dims;
        config.distance_metric = static_cast<DistanceMetric>(metric);
        config.Validate();

        num_embeddings = static_cast<size_t>(rows);
        n_probe = std::max<uint32_t>(1, restored_n_probe);

        dim_major_embeddings.assign(static_cast<size_t>(dim_major_size), 0.0f);
        squared_norms.assign(static_cast<size_t>(norms_size), 0.0f);

        if (!dim_major_embeddings.empty())
            in.read(reinterpret_cast<char *>(dim_major_embeddings.data()), static_cast<std::streamsize>(dim_major_embeddings.size() * sizeof(float)));
        if (!squared_norms.empty())
            in.read(reinterpret_cast<char *>(squared_norms.data()), static_cast<std::streamsize>(squared_norms.size() * sizeof(float)));

        if (!in)
            throw std::runtime_error("Failed to read PDX file: " + path);
    }

    uint32_t GetNumDimensions() const override
    {
        return config.num_dimensions;
    }

    uint32_t GetNumClusters() const override
    {
        /// MVP implementation keeps one cluster and computes distances dimension-by-dimension.
        return 1;
    }

    uint32_t GetClusterSize(uint32_t cluster_id) const override
    {
        if (cluster_id != 0)
            return 0;
        return static_cast<uint32_t>(num_embeddings);
    }

    std::vector<uint32_t> GetClusterRowIds(uint32_t cluster_id) const override
    {
        if (cluster_id != 0)
            return {};

        std::vector<uint32_t> result(num_embeddings);
        std::iota(result.begin(), result.end(), 0);
        return result;
    }

    size_t GetInMemorySizeInBytes() const override
    {
        return sizeof(*this)
            + dim_major_embeddings.size() * sizeof(float)
            + squared_norms.size() * sizeof(float);
    }

private:
    struct FarthestFirst
    {
        bool operator()(const KNNCandidate & lhs, const KNNCandidate & rhs) const
        {
            if (lhs.distance == rhs.distance)
                return lhs.index < rhs.index;
            return lhs.distance < rhs.distance;
        }
    };

    template <typename T>
    static void writeBinary(std::ofstream & out, const T & value)
    {
        out.write(reinterpret_cast<const char *>(&value), static_cast<std::streamsize>(sizeof(T)));
    }

    template <typename T>
    static void readBinary(std::ifstream & in, T & value)
    {
        in.read(reinterpret_cast<char *>(&value), static_cast<std::streamsize>(sizeof(T)));
    }

    float computeSquaredNorm(const float * vector) const
    {
        float result = 0.0f;
        for (size_t dim = 0; dim < config.num_dimensions; ++dim)
            result += vector[dim] * vector[dim];
        return result;
    }

    float computeDistance(size_t row, const float * query_embedding, float query_sq_norm) const
    {
        if (config.distance_metric == DistanceMetric::L2SQ)
        {
            float distance = 0.0f;
            for (size_t dim = 0; dim < config.num_dimensions; ++dim)
            {
                const float value = dim_major_embeddings[dim * num_embeddings + row];
                const float diff = query_embedding[dim] - value;
                distance += diff * diff;
            }
            return distance;
        }

        float dot = 0.0f;
        for (size_t dim = 0; dim < config.num_dimensions; ++dim)
            dot += query_embedding[dim] * dim_major_embeddings[dim * num_embeddings + row];

        if (config.distance_metric == DistanceMetric::IP)
            return -dot;

        const float embedding_sq_norm = squared_norms[row];
        const float denominator = std::sqrt(query_sq_norm * embedding_sq_norm);
        if (denominator <= std::numeric_limits<float>::epsilon())
            return 1.0f;
        return 1.0f - (dot / denominator);
    }

    template <typename CandidateHeap>
    void addCandidate(
        size_t row,
        const float * query_embedding,
        float query_sq_norm,
        size_t knn,
        CandidateHeap & heap) const
    {
        const float distance = computeDistance(row, query_embedding, query_sq_norm);
        KNNCandidate candidate{static_cast<uint32_t>(row), distance};

        if (heap.size() < knn)
        {
            heap.push(candidate);
            return;
        }

        const auto & top = heap.top();
        if (candidate.distance < top.distance || (candidate.distance == top.distance && candidate.index < top.index))
        {
            heap.pop();
            heap.push(candidate);
        }
    }

    template <typename CandidateHeap>
    static std::vector<KNNCandidate> drainHeapToSortedResult(CandidateHeap & heap)
    {
        std::vector<KNNCandidate> result;
        result.reserve(heap.size());

        while (!heap.empty())
        {
            result.push_back(heap.top());
            heap.pop();
        }

        std::sort(result.begin(), result.end(), [](const KNNCandidate & lhs, const KNNCandidate & rhs)
        {
            if (lhs.distance == rhs.distance)
                return lhs.index < rhs.index;
            return lhs.distance < rhs.distance;
        });

        return result;
    }

    PDXIndexConfig config;
    uint32_t n_probe = 1;
    size_t num_embeddings = 0;
    std::vector<float> dim_major_embeddings;
    std::vector<float> squared_norms;
};

/// The current ClickHouse integration uses only the f32 implementation.
using PDXIndexU8 = PDXIndexF32;
using PDXTreeIndexF32 = PDXIndexF32;
using PDXTreeIndexU8 = PDXIndexF32;

inline std::unique_ptr<IPDXIndex> LoadPDXIndex(const std::string & path)
{
    PDXIndexConfig config;
    config.num_dimensions = 1;
    auto index = std::make_unique<PDXIndexF32>(config);
    index->Restore(path);
    return index;
}

}
