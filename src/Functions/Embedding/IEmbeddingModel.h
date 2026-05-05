#pragma once

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

/// Common interface for all embedding model backends.
/// Both static (matrix lookup) and dense (transformer/ORT) models implement this.
class IEmbeddingModel
{
public:

    virtual ~IEmbeddingModel() = default;

    /// Embed a single text. Returns L2-normalized vector truncated to dims.
    virtual std::vector<float> embedText(std::string_view text, size_t dims) const = 0;

    /// Embed a batch of texts. Each result is L2-normalized and truncated to dims.
    virtual std::vector<std::vector<float>> embedBatch(
        const std::vector<std::string_view> & texts, size_t dims) const = 0;

    /// Embed pre-tokenized IDs. Returns L2-normalized vector truncated to dims.
    /// Default implementation tokenizes text via embedText — static model overrides for performance.
    virtual std::vector<float> embed(const std::vector<int32_t> & token_ids, size_t dims) const;

    /// Embed pre-tokenized IDs into caller-provided buffer. Avoids allocation.
    /// Default implementation calls embed() — static model overrides for performance.
    virtual void embedInto(const int32_t * token_ids, size_t n_tokens, float * output, size_t dims) const;

    /// Tokenize text into token IDs.
    virtual std::vector<int32_t> tokenize(std::string_view text) const = 0;

    /// Tokenize text into human-readable token strings (for debug/introspection).
    virtual std::vector<std::string> tokenizeToStrings(std::string_view text) const = 0;

    /// Validate and clamp dims to model's maximum.
    virtual size_t validateDims(size_t dims) const = 0;

    /// Maximum output dimensions this model supports.
    virtual size_t getMaxDims() const = 0;

    /// Default output dimensions (used when dims=0 or not specified).
    virtual size_t getDefaultDims() const = 0;

    /// Model type string (e.g. "static", "onnx", "onnx_static").
    virtual std::string getTypeName() const = 0;

    /// Vocabulary size.
    virtual size_t getVocabSize() const = 0;

    /// Approximate memory usage in bytes.
    virtual size_t getMemoryBytes() const = 0;

    /// Model info as JSON strings for system.embedding_models table.
    /// Default implementations return "{}". Backends override with real data.
    virtual std::string getMetadataJSON() const { return "{}"; }
    virtual std::string getArchitectureJSON() const { return "{}"; }
    virtual std::string getQuantizationJSON() const { return "{}"; }
    virtual std::string getTokenizerJSON() const { return "{}"; }
    virtual std::string getRuntimeJSON() const { return "{}"; }
    virtual std::string getStatsJSON() const { return "{}"; }

    /// Cosine similarity between two vectors.
    static float cosineSimilarity(std::span<const float> a, std::span<const float> b);
};

}
