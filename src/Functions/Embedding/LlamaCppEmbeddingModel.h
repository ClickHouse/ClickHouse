#pragma once

#include <Functions/Embedding/IEmbeddingModel.h>

#include <atomic>
#include <memory>
#include <string>

namespace DB
{

/// GGUF-based embedding model using llama.cpp.
///
/// Supports any GGUF embedding model (e.g. quantized Q4_K_M models).
/// Uses llama_encode() for embedding (no KV cache).
/// Thread safety: model is read-only after construction; context is per-call.
class LlamaCppEmbeddingModel : public IEmbeddingModel
{
public:
    /// Construct from GGUF model path. No separate tokenizer needed — GGUF bundles vocabulary.
    /// @param n_threads  Number of threads for inference (0 = auto-detect).
    explicit LlamaCppEmbeddingModel(const std::string & model_path, int n_threads = 32);
    ~LlamaCppEmbeddingModel() override;

    // -- IEmbeddingModel interface --
    std::vector<float> embedText(std::string_view text, size_t dims) const override;
    std::vector<std::vector<float>> embedBatch(
        const std::vector<std::string_view> & texts, size_t dims) const override;
    std::vector<int32_t> tokenize(std::string_view text) const override;
    std::vector<std::string> tokenizeToStrings(std::string_view text) const override;
    size_t validateDims(size_t dims) const override;
    size_t getMaxDims() const override;
    size_t getDefaultDims() const override;
    std::string getTypeName() const override { return "llamacpp"; }
    size_t getVocabSize() const override;
    size_t getMemoryBytes() const override;

    std::string getMetadataJSON() const override;
    std::string getArchitectureJSON() const override;
    std::string getQuantizationJSON() const override;
    std::string getTokenizerJSON() const override;
    std::string getRuntimeJSON() const override;
    std::string getStatsJSON() const override;

    struct InferenceStats
    {
        std::atomic<uint64_t> total_calls{0};
        std::atomic<uint64_t> total_tokens{0};
        std::atomic<uint64_t> total_time_us{0};
    };

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
    mutable InferenceStats stats;
};

}
