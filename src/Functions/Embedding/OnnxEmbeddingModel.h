#pragma once

#if defined(USE_ONNXRUNTIME)

#include <Functions/Embedding/IEmbeddingModel.h>

#include <onnxruntime_cxx_api.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace DB
{

/// Dense ONNX-Runtime-based text embedding model.
///
/// Supports any ONNX embedding model with inputs (input_ids, attention_mask)
/// and output (sentence_embedding). Auto-detects tokenizer type (BPE or WordPiece)
/// from tokenizer.json. Output dimensions are detected from the model at load time.
///
/// Thread safety: ORT Session::Run is thread-safe. Read-only after construction.
class OnnxEmbeddingModel : public IEmbeddingModel
{
public:
    static constexpr size_t kDenseDefaultDims = 256;
    static constexpr size_t kMaxSeqLen = 512;

    /// Construct from model and tokenizer paths.
    OnnxEmbeddingModel(const std::string & model_path, const std::string & tokenizer_path, int intra_threads = 32);
    ~OnnxEmbeddingModel() override;

    // -- IEmbeddingModel interface --
    std::vector<float> embedText(std::string_view text, size_t dims) const override;
    std::vector<std::vector<float>> embedBatch(
        const std::vector<std::string_view> & texts, size_t dims) const override;
    std::vector<int32_t> tokenize(std::string_view text) const override;
    std::vector<std::string> tokenizeToStrings(std::string_view text) const override;
    size_t validateDims(size_t dims) const override;
    size_t getMaxDims() const override;
    size_t getDefaultDims() const override { return kDenseDefaultDims; }
    std::string getTypeName() const override { return "onnx"; }
    size_t getVocabSize() const override;
    size_t getMemoryBytes() const override;

    /// All extractable model info for system.embedding_models.
    struct OnnxModelInfo
    {
        // Metadata
        std::string producer;
        std::string domain;
        int64_t ir_version = 0;
        int64_t opset_version = 0;

        // Architecture
        size_t num_layers = 0;
        size_t num_heads = 0;
        size_t hidden_size = 0;
        size_t max_seq_length = 0;
        size_t num_parameters = 0;
        size_t num_ops = 0;
        std::vector<std::string> op_types;

        // Quantization
        std::string quantization; // none, int8, int4, fp16
        bool has_external_data = false;

        // Tokenizer
        std::string tokenizer_type;
        size_t vocab_size = 0;

        // Runtime
        int intra_op_threads = 0;
        std::string execution_provider;
    };
    OnnxModelInfo getModelInfo() const;

    std::string getMetadataJSON() const override;
    std::string getArchitectureJSON() const override;
    std::string getQuantizationJSON() const override;
    std::string getTokenizerJSON() const override;
    std::string getRuntimeJSON() const override;
    std::string getStatsJSON() const override;

    /// Inference stats (thread-safe atomic counters).
    struct InferenceStats
    {
        std::atomic<uint64_t> total_calls{0};
        std::atomic<uint64_t> total_tokens{0};
        std::atomic<uint64_t> total_time_us{0};
    };
    const InferenceStats & getStats() const { return stats; }

private:
    void init(const std::string & model_path, const std::string & tokenizer_path, int intra_threads);

    struct Impl;
    std::unique_ptr<Impl> impl;
    mutable InferenceStats stats;


    template <typename T>
    std::vector<std::vector<float>> extractEmbeddings(
        const std::vector<Ort::Value> & outputs,
        size_t batch,
        size_t max_len,
        const std::vector<std::vector<int64_t>> & all_ids,
        size_t dims) const
        requires(std::is_same_v<T, Ort::Float16_t> || std::is_same_v<T, float>);
};

}
#endif
