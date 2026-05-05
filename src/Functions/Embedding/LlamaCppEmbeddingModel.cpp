#include <Functions/Embedding/LlamaCppEmbeddingModel.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <llama.h>

extern "C" const char * ggml_cpu_variant_name(void);

#include <fmt/format.h>

#include <cmath>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
}

// ---------------------------------------------------------------------------
// Impl
// ---------------------------------------------------------------------------

struct LlamaCppEmbeddingModel::Impl
{
    struct llama_model * model = nullptr;
    const struct llama_vocab * vocab = nullptr;
    int32_t n_embd = 0;
    int n_threads = 0;
    int32_t max_seq_len = 512;

    // Cached model info
    std::string model_path;
    size_t model_size_bytes = 0;
    size_t vocab_size = 0;
    std::string model_arch;
    std::string model_type;
    std::string quantization_type;

    static constexpr int32_t kMaxBatchSeqs = 16;

    struct LlamaCtxDeleter
    {
        void operator()(struct llama_context * ctx) const noexcept
        {
            if (ctx)
                llama_free(ctx);
        }
    };
    using LlamaCtxPtr = std::unique_ptr<struct llama_context, LlamaCtxDeleter>;

    /// Thread-local context reused across calls. ~300MB per thread.
    /// The unique_ptr's destructor fires on thread exit, so contexts don't leak
    /// when worker threads are torn down. The (tl_model != model) check
    /// invalidates the cache if this Impl's model pointer changes, though in
    /// practice Impl instances aren't reused across different underlying models.
    struct llama_context * getContext() const
    {
        thread_local LlamaCtxPtr tl_ctx;
        thread_local struct llama_model * tl_model = nullptr;

        if (tl_ctx && tl_model != model)
            tl_ctx.reset();

        if (!tl_ctx)
        {
            int32_t total_ctx = max_seq_len * kMaxBatchSeqs;
            struct llama_context_params params = llama_context_default_params();
            params.n_ctx = total_ctx;
            params.n_batch = total_ctx;
            params.n_ubatch = total_ctx;
            params.n_seq_max = kMaxBatchSeqs;
            params.n_threads = n_threads;
            params.n_threads_batch = n_threads;
            params.embeddings = true;
            params.pooling_type = LLAMA_POOLING_TYPE_MEAN;
            params.no_perf = true;

            tl_ctx.reset(llama_init_from_model(model, params));
            tl_model = model;
        }

        return tl_ctx.get();
    }

    ~Impl()
    {
        if (model)
            llama_model_free(model);
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace
{

/// Truncate to dims, then L2-normalize in-place.
void truncateAndNormalize(std::vector<float> & vec, size_t dims)
{
    if (dims < vec.size())
        vec.resize(dims);
    float norm = 0.0f;
    for (float v : vec)
        norm += v * v;
    norm = std::sqrt(norm);
    if (norm > 0.0f)
    {
        float inv = 1.0f / norm;
        for (float & v : vec)
            v *= inv;
    }
}

}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

LlamaCppEmbeddingModel::LlamaCppEmbeddingModel(const std::string & model_path, int n_threads)
    : impl(std::make_unique<Impl>())
{
    LoggerPtr log = getLogger("LlamaCppEmbeddingModel");

    if (n_threads <= 0)
        n_threads = 32;

    struct llama_model_params mparams = llama_model_default_params();
    mparams.n_gpu_layers = 0; // CPU only
    mparams.use_mmap = true;

    impl->model = llama_model_load_from_file(model_path.c_str(), mparams);
    if (!impl->model)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "llama.cpp: failed to load model from '{}'", model_path);

    impl->vocab = llama_model_get_vocab(impl->model);
    impl->n_embd = llama_model_n_embd(impl->model);
    impl->n_threads = n_threads;
    impl->model_path = model_path;
    impl->vocab_size = llama_vocab_n_tokens(impl->vocab);
    impl->model_size_bytes = llama_model_size(impl->model);

    // Extract metadata
    {
        char buf[256];
        int32_t len;

        len = llama_model_meta_val_str(impl->model, "general.architecture", buf, sizeof(buf));
        if (len > 0) impl->model_arch = std::string(buf, len);

        len = llama_model_meta_val_str(impl->model, "general.type", buf, sizeof(buf));
        if (len > 0) impl->model_type = std::string(buf, len);

        len = llama_model_meta_val_str(impl->model, "general.file_type", buf, sizeof(buf));
        if (len > 0)
            impl->quantization_type = std::string(buf, len);
    }

    LOG_INFO(log, "Loaded GGUF model: arch={}, embd={}, vocab={}, size={:.1f}MB, threads={}, simd={}",
        impl->model_arch, impl->n_embd, impl->vocab_size,
        static_cast<double>(impl->model_size_bytes) / 1048576.0, n_threads, ggml_cpu_variant_name());
}

LlamaCppEmbeddingModel::~LlamaCppEmbeddingModel() = default;

// ---------------------------------------------------------------------------
// Tokenize
// ---------------------------------------------------------------------------

std::vector<int32_t> LlamaCppEmbeddingModel::tokenize(std::string_view text) const
{
    // First call to get required size
    int32_t n = llama_tokenize(impl->vocab, text.data(), static_cast<int32_t>(text.size()), nullptr, 0, true, true);
    if (n < 0)
        n = -n;

    std::vector<int32_t> tokens(n);
    int32_t actual = llama_tokenize(impl->vocab, text.data(), static_cast<int32_t>(text.size()),
        tokens.data(), static_cast<int32_t>(tokens.size()), true, true);
    if (actual < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "llama.cpp: tokenization failed");

    tokens.resize(actual);
    return tokens;
}

std::vector<std::string> LlamaCppEmbeddingModel::tokenizeToStrings(std::string_view text) const
{
    auto ids = tokenize(text);
    std::vector<std::string> result;
    result.reserve(ids.size());

    char buf[256];
    for (auto id : ids)
    {
        int32_t len = llama_token_to_piece(impl->vocab, id, buf, sizeof(buf), 0, true);
        if (len > 0)
            result.emplace_back(buf, len);
        else
            result.emplace_back("<unk>");
    }
    return result;
}

// ---------------------------------------------------------------------------
// Embed
// ---------------------------------------------------------------------------

std::vector<float> LlamaCppEmbeddingModel::embedText(std::string_view text, size_t dims) const
{
    dims = validateDims(dims);

    auto tokens = tokenize(text);
    if (tokens.empty())
        return std::vector<float>(dims, 0.0f);

    Stopwatch watch;

    int32_t n_tokens = static_cast<int32_t>(tokens.size());
    struct llama_context * ctx = impl->getContext();
    if (!ctx)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "llama.cpp: failed to create context");

    struct llama_batch batch = llama_batch_init(n_tokens, 0, 1);
    for (int32_t i = 0; i < n_tokens; ++i)
    {
        batch.token[i] = tokens[i];
        batch.pos[i] = i;
        batch.n_seq_id[i] = 1;
        batch.seq_id[i][0] = 0;
        batch.logits[i] = (i == n_tokens - 1) ? 1 : 0;
    }
    batch.n_tokens = n_tokens;

    int32_t ret = llama_encode(ctx, batch);
    llama_batch_free(batch);
    if (ret != 0)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "llama.cpp: encode failed with code {}", ret);

    float * emb = llama_get_embeddings_seq(ctx, 0);
    if (!emb)
        emb = llama_get_embeddings_ith(ctx, -1);

    std::vector<float> result;
    if (emb)
    {
        result.assign(emb, emb + impl->n_embd);
        truncateAndNormalize(result, dims);
    }
    else
    {
        result.resize(dims, 0.0f);
    }

    stats.total_calls.fetch_add(1, std::memory_order_relaxed);
    stats.total_tokens.fetch_add(n_tokens, std::memory_order_relaxed);
    stats.total_time_us.fetch_add(watch.elapsedMicroseconds(), std::memory_order_relaxed);

    return result;
}

std::vector<std::vector<float>> LlamaCppEmbeddingModel::embedBatch(
    const std::vector<std::string_view> & texts, size_t dims) const
{
    dims = validateDims(dims);
    const size_t batch = texts.size();
    if (batch == 0) return {};

    /// Sub-batch large inputs via recursive call
    static constexpr size_t MAX_BATCH = Impl::kMaxBatchSeqs;
    if (batch > MAX_BATCH)
    {
        std::vector<std::vector<float>> result(batch);
        for (size_t i = 0; i < batch; i += MAX_BATCH)
        {
            size_t end = std::min(i + MAX_BATCH, batch);
            std::vector<std::string_view> sub(texts.begin() + i, texts.begin() + end);
            auto sub_embs = embedBatch(sub, dims);
            for (size_t j = 0; j < sub_embs.size(); ++j)
                result[i + j] = std::move(sub_embs[j]);
        }
        return result;
    }

    using Clock = std::chrono::steady_clock;
    using Ms = std::chrono::milliseconds;
    auto ts = [](auto a, auto b){ return std::chrono::duration_cast<Ms>(b - a).count(); };

    // Allocate batch for worst case, tokenize directly into it — single pass
    auto t0 = Clock::now();
    int32_t max_total = static_cast<int32_t>(batch) * impl->max_seq_len;

    auto t1 = Clock::now();
    struct llama_batch llama_batch = llama_batch_init(max_total, 0, 1);
    int32_t pos = 0;
    for (size_t seq = 0; seq < batch; ++seq)
    {
        int32_t n = llama_tokenize(impl->vocab, texts[seq].data(), static_cast<int32_t>(texts[seq].size()),
            llama_batch.token + pos, impl->max_seq_len, true, true);
        if (n < 0) n = 0; // truncated
        for (int32_t i = 0; i < n; ++i)
        {
            llama_batch.pos[pos + i] = i;
            llama_batch.n_seq_id[pos + i] = 1;
            llama_batch.seq_id[pos + i][0] = static_cast<int32_t>(seq);
            llama_batch.logits[pos + i] = 0;
        }
        if (n > 0)
            llama_batch.logits[pos + n - 1] = 1;
        pos += n;
    }
    llama_batch.n_tokens = pos;

    if (pos == 0)
    {
        llama_batch_free(llama_batch);
        return std::vector<std::vector<float>>(batch, std::vector<float>(dims, 0.0f));
    }

    struct llama_context * ctx = impl->getContext();
    if (!ctx)
    {
        llama_batch_free(llama_batch);
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "llama.cpp: failed to create context");
    }

    // Encode
    auto t2 = Clock::now();
    int32_t ret = llama_encode(ctx, llama_batch);
    llama_batch_free(llama_batch);
    if (ret != 0)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "llama.cpp: encode failed with code {}", ret);

    // Extract embeddings
    auto t3 = Clock::now();
    std::vector<std::vector<float>> results(batch);
    for (size_t seq = 0; seq < batch; ++seq)
    {
        float * emb = llama_get_embeddings_seq(ctx, static_cast<int32_t>(seq));
        if (emb)
        {
            results[seq].assign(emb, emb + impl->n_embd);
            truncateAndNormalize(results[seq], dims);
        }
        else
        {
            results[seq].resize(dims, 0.0f);
        }
    }
    auto t4 = Clock::now();

    LoggerPtr log = getLogger("LlamaCppEmbeddingModel");
    LOG_DEBUG(log, "embedBatch batch={} total_tokens={} dims={} | tokenize={}ms prepare={}ms encode={}ms extract={}ms total={}ms",
        batch, pos, dims, ts(t0,t1), ts(t1,t2), ts(t2,t3), ts(t3,t4), ts(t0,t4));

    stats.total_calls.fetch_add(1, std::memory_order_relaxed);
    stats.total_tokens.fetch_add(pos, std::memory_order_relaxed);
    stats.total_time_us.fetch_add(std::chrono::duration_cast<std::chrono::microseconds>(t4 - t0).count(), std::memory_order_relaxed);

    return results;
}

// ---------------------------------------------------------------------------
// Dims
// ---------------------------------------------------------------------------

size_t LlamaCppEmbeddingModel::validateDims(size_t dims) const
{
    size_t max = getMaxDims();
    if (dims == 0)
        return getDefaultDims();
    return std::min(dims, max);
}

size_t LlamaCppEmbeddingModel::getMaxDims() const
{
    return static_cast<size_t>(impl->n_embd);
}

size_t LlamaCppEmbeddingModel::getDefaultDims() const
{
    return std::min(static_cast<size_t>(256), getMaxDims());
}

size_t LlamaCppEmbeddingModel::getVocabSize() const
{
    return impl->vocab_size;
}

size_t LlamaCppEmbeddingModel::getMemoryBytes() const
{
    return impl->model_size_bytes;
}

// ---------------------------------------------------------------------------
// JSON metadata for system.embedding_models
// ---------------------------------------------------------------------------

std::string LlamaCppEmbeddingModel::getMetadataJSON() const
{
    return fmt::format(R"({{"path":"{}","arch":"{}","type":"{}"}})",
        impl->model_path, impl->model_arch, impl->model_type);
}

std::string LlamaCppEmbeddingModel::getArchitectureJSON() const
{
    return fmt::format(R"({{"n_embd":{},"n_params":{}}})",
        impl->n_embd, llama_model_n_params(impl->model));
}

std::string LlamaCppEmbeddingModel::getQuantizationJSON() const
{
    return fmt::format(R"({{"ftype":"{}"}})", impl->quantization_type);
}

std::string LlamaCppEmbeddingModel::getTokenizerJSON() const
{
    return fmt::format(R"({{"vocab_size":{},"add_bos":{},"add_eos":{}}})",
        impl->vocab_size,
        llama_vocab_get_add_bos(impl->vocab) ? "true" : "false",
        llama_vocab_get_add_eos(impl->vocab) ? "true" : "false");
}

std::string LlamaCppEmbeddingModel::getRuntimeJSON() const
{
    return fmt::format(R"({{"backend":"llamacpp","n_threads":{},"simd":"{}"}})", impl->n_threads, ggml_cpu_variant_name());
}

std::string LlamaCppEmbeddingModel::getStatsJSON() const
{
    return fmt::format(R"({{"total_calls":{},"total_tokens":{},"total_time_us":{}}})",
        stats.total_calls.load(std::memory_order_relaxed),
        stats.total_tokens.load(std::memory_order_relaxed),
        stats.total_time_us.load(std::memory_order_relaxed));
}

}
