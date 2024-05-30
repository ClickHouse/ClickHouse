#define LLAMA_API_INTERNAL
#include "llama.h"

#include "unicode.h"

#include "ggml-alloc.h"
#include "ggml-backend.h"
#include "ggml.h"

#ifdef GGML_USE_RPC
#    include "ggml-rpc.h"
#endif

#ifdef GGML_USE_CUDA
#    include "ggml-cuda.h"
#elif defined(GGML_USE_CLBLAST)
#    include "ggml-opencl.h"
#elif defined(GGML_USE_VULKAN)
#    include "ggml-vulkan.h"
#elif defined(GGML_USE_SYCL)
#    include "ggml-sycl.h"
#elif defined(GGML_USE_KOMPUTE)
#    include "ggml-kompute.h"
#endif

#ifdef GGML_USE_METAL
#    include "ggml-metal.h"
#endif

// TODO: replace with ggml API call
#define QK_K 256

#ifdef __has_include
#    if __has_include(<unistd.h>)
#        include <unistd.h>
#        if defined(_POSIX_MAPPED_FILES)
#            include <fcntl.h>
#            include <sys/mman.h>
#        endif
#        if defined(_POSIX_MEMLOCK_RANGE)
#            include <sys/resource.h>
#        endif
#    endif
#endif

#if defined(_WIN32)
#    define WIN32_LEAN_AND_MEAN
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    include <windows.h>
#    ifndef PATH_MAX
#        define PATH_MAX MAX_PATH
#    endif
#    include <io.h>
#endif

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cfloat>
#include <cinttypes>
#include <climits>
#include <cmath>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <forward_list>
#include <fstream>
#include <functional>
#include <future>
#include <initializer_list>
#include <locale>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <pcg-random/pcg_random.hpp>

// NOLINTBEGIN
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wcovered-switch-default"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#pragma clang diagnostic ignored "-Wtautological-unsigned-enum-zero-compare"
#pragma clang diagnostic ignored "-Wextra-semi-stmt"
#pragma clang diagnostic ignored "-Wfloat-conversion"
#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wconditional-uninitialized"
#pragma clang diagnostic ignored "-Wunused-macros"
#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"
#pragma clang diagnostic ignored "-Wsign-compare"

#if defined(_MSC_VER)
#    pragma warning(disable : 4244 4267) // possible loss of data
#endif

#ifdef __GNUC__
#    ifdef __MINGW32__
#        define LLAMA_ATTRIBUTE_FORMAT(...) __attribute__((format(gnu_printf, __VA_ARGS__)))
#    else
#        define LLAMA_ATTRIBUTE_FORMAT(...) __attribute__((format(printf, __VA_ARGS__)))
#    endif
#else
#    define LLAMA_ATTRIBUTE_FORMAT(...)
#endif

#define LLAMA_MAX_NODES 8192
#define LLAMA_MAX_EXPERTS 128


//
// logging
//

LLAMA_ATTRIBUTE_FORMAT(2, 3)
static void llama_log_internal(ggml_log_level level, const char * format, ...);
static void llama_log_callback_default(ggml_log_level level, const char * text, void * user_data);

#define LLAMA_LOG_INFO(...) llama_log_internal(GGML_LOG_LEVEL_INFO, __VA_ARGS__)
#define LLAMA_LOG_WARN(...) llama_log_internal(GGML_LOG_LEVEL_WARN, __VA_ARGS__)
#define LLAMA_LOG_ERROR(...) llama_log_internal(GGML_LOG_LEVEL_ERROR, __VA_ARGS__)

//
// helpers
//

static size_t utf8_len(char src)
{
    const size_t lookup[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4};
    uint8_t highbits = static_cast<uint8_t>(src) >> 4;
    return lookup[highbits];
}

static void replace_all(std::string & s, const std::string & search, const std::string & replace)
{
    std::string result;
    for (size_t pos = 0;; pos += search.length())
    {
        auto new_pos = s.find(search, pos);
        if (new_pos == std::string::npos)
        {
            result += s.substr(pos, s.size() - pos);
            break;
        }
        result += s.substr(pos, new_pos - pos) + replace;
        pos = new_pos;
    }
    s = std::move(result);
}

static bool is_float_close(float a, float b, float abs_tol)
{
    // Check for non-negative tolerance
    if (abs_tol < 0.0)
    {
        throw std::invalid_argument("Tolerance must be non-negative");
    }

    // Exact equality check
    if (a == b)
    {
        return true;
    }

    // Check for infinities
    if (std::isinf(a) || std::isinf(b))
    {
        return false;
    }

    // Regular comparison using the provided absolute tolerance
    return std::fabs(b - a) <= abs_tol;
}

static void zeros(std::ofstream & file, size_t n)
{
    char zero = 0;
    for (size_t i = 0; i < n; ++i)
    {
        file.write(&zero, 1);
    }
}

LLAMA_ATTRIBUTE_FORMAT(1, 2)
static std::string format(const char * fmt, ...)
{
    va_list ap;
    va_list ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    int size = vsnprintf(nullptr, 0, fmt, ap);
    GGML_ASSERT(size >= 0 && size < INT_MAX); // NOLINT
    std::vector<char> buf(size + 1);
    int size2 = vsnprintf(buf.data(), size + 1, fmt, ap2);
    GGML_ASSERT(size2 == size);
    va_end(ap2);
    va_end(ap);
    return std::string(buf.data(), size);
}

//
// gguf constants (sync with gguf.py)
//

enum llm_arch
{
    LLM_ARCH_LLAMA,
    LLM_ARCH_FALCON,
    LLM_ARCH_BAICHUAN,
    LLM_ARCH_GROK,
    LLM_ARCH_GPT2,
    LLM_ARCH_GPTJ,
    LLM_ARCH_GPTNEOX,
    LLM_ARCH_MPT,
    LLM_ARCH_STARCODER,
    LLM_ARCH_REFACT,
    LLM_ARCH_BERT,
    LLM_ARCH_NOMIC_BERT,
    LLM_ARCH_JINA_BERT_V2,
    LLM_ARCH_BLOOM,
    LLM_ARCH_STABLELM,
    LLM_ARCH_QWEN,
    LLM_ARCH_QWEN2,
    LLM_ARCH_QWEN2MOE,
    LLM_ARCH_PHI2,
    LLM_ARCH_PHI3,
    LLM_ARCH_PLAMO,
    LLM_ARCH_CODESHELL,
    LLM_ARCH_ORION,
    LLM_ARCH_INTERNLM2,
    LLM_ARCH_MINICPM,
    LLM_ARCH_GEMMA,
    LLM_ARCH_STARCODER2,
    LLM_ARCH_MAMBA,
    LLM_ARCH_XVERSE,
    LLM_ARCH_COMMAND_R,
    LLM_ARCH_DBRX,
    LLM_ARCH_OLMO,
    LLM_ARCH_ARCTIC,
    LLM_ARCH_UNKNOWN,
};

static const std::map<llm_arch, const char *> LLM_ARCH_NAMES = {
    {LLM_ARCH_LLAMA, "llama"},
    {LLM_ARCH_FALCON, "falcon"},
    {LLM_ARCH_GROK, "grok"},
    {LLM_ARCH_GPT2, "gpt2"},
    {LLM_ARCH_GPTJ, "gptj"},
    {LLM_ARCH_GPTNEOX, "gptneox"},
    {LLM_ARCH_MPT, "mpt"},
    {LLM_ARCH_BAICHUAN, "baichuan"},
    {LLM_ARCH_STARCODER, "starcoder"},
    {LLM_ARCH_REFACT, "refact"},
    {LLM_ARCH_BERT, "bert"},
    {LLM_ARCH_NOMIC_BERT, "nomic-bert"},
    {LLM_ARCH_JINA_BERT_V2, "jina-bert-v2"},
    {LLM_ARCH_BLOOM, "bloom"},
    {LLM_ARCH_STABLELM, "stablelm"},
    {LLM_ARCH_QWEN, "qwen"},
    {LLM_ARCH_QWEN2, "qwen2"},
    {LLM_ARCH_QWEN2MOE, "qwen2moe"},
    {LLM_ARCH_PHI2, "phi2"},
    {LLM_ARCH_PHI3, "phi3"},
    {LLM_ARCH_PLAMO, "plamo"},
    {LLM_ARCH_CODESHELL, "codeshell"},
    {LLM_ARCH_ORION, "orion"},
    {LLM_ARCH_INTERNLM2, "internlm2"},
    {LLM_ARCH_MINICPM, "minicpm"},
    {LLM_ARCH_GEMMA, "gemma"},
    {LLM_ARCH_STARCODER2, "starcoder2"},
    {LLM_ARCH_MAMBA, "mamba"},
    {LLM_ARCH_XVERSE, "xverse"},
    {LLM_ARCH_COMMAND_R, "command-r"},
    {LLM_ARCH_DBRX, "dbrx"},
    {LLM_ARCH_OLMO, "olmo"},
    {LLM_ARCH_ARCTIC, "arctic"},
    {LLM_ARCH_UNKNOWN, "(unknown)"},
};

enum llm_kv
{
    LLM_KV_GENERAL_ARCHITECTURE,
    LLM_KV_GENERAL_QUANTIZATION_VERSION,
    LLM_KV_GENERAL_ALIGNMENT,
    LLM_KV_GENERAL_NAME,
    LLM_KV_GENERAL_AUTHOR,
    LLM_KV_GENERAL_VERSION,
    LLM_KV_GENERAL_URL,
    LLM_KV_GENERAL_DESCRIPTION,
    LLM_KV_GENERAL_LICENSE,
    LLM_KV_GENERAL_SOURCE_URL,
    LLM_KV_GENERAL_SOURCE_HF_REPO,

    LLM_KV_VOCAB_SIZE,
    LLM_KV_CONTEXT_LENGTH,
    LLM_KV_EMBEDDING_LENGTH,
    LLM_KV_BLOCK_COUNT,
    LLM_KV_FEED_FORWARD_LENGTH,
    LLM_KV_USE_PARALLEL_RESIDUAL,
    LLM_KV_TENSOR_DATA_LAYOUT,
    LLM_KV_EXPERT_COUNT,
    LLM_KV_EXPERT_USED_COUNT,
    LLM_KV_POOLING_TYPE,
    LLM_KV_LOGIT_SCALE,

    LLM_KV_ATTENTION_HEAD_COUNT,
    LLM_KV_ATTENTION_HEAD_COUNT_KV,
    LLM_KV_ATTENTION_MAX_ALIBI_BIAS,
    LLM_KV_ATTENTION_CLAMP_KQV,
    LLM_KV_ATTENTION_KEY_LENGTH,
    LLM_KV_ATTENTION_VALUE_LENGTH,
    LLM_KV_ATTENTION_LAYERNORM_EPS,
    LLM_KV_ATTENTION_LAYERNORM_RMS_EPS,
    LLM_KV_ATTENTION_CAUSAL,

    LLM_KV_ROPE_DIMENSION_COUNT,
    LLM_KV_ROPE_FREQ_BASE,
    LLM_KV_ROPE_SCALE_LINEAR,
    LLM_KV_ROPE_SCALING_TYPE,
    LLM_KV_ROPE_SCALING_FACTOR,
    LLM_KV_ROPE_SCALING_ATTN_FACTOR,
    LLM_KV_ROPE_SCALING_ORIG_CTX_LEN,
    LLM_KV_ROPE_SCALING_FINETUNED,

    LLM_KV_SPLIT_NO,
    LLM_KV_SPLIT_COUNT,
    LLM_KV_SPLIT_TENSORS_COUNT,

    LLM_KV_SSM_INNER_SIZE,
    LLM_KV_SSM_CONV_KERNEL,
    LLM_KV_SSM_STATE_SIZE,
    LLM_KV_SSM_TIME_STEP_RANK,

    LLM_KV_TOKENIZER_MODEL,
    LLM_KV_TOKENIZER_PRE,
    LLM_KV_TOKENIZER_LIST,
    LLM_KV_TOKENIZER_TOKEN_TYPE,
    LLM_KV_TOKENIZER_TOKEN_TYPE_COUNT,
    LLM_KV_TOKENIZER_SCORES,
    LLM_KV_TOKENIZER_MERGES,
    LLM_KV_TOKENIZER_BOS_ID,
    LLM_KV_TOKENIZER_EOS_ID,
    LLM_KV_TOKENIZER_UNK_ID,
    LLM_KV_TOKENIZER_SEP_ID,
    LLM_KV_TOKENIZER_PAD_ID,
    LLM_KV_TOKENIZER_CLS_ID,
    LLM_KV_TOKENIZER_MASK_ID,
    LLM_KV_TOKENIZER_ADD_BOS,
    LLM_KV_TOKENIZER_ADD_EOS,
    LLM_KV_TOKENIZER_ADD_PREFIX,
    LLM_KV_TOKENIZER_HF_JSON,
    LLM_KV_TOKENIZER_RWKV,
    LLM_KV_TOKENIZER_PREFIX_ID,
    LLM_KV_TOKENIZER_SUFFIX_ID,
    LLM_KV_TOKENIZER_MIDDLE_ID,
    LLM_KV_TOKENIZER_EOT_ID,
};

static const std::map<llm_kv, const char *> LLM_KV_NAMES = {
    {LLM_KV_GENERAL_ARCHITECTURE, "general.architecture"},
    {LLM_KV_GENERAL_QUANTIZATION_VERSION, "general.quantization_version"},
    {LLM_KV_GENERAL_ALIGNMENT, "general.alignment"},
    {LLM_KV_GENERAL_NAME, "general.name"},
    {LLM_KV_GENERAL_AUTHOR, "general.author"},
    {LLM_KV_GENERAL_VERSION, "general.version"},
    {LLM_KV_GENERAL_URL, "general.url"},
    {LLM_KV_GENERAL_DESCRIPTION, "general.description"},
    {LLM_KV_GENERAL_LICENSE, "general.license"},
    {LLM_KV_GENERAL_SOURCE_URL, "general.source.url"},
    {LLM_KV_GENERAL_SOURCE_HF_REPO, "general.source.huggingface.repository"},

    {LLM_KV_VOCAB_SIZE, "%s.vocab_size"},
    {LLM_KV_CONTEXT_LENGTH, "%s.context_length"},
    {LLM_KV_EMBEDDING_LENGTH, "%s.embedding_length"},
    {LLM_KV_BLOCK_COUNT, "%s.block_count"},
    {LLM_KV_FEED_FORWARD_LENGTH, "%s.feed_forward_length"},
    {LLM_KV_USE_PARALLEL_RESIDUAL, "%s.use_parallel_residual"},
    {LLM_KV_TENSOR_DATA_LAYOUT, "%s.tensor_data_layout"},
    {LLM_KV_EXPERT_COUNT, "%s.expert_count"},
    {LLM_KV_EXPERT_USED_COUNT, "%s.expert_used_count"},
    {LLM_KV_POOLING_TYPE, "%s.pooling_type"},
    {LLM_KV_LOGIT_SCALE, "%s.logit_scale"},

    {LLM_KV_ATTENTION_HEAD_COUNT, "%s.attention.head_count"},
    {LLM_KV_ATTENTION_HEAD_COUNT_KV, "%s.attention.head_count_kv"},
    {LLM_KV_ATTENTION_MAX_ALIBI_BIAS, "%s.attention.max_alibi_bias"},
    {LLM_KV_ATTENTION_CLAMP_KQV, "%s.attention.clamp_kqv"},
    {LLM_KV_ATTENTION_KEY_LENGTH, "%s.attention.key_length"},
    {LLM_KV_ATTENTION_VALUE_LENGTH, "%s.attention.value_length"},
    {LLM_KV_ATTENTION_LAYERNORM_EPS, "%s.attention.layer_norm_epsilon"},
    {LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, "%s.attention.layer_norm_rms_epsilon"},
    {LLM_KV_ATTENTION_CAUSAL, "%s.attention.causal"},

    {LLM_KV_ROPE_DIMENSION_COUNT, "%s.rope.dimension_count"},
    {LLM_KV_ROPE_FREQ_BASE, "%s.rope.freq_base"},
    {LLM_KV_ROPE_SCALE_LINEAR, "%s.rope.scale_linear"},
    {LLM_KV_ROPE_SCALING_TYPE, "%s.rope.scaling.type"},
    {LLM_KV_ROPE_SCALING_FACTOR, "%s.rope.scaling.factor"},
    {LLM_KV_ROPE_SCALING_ATTN_FACTOR, "%s.rope.scaling.attn_factor"},
    {LLM_KV_ROPE_SCALING_ORIG_CTX_LEN, "%s.rope.scaling.original_context_length"},
    {LLM_KV_ROPE_SCALING_FINETUNED, "%s.rope.scaling.finetuned"},

    {LLM_KV_SPLIT_NO, "split.no"},
    {LLM_KV_SPLIT_COUNT, "split.count"},
    {LLM_KV_SPLIT_TENSORS_COUNT, "split.tensors.count"},

    {LLM_KV_SSM_CONV_KERNEL, "%s.ssm.conv_kernel"},
    {LLM_KV_SSM_INNER_SIZE, "%s.ssm.inner_size"},
    {LLM_KV_SSM_STATE_SIZE, "%s.ssm.state_size"},
    {LLM_KV_SSM_TIME_STEP_RANK, "%s.ssm.time_step_rank"},

    {LLM_KV_TOKENIZER_MODEL, "tokenizer.ggml.model"},
    {LLM_KV_TOKENIZER_PRE, "tokenizer.ggml.pre"},
    {LLM_KV_TOKENIZER_LIST, "tokenizer.ggml.tokens"},
    {LLM_KV_TOKENIZER_TOKEN_TYPE, "tokenizer.ggml.token_type"},
    {LLM_KV_TOKENIZER_TOKEN_TYPE_COUNT, "tokenizer.ggml.token_type_count"},
    {LLM_KV_TOKENIZER_SCORES, "tokenizer.ggml.scores"},
    {LLM_KV_TOKENIZER_MERGES, "tokenizer.ggml.merges"},
    {LLM_KV_TOKENIZER_BOS_ID, "tokenizer.ggml.bos_token_id"},
    {LLM_KV_TOKENIZER_EOS_ID, "tokenizer.ggml.eos_token_id"},
    {LLM_KV_TOKENIZER_UNK_ID, "tokenizer.ggml.unknown_token_id"},
    {LLM_KV_TOKENIZER_SEP_ID, "tokenizer.ggml.seperator_token_id"},
    {LLM_KV_TOKENIZER_PAD_ID, "tokenizer.ggml.padding_token_id"},
    {LLM_KV_TOKENIZER_CLS_ID, "tokenizer.ggml.cls_token_id"},
    {LLM_KV_TOKENIZER_MASK_ID, "tokenizer.ggml.mask_token_id"},
    {LLM_KV_TOKENIZER_ADD_BOS, "tokenizer.ggml.add_bos_token"},
    {LLM_KV_TOKENIZER_ADD_EOS, "tokenizer.ggml.add_eos_token"},
    {LLM_KV_TOKENIZER_ADD_PREFIX, "tokenizer.ggml.add_space_prefix"},
    {LLM_KV_TOKENIZER_HF_JSON, "tokenizer.huggingface.json"},
    {LLM_KV_TOKENIZER_RWKV, "tokenizer.rwkv.world"},
    {LLM_KV_TOKENIZER_PREFIX_ID, "tokenizer.ggml.prefix_token_id"},
    {LLM_KV_TOKENIZER_SUFFIX_ID, "tokenizer.ggml.suffix_token_id"},
    {LLM_KV_TOKENIZER_MIDDLE_ID, "tokenizer.ggml.middle_token_id"},
    {LLM_KV_TOKENIZER_EOT_ID, "tokenizer.ggml.eot_token_id"},
};

struct LLM_KV
{
    LLM_KV(llm_arch arch) : arch(arch) { }

    llm_arch arch;

    std::string operator()(llm_kv kv) const { return ::format(LLM_KV_NAMES.at(kv), LLM_ARCH_NAMES.at(arch)); }
};

enum llm_tensor
{
    LLM_TENSOR_TOKEN_EMBD,
    LLM_TENSOR_TOKEN_EMBD_NORM,
    LLM_TENSOR_TOKEN_TYPES,
    LLM_TENSOR_POS_EMBD,
    LLM_TENSOR_OUTPUT,
    LLM_TENSOR_OUTPUT_NORM,
    LLM_TENSOR_ROPE_FREQS,
    LLM_TENSOR_ROPE_FACTORS_LONG,
    LLM_TENSOR_ROPE_FACTORS_SHORT,
    LLM_TENSOR_ATTN_Q,
    LLM_TENSOR_ATTN_K,
    LLM_TENSOR_ATTN_V,
    LLM_TENSOR_ATTN_QKV,
    LLM_TENSOR_ATTN_OUT,
    LLM_TENSOR_ATTN_NORM,
    LLM_TENSOR_ATTN_NORM_2,
    LLM_TENSOR_ATTN_OUT_NORM,
    LLM_TENSOR_ATTN_ROT_EMBD,
    LLM_TENSOR_FFN_GATE_INP,
    LLM_TENSOR_FFN_GATE_INP_SHEXP,
    LLM_TENSOR_FFN_NORM,
    LLM_TENSOR_FFN_GATE,
    LLM_TENSOR_FFN_DOWN,
    LLM_TENSOR_FFN_UP,
    LLM_TENSOR_FFN_ACT,
    LLM_TENSOR_FFN_DOWN_EXP, // split experts for backward compatibility
    LLM_TENSOR_FFN_GATE_EXP,
    LLM_TENSOR_FFN_UP_EXP,
    LLM_TENSOR_FFN_NORM_EXPS,
    LLM_TENSOR_FFN_DOWN_EXPS, // merged experts
    LLM_TENSOR_FFN_GATE_EXPS,
    LLM_TENSOR_FFN_UP_EXPS,
    LLM_TENSOR_FFN_DOWN_SHEXP,
    LLM_TENSOR_FFN_GATE_SHEXP,
    LLM_TENSOR_FFN_UP_SHEXP,
    LLM_TENSOR_ATTN_Q_NORM,
    LLM_TENSOR_ATTN_K_NORM,
    LLM_TENSOR_LAYER_OUT_NORM,
    LLM_TENSOR_SSM_IN,
    LLM_TENSOR_SSM_CONV1D,
    LLM_TENSOR_SSM_X,
    LLM_TENSOR_SSM_DT,
    LLM_TENSOR_SSM_A,
    LLM_TENSOR_SSM_D,
    LLM_TENSOR_SSM_OUT,
};

static const std::map<llm_arch, std::map<llm_tensor, std::string>> LLM_TENSOR_NAMES = {
    {
        LLM_ARCH_LLAMA,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_GATE_INP, "blk.%d.ffn_gate_inp"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_GATE_EXP, "blk.%d.ffn_gate.%d"},
            {LLM_TENSOR_FFN_DOWN_EXP, "blk.%d.ffn_down.%d"},
            {LLM_TENSOR_FFN_UP_EXP, "blk.%d.ffn_up.%d"},
            {LLM_TENSOR_FFN_GATE_EXPS, "blk.%d.ffn_gate_exps"},
            {LLM_TENSOR_FFN_DOWN_EXPS, "blk.%d.ffn_down_exps"},
            {LLM_TENSOR_FFN_UP_EXPS, "blk.%d.ffn_up_exps"},
        },
    },
    {
        LLM_ARCH_BAICHUAN,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_FALCON,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_NORM_2, "blk.%d.attn_norm_2"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_GROK,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_GATE_INP, "blk.%d.ffn_gate_inp"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE_EXP, "blk.%d.ffn_gate.%d"},
            {LLM_TENSOR_FFN_DOWN_EXP, "blk.%d.ffn_down.%d"},
            {LLM_TENSOR_FFN_UP_EXP, "blk.%d.ffn_up.%d"},
            {LLM_TENSOR_FFN_GATE_EXPS, "blk.%d.ffn_gate_exps"},
            {LLM_TENSOR_FFN_DOWN_EXPS, "blk.%d.ffn_down_exps"},
            {LLM_TENSOR_FFN_UP_EXPS, "blk.%d.ffn_up_exps"},
            {LLM_TENSOR_LAYER_OUT_NORM, "blk.%d.layer_output_norm"},
            {LLM_TENSOR_ATTN_OUT_NORM, "blk.%d.attn_output_norm"},
        },
    },
    {
        LLM_ARCH_GPT2,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_POS_EMBD, "position_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
        },
    },
    {
        LLM_ARCH_GPTJ,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
        },
    },
    {
        LLM_ARCH_GPTNEOX,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_MPT,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_ACT, "blk.%d.ffn.act"},
            {LLM_TENSOR_POS_EMBD, "position_embd"},
            {LLM_TENSOR_ATTN_Q_NORM, "blk.%d.attn_q_norm"},
            {LLM_TENSOR_ATTN_K_NORM, "blk.%d.attn_k_norm"},
        },
    },
    {
        LLM_ARCH_STARCODER,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_POS_EMBD, "position_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
        },
    },
    {
        LLM_ARCH_REFACT,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_BERT,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_TOKEN_EMBD_NORM, "token_embd_norm"},
            {LLM_TENSOR_TOKEN_TYPES, "token_types"},
            {LLM_TENSOR_POS_EMBD, "position_embd"},
            {LLM_TENSOR_ATTN_OUT_NORM, "blk.%d.attn_output_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_LAYER_OUT_NORM, "blk.%d.layer_output_norm"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_NOMIC_BERT,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_TOKEN_EMBD_NORM, "token_embd_norm"},
            {LLM_TENSOR_TOKEN_TYPES, "token_types"},
            {LLM_TENSOR_ATTN_OUT_NORM, "blk.%d.attn_output_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_LAYER_OUT_NORM, "blk.%d.layer_output_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_JINA_BERT_V2,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_TOKEN_EMBD_NORM, "token_embd_norm"},
            {LLM_TENSOR_TOKEN_TYPES, "token_types"},
            {LLM_TENSOR_ATTN_OUT_NORM, "blk.%d.attn_output_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_Q_NORM, "blk.%d.attn_q_norm"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_K_NORM, "blk.%d.attn_k_norm"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_LAYER_OUT_NORM, "blk.%d.layer_output_norm"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_BLOOM,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_TOKEN_EMBD_NORM, "token_embd_norm"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
        },
    },
    {
        LLM_ARCH_STABLELM,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_ATTN_Q_NORM, "blk.%d.attn_q_norm"},
            {LLM_TENSOR_ATTN_K_NORM, "blk.%d.attn_k_norm"},
        },
    },
    {
        LLM_ARCH_QWEN,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_QWEN2,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_QWEN2MOE,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE_INP, "blk.%d.ffn_gate_inp"},
            {LLM_TENSOR_FFN_GATE_EXPS, "blk.%d.ffn_gate_exps"},
            {LLM_TENSOR_FFN_DOWN_EXPS, "blk.%d.ffn_down_exps"},
            {LLM_TENSOR_FFN_UP_EXPS, "blk.%d.ffn_up_exps"},
            {LLM_TENSOR_FFN_GATE_INP_SHEXP, "blk.%d.ffn_gate_inp_shexp"},
            {LLM_TENSOR_FFN_GATE_SHEXP, "blk.%d.ffn_gate_shexp"},
            {LLM_TENSOR_FFN_DOWN_SHEXP, "blk.%d.ffn_down_shexp"},
            {LLM_TENSOR_FFN_UP_SHEXP, "blk.%d.ffn_up_shexp"},
        },
    },
    {
        LLM_ARCH_PHI2,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_PHI3,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FACTORS_LONG, "rope_factors_long"},
            {LLM_TENSOR_ROPE_FACTORS_SHORT, "rope_factors_short"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_PLAMO,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_CODESHELL,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_ORION,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_INTERNLM2,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_MINICPM,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_GATE_INP, "blk.%d.ffn_gate_inp"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_GATE_EXP, "blk.%d.ffn_gate.%d"},
            {LLM_TENSOR_FFN_DOWN_EXP, "blk.%d.ffn_down.%d"},
            {LLM_TENSOR_FFN_UP_EXP, "blk.%d.ffn_up.%d"},
        },
    },
    {
        LLM_ARCH_GEMMA,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_STARCODER2,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_MAMBA,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_SSM_IN, "blk.%d.ssm_in"},
            {LLM_TENSOR_SSM_CONV1D, "blk.%d.ssm_conv1d"},
            {LLM_TENSOR_SSM_X, "blk.%d.ssm_x"},
            {LLM_TENSOR_SSM_DT, "blk.%d.ssm_dt"},
            {LLM_TENSOR_SSM_A, "blk.%d.ssm_a"},
            {LLM_TENSOR_SSM_D, "blk.%d.ssm_d"},
            {LLM_TENSOR_SSM_OUT, "blk.%d.ssm_out"},
        },
    },
    {
        LLM_ARCH_XVERSE,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ROPE_FREQS, "rope_freqs"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_ROT_EMBD, "blk.%d.attn_rot_embd"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_COMMAND_R,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_ATTN_Q_NORM, "blk.%d.attn_q_norm"},
            {LLM_TENSOR_ATTN_K_NORM, "blk.%d.attn_k_norm"},
        },
    },
    {
        LLM_ARCH_DBRX,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_QKV, "blk.%d.attn_qkv"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_ATTN_OUT_NORM, "blk.%d.attn_output_norm"},
            {LLM_TENSOR_FFN_GATE_INP, "blk.%d.ffn_gate_inp"},
            {LLM_TENSOR_FFN_GATE_EXPS, "blk.%d.ffn_gate_exps"},
            {LLM_TENSOR_FFN_DOWN_EXPS, "blk.%d.ffn_down_exps"},
            {LLM_TENSOR_FFN_UP_EXPS, "blk.%d.ffn_up_exps"},
        },
    },
    {
        LLM_ARCH_OLMO,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
        },
    },
    {
        LLM_ARCH_ARCTIC,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
            {LLM_TENSOR_OUTPUT_NORM, "output_norm"},
            {LLM_TENSOR_OUTPUT, "output"},
            {LLM_TENSOR_ATTN_NORM, "blk.%d.attn_norm"},
            {LLM_TENSOR_ATTN_Q, "blk.%d.attn_q"},
            {LLM_TENSOR_ATTN_K, "blk.%d.attn_k"},
            {LLM_TENSOR_ATTN_V, "blk.%d.attn_v"},
            {LLM_TENSOR_ATTN_OUT, "blk.%d.attn_output"},
            {LLM_TENSOR_FFN_GATE_INP, "blk.%d.ffn_gate_inp"},
            {LLM_TENSOR_FFN_NORM, "blk.%d.ffn_norm"},
            {LLM_TENSOR_FFN_GATE, "blk.%d.ffn_gate"},
            {LLM_TENSOR_FFN_DOWN, "blk.%d.ffn_down"},
            {LLM_TENSOR_FFN_UP, "blk.%d.ffn_up"},
            {LLM_TENSOR_FFN_NORM_EXPS, "blk.%d.ffn_norm_exps"},
            {LLM_TENSOR_FFN_GATE_EXPS, "blk.%d.ffn_gate_exps"},
            {LLM_TENSOR_FFN_DOWN_EXPS, "blk.%d.ffn_down_exps"},
            {LLM_TENSOR_FFN_UP_EXPS, "blk.%d.ffn_up_exps"},
        },
    },
    {
        LLM_ARCH_UNKNOWN,
        {
            {LLM_TENSOR_TOKEN_EMBD, "token_embd"},
        },
    },
};

static llm_arch llm_arch_from_string(const std::string & name)
{
    for (const auto & kv : LLM_ARCH_NAMES)
    { // NOLINT
        if (kv.second == name)
        {
            return kv.first;
        }
    }

    return LLM_ARCH_UNKNOWN;
}

// helper to handle gguf constants
// usage:
//
//   const auto tn = LLM_TN(LLM_ARCH_LLAMA);
//
//   std::string name = tn(LLM_TENSOR_OUTPUT);                     -> "output"
//   std::string name = tn(LLM_TENSOR_TOKEN_EMBD, "bias");         -> "token_embd.bias"
//   std::string name = tn(LLM_TENSOR_ATTN_NORM, "weight", 3);     -> "blk.3.attn_norm.weight"
//

struct LLM_TN
{
    LLM_TN(llm_arch arch) : arch(arch) { }

    llm_arch arch;

    std::string operator()(llm_tensor tensor) const
    {
        if (LLM_TENSOR_NAMES.at(arch).find(tensor) == LLM_TENSOR_NAMES.at(arch).end())
        {
            return "__missing__";
        }
        return LLM_TENSOR_NAMES.at(arch).at(tensor);
    }

    std::string operator()(llm_tensor tensor, const std::string & suffix) const
    {
        if (LLM_TENSOR_NAMES.at(arch).find(tensor) == LLM_TENSOR_NAMES.at(arch).end())
        {
            return "__missing__";
        }
        return LLM_TENSOR_NAMES.at(arch).at(tensor) + "." + suffix;
    }

    std::string operator()(llm_tensor tensor, int bid) const
    {
        if (LLM_TENSOR_NAMES.at(arch).find(tensor) == LLM_TENSOR_NAMES.at(arch).end())
        {
            return "__missing__";
        }
        return ::format(LLM_TENSOR_NAMES.at(arch).at(tensor).c_str(), bid);
    }

    std::string operator()(llm_tensor tensor, const std::string & suffix, int bid) const
    {
        if (LLM_TENSOR_NAMES.at(arch).find(tensor) == LLM_TENSOR_NAMES.at(arch).end())
        {
            return "__missing__";
        }
        return ::format(LLM_TENSOR_NAMES.at(arch).at(tensor).c_str(), bid) + "." + suffix;
    }

    std::string operator()(llm_tensor tensor, const std::string & suffix, int bid, int xid) const
    {
        if (LLM_TENSOR_NAMES.at(arch).find(tensor) == LLM_TENSOR_NAMES.at(arch).end())
        {
            return "__missing__";
        }
        return ::format(LLM_TENSOR_NAMES.at(arch).at(tensor).c_str(), bid, xid) + "." + suffix;
    }
};

//
// gguf helpers
//

static const std::map<llama_rope_scaling_type, const char *> LLAMA_ROPE_SCALING_TYPES = {
    {LLAMA_ROPE_SCALING_TYPE_NONE, "none"},
    {LLAMA_ROPE_SCALING_TYPE_LINEAR, "linear"},
    {LLAMA_ROPE_SCALING_TYPE_YARN, "yarn"},
};

static llama_rope_scaling_type llama_rope_scaling_type_from_string(const std::string & name)
{
    for (const auto & kv : LLAMA_ROPE_SCALING_TYPES)
    {
        if (kv.second == name)
        {
            return (llama_rope_scaling_type)kv.first;
        }
    }

    return LLAMA_ROPE_SCALING_TYPE_UNSPECIFIED;
}

static std::string gguf_data_to_str(enum gguf_type type, const void * data, int i)
{
    switch (type)
    {
        case GGUF_TYPE_UINT8:
            return std::to_string(((const uint8_t *)data)[i]);
        case GGUF_TYPE_INT8:
            return std::to_string(((const int8_t *)data)[i]);
        case GGUF_TYPE_UINT16:
            return std::to_string(((const uint16_t *)data)[i]);
        case GGUF_TYPE_INT16:
            return std::to_string(((const int16_t *)data)[i]);
        case GGUF_TYPE_UINT32:
            return std::to_string(((const uint32_t *)data)[i]);
        case GGUF_TYPE_INT32:
            return std::to_string(((const int32_t *)data)[i]);
        case GGUF_TYPE_UINT64:
            return std::to_string(((const uint64_t *)data)[i]);
        case GGUF_TYPE_INT64:
            return std::to_string(((const int64_t *)data)[i]);
        case GGUF_TYPE_FLOAT32:
            return std::to_string(((const float *)data)[i]);
        case GGUF_TYPE_FLOAT64:
            return std::to_string(((const double *)data)[i]);
        case GGUF_TYPE_BOOL:
            return ((const bool *)data)[i] ? "true" : "false";
        default:
            return format("unknown type %d", type);
    }
}

static std::string gguf_kv_to_str(const struct gguf_context * ctx_gguf, int i)
{
    const enum gguf_type type = gguf_get_kv_type(ctx_gguf, i);

    switch (type)
    {
        case GGUF_TYPE_STRING:
            return gguf_get_val_str(ctx_gguf, i);
        case GGUF_TYPE_ARRAY: {
            const enum gguf_type arr_type = gguf_get_arr_type(ctx_gguf, i);
            int arr_n = gguf_get_arr_n(ctx_gguf, i);
            const void * data = gguf_get_arr_data(ctx_gguf, i);
            std::string ss;
            ss += "[";
            for (int j = 0; j < arr_n; j++)
            {
                if (arr_type == GGUF_TYPE_STRING)
                {
                    std::string val = gguf_get_arr_str(ctx_gguf, i, j);
                    // escape quotes
                    replace_all(val, "\\", "\\\\");
                    replace_all(val, "\"", "\\\"");
                    ss += '"' + val + '"';
                }
                else if (arr_type == GGUF_TYPE_ARRAY)
                {
                    ss += "???";
                }
                else
                {
                    ss += gguf_data_to_str(arr_type, data, j);
                }
                if (j < arr_n - 1)
                {
                    ss += ", ";
                }
            }
            ss += "]";
            return ss;
        }
        default:
            return gguf_data_to_str(type, gguf_get_val_data(ctx_gguf, i), 0);
    }
}

//
// llama helpers
//

#if defined(_WIN32)
static std::string llama_format_win_err(DWORD err)
{
    LPSTR buf;
    size_t size = FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        nullptr,
        err,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&buf,
        0,
        nullptr);
    if (!size)
    {
        return "FormatMessageA failed";
    }
    std::string ret(buf, size);
    LocalFree(buf);
    return ret;
}
#endif

template <typename T>
struct no_init
{
    T value;
    no_init() { /* do nothing */ }
};

struct llama_file
{
    // use FILE * so we don't have to re-open the file to mmap
    FILE * fp;
    size_t size;

    llama_file(const char * fname, const char * mode)
    {
        fp = ggml_fopen(fname, mode);
        if (fp == nullptr)
        {
            throw std::runtime_error(format("failed to open %s: %s", fname, strerror(errno)));
        }
        seek(0, SEEK_END);
        size = tell();
        seek(0, SEEK_SET);
    }

    size_t tell() const
    {
#ifdef _WIN32
        __int64 ret = _ftelli64(fp);
#else
        long ret = std::ftell(fp);
#endif
        GGML_ASSERT(ret != -1); // this really shouldn't fail
        return (size_t)ret;
    }

    void seek(size_t offset, int whence) const
    {
#ifdef _WIN32
        int ret = _fseeki64(fp, (__int64)offset, whence);
#else
        int ret = std::fseek(fp, (long)offset, whence);
#endif
        GGML_ASSERT(ret == 0); // same
    }

    void read_raw(void * ptr, size_t len) const
    {
        if (len == 0)
        {
            return;
        }
        errno = 0;
        std::size_t ret = std::fread(ptr, len, 1, fp);
        if (ferror(fp))
        {
            throw std::runtime_error(format("read error: %s", strerror(errno)));
        }
        if (ret != 1)
        {
            throw std::runtime_error("unexpectedly reached end of file");
        }
    }

    uint32_t read_u32() const
    {
        uint32_t ret;
        read_raw(&ret, sizeof(ret));
        return ret;
    }

    void write_raw(const void * ptr, size_t len) const
    {
        if (len == 0)
        {
            return;
        }
        errno = 0;
        size_t ret = std::fwrite(ptr, len, 1, fp);
        if (ret != 1)
        {
            throw std::runtime_error(format("write error: %s", strerror(errno)));
        }
    }

    void write_u32(std::uint32_t val) const { write_raw(&val, sizeof(val)); }

    ~llama_file()
    {
        if (fp)
        {
            std::fclose(fp);
        }
    }
};
using llama_files = std::vector<std::unique_ptr<llama_file>>;

struct llama_mmap
{
    void * addr;
    size_t size;

    llama_mmap(const llama_mmap &) = delete;

#ifdef _POSIX_MAPPED_FILES
    static constexpr bool SUPPORTED = true;

    // list of mapped fragments (first_offset, last_offset)
    std::vector<std::pair<size_t, size_t>> mapped_fragments;

    llama_mmap(struct llama_file * file, size_t prefetch = (size_t)-1 /* -1 = max value */, bool numa = false)
    {
        size = file->size;
        int fd = fileno(file->fp);
        int flags = MAP_SHARED;
        // prefetch/readahead impairs performance on NUMA systems
        if (numa)
        {
            prefetch = 0;
        }
#    ifdef __linux__
        // advise the kernel to read the file sequentially (increases readahead)
        if (posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL))
        {
            LLAMA_LOG_WARN("warning: posix_fadvise(.., POSIX_FADV_SEQUENTIAL) failed: %s\n", strerror(errno));
        }
        if (prefetch)
        {
            flags |= MAP_POPULATE;
        }
#    endif
        addr = mmap(nullptr, file->size, PROT_READ, flags, fd, 0);
        if (addr == MAP_FAILED)
        { // NOLINT
            throw std::runtime_error(format("mmap failed: %s", strerror(errno)));
        }

        if (prefetch > 0)
        {
            // advise the kernel to preload the mapped memory
            if (posix_madvise(addr, std::min(file->size, prefetch), POSIX_MADV_WILLNEED))
            {
                LLAMA_LOG_WARN("warning: posix_madvise(.., POSIX_MADV_WILLNEED) failed: %s\n", strerror(errno));
            }
        }
        if (numa)
        {
            // advise the kernel not to use readahead
            // (because the next page might not belong on the same node)
            if (posix_madvise(addr, file->size, POSIX_MADV_RANDOM))
            {
                LLAMA_LOG_WARN("warning: posix_madvise(.., POSIX_MADV_RANDOM) failed: %s\n", strerror(errno));
            }
        }

        // initialize list of mapped_fragments
        mapped_fragments.emplace_back(0, file->size);
    }

    static void align_range(size_t * first, size_t * last, size_t page_size)
    {
        // align first to the next page
        size_t offset_in_page = *first & (page_size - 1);
        size_t offset_to_page = offset_in_page == 0 ? 0 : page_size - offset_in_page;
        *first += offset_to_page;

        // align last to the previous page
        *last = *last & ~(page_size - 1);

        if (*last <= *first)
        {
            *last = *first;
        }
    }

    // partially unmap the file in the range [first, last)
    void unmap_fragment(size_t first, size_t last)
    {
        // note: this function must not be called multiple times with overlapping ranges
        // otherwise, there is a risk of invalidating addresses that have been repurposed for other mappings
        int page_size = sysconf(_SC_PAGESIZE);
        align_range(&first, &last, page_size);
        size_t len = last - first;

        if (len == 0)
        {
            return;
        }

        GGML_ASSERT(first % page_size == 0);
        GGML_ASSERT(last % page_size == 0);
        GGML_ASSERT(last > first);

        void * next_page_start = (uint8_t *)addr + first;

        // unmap the range
        if (munmap(next_page_start, len))
        {
            LLAMA_LOG_WARN("warning: munmap failed: %s\n", strerror(errno));
        }

        // update the list of mapped fragments to avoid unmapping the same range again in the destructor
        std::vector<std::pair<size_t, size_t>> new_mapped_fragments;
        for (const auto & frag : mapped_fragments)
        {
            if (frag.first < first && frag.second > last)
            {
                // the range is in the middle of the fragment, split it
                new_mapped_fragments.emplace_back(frag.first, first);
                new_mapped_fragments.emplace_back(last, frag.second);
            }
            else if (frag.first < first && frag.second > first)
            {
                // the range starts in the middle of the fragment
                new_mapped_fragments.emplace_back(frag.first, first);
            }
            else if (frag.first < last && frag.second > last)
            {
                // the range ends in the middle of the fragment
                new_mapped_fragments.emplace_back(last, frag.second);
            }
            else if (frag.first >= first && frag.second <= last)
            {
                // the range covers the entire fragment
            }
            else
            {
                // the range is outside the fragment
                new_mapped_fragments.push_back(frag);
            }
        }
        mapped_fragments = std::move(new_mapped_fragments);
    }

    ~llama_mmap()
    {
        for (const auto & frag : mapped_fragments)
        {
            if (munmap((char *)addr + frag.first, frag.second - frag.first))
            {
                LLAMA_LOG_WARN("warning: munmap failed: %s\n", strerror(errno));
            }
        }
    }
#elif defined(_WIN32)
    static constexpr bool SUPPORTED = true;

    llama_mmap(struct llama_file * file, size_t prefetch = (size_t)-1, bool numa = false)
    {
        GGML_UNUSED(numa);

        size = file->size;

        HANDLE hFile = (HANDLE)_get_osfhandle(_fileno(file->fp));

        HANDLE hMapping = CreateFileMappingA(hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);

        if (hMapping == nullptr)
        {
            DWORD error = GetLastError();
            throw std::runtime_error(format("CreateFileMappingA failed: %s", llama_format_win_err(error).c_str()));
        }

        addr = MapViewOfFile(hMapping, FILE_MAP_READ, 0, 0, 0);
        DWORD error = GetLastError();
        CloseHandle(hMapping);

        if (addr == nullptr)
        {
            throw std::runtime_error(format("MapViewOfFile failed: %s", llama_format_win_err(error).c_str()));
        }

        if (prefetch > 0)
        {
#    if _WIN32_WINNT >= 0x602
            // PrefetchVirtualMemory is only present on Windows 8 and above, so we dynamically load it
            BOOL(WINAPI * pPrefetchVirtualMemory)(HANDLE, ULONG_PTR, PWIN32_MEMORY_RANGE_ENTRY, ULONG);
            HMODULE hKernel32 = GetModuleHandleW(L"kernel32.dll");

            // may fail on pre-Windows 8 systems
            pPrefetchVirtualMemory = reinterpret_cast<decltype(pPrefetchVirtualMemory)>(GetProcAddress(hKernel32, "PrefetchVirtualMemory"));

            if (pPrefetchVirtualMemory)
            {
                // advise the kernel to preload the mapped memory
                WIN32_MEMORY_RANGE_ENTRY range;
                range.VirtualAddress = addr;
                range.NumberOfBytes = (SIZE_T)std::min(size, prefetch);
                if (!pPrefetchVirtualMemory(GetCurrentProcess(), 1, &range, 0))
                {
                    LLAMA_LOG_WARN("warning: PrefetchVirtualMemory failed: %s\n", llama_format_win_err(GetLastError()).c_str());
                }
            }
#    else
            throw std::runtime_error("PrefetchVirtualMemory unavailable");
#    endif
        }
    }

    void unmap_fragment(size_t first, size_t last)
    {
        // not supported
        GGML_UNUSED(first);
        GGML_UNUSED(last);
    }

    ~llama_mmap()
    {
        if (!UnmapViewOfFile(addr))
        {
            LLAMA_LOG_WARN("warning: UnmapViewOfFile failed: %s\n", llama_format_win_err(GetLastError()).c_str());
        }
    }
#else
    static constexpr bool SUPPORTED = false;

    llama_mmap(struct llama_file * file, size_t prefetch = -1, bool numa = false)
    {
        GGML_UNUSED(file);
        GGML_UNUSED(prefetch);
        GGML_UNUSED(numa);

        throw std::runtime_error("mmap not supported");
    }

    void unmap_fragment(size_t first, size_t last)
    {
        GGML_UNUSED(first);
        GGML_UNUSED(last);

        throw std::runtime_error("mmap not supported");
    }
#endif
};
using llama_mmaps = std::vector<std::unique_ptr<llama_mmap>>;

// Represents some region of memory being locked using mlock or VirtualLock;
// will automatically unlock on destruction.
struct llama_mlock
{
    void * addr = nullptr;
    size_t size = 0;

    bool failed_already = false;

    llama_mlock() { }
    llama_mlock(const llama_mlock &) = delete;

    ~llama_mlock()
    {
        if (size)
        {
            raw_unlock(addr, size);
        }
    }

    void init(void * ptr)
    {
        GGML_ASSERT(addr == nullptr && size == 0); // NOLINT
        addr = ptr;
    }

    void grow_to(size_t target_size)
    {
        GGML_ASSERT(addr);
        if (failed_already)
        {
            return;
        }
        size_t granularity = lock_granularity();
        target_size = (target_size + granularity - 1) & ~(granularity - 1);
        if (target_size > size)
        {
            if (raw_lock((uint8_t *)addr + size, target_size - size))
            {
                size = target_size;
            }
            else
            {
                failed_already = true;
            }
        }
    }

#ifdef _POSIX_MEMLOCK_RANGE
    static constexpr bool SUPPORTED = true;

    static size_t lock_granularity() { return (size_t)sysconf(_SC_PAGESIZE); }

#    ifdef __APPLE__
#        define MLOCK_SUGGESTION \
            "Try increasing the sysctl values 'vm.user_wire_limit' and 'vm.global_user_wire_limit' and/or " \
            "decreasing 'vm.global_no_user_wire_amount'.  Also try increasing RLIMIT_MEMLOCK (ulimit -l).\n"
#    else
#        define MLOCK_SUGGESTION "Try increasing RLIMIT_MEMLOCK ('ulimit -l' as root).\n"
#    endif

    bool raw_lock(const void * addr, size_t size) const
    {
        if (!mlock(addr, size))
        {
            return true;
        }

        char * errmsg = std::strerror(errno);
        bool suggest = (errno == ENOMEM);

        // Check if the resource limit is fine after all
        struct rlimit lock_limit;
        if (suggest && getrlimit(RLIMIT_MEMLOCK, &lock_limit))
        {
            suggest = false;
        }
        if (suggest && (lock_limit.rlim_max > lock_limit.rlim_cur + size))
        {
            suggest = false;
        }

        LLAMA_LOG_WARN(
            "warning: failed to mlock %zu-byte buffer (after previously locking %zu bytes): %s\n%s",
            size,
            this->size,
            errmsg,
            suggest ? MLOCK_SUGGESTION : "");
        return false;
    }

#    undef MLOCK_SUGGESTION

    static void raw_unlock(void * addr, size_t size)
    {
        if (munlock(addr, size))
        {
            LLAMA_LOG_WARN("warning: failed to munlock buffer: %s\n", std::strerror(errno));
        }
    }
#elif defined(_WIN32)
    static constexpr bool SUPPORTED = true;

    static size_t lock_granularity()
    {
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        return (size_t)si.dwPageSize;
    }

    bool raw_lock(void * ptr, size_t len) const
    {
        for (int tries = 1;; tries++)
        {
            if (VirtualLock(ptr, len))
            {
                return true;
            }
            if (tries == 2)
            {
                LLAMA_LOG_WARN(
                    "warning: failed to VirtualLock %zu-byte buffer (after previously locking %zu bytes): %s\n",
                    len,
                    size,
                    llama_format_win_err(GetLastError()).c_str());
                return false;
            }

            // It failed but this was only the first try; increase the working
            // set size and try again.
            SIZE_T min_ws_size, max_ws_size;
            if (!GetProcessWorkingSetSize(GetCurrentProcess(), &min_ws_size, &max_ws_size))
            {
                LLAMA_LOG_WARN("warning: GetProcessWorkingSetSize failed: %s\n", llama_format_win_err(GetLastError()).c_str());
                return false;
            }
            // Per MSDN: "The maximum number of pages that a process can lock
            // is equal to the number of pages in its minimum working set minus
            // a small overhead."
            // Hopefully a megabyte is enough overhead:
            size_t increment = len + 1048576;
            // The minimum must be <= the maximum, so we need to increase both:
            min_ws_size += increment;
            max_ws_size += increment;
            if (!SetProcessWorkingSetSize(GetCurrentProcess(), min_ws_size, max_ws_size))
            {
                LLAMA_LOG_WARN("warning: SetProcessWorkingSetSize failed: %s\n", llama_format_win_err(GetLastError()).c_str());
                return false;
            }
        }
    }

    static void raw_unlock(void * ptr, size_t len)
    {
        if (!VirtualUnlock(ptr, len))
        {
            LLAMA_LOG_WARN("warning: failed to VirtualUnlock buffer: %s\n", llama_format_win_err(GetLastError()).c_str());
        }
    }
#else
    static constexpr bool SUPPORTED = false;

    static size_t lock_granularity() { return (size_t)65536; }

    bool raw_lock(const void * addr, size_t len) const
    {
        LLAMA_LOG_WARN("warning: mlock not supported on this system\n");
        return false;
    }

    static void raw_unlock(const void * addr, size_t len) { }
#endif
};
using llama_mlocks = std::vector<std::unique_ptr<llama_mlock>>;

static std::string llama_token_to_piece(const struct llama_context * ctx, llama_token token, bool special)
{
    std::vector<char> result(8, 0);
    const int n_tokens = llama_token_to_piece(llama_get_model(ctx), token, result.data(), result.size(), special);
    if (n_tokens < 0)
    {
        result.resize(-n_tokens);
        int check = llama_token_to_piece(llama_get_model(ctx), token, result.data(), result.size(), special);
        GGML_ASSERT(check == -n_tokens);
    }
    else
    {
        result.resize(n_tokens);
    }

    return std::string(result.data(), result.size());
}

static ggml_backend_buffer_type_t llama_default_buffer_type_cpu(bool host_buffer)
{
    ggml_backend_buffer_type_t buft = nullptr;

#if defined(GGML_USE_CUDA)
    // host buffers should only be used when data is expected to be copied to/from the GPU
    if (host_buffer)
    {
        buft = ggml_backend_cuda_host_buffer_type();
    }
#elif defined(GGML_USE_SYCL)
    if (host_buffer)
    {
        buft = ggml_backend_sycl_host_buffer_type();
    }
#elif defined(GGML_USE_CPU_HBM)
    buft = ggml_backend_cpu_hbm_buffer_type();
#elif defined(GGML_USE_VULKAN)
    if (host_buffer)
    {
        buft = ggml_backend_vk_host_buffer_type();
    }
#endif

    if (buft == nullptr)
    {
        buft = ggml_backend_cpu_buffer_type();
    }
    return buft;

    GGML_UNUSED(host_buffer);
}

//
// globals
//

struct llama_state
{
    llama_state()
    {
#ifdef GGML_USE_METAL
        ggml_backend_metal_log_set_callback(log_callback, log_callback_user_data);
#elif defined(GGML_USE_CUDA)
        ggml_backend_cuda_log_set_callback(log_callback, log_callback_user_data);
#endif
    }

    // We save the log callback globally
    ggml_log_callback log_callback = llama_log_callback_default;
    void * log_callback_user_data = nullptr;
};

static llama_state g_state;

// available llama models
enum e_model
{
    MODEL_UNKNOWN,
    MODEL_14M,
    MODEL_17M,
    MODEL_22M,
    MODEL_33M,
    MODEL_70M,
    MODEL_109M,
    MODEL_137M,
    MODEL_160M,
    MODEL_335M,
    MODEL_410M,
    MODEL_0_5B,
    MODEL_1B,
    MODEL_1_4B,
    MODEL_2B,
    MODEL_2_8B,
    MODEL_3B,
    MODEL_4B,
    MODEL_6_9B,
    MODEL_7B,
    MODEL_8B,
    MODEL_12B,
    MODEL_13B,
    MODEL_14B,
    MODEL_15B,
    MODEL_20B,
    MODEL_30B,
    MODEL_34B,
    MODEL_35B,
    MODEL_40B,
    MODEL_65B,
    MODEL_70B,
    MODEL_314B,
    MODEL_SMALL,
    MODEL_MEDIUM,
    MODEL_LARGE,
    MODEL_XL,
    MODEL_A2_7B,
    MODEL_8x7B,
    MODEL_8x22B,
    MODEL_16x12B,
    MODEL_10B_128x3_66B,
};

static const size_t kiB = 1024;
static const size_t MiB = 1024 * kiB;
static const size_t GiB = 1024 * MiB;

struct llama_hparams
{
    bool vocab_only;
    bool rope_finetuned;
    bool use_par_res;

    uint32_t n_vocab;
    uint32_t n_ctx_train; // context size the model was trained on
    uint32_t n_embd;
    uint32_t n_head;
    uint32_t n_head_kv;
    uint32_t n_layer;
    uint32_t n_rot;
    uint32_t
        n_embd_head_k; // dimension of keys (d_k). d_q is assumed to be the same, but there are n_head q heads, and only n_head_kv k-v heads
    uint32_t n_embd_head_v; // dimension of values (d_v) aka n_embd_head
    uint32_t n_ff;
    uint32_t n_expert = 0;
    uint32_t n_expert_used = 0;
    uint32_t n_vocab_type = 0; // for BERT-style token types

    float f_norm_eps;
    float f_norm_rms_eps;

    float rope_attn_factor = 1.0f;
    float rope_freq_base_train;
    float rope_freq_scale_train;
    uint32_t n_yarn_orig_ctx;

    // for State Space Models
    uint32_t ssm_d_conv = 0;
    uint32_t ssm_d_inner = 0;
    uint32_t ssm_d_state = 0;
    uint32_t ssm_dt_rank = 0;

    float f_clamp_kqv = 0.0f;
    float f_max_alibi_bias = 0.0f;
    float f_logit_scale = 0.0f;

    bool causal_attn = true;
    bool use_alibi = false;

    enum llama_pooling_type pooling_type = LLAMA_POOLING_TYPE_NONE;
    enum llama_rope_type rope_type = LLAMA_ROPE_TYPE_NONE;
    enum llama_rope_scaling_type rope_scaling_type_train = LLAMA_ROPE_SCALING_TYPE_NONE;

    bool operator!=(const llama_hparams & other) const
    {
        if (this->vocab_only != other.vocab_only)
            return true;
        if (this->n_vocab != other.n_vocab)
            return true;
        if (this->n_ctx_train != other.n_ctx_train)
            return true;
        if (this->n_embd != other.n_embd)
            return true;
        if (this->n_head != other.n_head)
            return true;
        if (this->n_head_kv != other.n_head_kv)
            return true;
        if (this->n_layer != other.n_layer)
            return true;
        if (this->n_rot != other.n_rot)
            return true;
        if (this->n_embd_head_k != other.n_embd_head_k)
            return true;
        if (this->n_embd_head_v != other.n_embd_head_v)
            return true;
        if (this->n_ff != other.n_ff)
            return true;
        if (this->n_expert != other.n_expert)
            return true;
        if (this->n_expert_used != other.n_expert_used)
            return true;

        if (this->rope_finetuned != other.rope_finetuned)
            return true;
        if (this->n_yarn_orig_ctx != other.n_yarn_orig_ctx)
            return true;

        if (this->ssm_d_conv != other.ssm_d_conv)
            return true;
        if (this->ssm_d_inner != other.ssm_d_inner)
            return true;
        if (this->ssm_d_state != other.ssm_d_state)
            return true;
        if (this->ssm_dt_rank != other.ssm_dt_rank)
            return true;

        const float EPSILON = 1e-9f;

        if (!is_float_close(this->f_norm_eps, other.f_norm_eps, EPSILON))
            return true;
        if (!is_float_close(this->f_norm_rms_eps, other.f_norm_rms_eps, EPSILON))
            return true;
        if (!is_float_close(this->rope_attn_factor, other.rope_attn_factor, EPSILON))
            return true;
        if (!is_float_close(this->rope_freq_base_train, other.rope_freq_base_train, EPSILON))
            return true;
        if (!is_float_close(this->rope_freq_scale_train, other.rope_freq_scale_train, EPSILON))
            return true;

        return false;
    }

    uint32_t n_gqa() const
    {
        if (n_head_kv == 0)
        {
            return 0;
        }
        return n_head / n_head_kv;
    }

    uint32_t n_embd_k_gqa() const
    { // dimension of key embeddings across all k-v heads
        return n_embd_head_k * n_head_kv;
    }

    uint32_t n_embd_v_gqa() const
    { // dimension of value embeddings across all k-v heads
        return n_embd_head_v * n_head_kv;
    }

    uint32_t n_embd_k_s() const
    { // dimension of the rolling state embeddings
        // corresponds to Mamba's conv_states size
        // TODO: maybe support other convolution strides than 1
        // NOTE: since the first column of the conv_state is shifted out each time, it's not actually needed
        return (ssm_d_conv > 0 ? ssm_d_conv - 1 : 0) * ssm_d_inner;
    }

    uint32_t n_embd_v_s() const
    { // dimension of the recurrent state embeddings
        // corresponds to Mamba's ssm_states size
        return ssm_d_state * ssm_d_inner;
    }
};

struct llama_cparams
{
    uint32_t n_ctx; // context size used during inference
    uint32_t n_batch;
    uint32_t n_ubatch;
    uint32_t n_seq_max;
    uint32_t n_threads; // number of threads to use for generation
    uint32_t n_threads_batch; // number of threads to use for batch processing

    float rope_freq_base;
    float rope_freq_scale;

    uint32_t n_yarn_orig_ctx;
    // These hyperparameters are not exposed in GGUF, because all
    // existing YaRN models use the same values for them.
    float yarn_ext_factor;
    float yarn_attn_factor;
    float yarn_beta_fast;
    float yarn_beta_slow;
    float defrag_thold;

    bool embeddings;
    bool causal_attn;
    bool offload_kqv;
    bool flash_attn;

    enum llama_pooling_type pooling_type;

    ggml_backend_sched_eval_callback cb_eval;
    void * cb_eval_user_data;
};

struct llama_layer
{
    // normalization
    struct ggml_tensor * attn_norm;
    struct ggml_tensor * attn_norm_b;
    struct ggml_tensor * attn_norm_2;
    struct ggml_tensor * attn_norm_2_b;
    struct ggml_tensor * attn_q_norm;
    struct ggml_tensor * attn_q_norm_b;
    struct ggml_tensor * attn_k_norm;
    struct ggml_tensor * attn_k_norm_b;
    struct ggml_tensor * attn_out_norm;
    struct ggml_tensor * attn_out_norm_b;

    // attention
    struct ggml_tensor * wq;
    struct ggml_tensor * wk;
    struct ggml_tensor * wv;
    struct ggml_tensor * wo;
    struct ggml_tensor * wqkv;

    // attention bias
    struct ggml_tensor * bq;
    struct ggml_tensor * bk;
    struct ggml_tensor * bv;
    struct ggml_tensor * bo;
    struct ggml_tensor * bqkv;

    // normalization
    struct ggml_tensor * ffn_norm;
    struct ggml_tensor * ffn_norm_b;
    struct ggml_tensor * layer_out_norm;
    struct ggml_tensor * layer_out_norm_b;
    struct ggml_tensor * ffn_norm_exps;

    // ff
    struct ggml_tensor * ffn_gate; // w1
    struct ggml_tensor * ffn_down; // w2
    struct ggml_tensor * ffn_up; // w3

    // ff MoE
    struct ggml_tensor * ffn_gate_inp;
    struct ggml_tensor * ffn_gate_exps;
    struct ggml_tensor * ffn_down_exps;
    struct ggml_tensor * ffn_up_exps;

    // ff shared expert (shexp)
    struct ggml_tensor * ffn_gate_inp_shexp;
    struct ggml_tensor * ffn_gate_shexp;
    struct ggml_tensor * ffn_down_shexp;
    struct ggml_tensor * ffn_up_shexp;

    // ff bias
    struct ggml_tensor * ffn_down_b; // b2
    struct ggml_tensor * ffn_up_b; // b3
    struct ggml_tensor * ffn_act;

    // mamba proj
    struct ggml_tensor * ssm_in;
    struct ggml_tensor * ssm_x;
    struct ggml_tensor * ssm_dt;
    struct ggml_tensor * ssm_out;

    // mamba
    struct ggml_tensor * ssm_conv1d;
    struct ggml_tensor * ssm_a;
    struct ggml_tensor * ssm_d;

    // mamba bias
    struct ggml_tensor * ssm_conv1d_b;
    struct ggml_tensor * ssm_dt_b;

    // long rope factors
    struct ggml_tensor * rope_long = nullptr;
    struct ggml_tensor * rope_short = nullptr;
};

struct llama_kv_cell
{
    llama_pos pos = -1;
    llama_pos delta = 0;
    int32_t src = 0; // used by recurrent state models to copy states

    std::set<llama_seq_id> seq_id;

    bool has_seq_id(const llama_seq_id & id) const { return seq_id.find(id) != seq_id.end(); }

    bool is_empty() const { return seq_id.empty(); }

    bool is_same_seq(const llama_kv_cell & other) const { return seq_id == other.seq_id; }
};

// ring-buffer of cached KV data
struct llama_kv_cache
{
    bool has_shift = false;
    bool do_defrag = false;
    bool do_copy = false;
    bool recurrent = false; // with recurrent state models, a cell can hold the state for more than one past token
    bool v_trans = true; // the value tensor is transposed

    // Note: The value of head isn't only used to optimize searching
    // for a free KV slot. llama_decode_internal also uses it, so it
    // cannot be freely changed after a slot has been allocated.
    uint32_t head = 0;
    uint32_t size = 0;
    uint32_t used = 0; // used cells (i.e. at least one seq_id)

    // computed before each graph build
    uint32_t n = 0;

    ggml_type type_k = GGML_TYPE_F16;
    ggml_type type_v = GGML_TYPE_F16;

    std::vector<llama_kv_cell> cells;

    std::vector<struct ggml_tensor *> k_l; // per layer
    std::vector<struct ggml_tensor *> v_l;

    std::vector<struct ggml_context *> ctxs;
    std::vector<ggml_backend_buffer_t> bufs;

    size_t total_size() const
    {
        size_t size = 0;
        for (ggml_backend_buffer_t buf : bufs)
        {
            size += ggml_backend_buffer_get_size(buf);
        }
        return size;
    }

    ~llama_kv_cache()
    {
        for (struct ggml_context * ctx : ctxs)
        {
            ggml_free(ctx);
        }
        for (ggml_backend_buffer_t buf : bufs)
        {
            ggml_backend_buffer_free(buf);
        }
    }
};

struct llama_control_vector
{
    std::vector<struct ggml_tensor *> tensors; // per layer
    std::vector<struct ggml_context *> ctxs;
    std::vector<ggml_backend_buffer_t> bufs;

    int32_t layer_start = -1;
    int32_t layer_end = -1;

    ggml_tensor * tensor_for(int il) const
    {
        if (il < 0 || il < layer_start || il > layer_end || (size_t)il >= tensors.size())
        {
            return nullptr;
        }
        return tensors[il];
    }

    ~llama_control_vector()
    {
        for (struct ggml_context * ctx : ctxs)
        {
            ggml_free(ctx);
        }
        for (ggml_backend_buffer_t buf : bufs)
        {
            ggml_backend_buffer_free(buf);
        }
    }
};

struct llama_vocab
{
    using id = int32_t;
    using token = std::string;
    using ttype = llama_token_type;

    struct token_data
    {
        token text;
        float score;
        ttype type;
    };

    enum llama_vocab_type type = LLAMA_VOCAB_TYPE_SPM;
    enum llama_vocab_pre_type type_pre = LLAMA_VOCAB_PRE_TYPE_DEFAULT;

    std::unordered_map<token, id> token_to_id;
    std::vector<token_data> id_to_token;

    std::unordered_map<token, id> special_tokens_cache;

    std::map<std::pair<std::string, std::string>, int> bpe_ranks;

    // default LLaMA special tokens
    id special_bos_id = 1;
    id special_eos_id = 2;
    id special_unk_id = 0;
    id special_sep_id = -1;
    id special_pad_id = -1;
    id special_cls_id = -1;
    id special_mask_id = -1;

    int special_add_bos = -1; // -1 unknown, 1 add, 0 don't add.
    int special_add_eos = -1; // -1 unknown, 1 add, 0 don't add.

    id linefeed_id = 13;
    id special_prefix_id = -1;
    id special_suffix_id = -1;
    id special_middle_id = -1;
    id special_eot_id = -1; // TODO: move above after "eos_id", and here add "file separator" token

    bool add_space_prefix = true;

    int find_bpe_rank(const std::string & token_left, const std::string & token_right) const
    {
        GGML_ASSERT(token_left.find(' ') == std::string::npos);
        GGML_ASSERT(token_left.find('\n') == std::string::npos);
        GGML_ASSERT(token_right.find(' ') == std::string::npos);
        GGML_ASSERT(token_right.find('\n') == std::string::npos);

        auto it = bpe_ranks.find(std::make_pair(token_left, token_right));
        if (it == bpe_ranks.end())
        {
            return -1;
        }

        return it->second;
    }
};

struct llama_model
{
    e_model type = MODEL_UNKNOWN;
    llm_arch arch = LLM_ARCH_UNKNOWN;
    llama_ftype ftype = LLAMA_FTYPE_ALL_F32;

    std::string name = "n/a";

    llama_hparams hparams = {};
    llama_vocab vocab;

    struct ggml_tensor * tok_embd;
    struct ggml_tensor * type_embd;
    struct ggml_tensor * pos_embd;
    struct ggml_tensor * tok_norm;
    struct ggml_tensor * tok_norm_b;

    struct ggml_tensor * output_norm;
    struct ggml_tensor * output_norm_b;
    struct ggml_tensor * output;
    struct ggml_tensor * output_b;

    std::vector<llama_layer> layers;

    llama_split_mode split_mode;
    int main_gpu;
    int n_gpu_layers;

    std::vector<std::string> rpc_servers;

    // gguf metadata
    std::unordered_map<std::string, std::string> gguf_kv;

    // layer -> buffer type mapping
    struct layer_buft
    {
        layer_buft() : buft_matrix(nullptr), buft(nullptr) { }
        layer_buft(ggml_backend_buffer_type_t matrix) : buft_matrix(matrix), buft(matrix) { }
        layer_buft(ggml_backend_buffer_type_t matrix, ggml_backend_buffer_type_t other) : buft_matrix(matrix), buft(other) { }

        ggml_backend_buffer_type_t
            buft_matrix; // matrices only - used by split buffers and backends that support only matrix multiplication
        ggml_backend_buffer_type_t buft; // everything else
    };

    layer_buft buft_input;
    layer_buft buft_output;
    std::vector<layer_buft> buft_layer;

    // contexts where the model tensors metadata is stored
    std::vector<struct ggml_context *> ctxs;

    // the model memory buffers for the tensor data
    std::vector<ggml_backend_buffer_t> bufs;

    // model memory mapped files
    llama_mmaps mappings;

    // objects representing data potentially being locked in memory
    llama_mlocks mlock_bufs;
    llama_mlocks mlock_mmaps;

    // for quantize-stats only
    std::vector<std::pair<std::string, struct ggml_tensor *>> tensors_by_name;

    int64_t t_load_us = 0;
    int64_t t_start_us = 0;

    ~llama_model()
    {
        for (struct ggml_context * ctx : ctxs)
        {
            ggml_free(ctx);
        }
        for (ggml_backend_buffer_t buf : bufs)
        {
#ifdef GGML_USE_CUDA
            if (ggml_backend_buffer_get_type(buf) == ggml_backend_cpu_buffer_type())
            {
                ggml_backend_cuda_unregister_host_buffer(ggml_backend_buffer_get_base(buf));
            }
#endif
            ggml_backend_buffer_free(buf);
        }
    }
};

struct llama_context
{
    llama_context(const llama_model & model) : model(model), t_start_us(model.t_start_us), t_load_us(model.t_load_us) { }
    ~llama_context()
    {
        ggml_backend_sched_free(sched);

        for (ggml_backend_t backend : backends)
        {
            ggml_backend_free(backend);
        }

        ggml_backend_buffer_free(buf_output);
    }

    llama_cparams cparams;

    std::vector<ggml_backend_t> backends;
#ifdef GGML_USE_METAL
    ggml_backend_t backend_metal = nullptr;
#endif
    ggml_backend_t backend_cpu = nullptr;

    const llama_model & model;

    // key + value cache for the self attention
    struct llama_kv_cache kv_self;

    pcg64_fast rng;

    bool has_evaluated_once = false;

    int64_t t_start_us;
    int64_t t_load_us;
    int64_t t_sample_us = 0;
    int64_t t_p_eval_us = 0;
    int64_t t_eval_us = 0;

    int64_t t_compute_start_us = 0;
    int64_t n_queued_tokens = 0;

    int32_t n_sample = 0; // number of tokens sampled
    int32_t n_p_eval = 0; // number of tokens in eval calls for the prompt (with batch size > 1)
    int32_t n_eval = 0; // number of eval calls

    // host buffer for the model output (logits and embeddings)
    ggml_backend_buffer_t buf_output = nullptr;

    // decode output (2-dimensional array: [n_outputs][n_vocab])
    size_t logits_size = 0; // capacity (of floats) for logits
    float * logits = nullptr;

    std::vector<int32_t> output_ids; // map batch token positions to ids of the logits and embd buffers
    size_t output_size = 0; // capacity (of tokens positions) for the output buffers
    int32_t n_outputs = 0; // number of actually-used outputs in the current ubatch or last logical batch

    bool logits_all = false;

    // embeddings output (2-dimensional array: [n_outputs][n_embd])
    // populated only when pooling_type == LLAMA_POOLING_TYPE_NONE
    size_t embd_size = 0; // capacity (of floats) for embeddings
    float * embd = nullptr;

    // sequence embeddings output (map of [n_embd] vectors)
    // populated only when pooling_type != LLAMA_POOLING_TYPE_NONE
    std::map<llama_seq_id, std::vector<float>> embd_seq;

    // memory buffers used to evaluate the model
    std::vector<uint8_t> buf_compute_meta;
    ggml_backend_sched_t sched = nullptr;

    ggml_abort_callback abort_callback = nullptr;
    void * abort_callback_data = nullptr;

    // input tensors
    struct ggml_tensor * inp_tokens; // I32 [n_batch]
    struct ggml_tensor * inp_embd; // F32 [n_embd, n_batch]
    struct ggml_tensor * inp_pos; // I32 [n_batch]
    struct ggml_tensor * inp_out_ids; // I32 [n_outputs]
    struct ggml_tensor * inp_KQ_mask; // F32 [kv_size, n_batch]
    struct ggml_tensor * inp_K_shift; // I32 [kv_size]
    struct ggml_tensor * inp_mean; // F32 [n_batch, n_batch]
    struct ggml_tensor * inp_cls; // I32 [n_batch]
    struct ggml_tensor * inp_s_copy; // I32 [kv_size]
    struct ggml_tensor * inp_s_mask; // F32 [1, n_kv]
    struct ggml_tensor * inp_s_seq; // I32 [n_kv, n_batch]

    // control vectors
    struct llama_control_vector cvec;
};

static ggml_backend_buffer_type_t llama_default_buffer_type_offload(const llama_model & model, int gpu)
{
    ggml_backend_buffer_type_t buft = nullptr;

#ifdef GGML_USE_RPC
    std::string endpoint = model.rpc_servers[gpu];
    buft = ggml_backend_rpc_buffer_type(endpoint.c_str());
#elif defined(GGML_USE_METAL)
    buft = ggml_backend_metal_buffer_type();
#elif defined(GGML_USE_CUDA)
    buft = ggml_backend_cuda_buffer_type(gpu);
#elif defined(GGML_USE_VULKAN)
    buft = ggml_backend_vk_buffer_type(gpu);
#elif defined(GGML_USE_SYCL)
    buft = ggml_backend_sycl_buffer_type(gpu);
#elif defined(GGML_USE_CLBLAST)
    buft = ggml_backend_opencl_buffer_type();
#elif defined(GGML_USE_KOMPUTE)
    buft = ggml_backend_kompute_buffer_type(gpu);
    if (buft == nullptr)
    {
        LLAMA_LOG_WARN("%s: cannot use GPU %d, check `vulkaninfo --summary`\n", __func__, gpu);
    }
#endif

    if (buft == nullptr)
    {
        buft = llama_default_buffer_type_cpu(true);
    }
    return buft;
    GGML_UNUSED(model);
    GGML_UNUSED(gpu);
}

static bool validate_float(float f, size_t i)
{
    if (isinf(f))
    {
        fprintf(stderr, "ggml_validate_row_data: found inf value at block %zu\n", i);
        return false;
    }

    if (isnan(f))
    {
        fprintf(stderr, "ggml_validate_row_data: found nan value at block %zu\n", i);
        return false;
    }

    return true;
}

static bool isinf_fp16(ggml_fp16_t f)
{
    return (f & 0x7c00) == 0x7c00 && (f & 0x03ff) == 0;
}

static bool isnan_fp16(ggml_fp16_t f)
{
    return (f & 0x7c00) == 0x7c00 && (f & 0x03ff) != 0;
}

static bool validate_fp16(ggml_fp16_t f, size_t i)
{
    if (isinf_fp16(f))
    {
        fprintf(stderr, "ggml_validate_row_data: found inf value at block %zu\n", i);
        return false;
    }

    if (isnan_fp16(f))
    {
        fprintf(stderr, "ggml_validate_row_data: found nan value at block %zu\n", i);
        return false;
    }

    return true;
}

#define VALIDATE_ROW_DATA_D_F16_IMPL(type, data, nb) \
    const type * q = (const type *)(data); \
    for (size_t i = 0; i < (nb); ++i) \
    { \
        if (!validate_fp16(q[i].d, i)) \
        { \
            return false; \
        } \
    }

#define VALIDATE_ROW_DATA_DM_F16_IMPL(type, data, nb, d, m) \
    const type * q = (const type *)(data); \
    for (size_t i = 0; i < (nb); ++i) \
    { \
        if (!validate_fp16(q[i].d, i) || !validate_fp16(q[i].m, i)) \
        { \
            return false; \
        } \
    }

// static void ggml_set_op_params(struct ggml_tensor * tensor, const void * params, size_t params_size) {
//     GGML_ASSERT(tensor != nullptr); // silence -Warray-bounds warnings
//     assert(params_size <= GGML_MAX_OP_PARAMS);
//     memcpy(tensor->op_params, params, params_size);
// }

// // ggml_rope

// static struct ggml_tensor * ggml_rope_impl(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         struct ggml_tensor  * c,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx,
//         int                   n_orig_ctx,
//         float                 freq_base,
//         float                 freq_scale,
//         float                 ext_factor,
//         float                 attn_factor,
//         float                 beta_fast,
//         float                 beta_slow,
//         float                 xpos_base,
//         bool                  xpos_down,
//         bool                  inplace) {
//     GGML_ASSERT((mode & 1) == 0 && "mode & 1 == 1 is no longer supported");

//     GGML_ASSERT(ggml_is_vector(b));
//     GGML_ASSERT(b->type == GGML_TYPE_I32);
//     GGML_ASSERT(a->ne[2] == b->ne[0]);

//     if (c) {
//         GGML_ASSERT(c->type == GGML_TYPE_F32);
//         GGML_ASSERT(c->ne[0] >= n_dims / 2);
//     }

//     bool is_node = false;

//     if (a->grad) {
//         is_node = true;
//     }

//     struct ggml_tensor * result = inplace ? ggml_view_tensor(ctx, a) : ggml_dup_tensor(ctx, a);

//     int32_t params[13] = { /*n_past*/ 0, n_dims, mode, n_ctx, n_orig_ctx };
//     memcpy(params +  5, &freq_base,    sizeof(float));
//     memcpy(params +  6, &freq_scale,   sizeof(float));
//     memcpy(params +  7, &ext_factor,   sizeof(float));
//     memcpy(params +  8, &attn_factor,  sizeof(float));
//     memcpy(params +  9, &beta_fast,    sizeof(float));
//     memcpy(params + 10, &beta_slow,    sizeof(float));
//     memcpy(params + 11, &xpos_base,    sizeof(float));
//     memcpy(params + 12, &xpos_down,    sizeof(bool));
//     ggml_set_op_params(result, params, sizeof(params));

//     result->op   = GGML_OP_ROPE;
//     result->grad = is_node ? ggml_dup_tensor(ctx, result) : nullptr;
//     result->src[0] = a;
//     result->src[1] = b;
//     result->src[2] = c;

//     return result;
// }

// static struct ggml_tensor * ggml_rope(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx) {
//     return ggml_rope_impl(
//         ctx, a, b, nullptr, n_dims, mode, n_ctx, 0, 10000.0f, 1.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, false, false
//     );
// }

// static struct ggml_tensor * ggml_rope_inplace(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx) {
//     return ggml_rope_impl(
//         ctx, a, b, nullptr, n_dims, mode, n_ctx, 0, 10000.0f, 1.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, false, true
//     );
// }

// static struct ggml_tensor * ggml_rope_ext(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         struct ggml_tensor  * c,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx,
//         int                   n_orig_ctx,
//         float                 freq_base,
//         float                 freq_scale,
//         float                 ext_factor,
//         float                 attn_factor,
//         float                 beta_fast,
//         float                 beta_slow) {
//     return ggml_rope_impl(
//         ctx, a, b, c, n_dims, mode, n_ctx, n_orig_ctx, freq_base, freq_scale,
//         ext_factor, attn_factor, beta_fast, beta_slow, 0.0f, false, false
//     );
// }

// static struct ggml_tensor * ggml_rope_ext_inplace(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         struct ggml_tensor  * c,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx,
//         int                   n_orig_ctx,
//         float                 freq_base,
//         float                 freq_scale,
//         float                 ext_factor,
//         float                 attn_factor,
//         float                 beta_fast,
//         float                 beta_slow) {
//     return ggml_rope_impl(
//         ctx, a, b, c, n_dims, mode, n_ctx, n_orig_ctx, freq_base, freq_scale,
//         ext_factor, attn_factor, beta_fast, beta_slow, 0.0f, false, true
//     );
// }

// static struct ggml_tensor * ggml_rope_custom(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx,
//         int                   n_orig_ctx,
//         float                 freq_base,
//         float                 freq_scale,
//         float                 ext_factor,
//         float                 attn_factor,
//         float                 beta_fast,
//         float                 beta_slow) {
//     return ggml_rope_impl(
//         ctx, a, b, nullptr, n_dims, mode, n_ctx, n_orig_ctx, freq_base, freq_scale,
//         ext_factor, attn_factor, beta_fast, beta_slow, 0.0f, false, false
//     );
// }

// static struct ggml_tensor * ggml_rope_custom_inplace(
//         struct ggml_context * ctx,
//         struct ggml_tensor  * a,
//         struct ggml_tensor  * b,
//         int                   n_dims,
//         int                   mode,
//         int                   n_ctx,
//         int                   n_orig_ctx,
//         float                 freq_base,
//         float                 freq_scale,
//         float                 ext_factor,
//         float                 attn_factor,
//         float                 beta_fast,
//         float                 beta_slow) {
//     return ggml_rope_impl(
//         ctx, a, b, nullptr, n_dims, mode, n_ctx, n_orig_ctx, freq_base, freq_scale,
//         ext_factor, attn_factor, beta_fast, beta_slow, 0.0f, false, true
//     );
// }

static ggml_backend_buffer_type_t llama_default_buffer_type_split(const llama_model & model, int fallback_gpu, const float * tensor_split)
{
    ggml_backend_buffer_type_t buft = nullptr;

#ifdef GGML_USE_CUDA
    if (ggml_backend_cuda_get_device_count() > 1)
    {
        buft = ggml_backend_cuda_split_buffer_type(tensor_split);
    }
#endif

#ifdef GGML_USE_SYCL
    if (ggml_backend_sycl_get_device_count() > 1)
    {
        buft = ggml_backend_sycl_split_buffer_type(tensor_split);
    }
#endif

    if (buft == nullptr)
    {
        buft = llama_default_buffer_type_offload(model, fallback_gpu);
    }
    return buft;

    GGML_UNUSED(tensor_split);
}

static size_t llama_get_device_count(const llama_model & model)
{
#if defined(GGML_USE_RPC)
    return model.rpc_servers.size();
#elif defined(GGML_USE_CUDA)
    return ggml_backend_cuda_get_device_count();
#elif defined(GGML_USE_SYCL)
    return ggml_backend_sycl_get_device_count();
#elif defined(GGML_USE_VULKAN)
    return ggml_backend_vk_get_device_count();
#else
    return 1;
#endif
    GGML_UNUSED(model);
}

static size_t llama_get_device_memory(const llama_model & model, int device)
{
#if defined(GGML_USE_RPC)
    size_t total;
    size_t free;
    std::string endpoint = model.rpc_servers[device];
    ggml_backend_rpc_get_device_memory(endpoint.c_str(), &free, &total);
    return free;
#elif defined(GGML_USE_CUDA)
    size_t total;
    size_t free;
    ggml_backend_cuda_get_device_memory(device, &free, &total);
    return free;
#elif defined(GGML_USE_SYCL)
    size_t total;
    size_t free;
    ggml_backend_sycl_get_device_memory(device, &free, &total);
    return free;
#elif defined(GGML_USE_VULKAN)
    size_t total;
    size_t free;
    ggml_backend_vk_get_device_memory(device, &free, &total);
    return free;
#else
    return 1;
#endif
    GGML_UNUSED(model);
    GGML_UNUSED(device);
}

//
// kv cache helpers
//

static bool llama_kv_cache_init(
    struct llama_kv_cache & cache, const llama_context * ctx, ggml_type type_k, ggml_type type_v, uint32_t kv_size, bool offload)
{
    const llama_model & model = ctx->model;
    const llama_cparams & cparams = ctx->cparams;

    const struct llama_hparams & hparams = model.hparams;

    const uint32_t n_embd_k_gqa = hparams.n_embd_k_gqa() + hparams.n_embd_k_s();
    const uint32_t n_embd_v_gqa = hparams.n_embd_v_gqa() + hparams.n_embd_v_s();
    const int64_t n_layer = hparams.n_layer;

    cache.has_shift = false;

    // TODO: find a nicer way to add other recurrent model architectures
    cache.recurrent = model.arch == LLM_ARCH_MAMBA;
    cache.v_trans = !cparams.flash_attn;

    // TODO: support mixed recurrent Transformer architectures
    // NOTE: (!a || b) is a logical implication (a -> b)
    GGML_ASSERT(!cache.recurrent || n_embd_k_gqa == hparams.n_embd_k_s());
    GGML_ASSERT(!cache.recurrent || n_embd_v_gqa == hparams.n_embd_v_s());
    GGML_ASSERT(cache.recurrent || n_embd_k_gqa == hparams.n_embd_k_gqa());
    GGML_ASSERT(cache.recurrent || n_embd_v_gqa == hparams.n_embd_v_gqa());

    cache.head = 0;
    cache.size = kv_size;
    cache.used = 0;

    cache.type_k = type_k;
    cache.type_v = type_v;

    cache.cells.clear();
    cache.cells.resize(kv_size);

    if (cache.recurrent)
    {
        // init state copy sources
        for (uint32_t i = 0; i < cache.size; ++i)
        {
            cache.cells[i].src = i;
        }
    }

#ifdef GGML_USE_CLBLAST
    offload = false;
#endif

    // count used buffer types
    std::map<ggml_backend_buffer_type_t, int> buft_layer_count;
    if (offload)
    {
        for (int64_t i = 0; i < n_layer; ++i)
        {
            buft_layer_count[model.buft_layer[i].buft]++;
        }
    }
    else
    {
        buft_layer_count[llama_default_buffer_type_cpu(true)] = n_layer;
    }

    // create a context for each buffer type
    std::map<ggml_backend_buffer_type_t, ggml_context *> ctx_map;
    for (auto & it : buft_layer_count)
    {
        int n_layers = it.second;
        ggml_init_params params = {
            .mem_size   = 2u * n_layers * ggml_tensor_overhead(),
            .mem_buffer = nullptr,
            .no_alloc   = true,
        };
        ggml_context * ctx = ggml_init(params);
        if (!ctx)
        {
            LLAMA_LOG_ERROR("%s: failed to allocate context for kv cache\n", __func__);
            return false;
        }
        ctx_map[it.first] = ctx;
        cache.ctxs.push_back(ctx);
    }

    cache.k_l.reserve(n_layer);
    cache.v_l.reserve(n_layer);

    for (int i = 0; i < (int)n_layer; i++)
    {
        struct ggml_context * ctx = offload ? ctx_map.at(model.buft_layer[i].buft) : cache.ctxs.front();
        ggml_tensor * k = ggml_new_tensor_1d(ctx, type_k, n_embd_k_gqa * kv_size);
        ggml_tensor * v = ggml_new_tensor_1d(ctx, type_v, n_embd_v_gqa * kv_size);
        ggml_format_name(k, "cache_k_l%d", i);
        ggml_format_name(v, "cache_v_l%d", i);
        cache.k_l.push_back(k);
        cache.v_l.push_back(v);
    }

    // allocate tensors and initialize the buffers to avoid NaNs in the padding
    for (auto it : ctx_map)
    {
        ggml_backend_buffer_type_t buft = it.first;
        ggml_context * ctx = it.second;
        ggml_backend_buffer_t buf = ggml_backend_alloc_ctx_tensors_from_buft(ctx, buft);
        if (!buf)
        {
            LLAMA_LOG_ERROR("%s: failed to allocate buffer for kv cache\n", __func__);
            return false;
        }
        ggml_backend_buffer_clear(buf, 0);
        LLAMA_LOG_INFO(
            "%s: %10s KV buffer size = %8.2f MiB\n",
            __func__,
            ggml_backend_buffer_name(buf),
            ggml_backend_buffer_get_size(buf) / 1024.0 / 1024.0);
        cache.bufs.push_back(buf);
    }

    return true;
}

// find an empty slot of size "n_tokens" in the cache
// updates the cache head
// Note: On success, it's important that cache.head points
// to the first cell of the slot.
static bool llama_kv_cache_find_slot(struct llama_kv_cache & cache, const struct llama_batch & batch)
{
    const uint32_t n_tokens = batch.n_tokens;

    if (cache.recurrent)
    {
        // For recurrent state architectures (like Mamba),
        // each KV cache cell can store the state for a whole sequence.

        llama_seq_id min = cache.size - 1;
        llama_seq_id max = 0;

        for (uint32_t i = 0; i < n_tokens; ++i)
        {
            for (int32_t j = 0; j < batch.n_seq_id[i]; ++j)
            {
                llama_seq_id seq_id = batch.seq_id[i][j];
                // make sure it's a valid seq_id
                if ((uint32_t)seq_id < cache.size)
                {
                    if (seq_id > max)
                    {
                        max = seq_id;
                    }
                    if (seq_id < min)
                    {
                        min = seq_id;
                    }
                    // Assuming the tokens are in-order
                    if (batch.pos[i] != cache.cells[seq_id].pos + 1)
                    {
                        // What should happen when the pos backtracks or skips a value?
                        // Clearing the state mid-batch would require special-casing which isn't done.
                        LLAMA_LOG_WARN(
                            "%s: non-consecutive token position %d after %d for sequence %d\n",
                            __func__,
                            batch.pos[i],
                            cache.cells[seq_id].pos,
                            seq_id);
                    }
                    if (cache.cells[seq_id].pos < 0 && 0 <= batch.pos[i])
                    {
                        cache.used += 1;
                    }
                    cache.cells[seq_id].pos = batch.pos[i];
                    // NOTE: seq_ids are not inserted here; they are handled when the input tensors are set
                }
                else
                {
                    // too big seq_id
                    // TODO: would it be possible to resize the KV cache size instead?
                    LLAMA_LOG_ERROR("%s: seq_id=%d >= kv_size=%d Try using a bigger --parallel value\n", __func__, seq_id, cache.size);
                    return false;
                }
            }
        }

        // allow getting the range of used cells, from head to head + n
        cache.head = min;
        cache.n = max - min + 1;

        // sanity check
        return max >= min;
    }
    // otherwise, one cell per token.

    if (n_tokens > cache.size)
    {
        LLAMA_LOG_ERROR("%s: n_tokens=%d > cache.size=%d\n", __func__, n_tokens, cache.size);
        return false;
    }

    uint32_t n_tested = 0;

    while (true)
    {
        if (cache.head + n_tokens > cache.size)
        {
            n_tested += cache.size - cache.head;
            cache.head = 0;
            continue;
        }

        bool found = true;
        for (uint32_t i = 0; i < n_tokens; i++)
        {
            if (cache.cells[cache.head + i].pos >= 0)
            {
                found = false;
                cache.head += i + 1;
                n_tested += i + 1;
                break;
            }
        }

        if (found)
        {
            break;
        }

        if (n_tested >= cache.size)
        {
            //LLAMA_LOG_ERROR("%s: failed to find a slot for %d tokens\n", __func__, n_tokens);
            return false;
        }
    }

    for (uint32_t i = 0; i < n_tokens; i++)
    {
        cache.cells[cache.head + i].pos = batch.pos[i];

        for (int32_t j = 0; j < batch.n_seq_id[i]; j++)
        {
            cache.cells[cache.head + i].seq_id.insert(batch.seq_id[i][j]);
        }
    }

    cache.used += n_tokens;

    return true;
}

// find how many cells are currently in use
static uint32_t llama_kv_cache_cell_max(const struct llama_kv_cache & cache)
{
    for (uint32_t i = cache.size; i > 0; --i)
    {
        const llama_kv_cell & cell = cache.cells[i - 1];

        if (cell.pos >= 0 && !cell.is_empty())
        {
            return i;
        }
    }

    return 0;
}

static void llama_kv_cache_clear(struct llama_kv_cache & cache)
{
    for (int32_t i = 0; i < (int32_t)cache.size; ++i)
    {
        cache.cells[i].pos = -1;
        cache.cells[i].seq_id.clear();
    }
    cache.head = 0;
    cache.used = 0;

    for (auto & buf : cache.bufs)
    {
        ggml_backend_buffer_clear(buf, 0);
    }
}

static bool llama_kv_cache_seq_rm(struct llama_kv_cache & cache, llama_seq_id seq_id, llama_pos p0, llama_pos p1)
{
    uint32_t new_head = cache.size;

    if (p0 < 0)
        p0 = 0;
    if (p1 < 0)
        p1 = std::numeric_limits<llama_pos>::max();

    // models like Mamba can't have a state partially erased
    if (cache.recurrent)
    {
        if (seq_id >= (int64_t)cache.size)
        {
            // could be fatal
            return false;
        }
        if (0 <= seq_id)
        {
            // partial intersection is invalid
            if ((0 < p0 && p0 <= cache.cells[seq_id].pos) || (0 < p1 && p1 <= cache.cells[seq_id].pos))
            {
                return false;
            }
        }
        else
        {
            // seq_id is negative, then the range should include everything or nothing
            if (p0 != p1 && (p0 != 0 || p1 != std::numeric_limits<llama_pos>::max()))
            {
                return false;
            }
        }
    }

    for (uint32_t i = 0; i < cache.size; ++i)
    {
        if (cache.cells[i].pos >= p0 && cache.cells[i].pos < p1)
        {
            if (seq_id < 0)
            {
                cache.cells[i].seq_id.clear();
            }
            else if (cache.cells[i].has_seq_id(seq_id))
            {
                cache.cells[i].seq_id.erase(seq_id);
            }
            else
            {
                continue;
            }
            if (cache.cells[i].is_empty())
            {
                // keep count of the number of used cells
                if (cache.cells[i].pos >= 0)
                    cache.used--;

                cache.cells[i].pos = -1;
                if (new_head == cache.size)
                    new_head = i;
            }
        }
    }

    // If we freed up a slot, set head to it so searching can start there.
    if (new_head != cache.size && new_head < cache.head)
        cache.head = new_head;

    return true;
}

static void
llama_kv_cache_seq_cp(struct llama_kv_cache & cache, llama_seq_id seq_id_src, llama_seq_id seq_id_dst, llama_pos p0, llama_pos p1)
{
    if (p0 < 0)
        p0 = 0;
    if (p1 < 0)
        p1 = std::numeric_limits<llama_pos>::max();

    if (cache.recurrent)
    {
        if ((uint32_t)seq_id_dst < cache.size && (uint32_t)seq_id_src < cache.size)
        {
            seq_id_src = cache.cells[seq_id_src].src;
            GGML_ASSERT((uint32_t)seq_id_src < cache.size);
            // intent to "copy from"
            // supports copy chains thanks to taking the source of the source
            cache.cells[seq_id_dst].src = seq_id_src;

            // preserve the "keep or clear" status of the copied sequence
            if (cache.cells[seq_id_src].has_seq_id(seq_id_src))
            {
                cache.cells[seq_id_dst].seq_id.insert(seq_id_dst);
            }
            else
            {
                cache.cells[seq_id_dst].seq_id.erase(seq_id_dst);
            }

            cache.do_copy = true;

            cache.cells[seq_id_dst].pos = cache.cells[seq_id_src].pos;
        }
        return;
    }
    // otherwise, this is the KV cache of a Transformer-like model

    cache.head = 0;

    for (uint32_t i = 0; i < cache.size; ++i)
    {
        if (cache.cells[i].has_seq_id(seq_id_src) && cache.cells[i].pos >= p0 && cache.cells[i].pos < p1)
        {
            cache.cells[i].seq_id.insert(seq_id_dst);
        }
    }
}

static void llama_kv_cache_seq_keep(struct llama_kv_cache & cache, llama_seq_id seq_id)
{
    uint32_t new_head = cache.size;

    for (uint32_t i = 0; i < cache.size; ++i)
    {
        if (!cache.cells[i].has_seq_id(seq_id))
        {
            if (cache.cells[i].pos >= 0)
                cache.used--;
            cache.cells[i].pos = -1;
            cache.cells[i].seq_id.clear();
            if (new_head == cache.size)
                new_head = i;
        }
        else
        {
            cache.cells[i].seq_id.clear();
            cache.cells[i].seq_id.insert(seq_id);
        }
    }

    // If we freed up a slot, set head to it so searching can start there.
    if (new_head != cache.size && new_head < cache.head)
        cache.head = new_head;
}

static void llama_kv_cache_seq_add(struct llama_kv_cache & cache, llama_seq_id seq_id, llama_pos p0, llama_pos p1, llama_pos delta)
{
    uint32_t new_head = cache.size;

    if (p0 < 0)
        p0 = 0;
    if (p1 < 0)
        p1 = std::numeric_limits<llama_pos>::max();

    if (cache.recurrent)
    {
        // for Mamba-like models, only the pos needs to be shifted
        if (0 <= seq_id && seq_id < (int64_t)cache.size)
        {
            llama_kv_cell & cell = cache.cells[seq_id];
            if (cell.has_seq_id(seq_id) && p0 <= cell.pos && cell.pos < p1)
            {
                cell.pos += delta;
            }
        }
        return;
    }

    for (uint32_t i = 0; i < cache.size; ++i)
    {
        if (cache.cells[i].has_seq_id(seq_id) && cache.cells[i].pos >= p0 && cache.cells[i].pos < p1)
        {
            cache.has_shift = true;
            cache.cells[i].pos += delta;
            cache.cells[i].delta += delta;

            if (cache.cells[i].pos < 0)
            {
                if (!cache.cells[i].is_empty())
                {
                    cache.used--;
                }
                cache.cells[i].pos = -1;
                cache.cells[i].seq_id.clear();
                if (new_head == cache.size)
                {
                    new_head = i;
                }
            }
        }
    }

    // If we freed up a slot, set head to it so searching can start there.
    // Otherwise we just start the next search from the beginning.
    cache.head = new_head != cache.size ? new_head : 0;
}

static void llama_kv_cache_seq_div(struct llama_kv_cache & cache, llama_seq_id seq_id, llama_pos p0, llama_pos p1, int d)
{
    if (p0 < 0)
        p0 = 0;
    if (p1 < 0)
        p1 = std::numeric_limits<llama_pos>::max();

    if (cache.recurrent)
    {
        // for Mamba-like models, only the pos needs to be changed
        if (0 <= seq_id && seq_id < (int64_t)cache.size)
        {
            llama_kv_cell & cell = cache.cells[seq_id];
            if (cell.has_seq_id(seq_id) && p0 <= cell.pos && cell.pos < p1)
            {
                cell.pos /= d;
            }
        }
        return;
    }

    for (uint32_t i = 0; i < cache.size; ++i)
    {
        if (cache.cells[i].has_seq_id(seq_id) && cache.cells[i].pos >= p0 && cache.cells[i].pos < p1)
        {
            cache.has_shift = true;

            {
                llama_pos p_old = cache.cells[i].pos;
                cache.cells[i].pos /= d;
                cache.cells[i].delta += cache.cells[i].pos - p_old;
            }
        }
    }
}

static llama_pos llama_kv_cache_seq_pos_max(struct llama_kv_cache & cache, llama_seq_id seq_id)
{
    llama_pos result = 0;

    for (uint32_t i = 0; i < cache.size; ++i)
    {
        if (cache.cells[i].has_seq_id(seq_id))
        {
            result = std::max(result, cache.cells[i].pos);
        }
    }

    return result;
}

static void llama_kv_cache_defrag(struct llama_kv_cache & cache)
{
    cache.do_defrag = true;
}

static uint32_t llama_kv_cache_get_padding(const struct llama_cparams & cparams)
{
    // the FA kernels require padding to avoid extra runtime boundary checks
    return cparams.flash_attn ? 256u : 32u;
}

//
// model loading and saving
//

enum llama_fver
{
    GGUF_FILE_VERSION_V1 = 1,
    GGUF_FILE_VERSION_V2 = 2,
    GGUF_FILE_VERSION_V3 = 3,
};

static const char * llama_file_version_name(llama_fver version)
{
    switch (version)
    {
        case GGUF_FILE_VERSION_V1:
            return "GGUF V1 (support until nov 2023)";
        case GGUF_FILE_VERSION_V2:
            return "GGUF V2";
        case GGUF_FILE_VERSION_V3:
            return "GGUF V3 (latest)";
    }

    return "unknown";
}

static std::string llama_format_tensor_shape(const std::vector<int64_t> & ne)
{
    char buf[256];
    snprintf(buf, sizeof(buf), "%5" PRId64, ne.at(0));
    for (size_t i = 1; i < ne.size(); i++)
    {
        snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), ", %5" PRId64, ne.at(i));
    }
    return buf;
}

static std::string llama_format_tensor_shape(const struct ggml_tensor * t)
{
    char buf[256];
    snprintf(buf, sizeof(buf), "%5" PRId64, t->ne[0]);
    for (int i = 1; i < GGML_MAX_DIMS; i++)
    {
        snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), ", %5" PRId64, t->ne[i]);
    }
    return buf;
}

namespace GGUFMeta
{
template <typename T, gguf_type gt_, T (*gfun)(const gguf_context *, const int)>
struct GKV_Base_Type
{
    static constexpr gguf_type gt = gt_;

    static T getter(const gguf_context * ctx, const int kid) { return gfun(ctx, kid); }
};

template <typename T>
struct GKV_Base;

template <>
struct GKV_Base<bool> : GKV_Base_Type<bool, GGUF_TYPE_BOOL, gguf_get_val_bool>
{
};
template <>
struct GKV_Base<uint8_t> : GKV_Base_Type<uint8_t, GGUF_TYPE_UINT8, gguf_get_val_u8>
{
};
template <>
struct GKV_Base<uint16_t> : GKV_Base_Type<uint16_t, GGUF_TYPE_UINT16, gguf_get_val_u16>
{
};
template <>
struct GKV_Base<uint32_t> : GKV_Base_Type<uint32_t, GGUF_TYPE_UINT32, gguf_get_val_u32>
{
};
template <>
struct GKV_Base<uint64_t> : GKV_Base_Type<uint64_t, GGUF_TYPE_UINT64, gguf_get_val_u64>
{
};
template <>
struct GKV_Base<int8_t> : GKV_Base_Type<int8_t, GGUF_TYPE_INT8, gguf_get_val_i8>
{
};
template <>
struct GKV_Base<int16_t> : GKV_Base_Type<int16_t, GGUF_TYPE_INT16, gguf_get_val_i16>
{
};
template <>
struct GKV_Base<int32_t> : GKV_Base_Type<int32_t, GGUF_TYPE_INT32, gguf_get_val_i32>
{
};
template <>
struct GKV_Base<int64_t> : GKV_Base_Type<int64_t, GGUF_TYPE_INT64, gguf_get_val_i64>
{
};
template <>
struct GKV_Base<float> : GKV_Base_Type<float, GGUF_TYPE_FLOAT32, gguf_get_val_f32>
{
};
template <>
struct GKV_Base<double> : GKV_Base_Type<double, GGUF_TYPE_FLOAT64, gguf_get_val_f64>
{
};
template <>
struct GKV_Base<const char *> : GKV_Base_Type<const char *, GGUF_TYPE_STRING, gguf_get_val_str>
{
};

template <>
struct GKV_Base<std::string>
{
    static constexpr gguf_type gt = GGUF_TYPE_STRING;

    static std::string getter(const gguf_context * ctx, const int kid) { return gguf_get_val_str(ctx, kid); }
};

struct ArrayInfo
{
    const gguf_type gt;
    const size_t length;
    const void * data;
};

template <>
struct GKV_Base<ArrayInfo>
{
public:
    static constexpr gguf_type gt = GGUF_TYPE_ARRAY;
    static ArrayInfo getter(const gguf_context * ctx, const int k)
    {
        return ArrayInfo{
            gguf_get_arr_type(ctx, k),
            size_t(gguf_get_arr_n(ctx, k)),
            gguf_get_arr_data(ctx, k),
        };
    }
};

template <typename T>
class GKV : public GKV_Base<T>
{
    GKV() = delete;

public:
    static T get_kv(const gguf_context * ctx, const int k)
    {
        const enum gguf_type kt = gguf_get_kv_type(ctx, k);

        if (kt != GKV::gt)
        {
            throw std::runtime_error(
                format("key %s has wrong type %s but expected type %s", gguf_get_key(ctx, k), gguf_type_name(kt), gguf_type_name(GKV::gt)));
        }
        return GKV::getter(ctx, k);
    }

    static const char * override_type_to_str(const llama_model_kv_override_type ty)
    {
        switch (ty)
        {
            case LLAMA_KV_OVERRIDE_TYPE_BOOL:
                return "bool";
            case LLAMA_KV_OVERRIDE_TYPE_INT:
                return "int";
            case LLAMA_KV_OVERRIDE_TYPE_FLOAT:
                return "float";
            case LLAMA_KV_OVERRIDE_TYPE_STR:
                return "str";
        }
        return "unknown";
    }

    static bool validate_override(const llama_model_kv_override_type expected_type, const struct llama_model_kv_override * ovrd)
    {
        if (!ovrd)
        {
            return false;
        }
        if (ovrd->tag == expected_type)
        {
            LLAMA_LOG_INFO("%s: Using metadata override (%5s) '%s' = ", __func__, override_type_to_str(ovrd->tag), ovrd->key);
            switch (ovrd->tag)
            {
                case LLAMA_KV_OVERRIDE_TYPE_BOOL: {
                    LLAMA_LOG_INFO("%s\n", ovrd->val_bool ? "true" : "false");
                }
                break;
                case LLAMA_KV_OVERRIDE_TYPE_INT: {
                    LLAMA_LOG_INFO("%" PRId64 "\n", ovrd->val_i64);
                }
                break;
                case LLAMA_KV_OVERRIDE_TYPE_FLOAT: {
                    LLAMA_LOG_INFO("%.6f\n", ovrd->val_f64);
                }
                break;
                case LLAMA_KV_OVERRIDE_TYPE_STR: {
                    LLAMA_LOG_INFO("%s\n", ovrd->val_str);
                }
                break;
                default:
                    // Shouldn't be possible to end up here, but just in case...
                    throw std::runtime_error(format(
                        "Unsupported attempt to override %s type for metadata key %s\n", override_type_to_str(ovrd->tag), ovrd->key));
            }
            return true;
        }
        LLAMA_LOG_WARN(
            "%s: Warning: Bad metadata override type for key '%s', expected %s but got %s\n",
            __func__,
            ovrd->key,
            override_type_to_str(expected_type),
            override_type_to_str(ovrd->tag));
        return false;
    }

    template <typename OT>
    static typename std::enable_if<std::is_same<OT, bool>::value, bool>::type
    try_override(OT & target, const struct llama_model_kv_override * ovrd)
    {
        if (validate_override(LLAMA_KV_OVERRIDE_TYPE_BOOL, ovrd))
        {
            target = ovrd->val_bool;
            return true;
        }
        return false;
    }

    template <typename OT>
    static typename std::enable_if<!std::is_same<OT, bool>::value && std::is_integral<OT>::value, bool>::type
    try_override(OT & target, const struct llama_model_kv_override * ovrd)
    {
        if (validate_override(LLAMA_KV_OVERRIDE_TYPE_INT, ovrd))
        {
            target = ovrd->val_i64;
            return true;
        }
        return false;
    }

    template <typename OT>
    static typename std::enable_if<std::is_floating_point<OT>::value, bool>::type
    try_override(T & target, const struct llama_model_kv_override * ovrd)
    {
        if (validate_override(LLAMA_KV_OVERRIDE_TYPE_FLOAT, ovrd))
        {
            target = ovrd->val_f64;
            return true;
        }
        return false;
    }

    template <typename OT>
    static typename std::enable_if<std::is_same<OT, std::string>::value, bool>::type
    try_override(T & target, const struct llama_model_kv_override * ovrd)
    {
        if (validate_override(LLAMA_KV_OVERRIDE_TYPE_STR, ovrd))
        {
            target = ovrd->val_str;
            return true;
        }
        return false;
    }

    static bool set(const gguf_context * ctx, const int k, T & target, const struct llama_model_kv_override * ovrd = nullptr)
    {
        if (try_override<T>(target, ovrd))
        {
            return true;
        }
        if (k < 0)
        {
            return false;
        }
        target = get_kv(ctx, k);
        return true;
    }

    static bool set(const gguf_context * ctx, const char * key, T & target, const struct llama_model_kv_override * ovrd = nullptr)
    {
        return set(ctx, gguf_find_key(ctx, key), target, ovrd);
    }

    static bool set(const gguf_context * ctx, const std::string & key, T & target, const struct llama_model_kv_override * ovrd = nullptr)
    {
        return set(ctx, key.c_str(), target, ovrd);
    }
};
}

using llama_buf_map = std::unordered_map<uint32_t, ggml_backend_buffer_t>;

struct llama_model_loader
{
    int n_kv = 0;
    int n_tensors = 0;
    int n_created = 0;

    int64_t n_elements = 0;
    size_t n_bytes = 0;

    bool use_mmap = false;
    bool check_tensors;

    llama_files files;
    llama_ftype ftype;
    llama_fver fver;

    llama_mmaps mappings;

    // Holds information on a model weight
    struct llama_tensor_weight
    {
        uint16_t idx; // source file index
        size_t offs; // tensor data offset in the original file

        ggml_tensor * tensor;

        llama_tensor_weight(
            const llama_file * file, uint16_t idx, const char * name, const struct gguf_context * gguf_ctx, ggml_tensor * tensor)
            : idx(idx), tensor(tensor)
        {
            const int tensor_idx = gguf_find_tensor(gguf_ctx, name);
            offs = gguf_get_data_offset(gguf_ctx) + gguf_get_tensor_offset(gguf_ctx, tensor_idx);

            if (offs + ggml_nbytes(tensor) < offs || offs + ggml_nbytes(tensor) > file->size)
            {
                throw std::runtime_error(format("tensor '%s' data is not within the file bounds, model is corrupted or incomplete", name));
            }
        }
    };
    std::vector<llama_tensor_weight> weights;

    std::unordered_map<std::string, struct llama_model_kv_override> kv_overrides;

    struct gguf_context * meta = nullptr;
    std::vector<ggml_context *> contexts;

    std::string arch_name;
    LLM_KV llm_kv = LLM_KV(LLM_ARCH_UNKNOWN);

    llama_model_loader(
        const std::string & fname, bool use_mmap, bool check_tensors, const struct llama_model_kv_override * param_overrides_p)
    {
        int trace = 0;
        if (getenv("LLAMA_TRACE"))
        {
            trace = atoi(getenv("LLAMA_TRACE"));
        }

        if (param_overrides_p != nullptr)
        {
            for (const struct llama_model_kv_override * p = param_overrides_p; p->key[0] != 0; p++)
            {
                kv_overrides.insert({std::string(p->key), *p});
            }
        }

        struct ggml_context * ctx = nullptr;
        gguf_init_params params = {
            .no_alloc = true,
            .ctx      = &ctx,
        };

        meta = gguf_init_from_file(fname.c_str(), params);
        if (!meta)
        {
            throw std::runtime_error(format("%s: failed to load model from %s\n", __func__, fname.c_str()));
        }

        get_key(llm_kv(LLM_KV_GENERAL_ARCHITECTURE), arch_name, false);
        llm_kv = LLM_KV(llm_arch_from_string(arch_name));

        files.emplace_back(new llama_file(fname.c_str(), "rb"));
        contexts.emplace_back(ctx);

        // Save tensors data offset of the main file.
        // For subsidiary files, `meta` tensor data offset must not be used,
        // so we build a unified tensors index for weights.
        for (ggml_tensor * cur = ggml_get_first_tensor(ctx); cur; cur = ggml_get_next_tensor(ctx, cur))
        {
            weights.emplace_back(files.back().get(), 0, cur->name, meta, cur);
        }
        uint16_t n_split = 0;
        get_key(llm_kv(LLM_KV_SPLIT_COUNT), n_split, false);

        // Load additional GGML contexts
        if (n_split > 1)
        {
            uint16_t idx = 0;
            get_key(llm_kv(LLM_KV_SPLIT_NO), idx);
            if (idx != 0)
            {
                throw std::runtime_error(format("illegal split file: %d, model must be loaded with the first split", idx));
            }

            char split_prefix[PATH_MAX] = {0};
            if (!llama_split_prefix(split_prefix, sizeof(split_prefix), fname.c_str(), idx, n_split))
            {
                throw std::runtime_error(format("invalid split file: %s", fname.c_str()));
            }

            if (trace > 0)
            {
                LLAMA_LOG_INFO("%s: loading additional %d GGUFs\n", __func__, n_split);
            }

            char split_path[PATH_MAX] = {0};
            for (idx = 1; idx < n_split; idx++)
            {
                llama_split_path(split_path, sizeof(split_path), split_prefix, idx, n_split);

                gguf_init_params split_params = {
                    .no_alloc = true,
                    .ctx      = &ctx,
                };
                struct gguf_context * ctx_gguf = gguf_init_from_file(split_path, split_params);
                if (!ctx_gguf)
                {
                    throw std::runtime_error(format("%s: failed to load GGUF split from %s\n", __func__, split_path));
                }

                files.emplace_back(new llama_file(split_path, "rb"));
                contexts.emplace_back(ctx);

                // Save tensors data offset info of the shard.
                for (ggml_tensor * cur = ggml_get_first_tensor(ctx); cur; cur = ggml_get_next_tensor(ctx, cur))
                {
                    weights.emplace_back(files.back().get(), idx, cur->name, ctx_gguf, cur);
                }

                gguf_free(ctx_gguf);
            }

            get_key(llm_kv(LLM_KV_SPLIT_TENSORS_COUNT), n_tensors);

            // sanity check
            {
                const int n_tensors_loaded = (int)weights.size();
                if (n_tensors != n_tensors_loaded)
                {
                    throw std::runtime_error(format("corrupted model: %d tensors expected but %d found", n_tensors, n_tensors_loaded));
                }
            }

            LLAMA_LOG_INFO("%s: additional %d GGUFs metadata loaded.\n", __func__, n_split - 1);
        }

        n_kv = gguf_get_n_kv(meta);
        n_tensors = weights.size();

        fver = (enum llama_fver)gguf_get_version(meta);

        std::set<std::string> tensor_names;
        for (auto & w : weights)
        {
            n_elements += ggml_nelements(w.tensor);
            n_bytes += ggml_nbytes(w.tensor);
            // make sure there is no duplicated tensor names
            const std::string name(w.tensor->name);
            auto found = tensor_names.find(name);
            if (found != tensor_names.end())
            {
                throw std::runtime_error(format("invalid model: tensor '%s' is duplicated", w.tensor->name));
            }
            tensor_names.insert(name);
        }

        LLAMA_LOG_INFO(
            "%s: loaded meta data with %d key-value pairs and %d tensors from %s (version %s)\n",
            __func__,
            n_kv,
            n_tensors,
            fname.c_str(),
            llama_file_version_name(fver));

        // determine file type based on the number of tensors for each quantization and print meta data
        // TODO: make optional
        {
            std::map<enum ggml_type, uint32_t> n_type;

            uint32_t n_type_max = 0;
            enum ggml_type type_max = GGML_TYPE_F32;

            for (int i = 0; i < n_tensors; i++)
            {
                const ggml_tensor * tensor = weights.at(i).tensor;
                enum ggml_type type = tensor->type;

                n_type[type]++;

                if (n_type_max < n_type[type])
                {
                    n_type_max = n_type[type];
                    type_max = type;
                }

                if (trace > 0)
                {
                    const uint16_t sid = weights.at(i).idx;
                    LLAMA_LOG_INFO(
                        "%s: - tensor %4d, split %2d: %32s %-8s [ %s ]\n",
                        __func__,
                        i,
                        sid,
                        ggml_get_name(tensor),
                        ggml_type_name(type),
                        llama_format_tensor_shape(tensor).c_str());
                }
            }

            switch (type_max)
            {
                case GGML_TYPE_F32:
                    ftype = LLAMA_FTYPE_ALL_F32;
                    break;
                case GGML_TYPE_F16:
                    ftype = LLAMA_FTYPE_MOSTLY_F16;
                    break;
                // case GGML_TYPE_BF16:    ftype = LLAMA_FTYPE_MOSTLY_BF16;    break;
                case GGML_TYPE_Q4_0:
                    ftype = LLAMA_FTYPE_MOSTLY_Q4_0;
                    break;
                case GGML_TYPE_Q4_1:
                    ftype = LLAMA_FTYPE_MOSTLY_Q4_1;
                    break;
                case GGML_TYPE_Q5_0:
                    ftype = LLAMA_FTYPE_MOSTLY_Q5_0;
                    break;
                case GGML_TYPE_Q5_1:
                    ftype = LLAMA_FTYPE_MOSTLY_Q5_1;
                    break;
                case GGML_TYPE_Q8_0:
                    ftype = LLAMA_FTYPE_MOSTLY_Q8_0;
                    break;
                case GGML_TYPE_Q2_K:
                    ftype = LLAMA_FTYPE_MOSTLY_Q2_K;
                    break;
                case GGML_TYPE_Q3_K:
                    ftype = LLAMA_FTYPE_MOSTLY_Q3_K_M;
                    break;
                case GGML_TYPE_Q4_K:
                    ftype = LLAMA_FTYPE_MOSTLY_Q4_K_M;
                    break;
                case GGML_TYPE_Q5_K:
                    ftype = LLAMA_FTYPE_MOSTLY_Q5_K_M;
                    break;
                case GGML_TYPE_Q6_K:
                    ftype = LLAMA_FTYPE_MOSTLY_Q6_K;
                    break;
                case GGML_TYPE_IQ2_XXS:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ2_XXS;
                    break;
                case GGML_TYPE_IQ2_XS:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ2_XS;
                    break;
                case GGML_TYPE_IQ2_S:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ2_S;
                    break;
                case GGML_TYPE_IQ3_XXS:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ3_XXS;
                    break;
                case GGML_TYPE_IQ1_S:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ1_S;
                    break;
                case GGML_TYPE_IQ1_M:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ1_M;
                    break;
                case GGML_TYPE_IQ4_NL:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ4_NL;
                    break;
                case GGML_TYPE_IQ4_XS:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ4_XS;
                    break;
                case GGML_TYPE_IQ3_S:
                    ftype = LLAMA_FTYPE_MOSTLY_IQ3_S;
                    break;
                default: {
                    LLAMA_LOG_WARN("%s: unknown type %s\n", __func__, ggml_type_name(type_max));
                    ftype = LLAMA_FTYPE_ALL_F32;
                }
                break;
            }

            // this is a way to mark that we have "guessed" the file type
            ftype = (llama_ftype)(ftype | LLAMA_FTYPE_GUESSED);

            {
                const int kid = gguf_find_key(meta, "general.file_type");
                if (kid >= 0)
                {
                    ftype = (llama_ftype)gguf_get_val_u32(meta, kid);
                }
            }

            LLAMA_LOG_INFO("%s: Dumping metadata keys/values. Note: KV overrides do not apply in this output.\n", __func__);

            for (int i = 0; i < n_kv; i++)
            {
                const char * name = gguf_get_key(meta, i);
                const enum gguf_type type = gguf_get_kv_type(meta, i);
                const std::string type_name = type == GGUF_TYPE_ARRAY
                    ? format("%s[%s,%d]", gguf_type_name(type), gguf_type_name(gguf_get_arr_type(meta, i)), gguf_get_arr_n(meta, i))
                    : gguf_type_name(type);

                std::string value = gguf_kv_to_str(meta, i);
                const size_t MAX_VALUE_LEN = 40;
                if (value.size() > MAX_VALUE_LEN)
                {
                    value = format("%s...", value.substr(0, MAX_VALUE_LEN - 3).c_str());
                }
                replace_all(value, "\n", "\\n");

                LLAMA_LOG_INFO("%s: - kv %3d: %42s %-16s = %s\n", __func__, i, name, type_name.c_str(), value.c_str());
            }

            // print type counts
            for (auto & kv : n_type)
            {
                if (kv.second == 0)
                {
                    continue;
                }

                LLAMA_LOG_INFO("%s: - type %4s: %4d tensors\n", __func__, ggml_type_name(kv.first), kv.second);
            }
        }

        if (!llama_mmap::SUPPORTED)
        {
            LLAMA_LOG_WARN("%s: mmap is not supported on this platform\n", __func__);
            use_mmap = false;
        }

        this->use_mmap = use_mmap;
        this->check_tensors = check_tensors;
    }

    ~llama_model_loader()
    {
        if (meta)
        {
            gguf_free(meta);
        }
        for (auto * ctx : contexts)
        {
            ggml_free(ctx);
        }
    }

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value, bool>::type
    get_arr_n(const std::string & key, T & result, const bool required = true)
    {
        const int kid = gguf_find_key(meta, key.c_str());

        if (kid < 0)
        {
            if (required)
            {
                throw std::runtime_error(format("key not found in model: %s", key.c_str()));
            }
            return false;
        }

        struct GGUFMeta::ArrayInfo arr_info = GGUFMeta::GKV<GGUFMeta::ArrayInfo>::get_kv(meta, kid);


        result = arr_info.length;
        return true;
    }

    template <typename T>
    typename std::enable_if<std::is_integral<T>::value, bool>::type get_arr_n(const enum llm_kv kid, T & result, const bool required = true)
    {
        return get_arr_n(llm_kv(kid), result, required);
    }

    template <typename T>
    bool get_arr(const std::string & key, std::vector<T> & result, const bool required = true)
    {
        const int kid = gguf_find_key(meta, key.c_str());

        if (kid < 0)
        {
            if (required)
            {
                throw std::runtime_error(format("key not found in model: %s", key.c_str()));
            }
            return false;
        }

        struct GGUFMeta::ArrayInfo arr_info = GGUFMeta::GKV<GGUFMeta::ArrayInfo>::get_kv(meta, kid);

        if (arr_info.gt != GGUF_TYPE_FLOAT32 && arr_info.gt != GGUF_TYPE_INT32)
        {
            throw std::runtime_error(format("%s is not a float32 or int32 array", key.c_str()));
        }

        // GGML_ASSERT(gguf_type_size(arr_info.gt) == sizeof(T));
        GGML_ASSERT((arr_info.gt != GGUF_TYPE_FLOAT32 || std::is_same<T, float>::value));
        GGML_ASSERT((arr_info.gt != GGUF_TYPE_INT32 || std::is_same<T, int>::value));

        result.resize(arr_info.length);
        result.assign((const T *)arr_info.data, (const T *)arr_info.data + arr_info.length);

        return true;
    }

    template <typename T>
    bool get_arr(const enum llm_kv kid, T & result, const bool required = true)
    {
        return get_arr(llm_kv(kid), result, required);
    }

    template <typename T>
    bool get_key(const std::string & key, T & result, const bool required = true)
    {
        auto it = kv_overrides.find(key);

        const struct llama_model_kv_override * override = it != kv_overrides.end() ? &it->second : nullptr;

        const bool found = GGUFMeta::GKV<T>::set(meta, key, result, override);

        if (required && !found)
        {
            throw std::runtime_error(format("key not found in model: %s", key.c_str()));
        }

        return found;
    }

    template <typename T>
    bool get_key(const enum llm_kv kid, T & result, const bool required = true)
    {
        return get_key(llm_kv(kid), result, required);
    }

    std::string get_arch_name() const { return arch_name; }

    enum llm_arch get_arch() const { return llm_kv.arch; }

    const char * get_tensor_name(int i) const { return weights.at(i).tensor->name; }

    const llama_tensor_weight * get_weight(const char * name) const
    {
        for (const auto & weight : weights)
        {
            if (strcmp(name, weight.tensor->name) == 0)
            {
                return &weight;
            }
        }
        return nullptr;
    }

    const llama_tensor_weight * get_weight(int i) const { return get_weight(get_tensor_name(i)); }

    const llama_tensor_weight & require_weight(const char * name) const
    {
        const llama_tensor_weight * weight = get_weight(name);
        if (!weight)
        {
            throw std::runtime_error(format("%s: tensor '%s' not found", __func__, name));
        }
        return *weight;
    }

    struct ggml_tensor * get_tensor_meta(const char * name) const
    {
        const auto * weight = get_weight(name);
        if (!weight)
        {
            return nullptr;
        }
        return weight->tensor;
    }

    struct ggml_tensor * require_tensor_meta(const char * name) const
    {
        struct ggml_tensor * tensor = get_tensor_meta(name);
        if (!tensor)
        {
            throw std::runtime_error(format("%s: tensor '%s' not found", __func__, name));
        }
        return tensor;
    }

    struct ggml_tensor * get_tensor_meta(int i) const { return get_tensor_meta(get_tensor_name(i)); }

    struct ggml_tensor * create_tensor_for(struct ggml_context * ctx, const struct ggml_tensor * cur, bool duplicated)
    {
        struct ggml_tensor * tensor = ggml_dup_tensor(ctx, cur);
        ggml_set_name(tensor, ggml_get_name(cur));

        if (duplicated)
        {
            size_data += ggml_nbytes(cur);
        }
        else
        {
            n_created++;
        }

        return tensor;
    }

    const struct ggml_tensor * check_tensor_dims(const std::string & name, const std::vector<int64_t> & ne, bool required) const
    {
        const struct ggml_tensor * cur = get_tensor_meta(name.c_str());

        if (cur == nullptr)
        {
            if (!required)
            {
                return nullptr;
            }
            throw std::runtime_error(format("%s: tensor '%s' not found", __func__, name.c_str()));
        }

        {
            bool is_ok = true;
            for (size_t i = 0; i < GGML_MAX_DIMS; ++i)
            {
                if ((i < ne.size() && ne[i] != cur->ne[i]) || (i >= ne.size() && cur->ne[i] != 1))
                {
                    is_ok = false;
                    break;
                }
            }
            if (!is_ok)
            {
                throw std::runtime_error(format(
                    "%s: tensor '%s' has wrong shape; expected %s, got %s",
                    __func__,
                    name.c_str(),
                    llama_format_tensor_shape(ne).c_str(),
                    llama_format_tensor_shape(cur).c_str()));
            }
        }

        return cur;
    }

    static const int TENSOR_NOT_REQUIRED = 1;
    static const int TENSOR_DUPLICATED = 2;

    struct ggml_tensor * create_tensor(struct ggml_context * ctx, const std::string & name, const std::vector<int64_t> & ne, int flags = 0)
    {
        const struct ggml_tensor * cur = check_tensor_dims(name, ne, !(flags & TENSOR_NOT_REQUIRED));

        if (cur == nullptr)
        {
            return nullptr;
        }

        return create_tensor_for(ctx, cur, flags & TENSOR_DUPLICATED);
    }

    struct ggml_tensor * create_tensor_as_view(
        struct ggml_context * ctx,
        struct ggml_tensor * base,
        const std::string & name,
        const std::vector<int64_t> & ne,
        size_t offset,
        bool required = true)
    {
        const struct ggml_tensor * cur = check_tensor_dims(name, ne, required);

        if (cur == nullptr)
        {
            return nullptr;
        }

        if (cur->type != base->type)
        {
            throw std::runtime_error(format(
                "%s: tensor '%s' has wrong type; expected %s, got %s",
                __func__,
                name.c_str(),
                ggml_type_name(base->type),
                ggml_type_name(cur->type)));
        }

        std::array<int64_t, GGML_MAX_DIMS> dims;
        for (size_t i = 0; i < GGML_MAX_DIMS; ++i)
        {
            dims[i] = i < ne.size() ? ne[i] : 1;
        }

        struct ggml_tensor * tensor
            = ggml_view_4d(ctx, base, dims[0], dims[1], dims[2], dims[3], cur->nb[1], cur->nb[2], cur->nb[3], offset);

        ggml_set_name(tensor, name.c_str());

        n_created++;

        return tensor;
    }

    void done_getting_tensors() const
    {
        if (n_created != n_tensors)
        {
            throw std::runtime_error(format("%s: wrong number of tensors; expected %d, got %d", __func__, n_tensors, n_created));
        }
    }

    void init_mappings(bool prefetch = true, llama_mlocks * mlock_mmaps = nullptr)
    {
        if (use_mmap)
        {
            mappings.reserve(files.size());
            mmaps_used.reserve(files.size());
            for (const auto & file : files)
            {
                std::unique_ptr<llama_mmap> mapping(new llama_mmap(file.get(), prefetch ? -1 : 0, ggml_is_numa()));
                mmaps_used.emplace_back(mapping->size, 0);
                if (mlock_mmaps)
                {
                    std::unique_ptr<llama_mlock> mlock_mmap(new llama_mlock());
                    mlock_mmap->init(mapping->addr);
                    mlock_mmaps->emplace_back(std::move(mlock_mmap));
                }
                mappings.emplace_back(std::move(mapping));
            }
        }

        // compute the total size of all tensors for progress reporting
        for (auto & w : weights)
        {
            size_data += ggml_nbytes(w.tensor);
        }
    }

    void get_mapping_range(size_t * first, size_t * last, void ** addr, int idx, ggml_context * ctx) const
    {
        GGML_ASSERT(!mappings.empty());
        const auto & mapping = mappings.at(idx);

        *first = mapping->size;
        *last = 0;
        *addr = mapping->addr;
        for (ggml_tensor * tensor = ggml_get_first_tensor(ctx); tensor; tensor = ggml_get_next_tensor(ctx, tensor))
        {
            try
            {
                const auto * weight = get_weight(ggml_get_name(tensor));
                if (!weight)
                {
                    continue;
                }
                if (weight->idx != idx)
                {
                    continue;
                }
                *first = std::min(*first, weight->offs);
                *last = std::max(*last, weight->offs + ggml_nbytes(tensor));
            }
            catch (...)
            {
                // the tensor is not in the model
            }
        }
    }

    // for backwards compatibility, does not support ggml-backend
    void load_data_for(struct ggml_tensor * cur) const
    {
        const auto & w = require_weight(ggml_get_name(cur));

        if (use_mmap)
        {
            const auto & mapping = mappings.at(w.idx);
            if (cur->data == nullptr)
            {
                cur->data = (uint8_t *)mapping->addr + w.offs;
            }
            else
            {
                memcpy(cur->data, (uint8_t *)mapping->addr + w.offs, ggml_nbytes(cur));
            }
        }
        else
        {
            GGML_ASSERT(cur->data != nullptr);
            GGML_ASSERT(w.idx < files.size());
            const auto & file = files.at(w.idx);
            file->seek(w.offs, SEEK_SET);
            file->read_raw(cur->data, ggml_nbytes(cur));
        }

        if (check_tensors && !ggml_validate_row_data(cur->type, cur->data, ggml_nbytes(cur)))
        {
            throw std::runtime_error(format("tensor '%s' has invalid data", ggml_get_name(cur)));
        }
    }

    size_t size_done = 0;
    size_t size_data = 0;
    std::vector<std::pair<size_t, size_t>> mmaps_used;

    // Returns false if cancelled by progress_callback
    bool load_all_data(
        struct ggml_context * ctx,
        llama_buf_map & bufs_mmap,
        llama_mlocks * lmlocks,
        llama_progress_callback progress_callback,
        void * progress_callback_user_data)
    {
        GGML_ASSERT(size_data != 0 && "call init_mappings() first");

        std::vector<no_init<uint8_t>> read_buf;
        std::vector<std::future<std::pair<ggml_tensor *, bool>>> validation_result;

        for (struct ggml_tensor * cur = ggml_get_first_tensor(ctx); cur != nullptr; cur = ggml_get_next_tensor(ctx, cur))
        {
            const auto * weight = get_weight(ggml_get_name(cur));
            if (weight == nullptr)
            {
                // this can happen with split experts models
                continue;
            }

            if (progress_callback)
            {
                if (!progress_callback((float)size_done / size_data, progress_callback_user_data))
                {
                    return false;
                }
            }

            size_t n_size = ggml_nbytes(cur);

            if (use_mmap)
            {
                const auto & mapping = mappings.at(weight->idx);
                ggml_backend_buffer_t buf_mmap = nullptr;
                if (bufs_mmap.count(weight->idx))
                {
                    buf_mmap = bufs_mmap.at(weight->idx);
                }
                uint8_t * data = (uint8_t *)mapping->addr + weight->offs;

                if (check_tensors)
                {
                    validation_result.emplace_back(std::async(
                        std::launch::async,
                        [cur, data, n_size] { return std::make_pair(cur, ggml_validate_row_data(cur->type, data, n_size)); }));
                }

                GGML_ASSERT(buf_mmap || cur->data); // either we have a buffer to allocate the tensor in, or it is already allocated
                if (buf_mmap && cur->data == nullptr)
                {
                    ggml_backend_tensor_alloc(buf_mmap, cur, data);
                    if (lmlocks)
                    {
                        const auto & lmlock = lmlocks->at(weight->idx);
                        lmlock->grow_to(weight->offs + n_size);
                    }

                    auto & mmap_used = mmaps_used[weight->idx];
                    mmap_used.first = std::min(mmap_used.first, weight->offs);
                    mmap_used.second = std::max(mmap_used.second, weight->offs + n_size);
                }
                else
                {
                    ggml_backend_tensor_set(cur, data, 0, n_size);
                }
            }
            else
            {
                GGML_ASSERT(weight->idx < files.size());
                const auto & file = files.at(weight->idx);
                if (ggml_backend_buffer_is_host(cur->buffer))
                {
                    file->seek(weight->offs, SEEK_SET);
                    file->read_raw(cur->data, n_size);
                    if (check_tensors)
                    {
                        validation_result.emplace_back(std::async(
                            std::launch::async,
                            [cur, n_size] { return std::make_pair(cur, ggml_validate_row_data(cur->type, cur->data, n_size)); }));
                    }
                }
                else
                {
                    read_buf.resize(n_size);
                    file->seek(weight->offs, SEEK_SET);
                    file->read_raw(read_buf.data(), n_size);
                    ggml_backend_tensor_set(cur, read_buf.data(), 0, n_size);
                    if (check_tensors && !ggml_validate_row_data(cur->type, read_buf.data(), n_size))
                    {
                        throw std::runtime_error(format("tensor '%s' has invalid data", ggml_get_name(cur)));
                    }
                }
            }

            size_done += n_size;
        }

        // check validation results
        bool validation_failed = false;
        for (auto & future : validation_result)
        {
            auto result = future.get();
            if (!result.second)
            {
                LLAMA_LOG_ERROR("%s: tensor '%s' has invalid data\n", __func__, ggml_get_name(result.first));
                validation_failed = true;
            }
        }
        if (validation_failed)
        {
            throw std::runtime_error("found tensors with invalid data");
        }

        // check if this is the last call and do final cleanup
        if (size_done >= size_data)
        {
            // unmap offloaded tensors and metadata
            if (use_mmap)
            {
                for (uint32_t idx = 0; idx < mappings.size(); idx++)
                {
                    const auto & mmap_used = mmaps_used.at(idx);
                    auto & mapping = mappings.at(idx);
                    mapping->unmap_fragment(0, mmap_used.first);
                    if (mmap_used.second != 0)
                    {
                        mapping->unmap_fragment(mmap_used.second, mapping->size);
                    }
                }
            }
            if (progress_callback)
            {
                // Even though the model is done loading, we still honor
                // cancellation since we need to free allocations.
                return progress_callback(1.0f, progress_callback_user_data);
            }
        }

        return true;
    }
};

template <>
bool llama_model_loader::get_key(const enum llm_kv kid, enum llama_pooling_type & result, const bool required)
{
    uint32_t tmp;
    const bool found = get_key(kid, tmp, required);
    if (found)
    {
        result = (enum llama_pooling_type)tmp;
    }
    else
    {
        result = LLAMA_POOLING_TYPE_UNSPECIFIED;
    }
    return found;
}


//
// load LLaMA models
//

static const char * llama_model_arch_name(llm_arch arch)
{
    auto it = LLM_ARCH_NAMES.find(arch);
    if (it == LLM_ARCH_NAMES.end())
    {
        return "unknown";
    }
    return it->second;
}

static std::string llama_model_ftype_name(llama_ftype ftype)
{
    if (ftype & LLAMA_FTYPE_GUESSED)
    {
        return llama_model_ftype_name((enum llama_ftype)(ftype & ~LLAMA_FTYPE_GUESSED)) + " (guessed)";
    }

    switch (ftype)
    {
        case LLAMA_FTYPE_ALL_F32:
            return "all F32";
        case LLAMA_FTYPE_MOSTLY_F16:
            return "F16";
        case LLAMA_FTYPE_MOSTLY_BF16:
            return "BF16";
        case LLAMA_FTYPE_MOSTLY_Q4_0:
            return "Q4_0";
        case LLAMA_FTYPE_MOSTLY_Q4_1:
            return "Q4_1";
        case LLAMA_FTYPE_MOSTLY_Q4_1_SOME_F16:
            return "Q4_1, some F16";
        case LLAMA_FTYPE_MOSTLY_Q5_0:
            return "Q5_0";
        case LLAMA_FTYPE_MOSTLY_Q5_1:
            return "Q5_1";
        case LLAMA_FTYPE_MOSTLY_Q8_0:
            return "Q8_0";

        // K-quants
        case LLAMA_FTYPE_MOSTLY_Q2_K:
            return "Q2_K - Medium";
        case LLAMA_FTYPE_MOSTLY_Q2_K_S:
            return "Q2_K - Small";
        case LLAMA_FTYPE_MOSTLY_Q3_K_S:
            return "Q3_K - Small";
        case LLAMA_FTYPE_MOSTLY_Q3_K_M:
            return "Q3_K - Medium";
        case LLAMA_FTYPE_MOSTLY_Q3_K_L:
            return "Q3_K - Large";
        case LLAMA_FTYPE_MOSTLY_Q4_K_S:
            return "Q4_K - Small";
        case LLAMA_FTYPE_MOSTLY_Q4_K_M:
            return "Q4_K - Medium";
        case LLAMA_FTYPE_MOSTLY_Q5_K_S:
            return "Q5_K - Small";
        case LLAMA_FTYPE_MOSTLY_Q5_K_M:
            return "Q5_K - Medium";
        case LLAMA_FTYPE_MOSTLY_Q6_K:
            return "Q6_K";
        case LLAMA_FTYPE_MOSTLY_IQ2_XXS:
            return "IQ2_XXS - 2.0625 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ2_XS:
            return "IQ2_XS - 2.3125 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ2_S:
            return "IQ2_S - 2.5 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ2_M:
            return "IQ2_M - 2.7 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ3_XS:
            return "IQ3_XS - 3.3 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ3_XXS:
            return "IQ3_XXS - 3.0625 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ1_S:
            return "IQ1_S - 1.5625 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ1_M:
            return "IQ1_M - 1.75 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ4_NL:
            return "IQ4_NL - 4.5 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ4_XS:
            return "IQ4_XS - 4.25 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ3_S:
            return "IQ3_S - 3.4375 bpw";
        case LLAMA_FTYPE_MOSTLY_IQ3_M:
            return "IQ3_S mix - 3.66 bpw";

        default:
            return "unknown, may not work";
    }
}

static const char * llama_model_type_name(e_model type)
{
    switch (type)
    {
        case MODEL_14M:
            return "14M";
        case MODEL_17M:
            return "17M";
        case MODEL_22M:
            return "22M";
        case MODEL_33M:
            return "33M";
        case MODEL_70M:
            return "70M";
        case MODEL_109M:
            return "109M";
        case MODEL_137M:
            return "137M";
        case MODEL_160M:
            return "160M";
        case MODEL_335M:
            return "335M";
        case MODEL_410M:
            return "410M";
        case MODEL_0_5B:
            return "0.5B";
        case MODEL_1B:
            return "1B";
        case MODEL_1_4B:
            return "1.4B";
        case MODEL_2B:
            return "2B";
        case MODEL_2_8B:
            return "2.8B";
        case MODEL_3B:
            return "3B";
        case MODEL_4B:
            return "4B";
        case MODEL_6_9B:
            return "6.9B";
        case MODEL_7B:
            return "7B";
        case MODEL_8B:
            return "8B";
        case MODEL_12B:
            return "12B";
        case MODEL_13B:
            return "13B";
        case MODEL_14B:
            return "14B";
        case MODEL_15B:
            return "15B";
        case MODEL_20B:
            return "20B";
        case MODEL_30B:
            return "30B";
        case MODEL_34B:
            return "34B";
        case MODEL_35B:
            return "35B";
        case MODEL_40B:
            return "40B";
        case MODEL_65B:
            return "65B";
        case MODEL_70B:
            return "70B";
        case MODEL_314B:
            return "314B";
        case MODEL_SMALL:
            return "0.1B";
        case MODEL_MEDIUM:
            return "0.4B";
        case MODEL_LARGE:
            return "0.8B";
        case MODEL_XL:
            return "1.5B";
        case MODEL_A2_7B:
            return "A2.7B";
        case MODEL_8x7B:
            return "8x7B";
        case MODEL_8x22B:
            return "8x22B";
        case MODEL_16x12B:
            return "16x12B";
        case MODEL_10B_128x3_66B:
            return "10B+128x3.66B";
        default:
            return "?B";
    }
}

static const char * llama_model_vocab_type_name(enum llama_vocab_type type)
{
    switch (type)
    {
        case LLAMA_VOCAB_TYPE_NONE:
            return "no vocab";
        case LLAMA_VOCAB_TYPE_SPM:
            return "SPM";
        case LLAMA_VOCAB_TYPE_BPE:
            return "BPE";
        case LLAMA_VOCAB_TYPE_WPM:
            return "WPM";
        default:
            return "unknown";
    }
}

static void llm_load_arch(llama_model_loader & ml, llama_model & model)
{
    model.arch = ml.get_arch();
    if (model.arch == LLM_ARCH_UNKNOWN)
    {
        throw std::runtime_error("unknown model architecture: '" + ml.get_arch_name() + "'");
    }
}

static void llm_load_hparams(llama_model_loader & ml, llama_model & model)
{
    auto & hparams = model.hparams;
    const gguf_context * ctx = ml.meta;

    // get metadata as string
    for (int i = 0; i < gguf_get_n_kv(ctx); i++)
    {
        enum gguf_type type = gguf_get_kv_type(ctx, i);
        if (type == GGUF_TYPE_ARRAY)
        {
            continue;
        }
        const char * name = gguf_get_key(ctx, i);
        const std::string value = gguf_kv_to_str(ctx, i);
        model.gguf_kv.emplace(name, value);
    }

    // get general kv
    ml.get_key(LLM_KV_GENERAL_NAME, model.name, false);

    // get hparams kv
    ml.get_key(LLM_KV_VOCAB_SIZE, hparams.n_vocab, false) || ml.get_arr_n(LLM_KV_TOKENIZER_LIST, hparams.n_vocab);

    // everything past this point is not vocab-related
    if (hparams.vocab_only)
    {
        return;
    }

    ml.get_key(LLM_KV_CONTEXT_LENGTH, hparams.n_ctx_train);
    ml.get_key(LLM_KV_EMBEDDING_LENGTH, hparams.n_embd);
    ml.get_key(LLM_KV_FEED_FORWARD_LENGTH, hparams.n_ff);
    ml.get_key(LLM_KV_ATTENTION_HEAD_COUNT, hparams.n_head);
    ml.get_key(LLM_KV_BLOCK_COUNT, hparams.n_layer);
    ml.get_key(LLM_KV_EXPERT_COUNT, hparams.n_expert, false);
    ml.get_key(LLM_KV_EXPERT_USED_COUNT, hparams.n_expert_used, false);

    GGML_ASSERT(hparams.n_expert <= LLAMA_MAX_EXPERTS);
    GGML_ASSERT(hparams.n_expert_used <= hparams.n_expert);
    if (hparams.n_expert > 0)
    {
        GGML_ASSERT(hparams.n_expert_used > 0);
    }
    else
    {
        GGML_ASSERT(hparams.n_expert_used == 0);
    }

    // n_head_kv is optional, default to n_head
    hparams.n_head_kv = hparams.n_head;
    ml.get_key(LLM_KV_ATTENTION_HEAD_COUNT_KV, hparams.n_head_kv, false);

    bool rope_finetuned = false;
    ml.get_key(LLM_KV_ROPE_SCALING_FINETUNED, rope_finetuned, false);
    hparams.rope_finetuned = rope_finetuned;

    hparams.n_yarn_orig_ctx = hparams.n_ctx_train;
    ml.get_key(LLM_KV_ROPE_SCALING_ORIG_CTX_LEN, hparams.n_yarn_orig_ctx, false);

    // rope_freq_base (optional)
    hparams.rope_freq_base_train = 10000.0f;
    ml.get_key(LLM_KV_ROPE_FREQ_BASE, hparams.rope_freq_base_train, false);

    std::string rope_scaling("linear");
    ml.get_key(LLM_KV_ROPE_SCALING_TYPE, rope_scaling, false);
    hparams.rope_scaling_type_train = llama_rope_scaling_type_from_string(rope_scaling);
    GGML_ASSERT(hparams.rope_scaling_type_train != LLAMA_ROPE_SCALING_TYPE_UNSPECIFIED);

    // rope_freq_scale (inverse of the kv) is optional
    float ropescale = 0.0f;
    if (!ml.get_key(LLM_KV_ROPE_SCALING_FACTOR, ropescale, false))
    {
        // try the old key name
        ml.get_key(LLM_KV_ROPE_SCALE_LINEAR, ropescale, false);
    }
    hparams.rope_freq_scale_train = ropescale == 0.0f ? 1.0f : 1.0f / ropescale;

    ml.get_key(LLM_KV_ROPE_SCALING_ATTN_FACTOR, hparams.rope_attn_factor, false);

    // sanity check for n_rot (optional)
    {
        hparams.n_rot = (hparams.n_head == 0) ? 0 : hparams.n_embd / hparams.n_head;

        ml.get_key(LLM_KV_ROPE_DIMENSION_COUNT, hparams.n_rot, false);

        if (model.arch == LLM_ARCH_LLAMA || model.arch == LLM_ARCH_FALCON)
        {
            if (hparams.n_rot != hparams.n_embd / hparams.n_head)
            {
                throw std::runtime_error(format("invalid n_rot: %u, expected %u", hparams.n_rot, hparams.n_embd / hparams.n_head));
            }
        }
        // gpt-neox n_rot = rotary_pct * (n_embd / n_head)
        // gpt-j n_rot = rotary_dim
    }

    hparams.n_embd_head_k = (hparams.n_head == 0) ? 0 : hparams.n_embd / hparams.n_head;
    ml.get_key(LLM_KV_ATTENTION_KEY_LENGTH, hparams.n_embd_head_k, false);

    hparams.n_embd_head_v = (hparams.n_head == 0) ? 0 : hparams.n_embd / hparams.n_head;
    ml.get_key(LLM_KV_ATTENTION_VALUE_LENGTH, hparams.n_embd_head_v, false);

    // arch-specific KVs
    switch (model.arch)
    {
        case LLM_ARCH_LLAMA: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            if (hparams.n_expert == 8)
            {
                switch (hparams.n_layer)
                {
                    case 32:
                        model.type = e_model::MODEL_8x7B;
                        break;
                    case 56:
                        model.type = e_model::MODEL_8x22B;
                        break;
                    default:
                        model.type = e_model::MODEL_UNKNOWN;
                }
            }
            else
            {
                switch (hparams.n_layer)
                {
                    case 22:
                        model.type = e_model::MODEL_1B;
                        break;
                    case 26:
                        model.type = e_model::MODEL_3B;
                        break;
                    case 32:
                        model.type = hparams.n_vocab < 40000 ? e_model::MODEL_7B : e_model::MODEL_8B;
                        break;
                    case 40:
                        model.type = e_model::MODEL_13B;
                        break;
                    case 48:
                        model.type = e_model::MODEL_34B;
                        break;
                    case 60:
                        model.type = e_model::MODEL_30B;
                        break;
                    case 80:
                        model.type = hparams.n_head == hparams.n_head_kv ? e_model::MODEL_65B : e_model::MODEL_70B;
                        break;
                    default:
                        model.type = e_model::MODEL_UNKNOWN;
                }
            }
        }
        break;
        case LLM_ARCH_MINICPM: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 40:
                    model.type = e_model::MODEL_2B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_GROK: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 64:
                    model.type = e_model::MODEL_314B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_FALCON: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);

            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 60:
                    model.type = e_model::MODEL_40B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_BAICHUAN: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);
            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 40:
                    model.type = e_model::MODEL_13B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }

            if (model.type == e_model::MODEL_13B)
            {
                // TODO: become GGUF KV parameter
                hparams.f_max_alibi_bias = 8.0f;
            }
        }
        break;
        case LLM_ARCH_STARCODER: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            switch (hparams.n_layer)
            {
                case 24:
                    model.type = e_model::MODEL_1B;
                    break;
                case 36:
                    model.type = e_model::MODEL_3B;
                    break;
                case 42:
                    model.type = e_model::MODEL_7B;
                    break;
                case 40:
                    model.type = e_model::MODEL_15B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_REFACT: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);
            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_1B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }

            // TODO: become GGUF KV parameter
            hparams.f_max_alibi_bias = 8.0f;
        }
        break;
        case LLM_ARCH_BERT: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_ATTENTION_CAUSAL, hparams.causal_attn);
            ml.get_key(LLM_KV_TOKENIZER_TOKEN_TYPE_COUNT, hparams.n_vocab_type);
            ml.get_key(LLM_KV_POOLING_TYPE, hparams.pooling_type, false);

            switch (hparams.n_layer)
            {
                case 3:
                    model.type = e_model::MODEL_17M;
                    break; // bge-micro
                case 6:
                    model.type = e_model::MODEL_22M;
                    break; // MiniLM-L6
                case 12:
                    switch (hparams.n_embd)
                    {
                        case 384:
                            model.type = e_model::MODEL_33M;
                            break; // MiniLM-L12, bge-small
                        case 768:
                            model.type = e_model::MODEL_109M;
                            break; // bge-base
                    }
                    break;
                case 24:
                    model.type = e_model::MODEL_335M;
                    break; // bge-large
            }
        }
        break;
        case LLM_ARCH_JINA_BERT_V2: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_ATTENTION_CAUSAL, hparams.causal_attn);
            ml.get_key(LLM_KV_TOKENIZER_TOKEN_TYPE_COUNT, hparams.n_vocab_type);
            ml.get_key(LLM_KV_POOLING_TYPE, hparams.pooling_type);
            hparams.f_max_alibi_bias = 8.0f;

            switch (hparams.n_layer)
            {
                case 4:
                    model.type = e_model::MODEL_33M;
                    break; // jina-embeddings-small
                case 12:
                    model.type = e_model::MODEL_137M;
                    break; // jina-embeddings-base
            }
        }
        break;
        case LLM_ARCH_NOMIC_BERT: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_ATTENTION_CAUSAL, hparams.causal_attn);
            ml.get_key(LLM_KV_TOKENIZER_TOKEN_TYPE_COUNT, hparams.n_vocab_type);
            ml.get_key(LLM_KV_POOLING_TYPE, hparams.pooling_type);

            if (hparams.n_layer == 12 && hparams.n_embd == 768)
            {
                model.type = e_model::MODEL_137M;
            }
        }
        break;
        case LLM_ARCH_BLOOM: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);

            switch (hparams.n_layer)
            {
                case 24:
                    model.type = e_model::MODEL_1B;
                    break;
                case 30:
                    switch (hparams.n_embd)
                    {
                        case 2560:
                            model.type = e_model::MODEL_3B;
                            break;
                        case 4096:
                            model.type = e_model::MODEL_7B;
                            break;
                    }
                    break;
            }

            // TODO: become GGUF KV parameter
            hparams.f_max_alibi_bias = 8.0f;
        }
        break;
        case LLM_ARCH_MPT: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_ATTENTION_CLAMP_KQV, hparams.f_clamp_kqv, false);
            ml.get_key(LLM_KV_ATTENTION_MAX_ALIBI_BIAS, hparams.f_max_alibi_bias);

            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 48:
                    model.type = e_model::MODEL_30B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_STABLELM: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);

            switch (hparams.n_layer)
            {
                case 24:
                    model.type = e_model::MODEL_1B;
                    break;
                case 32:
                    model.type = e_model::MODEL_3B;
                    break;
                case 40:
                    model.type = e_model::MODEL_12B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_QWEN: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 40:
                    model.type = e_model::MODEL_13B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_QWEN2: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);
            switch (hparams.n_layer)
            {
                case 24:
                    model.type = hparams.n_embd == 1024 ? e_model::MODEL_0_5B : e_model::MODEL_1B;
                    break;
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 40:
                    model.type = hparams.n_head == 20 ? e_model::MODEL_4B : e_model::MODEL_13B;
                    break;
                case 80:
                    model.type = e_model::MODEL_70B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_QWEN2MOE: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);
            switch (hparams.n_layer)
            {
                case 24:
                    model.type = e_model::MODEL_A2_7B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_PHI2: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);

            switch (hparams.n_layer)
            {
                case 24:
                    model.type = e_model::MODEL_1B;
                    break;
                case 32:
                    model.type = e_model::MODEL_3B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_PHI3: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 24:
                    model.type = e_model::MODEL_1B;
                    break;
                case 32:
                    model.type = e_model::MODEL_3B;
                    break;
                case 40:
                    model.type = e_model::MODEL_14B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_PLAMO: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 40:
                    model.type = e_model::MODEL_13B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_GPT2: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            switch (hparams.n_layer)
            {
                case 12:
                    model.type = e_model::MODEL_SMALL;
                    break;
                case 24:
                    model.type = e_model::MODEL_MEDIUM;
                    break;
                case 36:
                    model.type = e_model::MODEL_LARGE;
                    break;
                case 48:
                    model.type = e_model::MODEL_XL;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_CODESHELL: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            switch (hparams.n_layer)
            {
                case 42:
                    model.type = e_model::MODEL_SMALL;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_ORION: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);

            switch (hparams.n_layer)
            {
                case 40:
                    model.type = e_model::MODEL_14B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_INTERNLM2: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);
            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 48:
                    model.type = e_model::MODEL_20B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_GEMMA: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 18:
                    model.type = e_model::MODEL_2B;
                    break;
                case 28:
                    model.type = e_model::MODEL_7B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_STARCODER2: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            switch (hparams.n_layer)
            {
                case 30:
                    model.type = e_model::MODEL_3B;
                    break;
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 40:
                    model.type = e_model::MODEL_15B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_MAMBA: {
            ml.get_key(LLM_KV_SSM_CONV_KERNEL, hparams.ssm_d_conv);
            ml.get_key(LLM_KV_SSM_INNER_SIZE, hparams.ssm_d_inner);
            ml.get_key(LLM_KV_SSM_STATE_SIZE, hparams.ssm_d_state);
            ml.get_key(LLM_KV_SSM_TIME_STEP_RANK, hparams.ssm_dt_rank);

            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            switch (hparams.n_layer)
            {
                case 24:
                    switch (hparams.n_embd)
                    {
                        case 768:
                            model.type = e_model::MODEL_SMALL;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 48:
                    switch (hparams.n_embd)
                    {
                        case 1024:
                            model.type = e_model::MODEL_MEDIUM;
                            break;
                        case 1536:
                            model.type = e_model::MODEL_LARGE;
                            break;
                        case 2048:
                            model.type = e_model::MODEL_XL;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 64:
                    switch (hparams.n_embd)
                    {
                        case 2560:
                            model.type = e_model::MODEL_3B;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_XVERSE: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);
            switch (hparams.n_layer)
            {
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 40:
                    model.type = e_model::MODEL_13B;
                    break;
                case 80:
                    model.type = e_model::MODEL_65B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_COMMAND_R: {
            ml.get_key(LLM_KV_LOGIT_SCALE, hparams.f_logit_scale);
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            switch (hparams.n_layer)
            {
                case 40:
                    model.type = e_model::MODEL_35B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_DBRX: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_ATTENTION_CLAMP_KQV, hparams.f_clamp_kqv);

            switch (hparams.n_layer)
            {
                case 40:
                    model.type = e_model::MODEL_16x12B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_OLMO: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_ATTENTION_CLAMP_KQV, hparams.f_clamp_kqv, false);

            switch (hparams.n_layer)
            {
                case 22:
                    model.type = e_model::MODEL_1B;
                    break;
                case 32:
                    model.type = e_model::MODEL_7B;
                    break;
                case 80:
                    model.type = e_model::MODEL_70B;
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_GPTNEOX: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_EPS, hparams.f_norm_eps);
            ml.get_key(LLM_KV_USE_PARALLEL_RESIDUAL, hparams.use_par_res);
            switch (hparams.n_layer)
            {
                case 6:
                    switch (hparams.n_ff)
                    {
                        case 512:
                            model.type = e_model::MODEL_14M;
                            break;
                        case 2048:
                            model.type = e_model::MODEL_70M;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 12:
                    switch (hparams.n_ff)
                    {
                        case 3072:
                            model.type = e_model::MODEL_160M;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 16:
                    switch (hparams.n_ff)
                    {
                        case 8192:
                            model.type = e_model::MODEL_1B;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 24:
                    switch (hparams.n_ff)
                    {
                        case 4096:
                            model.type = e_model::MODEL_410M;
                            break;
                        case 8192:
                            model.type = e_model::MODEL_1_4B;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 32:
                    switch (hparams.n_ff)
                    {
                        case 10240:
                            model.type = e_model::MODEL_2_8B;
                            break;
                        case 16384:
                            model.type = e_model::MODEL_6_9B;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 36:
                    switch (hparams.n_ff)
                    {
                        case 20480:
                            model.type = e_model::MODEL_12B;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                case 44:
                    switch (hparams.n_ff)
                    {
                        case 24576:
                            model.type = e_model::MODEL_20B;
                            break;
                        default:
                            model.type = e_model::MODEL_UNKNOWN;
                    }
                    break;
                default:
                    model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        case LLM_ARCH_ARCTIC: {
            ml.get_key(LLM_KV_ATTENTION_LAYERNORM_RMS_EPS, hparams.f_norm_rms_eps);

            if (hparams.n_expert == 128)
            {
                switch (hparams.n_layer)
                {
                    case 35:
                        model.type = e_model::MODEL_10B_128x3_66B;
                        break;
                    default:
                        model.type = e_model::MODEL_UNKNOWN;
                }
            }
            else
            {
                model.type = e_model::MODEL_UNKNOWN;
            }
        }
        break;
        default:
            (void)0;
    }

    model.ftype = ml.ftype;

    if (hparams.f_max_alibi_bias > 0.0f)
    {
        hparams.use_alibi = true;
    }

    hparams.rope_type = llama_rope_type(&model);
}

// TODO: This should probably be in llama.h
static std::vector<llama_vocab::id>
llama_tokenize_internal(const llama_vocab & vocab, std::string raw_text, bool add_special, bool parse_special = false);
static llama_token llama_byte_to_token(const llama_vocab & vocab, uint8_t ch);

static void llm_load_vocab(llama_model_loader & ml, llama_model & model)
{
    auto & vocab = model.vocab;

    struct gguf_context * ctx = ml.meta;

    const auto kv = LLM_KV(model.arch);

    // determine vocab type
    {
        std::string tokenizer_model;
        std::string tokenizer_pre;

        ml.get_key(LLM_KV_TOKENIZER_MODEL, tokenizer_model);
        ml.get_key(LLM_KV_TOKENIZER_PRE, tokenizer_pre, false);

        if (tokenizer_model == "no_vocab")
        {
            vocab.type = LLAMA_VOCAB_TYPE_NONE;

            // default special tokens
            vocab.special_bos_id = -1;
            vocab.special_eos_id = -1;
            vocab.special_unk_id = -1;
            vocab.special_sep_id = -1;
            vocab.special_pad_id = -1;
            vocab.special_cls_id = -1;
            vocab.special_mask_id = -1;
            vocab.linefeed_id = -1;

            return;
        }
        else if (tokenizer_model == "llama")
        {
            vocab.type = LLAMA_VOCAB_TYPE_SPM;

            // default special tokens
            vocab.special_bos_id = 1;
            vocab.special_eos_id = 2;
            vocab.special_unk_id = 0;
            vocab.special_sep_id = -1;
            vocab.special_pad_id = -1;
            vocab.special_cls_id = -1;
            vocab.special_mask_id = -1;

            // For Fill-In-the-Middle (FIM)/infill models which where converted
            // prior to support of FIM special tokens in GGUF, the following
            // will allow those models to continue to work. The general names
            // of the known models are currently CodeLlama (LLM_ARCH_LLAMA) and
            // CodeGemma (LLM_ARCH_GEMMA). This can potentially be removed once
            // new versions of these models have been published.
            std::string gen_name;
            ml.get_key(LLM_KV_GENERAL_NAME, gen_name, false);

            std::transform(gen_name.begin(), gen_name.end(), gen_name.begin(), [](unsigned char c) { return std::tolower(c); });

            if (gen_name.find("code") != std::string::npos)
            {
                if (model.arch == LLM_ARCH_LLAMA)
                {
                    vocab.special_prefix_id = 32007;
                    vocab.special_suffix_id = 32008;
                    vocab.special_middle_id = 32009;
                    vocab.special_eot_id = 32010;
                }
                else if (model.arch == LLM_ARCH_GEMMA)
                {
                    vocab.special_prefix_id = 67;
                    vocab.special_suffix_id = 69;
                    vocab.special_middle_id = 68;
                    // TODO: this is not EOT, it is "file separator" token, needs fix
                    //       https://huggingface.co/google/codegemma-7b-it/blob/9b1d9231388358c04d90bd003458f5070d97db44/tokenizer_config.json#L565-L572
                    //vocab.special_eot_id    = 70;
                    vocab.special_eot_id = 107;
                }
            }

            const int add_space_prefix_keyidx = gguf_find_key(ctx, kv(LLM_KV_TOKENIZER_ADD_PREFIX).c_str());
            if (add_space_prefix_keyidx != -1)
            {
                vocab.add_space_prefix = gguf_get_val_bool(ctx, add_space_prefix_keyidx);
            } // The default value of add_space_prefix is true.
        }
        else if (tokenizer_model == "bert")
        {
            vocab.type = LLAMA_VOCAB_TYPE_WPM;

            // default special tokens
            vocab.special_bos_id = -1;
            vocab.special_eos_id = -1;
            vocab.special_unk_id = 100;
            vocab.special_sep_id = 102;
            vocab.special_pad_id = 0;
            vocab.special_cls_id = 101;
            vocab.special_mask_id = 103;
            vocab.add_space_prefix = false;
        }
        else
        {
            if (tokenizer_model == "gpt2")
            {
                vocab.type = LLAMA_VOCAB_TYPE_BPE;
            }
            else
            {
                LLAMA_LOG_WARN("%s: unknown tokenizer: '%s'", __func__, tokenizer_model.c_str());
                LLAMA_LOG_WARN("%s: using default tokenizer: 'llama'", __func__);
                vocab.type = LLAMA_VOCAB_TYPE_SPM;
                return;
            }
            // read bpe merges and populate bpe ranks
            const int merges_keyidx = gguf_find_key(ctx, kv(LLM_KV_TOKENIZER_MERGES).c_str());
            if (merges_keyidx == -1)
            {
                throw std::runtime_error("cannot find tokenizer merges in model file\n");
            }

            const int n_merges = gguf_get_arr_n(ctx, merges_keyidx);

            for (int i = 0; i < n_merges; i++)
            {
                const std::string word = gguf_get_arr_str(ctx, merges_keyidx, i);
                GGML_ASSERT(unicode_cpts_from_utf8(word).size() > 0);

                std::string first;
                std::string second;

                const size_t pos = word.find(' ', 1);

                if (pos != std::string::npos)
                {
                    first = word.substr(0, pos);
                    second = word.substr(pos + 1);
                }

                vocab.bpe_ranks.emplace(std::make_pair(first, second), i);
            }

            // default special tokens
            vocab.special_bos_id = 11;
            vocab.special_eos_id = 11;
            vocab.special_unk_id = -1;
            vocab.special_sep_id = -1;
            vocab.special_pad_id = -1;
            vocab.special_cls_id = -1;
            vocab.special_mask_id = -1;
        }

        // for now, only BPE models have pre-tokenizers
        if (vocab.type == LLAMA_VOCAB_TYPE_BPE)
        {
            if (tokenizer_pre.empty())
            {
                LLAMA_LOG_WARN("%s: missing pre-tokenizer type, using: 'default'\n", __func__);
                LLAMA_LOG_WARN("%s:                                             \n", __func__);
                LLAMA_LOG_WARN("%s: ************************************        \n", __func__);
                LLAMA_LOG_WARN("%s: GENERATION QUALITY WILL BE DEGRADED!        \n", __func__);
                LLAMA_LOG_WARN("%s: CONSIDER REGENERATING THE MODEL             \n", __func__);
                LLAMA_LOG_WARN("%s: ************************************        \n", __func__);
                LLAMA_LOG_WARN("%s:                                             \n", __func__);
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_DEFAULT;
            }
            else if (tokenizer_pre == "default")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_DEFAULT;
            }
            else if (tokenizer_pre == "llama3" || tokenizer_pre == "llama-v3" || tokenizer_pre == "llama-bpe")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_LLAMA3;
            }
            else if (tokenizer_pre == "deepseek-llm")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_DEEPSEEK_LLM;
            }
            else if (tokenizer_pre == "deepseek-coder")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_DEEPSEEK_CODER;
            }
            else if (tokenizer_pre == "falcon")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_FALCON;
            }
            else if (tokenizer_pre == "mpt")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_MPT;
            }
            else if (tokenizer_pre == "starcoder")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_STARCODER;
            }
            else if (
                tokenizer_pre == "gpt-2" || tokenizer_pre == "jina-es" || tokenizer_pre == "jina-de" || tokenizer_pre == "jina-v2-es"
                || tokenizer_pre == "jina-v2-de")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_GPT2;
            }
            else if (tokenizer_pre == "refact")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_REFACT;
            }
            else if (tokenizer_pre == "command-r")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_COMMAND_R;
            }
            else if (tokenizer_pre == "qwen2")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_QWEN2;
            }
            else if (tokenizer_pre == "stablelm2")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_STABLELM2;
            }
            else if (tokenizer_pre == "olmo")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_OLMO;
            }
            else if (tokenizer_pre == "dbrx")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_DBRX;
            }
            else if (tokenizer_pre == "smaug-bpe")
            {
                vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_SMAUG;
            }
            else
            {
                throw std::runtime_error(format("unknown pre-tokenizer type: '%s'", tokenizer_pre.c_str()));
            }
        }
        else
        {
            vocab.type_pre = LLAMA_VOCAB_PRE_TYPE_DEFAULT;
        }
    }

    const int token_idx = gguf_find_key(ctx, kv(LLM_KV_TOKENIZER_LIST).c_str());
    if (token_idx == -1)
    {
        throw std::runtime_error("cannot find tokenizer vocab in model file\n");
    }

    const float * scores = nullptr;
    const int score_idx = gguf_find_key(ctx, kv(LLM_KV_TOKENIZER_SCORES).c_str());
    if (score_idx != -1)
    {
        scores = (const float *)gguf_get_arr_data(ctx, score_idx);
    }

    const int * toktypes = nullptr;
    const int toktype_idx = gguf_find_key(ctx, kv(LLM_KV_TOKENIZER_TOKEN_TYPE).c_str());
    if (toktype_idx != -1)
    {
        toktypes = (const int *)gguf_get_arr_data(ctx, toktype_idx);
    }

    const uint32_t n_vocab = gguf_get_arr_n(ctx, token_idx);

    vocab.id_to_token.resize(n_vocab);

    for (uint32_t i = 0; i < n_vocab; i++)
    {
        std::string word = gguf_get_arr_str(ctx, token_idx, i);
        GGML_ASSERT(unicode_cpts_from_utf8(word).size() > 0);

        vocab.token_to_id[word] = i;

        auto & token_data = vocab.id_to_token[i];
        token_data.text = std::move(word);
        token_data.score = scores ? scores[i] : 0.0f;
        token_data.type = toktypes ? (llama_token_type)toktypes[i] : LLAMA_TOKEN_TYPE_NORMAL;
    }
    GGML_ASSERT(vocab.id_to_token.size() == vocab.token_to_id.size());

    // determine the newline token: LLaMA "<0x0A>" == 10 == '\n', Falcon 193 == '\n'
    if (vocab.type == LLAMA_VOCAB_TYPE_SPM)
    {
        try
        {
            vocab.linefeed_id = llama_byte_to_token(vocab, '\n');
        }
        catch (const std::exception & e)
        {
            LLAMA_LOG_WARN("%s: SPM vocabulary, but newline token not found: %s! Using special_pad_id instead.", __func__, e.what());
            vocab.linefeed_id = vocab.special_pad_id;
        }
    }
    else if (vocab.type == LLAMA_VOCAB_TYPE_WPM)
    {
        vocab.linefeed_id = vocab.special_pad_id;
    }
    else
    {
        const std::vector<int> ids = llama_tokenize_internal(vocab, "\xC4\x8A", false); // U+010A
        GGML_ASSERT(!ids.empty() && "model vocab missing newline token");
        vocab.linefeed_id = ids[0];
    }

    // special tokens
    {
        const std::vector<std::pair<llm_kv, int32_t &>> special_token_types = {
            {LLM_KV_TOKENIZER_BOS_ID, vocab.special_bos_id},
            {LLM_KV_TOKENIZER_EOS_ID, vocab.special_eos_id},
            {LLM_KV_TOKENIZER_UNK_ID, vocab.special_unk_id},
            {LLM_KV_TOKENIZER_SEP_ID, vocab.special_sep_id},
            {LLM_KV_TOKENIZER_PAD_ID, vocab.special_pad_id},
            {LLM_KV_TOKENIZER_CLS_ID, vocab.special_cls_id},
            {LLM_KV_TOKENIZER_MASK_ID, vocab.special_mask_id},
            {LLM_KV_TOKENIZER_PREFIX_ID, vocab.special_prefix_id},
            {LLM_KV_TOKENIZER_SUFFIX_ID, vocab.special_suffix_id},
            {LLM_KV_TOKENIZER_MIDDLE_ID, vocab.special_middle_id},
            {LLM_KV_TOKENIZER_EOT_ID, vocab.special_eot_id},
        };

        for (const auto & it : special_token_types)
        {
            const std::string & key = kv(std::get<0>(it));
            int32_t & id = std::get<1>(it);

            uint32_t new_id;
            if (!ml.get_key(std::get<0>(it), new_id, false))
            {
                continue;
            }
            if (new_id >= vocab.id_to_token.size())
            {
                LLAMA_LOG_WARN("%s: bad special token: '%s' = %ud, using default id %d\n", __func__, key.c_str(), new_id, id);
            }
            else
            {
                id = new_id;
            }
        }

        // Handle add_bos_token and add_eos_token
        {
            bool temp = true;

            if (ml.get_key(LLM_KV_TOKENIZER_ADD_BOS, temp, false))
            {
                vocab.special_add_bos = int(temp);
            }
            if (ml.get_key(LLM_KV_TOKENIZER_ADD_EOS, temp, false))
            {
                vocab.special_add_eos = int(temp);
            }
        }

        // find EOT token: "<|eot_id|>", "<|im_end|>", "<end_of_turn>", etc.
        //
        // TODO: convert scripts should provide this token through the KV metadata LLAMA_KV_TOKENIZER_EOT_ID
        //       for now, we apply this workaround to find the EOT token based on its text
        if (vocab.special_eot_id == -1)
        {
            for (const auto & t : vocab.token_to_id)
            {
                if (
                    // TODO: gemma "<end_of_turn>" is exported as a normal token, so the following check does not work
                    //       need to fix convert script
                    //vocab.id_to_token[t.second].type == LLAMA_TOKEN_TYPE_CONTROL &&
                    (t.first == "<|eot_id|>" || t.first == "<|im_end|>" || t.first == "<|end|>" || t.first == "<end_of_turn>"
                     || t.first == "<|endoftext|>"))
                {
                    vocab.special_eot_id = t.second;
                    break;
                }
            }
        }
    }

    // build special tokens cache
    {
        // TODO: It is unclear (to me) at this point, whether special tokes are guaranteed to be of a deterministic type,
        //  and will always be correctly labeled in 'added_tokens.json' etc.
        // The assumption is, since special tokens aren't meant to be exposed to end user, they are designed
        //  to be unmatchable by the tokenizer, therefore tokens from the vocab, which are unmatchable by the tokenizer
        //  are special tokens.
        // From testing, this appears to correlate 1:1 with special tokens.
        //

        // Counting special tokens and verifying in only one direction
        //  is sufficient to detect difference in those two sets.
        //
        // uint32_t special_tokens_count_by_type = 0;
        // uint32_t special_tokens_count_from_verification = 0;

        // bool special_tokens_definition_mismatch = false;

        for (const auto & t : vocab.token_to_id)
        {
            const auto & token = t.first;
            const auto & id = t.second;

            // Count all non-normal tokens in the vocab while iterating
            // if (vocab.id_to_token[id].type != LLAMA_TOKEN_TYPE_NORMAL)
            // {
            //     special_tokens_count_by_type++;
            // }

            // Skip single character tokens
            if (token.length() > 1)
            {
                bool is_tokenizable = false;

                // Split token string representation in two, in all possible ways
                //  and check if both halves can be matched to a valid token
                for (unsigned i = 1; i < token.length();)
                {
                    const auto left = token.substr(0, i);
                    const auto right = token.substr(i);

                    // check if we didn't partition in the middle of a utf sequence
                    auto utf = utf8_len(left.at(left.length() - 1));

                    if (utf == 1)
                    {
                        if (vocab.token_to_id.find(left) != vocab.token_to_id.end()
                            && vocab.token_to_id.find(right) != vocab.token_to_id.end())
                        {
                            is_tokenizable = true;
                            break;
                        }
                        i++;
                    }
                    else
                    {
                        // skip over the rest of multibyte utf sequence
                        i += utf - 1;
                    }
                }

                if (!is_tokenizable)
                {
                    // Some tokens are multibyte, but they are utf sequences with equivalent text length of 1
                    //  it's faster to re-filter them here, since there are way less candidates now

                    // Calculate a total "utf" length of a token string representation
                    size_t utf8_str_len = 0;
                    for (unsigned i = 0; i < token.length();)
                    {
                        utf8_str_len++;
                        i += utf8_len(token.at(i));
                    }

                    // And skip the ones which are one character
                    if (utf8_str_len > 1)
                    {
                        // At this point what we have left are special tokens only
                        vocab.special_tokens_cache[token] = id;

                        // Count manually found special tokens
                        // special_tokens_count_from_verification++;

                        // If this manually found special token is not marked as such, flag a mismatch
                        // if (vocab.id_to_token[id].type == LLAMA_TOKEN_TYPE_NORMAL)
                        // {
                            // special_tokens_definition_mismatch = true;
                        // }
                    }
                }
            }
        }

        // if (special_tokens_definition_mismatch || special_tokens_count_from_verification != special_tokens_count_by_type)
        // {
        //     LLAMA_LOG_WARN(
        //         "%s: mismatch in special tokens definition ( %u/%zu vs %u/%zu ).\n",
        //         __func__,
        //         special_tokens_count_from_verification,
        //         vocab.id_to_token.size(),
        //         special_tokens_count_by_type,
        //         vocab.id_to_token.size());
        // }
        // else
        // {
        //     LLAMA_LOG_INFO(
        //         "%s: special tokens definition check successful ( %u/%zu ).\n",
        //         __func__,
        //         special_tokens_count_from_verification,
        //         vocab.id_to_token.size());
        // }
    }
}

static void llm_load_print_meta(llama_model_loader & ml, llama_model & model)
{
    const auto & hparams = model.hparams;
    const auto & vocab = model.vocab;

    const char * rope_scaling_type = LLAMA_ROPE_SCALING_TYPES.at(hparams.rope_scaling_type_train);

    // hparams
    LLAMA_LOG_INFO("%s: format           = %s\n", __func__, llama_file_version_name(ml.fver));
    LLAMA_LOG_INFO("%s: arch             = %s\n", __func__, LLM_ARCH_NAMES.at(model.arch));
    LLAMA_LOG_INFO("%s: vocab type       = %s\n", __func__, llama_model_vocab_type_name(vocab.type));
    LLAMA_LOG_INFO("%s: n_vocab          = %u\n", __func__, hparams.n_vocab);
    LLAMA_LOG_INFO("%s: n_merges         = %u\n", __func__, (int)vocab.bpe_ranks.size());
    LLAMA_LOG_INFO("%s: n_ctx_train      = %u\n", __func__, hparams.n_ctx_train);
    LLAMA_LOG_INFO("%s: n_embd           = %u\n", __func__, hparams.n_embd);
    LLAMA_LOG_INFO("%s: n_head           = %u\n", __func__, hparams.n_head);
    LLAMA_LOG_INFO("%s: n_head_kv        = %u\n", __func__, hparams.n_head_kv);
    LLAMA_LOG_INFO("%s: n_layer          = %u\n", __func__, hparams.n_layer);
    LLAMA_LOG_INFO("%s: n_rot            = %u\n", __func__, hparams.n_rot);
    LLAMA_LOG_INFO("%s: n_embd_head_k    = %u\n", __func__, hparams.n_embd_head_k);
    LLAMA_LOG_INFO("%s: n_embd_head_v    = %u\n", __func__, hparams.n_embd_head_v);
    LLAMA_LOG_INFO("%s: n_gqa            = %u\n", __func__, hparams.n_gqa());
    LLAMA_LOG_INFO("%s: n_embd_k_gqa     = %u\n", __func__, hparams.n_embd_k_gqa());
    LLAMA_LOG_INFO("%s: n_embd_v_gqa     = %u\n", __func__, hparams.n_embd_v_gqa());
    LLAMA_LOG_INFO("%s: f_norm_eps       = %.1e\n", __func__, hparams.f_norm_eps);
    LLAMA_LOG_INFO("%s: f_norm_rms_eps   = %.1e\n", __func__, hparams.f_norm_rms_eps);
    LLAMA_LOG_INFO("%s: f_clamp_kqv      = %.1e\n", __func__, hparams.f_clamp_kqv);
    LLAMA_LOG_INFO("%s: f_max_alibi_bias = %.1e\n", __func__, hparams.f_max_alibi_bias);
    LLAMA_LOG_INFO("%s: f_logit_scale    = %.1e\n", __func__, hparams.f_logit_scale);
    LLAMA_LOG_INFO("%s: n_ff             = %u\n", __func__, hparams.n_ff);
    LLAMA_LOG_INFO("%s: n_expert         = %u\n", __func__, hparams.n_expert);
    LLAMA_LOG_INFO("%s: n_expert_used    = %u\n", __func__, hparams.n_expert_used);
    LLAMA_LOG_INFO("%s: causal attn      = %d\n", __func__, hparams.causal_attn);
    LLAMA_LOG_INFO("%s: pooling type     = %d\n", __func__, hparams.pooling_type);
    LLAMA_LOG_INFO("%s: rope type        = %d\n", __func__, hparams.rope_type);
    LLAMA_LOG_INFO("%s: rope scaling     = %s\n", __func__, rope_scaling_type);
    LLAMA_LOG_INFO("%s: freq_base_train  = %.1f\n", __func__, hparams.rope_freq_base_train);
    LLAMA_LOG_INFO("%s: freq_scale_train = %g\n", __func__, hparams.rope_freq_scale_train);
    LLAMA_LOG_INFO("%s: n_yarn_orig_ctx  = %u\n", __func__, hparams.n_yarn_orig_ctx);
    LLAMA_LOG_INFO("%s: rope_finetuned   = %s\n", __func__, hparams.rope_finetuned ? "yes" : "unknown");
    LLAMA_LOG_INFO("%s: ssm_d_conv       = %u\n", __func__, hparams.ssm_d_conv);
    LLAMA_LOG_INFO("%s: ssm_d_inner      = %u\n", __func__, hparams.ssm_d_inner);
    LLAMA_LOG_INFO("%s: ssm_d_state      = %u\n", __func__, hparams.ssm_d_state);
    LLAMA_LOG_INFO("%s: ssm_dt_rank      = %u\n", __func__, hparams.ssm_dt_rank);
    LLAMA_LOG_INFO("%s: model type       = %s\n", __func__, llama_model_type_name(model.type));
    LLAMA_LOG_INFO("%s: model ftype      = %s\n", __func__, llama_model_ftype_name(model.ftype).c_str());
    if (ml.n_elements >= 1e12)
    {
        LLAMA_LOG_INFO("%s: model params     = %.2f T\n", __func__, ml.n_elements * 1e-12);
    }
    else if (ml.n_elements >= 1e9)
    {
        LLAMA_LOG_INFO("%s: model params     = %.2f B\n", __func__, ml.n_elements * 1e-9);
    }
    else if (ml.n_elements >= 1e6)
    {
        LLAMA_LOG_INFO("%s: model params     = %.2f M\n", __func__, ml.n_elements * 1e-6);
    }
    else
    {
        LLAMA_LOG_INFO("%s: model params     = %.2f K\n", __func__, ml.n_elements * 1e-3);
    }
    if (ml.n_bytes < GiB)
    {
        LLAMA_LOG_INFO(
            "%s: model size       = %.2f MiB (%.2f BPW) \n", __func__, ml.n_bytes / 1024.0 / 1024.0, ml.n_bytes * 8.0 / ml.n_elements);
    }
    else
    {
        LLAMA_LOG_INFO(
            "%s: model size       = %.2f GiB (%.2f BPW) \n",
            __func__,
            ml.n_bytes / 1024.0 / 1024.0 / 1024.0,
            ml.n_bytes * 8.0 / ml.n_elements);
    }

    // general kv
    LLAMA_LOG_INFO("%s: general.name     = %s\n", __func__, model.name.c_str());

    // special tokens
    if (vocab.special_bos_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: BOS token        = %d '%s'\n", __func__, vocab.special_bos_id, vocab.id_to_token[vocab.special_bos_id].text.c_str());
    }
    if (vocab.special_eos_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: EOS token        = %d '%s'\n", __func__, vocab.special_eos_id, vocab.id_to_token[vocab.special_eos_id].text.c_str());
    }
    if (vocab.special_unk_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: UNK token        = %d '%s'\n", __func__, vocab.special_unk_id, vocab.id_to_token[vocab.special_unk_id].text.c_str());
    }
    if (vocab.special_sep_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: SEP token        = %d '%s'\n", __func__, vocab.special_sep_id, vocab.id_to_token[vocab.special_sep_id].text.c_str());
    }
    if (vocab.special_pad_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: PAD token        = %d '%s'\n", __func__, vocab.special_pad_id, vocab.id_to_token[vocab.special_pad_id].text.c_str());
    }
    if (vocab.special_cls_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: CLS token        = %d '%s'\n", __func__, vocab.special_cls_id, vocab.id_to_token[vocab.special_cls_id].text.c_str());
    }
    if (vocab.special_mask_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: MASK token       = %d '%s'\n", __func__, vocab.special_mask_id, vocab.id_to_token[vocab.special_mask_id].text.c_str());
    }

    if (vocab.linefeed_id != -1)
    {
        LLAMA_LOG_INFO("%s: LF token         = %d '%s'\n", __func__, vocab.linefeed_id, vocab.id_to_token[vocab.linefeed_id].text.c_str());
    }
    if (vocab.special_prefix_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: PRE token        = %d '%s'\n", __func__, vocab.special_prefix_id, vocab.id_to_token[vocab.special_prefix_id].text.c_str());
    }
    if (vocab.special_suffix_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: SUF token        = %d '%s'\n", __func__, vocab.special_suffix_id, vocab.id_to_token[vocab.special_suffix_id].text.c_str());
    }
    if (vocab.special_middle_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: MID token        = %d '%s'\n", __func__, vocab.special_middle_id, vocab.id_to_token[vocab.special_middle_id].text.c_str());
    }
    if (vocab.special_eot_id != -1)
    {
        LLAMA_LOG_INFO(
            "%s: EOT token        = %d '%s'\n", __func__, vocab.special_eot_id, vocab.id_to_token[vocab.special_eot_id].text.c_str());
    }
}

// Returns false if cancelled by progress_callback
static bool llm_load_tensors(
    llama_model_loader & ml,
    llama_model & model,
    int n_gpu_layers,
    enum llama_split_mode split_mode,
    int main_gpu,
    const float * tensor_split,
    bool use_mlock,
    llama_progress_callback progress_callback,
    void * progress_callback_user_data)
{
    model.t_start_us = ggml_time_us();

    auto & hparams = model.hparams;

#ifdef GGML_USE_SYCL
    // disable MoE with SYCL until mul_mat_id is updated
    if (hparams.n_expert > 0)
    {
        n_gpu_layers = 0;
    }
#endif

    model.split_mode = split_mode;
    model.main_gpu = main_gpu;
    model.n_gpu_layers = n_gpu_layers;

    const int64_t n_layer = hparams.n_layer;
    const int64_t i_gpu_start = std::max((int64_t)hparams.n_layer - n_gpu_layers, (int64_t)0);
    bool use_mmap_buffer = true;

    // there is very little benefit to offloading the input layer, so always keep it on the CPU
    model.buft_input = llama_default_buffer_type_cpu(true);
    //model.buft_input = llama_default_buffer_type_offload(main_gpu);

    model.buft_layer.resize(n_layer);

    // assign cpu layers
    for (int64_t i = 0; i < i_gpu_start; ++i)
    {
        model.buft_layer[i] = llama_default_buffer_type_cpu(true);
    }

    if (split_mode == LLAMA_SPLIT_MODE_LAYER)
    {
        // calculate the split points
        int device_count = llama_get_device_count(model);
        bool all_zero
            = tensor_split == nullptr || std::all_of(tensor_split, tensor_split + device_count, [](float x) { return x == 0.0f; });
        std::vector<float> splits(device_count);
        if (all_zero)
        {
            // default split, by free memory
            for (int i = 0; i < device_count; ++i)
            {
                splits[i] = llama_get_device_memory(model, i);
            }
        }
        else
        {
            std::copy(tensor_split, tensor_split + device_count, splits.begin());
        }

        // sum and normalize the splits to get the split points
        float split_sum = 0.0f;
        for (int i = 0; i < device_count; ++i)
        {
            split_sum += splits[i];
            splits[i] = split_sum;
        }
        for (int i = 0; i < device_count; ++i)
        {
            splits[i] /= split_sum;
        }

        // assign the repeating layers to the devices according to the splits
        int act_gpu_layers = std::min(n_gpu_layers, (int)n_layer + 1);
        for (int64_t i = i_gpu_start; i < n_layer; ++i)
        {
            int layer_gpu
                = std::upper_bound(splits.begin(), splits.begin() + device_count, float(i - i_gpu_start) / act_gpu_layers) - splits.begin();
            model.buft_layer[i] = llama_default_buffer_type_offload(model, layer_gpu);
        }
        // assign the output layer
        if (n_gpu_layers > n_layer)
        {
            int layer_gpu = std::upper_bound(splits.begin(), splits.begin() + device_count, float(act_gpu_layers - 1) / act_gpu_layers)
                - splits.begin();
            model.buft_output = llama_default_buffer_type_offload(model, layer_gpu);
        }
        else
        {
            model.buft_output = llama_default_buffer_type_cpu(true);
        }
    }
    else
    {
        ggml_backend_buffer_type_t split_buft;
        if (split_mode == LLAMA_SPLIT_MODE_ROW)
        {
            split_buft = llama_default_buffer_type_split(model, main_gpu, tensor_split);
        }
        else
        {
            // LLAMA_SPLIT_MODE_NONE or LLAMA_SPLIT_MODE_LAYER in backends where it is not supported
            split_buft = llama_default_buffer_type_offload(model, main_gpu);
        }
        // assign the repeating layers
        for (int64_t i = i_gpu_start; i < n_layer; ++i)
        {
            model.buft_layer[i] = {split_buft, llama_default_buffer_type_offload(model, main_gpu)};
        }
        // assign the output layer
        if (n_gpu_layers > n_layer)
        {
            model.buft_output = {split_buft, llama_default_buffer_type_offload(model, main_gpu)};
        }
        else
        {
            model.buft_output = llama_default_buffer_type_cpu(true);
        }
    }

    // count used buffer types
    std::map<ggml_backend_buffer_type_t, int> buft_layer_count;
    buft_layer_count[model.buft_input.buft]++;
    buft_layer_count[model.buft_input.buft_matrix]++;
    buft_layer_count[model.buft_output.buft]++;
    buft_layer_count[model.buft_output.buft_matrix]++;
    for (int64_t i = 0; i < n_layer; ++i)
    {
        buft_layer_count[model.buft_layer[i].buft]++;
        buft_layer_count[model.buft_layer[i].buft_matrix]++;
    }

    // create one context per buffer type
    size_t ctx_size = ggml_tensor_overhead() * (ml.n_tensors + 1); // +1 for models where tok_embd is duplicated as output

    // for moe merged tensors
    ctx_size += ggml_tensor_overhead() * n_layer * 3;

    std::map<ggml_backend_buffer_type_t, ggml_context *> ctx_map;
    for (auto & it : buft_layer_count)
    {
        ggml_init_params params = {
            .mem_size   =ctx_size,
            .mem_buffer =nullptr,
            .no_alloc   =true,
        };
        ggml_context * ctx = ggml_init(params);
        if (!ctx)
        {
            throw std::runtime_error(format("failed to create context"));
        }
        ctx_map[it.first] = ctx;
        model.ctxs.push_back(ctx);
    }

    LLAMA_LOG_INFO("%s: ggml ctx size = %7.2f MiB\n", __func__, model.ctxs.size() * ctx_size / 1024.0 / 1024.0);

    // create tensors for the weights
    {
        const int64_t n_embd = hparams.n_embd;
        const int64_t n_embd_head = n_embd / hparams.n_head;
        const int64_t n_embd_k_gqa = hparams.n_embd_k_gqa();
        const int64_t n_embd_v_gqa = hparams.n_embd_v_gqa();
        const int64_t n_embd_gqa = n_embd_v_gqa;
        const int64_t n_vocab = hparams.n_vocab;
        const int64_t n_vocab_type = hparams.n_vocab_type;
        const int64_t n_ff = hparams.n_ff;
        const int64_t n_expert = hparams.n_expert;

        if (n_expert > 0 && hparams.n_expert_used == 0)
        {
            throw std::runtime_error("model has expert layers but no expert layers are used");
        }

        GGML_ASSERT(n_embd_gqa == n_embd_k_gqa);

        ggml_context * ctx_input = ctx_map.at(model.buft_input.buft);
        ggml_context * ctx_output = ctx_map.at(model.buft_output.buft);
        ggml_context * ctx_output_split = ctx_map.at(model.buft_output.buft_matrix);
        auto ctx_for_layer = [&](int i) { return ctx_map.at(model.buft_layer[i].buft); };
        auto ctx_for_layer_split = [&](int i) { return ctx_map.at(model.buft_layer[i].buft_matrix); };

        model.layers.resize(n_layer);

        const auto tn = LLM_TN(model.arch);
        switch (model.arch)
        {
            case LLM_ARCH_LLAMA:
            case LLM_ARCH_REFACT:
            case LLM_ARCH_MINICPM: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    if (model.arch != LLM_ARCH_MINICPM)
                    {
                        model.output = ml.create_tensor(
                            ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                        // if output is nullptr, init from the input tok embed
                        if (model.output == nullptr)
                        {
                            model.output = ml.create_tensor(
                                ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                        }
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    // optional bias tensors
                    layer.bq
                        = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.bk = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.bv = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.bo = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    if (n_expert == 0)
                    {
                        layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                        layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                        layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    }
                    else
                    {
                        layer.ffn_gate_inp = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_GATE_INP, "weight", i), {n_embd, n_expert});

                        layer.ffn_gate_exps = ml.create_tensor(
                            ctx_split,
                            tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i),
                            {n_embd, n_ff, n_expert},
                            llama_model_loader::TENSOR_NOT_REQUIRED);
                        if (layer.ffn_gate_exps)
                        {
                            layer.ffn_down_exps
                                = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i), {n_ff, n_embd, n_expert});
                            layer.ffn_up_exps
                                = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i), {n_embd, n_ff, n_expert});
                        }
                        else
                        {
                            // merge split expert into a single tensor for compatibility with older models
                            // requires disabling mmap
                            use_mmap_buffer = false;

                            ggml_type type_gate = ml.require_tensor_meta(tn(LLM_TENSOR_FFN_GATE_EXP, "weight", i, 0).c_str())->type;
                            ggml_type type_down = ml.require_tensor_meta(tn(LLM_TENSOR_FFN_DOWN_EXP, "weight", i, 0).c_str())->type;
                            ggml_type type_up = ml.require_tensor_meta(tn(LLM_TENSOR_FFN_UP_EXP, "weight", i, 0).c_str())->type;

                            layer.ffn_gate_exps = ggml_new_tensor_3d(ctx_split, type_gate, n_embd, n_ff, n_expert);
                            layer.ffn_down_exps = ggml_new_tensor_3d(ctx_split, type_down, n_ff, n_embd, n_expert);
                            layer.ffn_up_exps = ggml_new_tensor_3d(ctx_split, type_up, n_embd, n_ff, n_expert);

                            ggml_set_name(layer.ffn_gate_exps, tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i).c_str());
                            ggml_set_name(layer.ffn_down_exps, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i).c_str());
                            ggml_set_name(layer.ffn_up_exps, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i).c_str());

                            for (uint32_t x = 0; x < n_expert; ++x)
                            {
                                // the individual experts are loaded into a view of the merged tensor
                                ml.create_tensor_as_view(
                                    ctx_split,
                                    layer.ffn_gate_exps,
                                    tn(LLM_TENSOR_FFN_GATE_EXP, "weight", i, x),
                                    {n_embd, n_ff},
                                    layer.ffn_gate_exps->nb[2] * x);
                                ml.create_tensor_as_view(
                                    ctx_split,
                                    layer.ffn_down_exps,
                                    tn(LLM_TENSOR_FFN_DOWN_EXP, "weight", i, x),
                                    {n_ff, n_embd},
                                    layer.ffn_down_exps->nb[2] * x);
                                ml.create_tensor_as_view(
                                    ctx_split,
                                    layer.ffn_up_exps,
                                    tn(LLM_TENSOR_FFN_UP_EXP, "weight", i, x),
                                    {n_embd, n_ff},
                                    layer.ffn_up_exps->nb[2] * x);
                            }
                        }
                    }
                }
            }
            break;
            case LLM_ARCH_GROK: {
                if (n_expert == 0)
                {
                    throw std::runtime_error("Grok model cannot have zero experts");
                }

                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    // if output is nullptr, init from the input tok embed
                    if (model.output == nullptr)
                    {
                        model.output = ml.create_tensor(
                            ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.attn_out_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT_NORM, "weight", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_gate_inp = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_GATE_INP, "weight", i), {n_embd, n_expert});

                    layer.ffn_gate_exps = ml.create_tensor(
                        ctx_split,
                        tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i),
                        {n_embd, n_ff, n_expert},
                        llama_model_loader::TENSOR_NOT_REQUIRED);
                    if (layer.ffn_gate_exps)
                    {
                        layer.ffn_down_exps
                            = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i), {n_ff, n_embd, n_expert});
                        layer.ffn_up_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i), {n_embd, n_ff, n_expert});
                    }
                    else
                    {
                        // merge split expert into a single tensor for compatibility with older models
                        // requires disabling mmap
                        use_mmap_buffer = false;

                        ggml_type type_gate = ml.require_tensor_meta(tn(LLM_TENSOR_FFN_GATE_EXP, "weight", i, 0).c_str())->type;
                        ggml_type type_down = ml.require_tensor_meta(tn(LLM_TENSOR_FFN_DOWN_EXP, "weight", i, 0).c_str())->type;
                        ggml_type type_up = ml.require_tensor_meta(tn(LLM_TENSOR_FFN_UP_EXP, "weight", i, 0).c_str())->type;

                        layer.ffn_gate_exps = ggml_new_tensor_3d(ctx_split, type_gate, n_embd, n_ff, n_expert);
                        layer.ffn_down_exps = ggml_new_tensor_3d(ctx_split, type_down, n_ff, n_embd, n_expert);
                        layer.ffn_up_exps = ggml_new_tensor_3d(ctx_split, type_up, n_embd, n_ff, n_expert);

                        ggml_set_name(layer.ffn_gate_exps, tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i).c_str());
                        ggml_set_name(layer.ffn_down_exps, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i).c_str());
                        ggml_set_name(layer.ffn_up_exps, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i).c_str());

                        for (uint32_t x = 0; x < n_expert; ++x)
                        {
                            // the individual experts are loaded into a view of the merged tensor
                            ml.create_tensor_as_view(
                                ctx_split,
                                layer.ffn_gate_exps,
                                tn(LLM_TENSOR_FFN_GATE_EXP, "weight", i, x),
                                {n_embd, n_ff},
                                layer.ffn_gate_exps->nb[2] * x);
                            ml.create_tensor_as_view(
                                ctx_split,
                                layer.ffn_down_exps,
                                tn(LLM_TENSOR_FFN_DOWN_EXP, "weight", i, x),
                                {n_ff, n_embd},
                                layer.ffn_down_exps->nb[2] * x);
                            ml.create_tensor_as_view(
                                ctx_split,
                                layer.ffn_up_exps,
                                tn(LLM_TENSOR_FFN_UP_EXP, "weight", i, x),
                                {n_embd, n_ff},
                                layer.ffn_up_exps->nb[2] * x);
                        }
                    }

                    layer.layer_out_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_LAYER_OUT_NORM, "weight", i), {n_embd});
                }
            }
            break;
            case LLM_ARCH_DBRX: {
                if (n_expert == 0)
                {
                    throw std::runtime_error("DBRX model cannot have zero experts");
                }

                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.attn_out_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT_NORM, "weight", i), {n_embd});

                    layer.ffn_gate_inp = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_GATE_INP, "weight", i), {n_embd, n_expert});
                    layer.ffn_gate_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i), {n_embd, n_ff, n_expert});
                    layer.ffn_down_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i), {n_ff, n_embd, n_expert});
                    layer.ffn_up_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i), {n_embd, n_ff, n_expert});
                }
            }
            break;
            case LLM_ARCH_BAICHUAN: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_FALCON: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});

                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    if (!model.output)
                    {
                        model.output = ml.create_tensor(
                            ctx_output_split,
                            tn(LLM_TENSOR_TOKEN_EMBD, "weight"),
                            {n_embd, n_vocab},
                            llama_model_loader::TENSOR_DUPLICATED); // needs to be on GPU
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.attn_norm_2 = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_NORM_2, "weight", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.attn_norm_2_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_NORM_2, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_STARCODER: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                model.pos_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_POS_EMBD, "weight"), {n_embd, hparams.n_ctx_train});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    if (!model.output)
                    {
                        // needs to be on GPU
                        model.output = ml.create_tensor(
                            ctx_output_split,
                            tn(LLM_TENSOR_TOKEN_EMBD, "weight"),
                            {n_embd, n_vocab},
                            llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.bqkv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_BERT:
            case LLM_ARCH_NOMIC_BERT: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                model.type_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_TYPES, "weight"), {n_embd, n_vocab_type});
                if (model.arch == LLM_ARCH_BERT)
                {
                    model.pos_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_POS_EMBD, "weight"), {n_embd, hparams.n_ctx_train});
                }

                model.tok_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_TOKEN_EMBD_NORM, "weight"), {n_embd});
                model.tok_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_TOKEN_EMBD_NORM, "bias"), {n_embd});

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    if (model.arch == LLM_ARCH_BERT)
                    {
                        layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                        layer.bq = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd});

                        layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                        layer.bk = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa});

                        layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                        layer.bv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa});
                    }
                    else
                    {
                        layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    }

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.attn_out_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT_NORM, "weight", i), {n_embd});
                    layer.attn_out_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT_NORM, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});

                    if (model.arch == LLM_ARCH_BERT)
                    {
                        layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});
                        layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});

                        layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});
                    }
                    else
                    {
                        layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    }

                    layer.layer_out_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_LAYER_OUT_NORM, "weight", i), {n_embd});
                    layer.layer_out_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_LAYER_OUT_NORM, "bias", i), {n_embd});
                }
            }
            break;
            case LLM_ARCH_JINA_BERT_V2: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}); // word_embeddings
                model.type_embd
                    = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_TYPES, "weight"), {n_embd, n_vocab_type}); //token_type_embeddings
                model.tok_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_TOKEN_EMBD_NORM, "weight"), {n_embd}); // LayerNorm
                model.tok_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_TOKEN_EMBD_NORM, "bias"), {n_embd}); //LayerNorm bias

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i]; // JinaBertLayer

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.bq = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd});

                    layer.attn_q_norm = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_Q_NORM, "weight", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.attn_q_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_Q_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.bk = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa});

                    layer.attn_k_norm = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_K_NORM, "weight", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.attn_k_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_K_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.bv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd}); //output_dens
                    layer.bo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd}); //output_dens

                    layer.attn_out_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT_NORM, "weight", i), {n_embd}); //output_norm
                    layer.attn_out_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT_NORM, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.layer_out_norm = ml.create_tensor(ctx_split, tn(LLM_TENSOR_LAYER_OUT_NORM, "weight", i), {n_embd});
                    layer.layer_out_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_LAYER_OUT_NORM, "bias", i), {n_embd});
                }
            }
            break;
            case LLM_ARCH_BLOOM: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                model.tok_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_TOKEN_EMBD_NORM, "weight"), {n_embd});
                model.tok_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_TOKEN_EMBD_NORM, "bias"), {n_embd});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.bqkv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_MPT: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                model.pos_embd = ml.create_tensor(
                    ctx_input, tn(LLM_TENSOR_POS_EMBD, "weight"), {n_embd, hparams.n_ctx_train}, llama_model_loader::TENSOR_NOT_REQUIRED);

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(
                        ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    if (!model.output)
                    {
                        model.output = ml.create_tensor(
                            ctx_output_split,
                            tn(LLM_TENSOR_TOKEN_EMBD, "weight"),
                            {n_embd, n_vocab},
                            llama_model_loader::TENSOR_DUPLICATED); // needs to be on GPU
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.bqkv = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b
                        = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.attn_q_norm = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_Q_NORM, "weight", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.attn_q_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_Q_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.attn_k_norm = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_K_NORM, "weight", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.attn_k_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_K_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    // AWQ ScaleActivation layer
                    layer.ffn_act
                        = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_ACT, "scales", i), {n_ff}, llama_model_loader::TENSOR_NOT_REQUIRED);
                }
            }
            break;
            case LLM_ARCH_STABLELM: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    // optional bias tensors, present in Stable LM 2 1.6B
                    layer.bq
                        = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.bk = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.bv = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    // optional q and k layernorms, present in StableLM 2 12B
                    layer.attn_q_norm = ml.create_tensor(
                        ctx_layer,
                        tn(LLM_TENSOR_ATTN_Q_NORM, "weight", i),
                        {hparams.n_embd_head_k, hparams.n_head},
                        llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.attn_k_norm = ml.create_tensor(
                        ctx_layer,
                        tn(LLM_TENSOR_ATTN_K_NORM, "weight", i),
                        {hparams.n_embd_head_k, hparams.n_head_kv},
                        llama_model_loader::TENSOR_NOT_REQUIRED);

                    // optional FFN norm, not present in StableLM 2 12B which uses parallel residual
                    layer.ffn_norm = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.ffn_norm_b = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_QWEN: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd * 3});
                    layer.bqkv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd * 3});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff / 2});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff / 2, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff / 2});
                }
            }
            break;
            case LLM_ARCH_QWEN2: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    // if output is nullptr, init from the input tok embed
                    if (model.output == nullptr)
                    {
                        model.output = ml.create_tensor(
                            ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    // optional bias tensors
                    layer.bq = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd});
                    layer.bk = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa});
                    layer.bv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_QWEN2MOE: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    // optional bias tensors
                    layer.bq = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd});
                    layer.bk = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa});
                    layer.bv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_gate_inp = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_GATE_INP, "weight", i), {n_embd, n_expert});

                    GGML_ASSERT(hparams.n_expert > 0);
                    GGML_ASSERT(hparams.n_expert_used > 0);

                    // MoE branch
                    auto n_ff_exp = n_ff / hparams.n_expert_used;
                    layer.ffn_gate_exps
                        = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i), {n_embd, n_ff_exp, n_expert});
                    layer.ffn_down_exps
                        = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i), {n_ff_exp, n_embd, n_expert});
                    layer.ffn_up_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i), {n_embd, n_ff_exp, n_expert});

                    // Shared expert branch
                    layer.ffn_gate_inp_shexp = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_GATE_INP_SHEXP, "weight", i), {n_embd});
                    layer.ffn_gate_shexp = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE_SHEXP, "weight", i), {n_embd, n_ff});
                    layer.ffn_down_shexp = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN_SHEXP, "weight", i), {n_ff, n_embd});
                    layer.ffn_up_shexp = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP_SHEXP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_PHI2: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                    model.output_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT, "bias"), {n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wqkv = ml.create_tensor(
                        ctx_split,
                        tn(LLM_TENSOR_ATTN_QKV, "weight", i),
                        {n_embd, n_embd + 2 * n_embd_gqa},
                        llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.bqkv = ml.create_tensor(
                        ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa}, llama_model_loader::TENSOR_NOT_REQUIRED);

                    if (layer.wqkv == nullptr)
                    {
                        layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                        layer.bq = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd});

                        layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                        layer.bk = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa});

                        layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                        layer.bv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa});
                    }

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_PHI3: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wqkv = ml.create_tensor(
                        ctx_split,
                        tn(LLM_TENSOR_ATTN_QKV, "weight", i),
                        {n_embd, n_embd + 2 * n_embd_gqa},
                        llama_model_loader::TENSOR_NOT_REQUIRED);
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, 2 * n_ff});

                    layer.rope_long = ml.create_tensor(
                        ctx_layer,
                        tn(LLM_TENSOR_ROPE_FACTORS_LONG, "weight"),
                        {n_embd_head / 2},
                        llama_model_loader::TENSOR_NOT_REQUIRED | (i != 0 ? llama_model_loader::TENSOR_DUPLICATED : 0));
                    layer.rope_short = ml.create_tensor(
                        ctx_layer,
                        tn(LLM_TENSOR_ROPE_FACTORS_SHORT, "weight"),
                        {n_embd_head / 2},
                        llama_model_loader::TENSOR_NOT_REQUIRED | (i != 0 ? llama_model_loader::TENSOR_DUPLICATED : 0));
                }
            }
            break;
            case LLM_ARCH_PLAMO: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_GPT2: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                model.pos_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_POS_EMBD, "weight"), {n_embd, hparams.n_ctx_train});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.bqkv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_CODESHELL: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.bqkv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_ORION: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }
                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_INTERNLM2: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    // layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2*n_embd_gqa});
                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_GEMMA: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                model.output = ml.create_tensor(
                    ctx_output,
                    tn(LLM_TENSOR_TOKEN_EMBD, "weight"),
                    {n_embd, n_vocab},
                    llama_model_loader::TENSOR_DUPLICATED); // same as tok_embd, duplicated to allow offloading

                const int64_t n_ff = hparams.n_ff;
                const int64_t n_embd_head_k = hparams.n_embd_head_k;
                const int64_t n_embd_k_gqa = hparams.n_embd_k_gqa();
                const int64_t n_embd_v_gqa = hparams.n_embd_v_gqa();

                for (uint32_t i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd_head_k * hparams.n_head});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_k_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_v_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd_head_k * hparams.n_head, n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                }
            }
            break;
            case LLM_ARCH_STARCODER2: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});

                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    // if output is nullptr, init from the input tok embed
                    if (model.output == nullptr)
                    {
                        model.output = ml.create_tensor(
                            ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    // optional bias tensors
                    layer.bq = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q, "bias", i), {n_embd});
                    layer.bk = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_K, "bias", i), {n_embd_gqa});
                    layer.bv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_V, "bias", i), {n_embd_gqa});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    // optional bias tensors
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_MAMBA: {
                const int64_t d_conv = hparams.ssm_d_conv;
                const int64_t d_inner = hparams.ssm_d_inner;
                const int64_t d_state = hparams.ssm_d_state;
                const int64_t dt_rank = hparams.ssm_dt_rank;
                // only an expansion factor of 2 is supported for now
                GGML_ASSERT(2 * n_embd == d_inner);

                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});

                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    // if output is nullptr, init from the input tok embed, duplicated to allow offloading
                    if (model.output == nullptr)
                    {
                        model.output = ml.create_tensor(
                            ctx_output_split,
                            tn(LLM_TENSOR_TOKEN_EMBD, "weight"),
                            {n_embd, n_vocab},
                            llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    // norm
                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.ssm_in = ml.create_tensor(ctx_split, tn(LLM_TENSOR_SSM_IN, "weight", i), {n_embd, 2 * d_inner});

                    layer.ssm_conv1d = ml.create_tensor(ctx_split, tn(LLM_TENSOR_SSM_CONV1D, "weight", i), {d_conv, d_inner});
                    layer.ssm_conv1d_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_SSM_CONV1D, "bias", i), {d_inner});

                    layer.ssm_x = ml.create_tensor(ctx_split, tn(LLM_TENSOR_SSM_X, "weight", i), {d_inner, dt_rank + 2 * d_state});

                    layer.ssm_dt = ml.create_tensor(ctx_split, tn(LLM_TENSOR_SSM_DT, "weight", i), {dt_rank, d_inner});
                    layer.ssm_dt_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_SSM_DT, "bias", i), {d_inner});

                    // no "weight" suffix for these
                    layer.ssm_a = ml.create_tensor(ctx_split, tn(LLM_TENSOR_SSM_A, i), {d_state, d_inner});
                    layer.ssm_d = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_SSM_D, i), {d_inner});

                    // out_proj
                    layer.ssm_out = ml.create_tensor(ctx_split, tn(LLM_TENSOR_SSM_OUT, "weight", i), {d_inner, n_embd});
                }
            }
            break;
            case LLM_ARCH_XVERSE: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }
                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);
                    auto & layer = model.layers[i];
                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_COMMAND_R: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    // init output from the input tok embed
                    model.output = ml.create_tensor(
                        ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    if (n_layer >= 64)
                    {
                        layer.attn_q_norm
                            = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_Q_NORM, "weight", i), {hparams.n_embd_head_k, hparams.n_head});
                        layer.attn_k_norm = ml.create_tensor(
                            ctx_layer, tn(LLM_TENSOR_ATTN_K_NORM, "weight", i), {hparams.n_embd_head_k, hparams.n_head_kv});
                    }

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_OLMO: // adapted from LLM_ARCH_LLAMA with norm params removed
            {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    // if output is nullptr, init from the input tok embed
                    if (model.output == nullptr)
                    {
                        model.output = ml.create_tensor(
                            ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});


                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_ff});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                }
            }
            break;
            case LLM_ARCH_GPTNEOX: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});
                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output_norm_b = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "bias"), {n_embd});
                    model.output = ml.create_tensor(ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab});
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});
                    layer.attn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "bias", i), {n_embd});

                    layer.wqkv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_QKV, "weight", i), {n_embd, n_embd + 2 * n_embd_gqa});
                    layer.bqkv = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_QKV, "bias", i), {n_embd + 2 * n_embd_gqa});

                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});
                    layer.bo = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_OUT, "bias", i), {n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});
                    layer.ffn_norm_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "bias", i), {n_embd});

                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_ff, n_embd});
                    layer.ffn_down_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_DOWN, "bias", i), {n_embd});

                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_ff});
                    layer.ffn_up_b = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_UP, "bias", i), {n_ff});
                }
            }
            break;
            case LLM_ARCH_ARCTIC: {
                model.tok_embd = ml.create_tensor(ctx_input, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab});

                // output
                {
                    model.output_norm = ml.create_tensor(ctx_output, tn(LLM_TENSOR_OUTPUT_NORM, "weight"), {n_embd});
                    model.output = ml.create_tensor(
                        ctx_output_split, tn(LLM_TENSOR_OUTPUT, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_NOT_REQUIRED);
                    // if output is nullptr, init from the input tok embed
                    if (model.output == nullptr)
                    {
                        model.output = ml.create_tensor(
                            ctx_output, tn(LLM_TENSOR_TOKEN_EMBD, "weight"), {n_embd, n_vocab}, llama_model_loader::TENSOR_DUPLICATED);
                    }
                }

                for (int i = 0; i < n_layer; ++i)
                {
                    ggml_context * ctx_layer = ctx_for_layer(i);
                    ggml_context * ctx_split = ctx_for_layer_split(i);

                    auto & layer = model.layers[i];

                    layer.attn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_ATTN_NORM, "weight", i), {n_embd});

                    layer.wq = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_Q, "weight", i), {n_embd, n_embd});
                    layer.wk = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_K, "weight", i), {n_embd, n_embd_gqa});
                    layer.wv = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_V, "weight", i), {n_embd, n_embd_gqa});
                    layer.wo = ml.create_tensor(ctx_split, tn(LLM_TENSOR_ATTN_OUT, "weight", i), {n_embd, n_embd});

                    layer.ffn_norm = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM, "weight", i), {n_embd});

                    layer.ffn_gate = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE, "weight", i), {n_embd, n_embd});
                    layer.ffn_down = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN, "weight", i), {n_embd, n_embd});
                    layer.ffn_up = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP, "weight", i), {n_embd, n_embd});

                    layer.ffn_gate_inp = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_GATE_INP, "weight", i), {n_embd, n_expert});
                    layer.ffn_norm_exps = ml.create_tensor(ctx_layer, tn(LLM_TENSOR_FFN_NORM_EXPS, "weight", i), {n_embd});
                    layer.ffn_gate_exps
                        = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_GATE_EXPS, "weight", i), {n_embd, n_ff, n_expert}, false);
                    layer.ffn_down_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_DOWN_EXPS, "weight", i), {n_ff, n_embd, n_expert});
                    layer.ffn_up_exps = ml.create_tensor(ctx_split, tn(LLM_TENSOR_FFN_UP_EXPS, "weight", i), {n_embd, n_ff, n_expert});
                }
            }
            break;
            default:
                throw std::runtime_error("unknown architecture");
        }
    }

    ml.done_getting_tensors();

    ml.init_mappings(true, use_mlock ? &model.mlock_mmaps : nullptr);
    model.mappings.reserve(ml.mappings.size());

    // create the backend buffers
    std::vector<std::pair<ggml_context *, llama_buf_map>> ctx_bufs;
    ctx_bufs.reserve(ctx_map.size());

    // Ensure we have enough capacity for the maximum backend buffer we will potentially create
    size_t n_max_backend_buffer = ctx_map.size() * ml.files.size();
    model.bufs.reserve(n_max_backend_buffer);

    for (auto & it : ctx_map)
    {
        ggml_backend_buffer_type_t buft = it.first;
        ggml_context * ctx = it.second;

        llama_buf_map bufs;
        bufs.reserve(n_max_backend_buffer);

        // only the mmap region containing the tensors in the model is mapped to the backend buffer
        // this is important for metal with apple silicon: if the entire model could be mapped to a metal buffer, then we could just use metal for all layers
        // this allows using partial offloading when the model size exceeds the metal buffer size, but not the RAM size
        if (ml.use_mmap && use_mmap_buffer && buft == llama_default_buffer_type_cpu(true))
        {
            for (uint32_t idx = 0; idx < ml.files.size(); idx++)
            {
                void * addr = nullptr;
                size_t first, last;
                ml.get_mapping_range(&first, &last, &addr, idx, ctx);
                if (first >= last)
                {
                    continue;
                }
                ggml_backend_buffer_t buf = ggml_backend_cpu_buffer_from_ptr((char *)addr + first, last - first);
                if (buf == nullptr)
                {
                    throw std::runtime_error("unable to allocate backend CPU buffer");
                }
                model.bufs.push_back(buf);
                bufs.emplace(idx, buf);
#ifdef GGML_USE_CUDA
                if (n_layer >= n_gpu_layers)
                {
                    ggml_backend_cuda_register_host_buffer(ggml_backend_buffer_get_base(buf), ggml_backend_buffer_get_size(buf));
                }
#endif
            }
        }
#ifdef GGML_USE_METAL
        else if (ml.use_mmap && use_mmap_buffer && buft == ggml_backend_metal_buffer_type())
        {
            for (uint32_t idx = 0; idx < ml.files.size(); idx++)
            {
                const size_t max_size = ggml_get_max_tensor_size(ctx);
                void * addr = nullptr;
                size_t first, last;
                ml.get_mapping_range(&first, &last, &addr, idx, ctx);
                if (first >= last)
                {
                    continue;
                }
                ggml_backend_buffer_t buf = ggml_backend_metal_buffer_from_ptr((char *)addr + first, last - first, max_size);
                if (buf == nullptr)
                {
                    throw std::runtime_error("unable to allocate backend metal buffer");
                }
                model.bufs.push_back(buf);
                bufs.emplace(idx, buf);
            }
        }
#endif
        else
        {
            ggml_backend_buffer_t buf = ggml_backend_alloc_ctx_tensors_from_buft(ctx, buft);
            if (buf == nullptr)
            {
                throw std::runtime_error("unable to allocate backend buffer");
            }
            model.bufs.push_back(buf);
            if (use_mlock && ggml_backend_buffer_is_host(buf))
            {
                model.mlock_bufs.emplace_back(new llama_mlock);
                auto & mlock_buf = model.mlock_bufs.back();
                mlock_buf->init(ggml_backend_buffer_get_base(buf));
                mlock_buf->grow_to(ggml_backend_buffer_get_size(buf));
            }
            for (uint32_t idx = 0; idx < ml.files.size(); idx++)
            {
                bufs.emplace(idx, buf);
            }
        }

        if (bufs.empty())
        {
            throw std::runtime_error("failed to allocate buffer");
        }

        for (auto & buf : bufs)
        {
            // indicate that this buffer contains weights
            // this is used by ggml_backend_sched to improve op scheduling -> ops that use a weight are preferably scheduled to the backend that contains the weight
            ggml_backend_buffer_set_usage(buf.second, GGML_BACKEND_BUFFER_USAGE_WEIGHTS);
        }

        ctx_bufs.emplace_back(ctx, bufs);
    }

    if (llama_supports_gpu_offload())
    {
        const int n_gpu = std::min(n_gpu_layers, int(hparams.n_layer));

        LLAMA_LOG_INFO("%s: offloading %d repeating layers to GPU\n", __func__, n_gpu);
        if (n_gpu_layers > (int)hparams.n_layer)
        {
            LLAMA_LOG_INFO("%s: offloading non-repeating layers to GPU\n", __func__);
        }

        const int max_backend_supported_layers = hparams.n_layer + 1;
        const int max_offloadable_layers = hparams.n_layer + 1;

        LLAMA_LOG_INFO(
            "%s: offloaded %d/%d layers to GPU\n", __func__, std::min(n_gpu_layers, max_offloadable_layers), max_backend_supported_layers);
    }

    // print memory requirements
    for (ggml_backend_buffer_t buf : model.bufs)
    {
        LLAMA_LOG_INFO(
            "%s: %10s buffer size = %8.2f MiB\n",
            __func__,
            ggml_backend_buffer_name(buf),
            ggml_backend_buffer_get_size(buf) / 1024.0 / 1024.0);
    }

    // populate tensors_by_name
    for (ggml_context * ctx : model.ctxs)
    {
        for (auto * cur = ggml_get_first_tensor(ctx); cur != nullptr; cur = ggml_get_next_tensor(ctx, cur))
        {
            model.tensors_by_name.emplace_back(ggml_get_name(cur), cur);
        }
    }

    // load tensor data
    for (auto & it : ctx_bufs)
    {
        ggml_context * ctx = it.first;
        auto & bufs = it.second;
        if (!ml.load_all_data(ctx, bufs, use_mlock ? &model.mlock_mmaps : nullptr, progress_callback, progress_callback_user_data))
        {
            return false;
        }
    }

    if (use_mmap_buffer)
    {
        for (auto & mapping : ml.mappings)
        {
            model.mappings.emplace_back(std::move(mapping));
        }
    }

    // loading time will be recalculate after the first eval, so
    // we take page faults deferred by mmap() into consideration
    model.t_load_us = ggml_time_us() - model.t_start_us;
    return true;
}

// Returns 0 on success, -1 on error, and -2 on cancellation via llama_progress_callback
static int llama_model_load(const std::string & fname, llama_model & model, llama_model_params & params)
{
    try
    {
        llama_model_loader ml(fname, params.use_mmap, params.check_tensors, params.kv_overrides);

        model.hparams.vocab_only = params.vocab_only;

        try
        {
            llm_load_arch(ml, model);
        }
        catch (const std::exception & e)
        {
            throw std::runtime_error("error loading model architecture: " + std::string(e.what()));
        }
        try
        {
            llm_load_hparams(ml, model);
        }
        catch (const std::exception & e)
        {
            throw std::runtime_error("error loading model hyperparameters: " + std::string(e.what()));
        }
        try
        {
            llm_load_vocab(ml, model);
        }
        catch (const std::exception & e)
        {
            throw std::runtime_error("error loading model vocabulary: " + std::string(e.what()));
        }

        llm_load_print_meta(ml, model);

        if (model.vocab.type != LLAMA_VOCAB_TYPE_NONE && model.hparams.n_vocab != model.vocab.id_to_token.size())
        {
            throw std::runtime_error("vocab size mismatch");
        }

        if (params.vocab_only)
        {
            LLAMA_LOG_INFO("%s: vocab only - skipping tensors\n", __func__);
            return 0;
        }

#ifdef GGML_USE_KOMPUTE
        if (params.n_gpu_layers > 0
            && (!(model.arch == LLM_ARCH_LLAMA || model.arch == LLM_ARCH_FALCON)
                || !(
                    model.ftype == LLAMA_FTYPE_ALL_F32 || model.ftype == LLAMA_FTYPE_MOSTLY_F16 || model.ftype == LLAMA_FTYPE_MOSTLY_BF16
                    || model.ftype == LLAMA_FTYPE_MOSTLY_Q4_0 || model.ftype == LLAMA_FTYPE_MOSTLY_Q4_1)))
        {
            // TODO(cebtenzzre): propagate this error outside of llama_load_model_from_file
            LLAMA_LOG_WARN("%s: disabling Kompute due to unsupported model arch or quantization\n", __func__);
            params.n_gpu_layers = 0;
        }
#endif

#ifdef GGML_USE_SYCL
        if (params.split_mode == LLAMA_SPLIT_MODE_NONE)
        {
            ggml_backend_sycl_set_single_device_mode(params.main_gpu);
            //SYCL use device index (0, 1, 2) directly, uer input device id, then convert to device index.
            params.main_gpu = ggml_backend_sycl_get_device_index(params.main_gpu);
        }
        else
        {
            ggml_backend_sycl_set_mul_device_mode();
        }
#endif

        if (!llm_load_tensors(
                ml,
                model,
                params.n_gpu_layers,
                params.split_mode,
                params.main_gpu,
                params.tensor_split,
                params.use_mlock,
                params.progress_callback,
                params.progress_callback_user_data))
        {
            return -2;
        }
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("%s: error loading model: %s\n", __func__, err.what());
        return -1;
    }

    return 0;
}

//
// llm_build
//

using llm_build_cb = std::function<void(struct ggml_tensor * cur, const char * name, int nl)>;

enum llm_ffn_op_type
{
    LLM_FFN_SILU,
    LLM_FFN_GELU,
    LLM_FFN_RELU,
    LLM_FFN_RELU_SQR,
};

enum llm_ffn_gate_type
{
    LLM_FFN_SEQ,
    LLM_FFN_PAR, // ffn_gate is parallel to ffn_up
};

enum llm_norm_type
{
    LLM_NORM,
    LLM_NORM_RMS,
};

static struct ggml_tensor * llm_build_inp_embd(
    struct ggml_context * ctx,
    struct llama_context & lctx,
    const llama_hparams & hparams,
    const llama_batch & batch,
    struct ggml_tensor * tok_embd,
    const llm_build_cb & cb)
{
    const int64_t n_embd = hparams.n_embd;

    struct ggml_tensor * inpL;

    if (batch.token)
    {
        lctx.inp_tokens = ggml_new_tensor_1d(ctx, GGML_TYPE_I32, batch.n_tokens);
        cb(lctx.inp_tokens, "inp_tokens", -1);
        ggml_set_input(lctx.inp_tokens);

        inpL = ggml_get_rows(ctx, tok_embd, lctx.inp_tokens);
    }
    else
    {
        lctx.inp_embd = ggml_new_tensor_2d(ctx, GGML_TYPE_F32, n_embd, batch.n_tokens);
        inpL = lctx.inp_embd;
        ggml_set_input(lctx.inp_embd);
    }

    cb(inpL, "inp_embd", -1);

    return inpL;
}

static void llm_build_kv_store(
    struct ggml_context * ctx,
    const llama_hparams & hparams,
    const llama_cparams & cparams,
    const llama_kv_cache & kv,
    struct ggml_cgraph * graph,
    struct ggml_tensor * k_cur,
    struct ggml_tensor * v_cur,
    int32_t n_tokens,
    int32_t kv_head,
    const llm_build_cb & cb,
    int64_t il)
{
    const int64_t n_ctx = cparams.n_ctx;

    const int64_t n_embd_k_gqa = hparams.n_embd_k_gqa();
    const int64_t n_embd_v_gqa = hparams.n_embd_v_gqa();

    GGML_ASSERT(kv.size == n_ctx);

    struct ggml_tensor * k_cache_view
        = ggml_view_1d(ctx, kv.k_l[il], n_tokens * n_embd_k_gqa, (ggml_row_size(kv.k_l[il]->type, n_embd_k_gqa)) * kv_head);
    cb(k_cache_view, "k_cache_view", il);

    // note: storing RoPE-ed version of K in the KV cache
    ggml_build_forward_expand(graph, ggml_cpy(ctx, k_cur, k_cache_view));

    assert(v_cur->ne[0] == n_embd_v_gqa && v_cur->ne[1] == n_tokens);

    struct ggml_tensor * v_cache_view = nullptr;

    if (cparams.flash_attn)
    {
        v_cache_view = ggml_view_1d(ctx, kv.v_l[il], n_tokens * n_embd_v_gqa, (kv_head)*ggml_row_size(kv.v_l[il]->type, n_embd_v_gqa));
    }
    else
    {
        // note: the V cache is transposed when not using flash attention
        v_cache_view = ggml_view_2d(
            ctx, kv.v_l[il], n_tokens, n_embd_v_gqa, (n_ctx)*ggml_element_size(kv.v_l[il]), (kv_head)*ggml_element_size(kv.v_l[il]));

        v_cur = ggml_transpose(ctx, v_cur);
    }
    cb(v_cache_view, "v_cache_view", il);

    ggml_build_forward_expand(graph, ggml_cpy(ctx, v_cur, v_cache_view));
}

static struct ggml_tensor * llm_build_norm(
    struct ggml_context * ctx,
    struct ggml_tensor * cur,
    const llama_hparams & hparams,
    struct ggml_tensor * mw,
    struct ggml_tensor * mb,
    llm_norm_type type,
    const llm_build_cb & cb,
    int il)
{
    switch (type)
    {
        case LLM_NORM:
            cur = ggml_norm(ctx, cur, hparams.f_norm_eps);
            break;
        case LLM_NORM_RMS:
            cur = ggml_rms_norm(ctx, cur, hparams.f_norm_rms_eps);
            break;
    }

    if (mw || mb)
    {
        cb(cur, "norm", il);
    }

    if (mw)
    {
        cur = ggml_mul(ctx, cur, mw);
        if (mb)
        {
            cb(cur, "norm_w", il);
        }
    }

    if (mb)
    {
        cur = ggml_add(ctx, cur, mb);
    }

    return cur;
}

static struct ggml_tensor * llm_build_ffn(
    struct ggml_context * ctx,
    struct ggml_tensor * cur,
    struct ggml_tensor * up,
    struct ggml_tensor * up_b,
    struct ggml_tensor * gate,
    struct ggml_tensor * gate_b,
    struct ggml_tensor * down,
    struct ggml_tensor * down_b,
    struct ggml_tensor * act_scales,
    llm_ffn_op_type type_op,
    llm_ffn_gate_type type_gate,
    const llm_build_cb & cb,
    int il)
{
    struct ggml_tensor * tmp = up ? ggml_mul_mat(ctx, up, cur) : cur;
    cb(tmp, "ffn_up", il);

    if (up_b)
    {
        tmp = ggml_add(ctx, tmp, up_b);
        cb(tmp, "ffn_up_b", il);
    }

    if (gate)
    {
        switch (type_gate)
        {
            case LLM_FFN_SEQ: {
                cur = ggml_mul_mat(ctx, gate, tmp);
                cb(cur, "ffn_gate", il);
            }
            break;
            case LLM_FFN_PAR: {
                cur = ggml_mul_mat(ctx, gate, cur);
                cb(cur, "ffn_gate", il);
            }
            break;
        }

        if (gate_b)
        {
            cur = ggml_add(ctx, cur, gate_b);
            cb(cur, "ffn_gate_b", il);
        }
    }
    else
    {
        cur = tmp;
    }

    switch (type_op)
    {
        case LLM_FFN_SILU: {
            cur = ggml_silu(ctx, cur);
            cb(cur, "ffn_silu", il);
        }
        break;
        case LLM_FFN_GELU: {
            cur = ggml_gelu(ctx, cur);
            cb(cur, "ffn_gelu", il);
            if (act_scales != nullptr)
            {
                cur = ggml_div(ctx, cur, act_scales);
                cb(cur, "ffn_act", il);
            }
        }
        break;
        case LLM_FFN_RELU: {
            cur = ggml_relu(ctx, cur);
            cb(cur, "ffn_relu", il);
        }
        break;
        case LLM_FFN_RELU_SQR: {
            cur = ggml_relu(ctx, cur);
            cb(cur, "ffn_relu", il);

            cur = ggml_sqr(ctx, cur);
            cb(cur, "ffn_sqr(relu)", il);
        }
        break;
    }

    if (type_gate == LLM_FFN_PAR)
    {
        cur = ggml_mul(ctx, cur, tmp);
        cb(cur, "ffn_gate_par", il);
    }

    cur = ggml_mul_mat(ctx, down, cur);
    if (down_b)
    {
        cb(cur, "ffn_down", il);
    }

    if (down_b)
    {
        cur = ggml_add(ctx, cur, down_b);
    }

    return cur;
}

static struct ggml_tensor * llm_build_moe_ffn(
    struct ggml_context * ctx,
    struct ggml_tensor * cur,
    struct ggml_tensor * gate_inp,
    struct ggml_tensor * up_exps,
    struct ggml_tensor * gate_exps,
    struct ggml_tensor * down_exps,
    int64_t n_expert,
    int64_t n_expert_used,
    llm_ffn_op_type type_op,
    bool norm_w,
    const llm_build_cb & cb,
    int il)
{
    int64_t n_embd = cur->ne[0];
    int64_t n_tokens = cur->ne[1];

    ggml_tensor * logits = ggml_mul_mat(ctx, gate_inp, cur); // [n_expert, n_tokens]
    cb(logits, "ffn_moe_logits", il);

    ggml_tensor * probs = ggml_soft_max(ctx, logits); // [n_expert, n_tokens]
    cb(probs, "ffn_moe_probs", il);

    // select experts
    ggml_tensor * selected_experts = ggml_top_k(ctx, probs, n_expert_used); // [n_expert_used, n_tokens]
    cb(selected_experts->src[0], "ffn_moe_argsort", il);
    cb(selected_experts, "ffn_moe_topk", il);

    ggml_tensor * weights
        = ggml_get_rows(ctx, ggml_reshape_3d(ctx, probs, 1, n_expert, n_tokens), selected_experts); // [1, n_expert_used, n_tokens]
    cb(weights, "ffn_moe_weights", il);

    if (norm_w)
    {
        weights = ggml_reshape_2d(ctx, weights, n_expert_used, n_tokens);

        ggml_tensor * weights_sum = ggml_sum_rows(ctx, weights); // [1, n_tokens]
        cb(weights_sum, "ffn_moe_weights_sum", il);

        weights = ggml_div(ctx, weights, weights_sum); // [n_expert_used, n_tokens]
        cb(weights, "ffn_moe_weights_norm", il);

        weights = ggml_reshape_3d(ctx, weights, 1, n_expert_used, n_tokens);
    }

    cur = ggml_reshape_3d(ctx, cur, n_embd, 1, n_tokens);
    ggml_tensor * up = ggml_mul_mat_id(ctx, up_exps, cur, selected_experts); // [n_ff, n_expert_used, n_tokens]
    cb(up, "ffn_moe_up", il);

    ggml_tensor * gate = ggml_mul_mat_id(ctx, gate_exps, cur, selected_experts); // [n_ff, n_expert_used, n_tokens]
    cb(gate, "ffn_moe_gate", il);

    switch (type_op)
    {
        case LLM_FFN_SILU: {
            gate = ggml_silu(ctx, gate);
            cb(gate, "ffn_moe_silu", il);
        }
        break;
        case LLM_FFN_GELU: {
            gate = ggml_gelu(ctx, gate);
            cb(gate, "ffn_moe_gelu", il);
        }
        break;
        default:
            GGML_ASSERT(false);
    }

    ggml_tensor * par = ggml_mul(ctx, up, gate); // [n_ff, n_expert_used, n_tokens]
    cb(par, "ffn_moe_gate_par", il);

    ggml_tensor * experts = ggml_mul_mat_id(ctx, down_exps, par, selected_experts); // [n_embd, n_expert_used, n_tokens]
    cb(experts, "ffn_moe_down", il);

    experts = ggml_mul(ctx, experts, weights);

    // aggregate experts
    ggml_tensor * moe_out = nullptr;
    for (int i = 0; i < n_expert_used; ++i)
    {
        ggml_tensor * cur_expert = ggml_view_2d(ctx, experts, n_embd, n_tokens, experts->nb[2], i * experts->nb[1]);

        if (i == 0)
        {
            moe_out = cur_expert;
        }
        else
        {
            moe_out = ggml_add(ctx, moe_out, cur_expert);
        }
    }

    if (n_expert_used == 1)
    {
        // avoid returning a non-contiguous tensor
        moe_out = ggml_cont(ctx, moe_out);
    }

    return moe_out;
}

static struct ggml_tensor * llm_build_kqv(
    struct ggml_context * ctx,
    const llama_model & model,
    const llama_hparams & hparams,
    const llama_cparams & cparams,
    const llama_kv_cache & kv,
    struct ggml_cgraph * graph,
    struct ggml_tensor * wo,
    struct ggml_tensor * wo_b,
    struct ggml_tensor * q_cur,
    struct ggml_tensor * kq_mask,
    int32_t n_tokens,
    int32_t n_kv,
    float kq_scale,
    const llm_build_cb & cb,
    int il)
{
    const int64_t n_ctx = cparams.n_ctx;
    const int64_t n_head = hparams.n_head;
    const int64_t n_head_kv = hparams.n_head_kv;
    const int64_t n_embd_head_k = hparams.n_embd_head_k;
    const int64_t n_embd_k_gqa = hparams.n_embd_k_gqa();
    const int64_t n_embd_head_v = hparams.n_embd_head_v;
    const int64_t n_embd_v_gqa = hparams.n_embd_v_gqa();

    struct ggml_tensor * q = ggml_permute(ctx, q_cur, 0, 2, 1, 3);
    cb(q, "q", il);

    struct ggml_tensor * k = ggml_view_3d(
        ctx,
        kv.k_l[il],
        n_embd_head_k,
        n_kv,
        n_head_kv,
        ggml_row_size(kv.k_l[il]->type, n_embd_k_gqa),
        ggml_row_size(kv.k_l[il]->type, n_embd_head_k),
        0);
    cb(k, "k", il);

    struct ggml_tensor * cur;

    if (cparams.flash_attn)
    {
        GGML_UNUSED(model);
        GGML_UNUSED(n_ctx);

        // split cached v into n_head heads (not transposed)
        struct ggml_tensor * v = ggml_view_3d(
            ctx,
            kv.v_l[il],
            n_embd_head_v,
            n_kv,
            n_head_kv,
            ggml_row_size(kv.v_l[il]->type, n_embd_v_gqa),
            ggml_row_size(kv.v_l[il]->type, n_embd_head_v),
            0);
        cb(v, "v", il);

        cur = ggml_flash_attn_ext(ctx, q, k, v, kq_mask, kq_scale, hparams.f_max_alibi_bias);

        if (model.arch == LLM_ARCH_PHI2 || model.arch == LLM_ARCH_PHI3 || model.arch == LLM_ARCH_GPTNEOX)
        {
            ggml_flash_attn_ext_set_prec(cur, GGML_PREC_F32);
        }

        cur = ggml_reshape_2d(ctx, cur, n_embd_head_v * n_head, n_tokens);
    }
    else
    {
        struct ggml_tensor * kq = ggml_mul_mat(ctx, k, q);
        cb(kq, "kq", il);

        if (model.arch == LLM_ARCH_PHI2 || model.arch == LLM_ARCH_PHI3 || model.arch == LLM_ARCH_GPTNEOX)
        {
            // for this arch, we need to perform the KQ multiplication with F32 precision, otherwise we get NaNs
            // ref: https://github.com/ggerganov/llama.cpp/pull/4490#issuecomment-1859055847
            ggml_mul_mat_set_prec(kq, GGML_PREC_F32);
        }

        if (model.arch == LLM_ARCH_GROK)
        {
            // need to do the following:
            // multiply by attn_output_multiplyer of 0.08838834764831845
            // and then :
            // kq = 30 * tanh(kq / 30)
            // before the softmax below

            //try from phi2
            //ggml_mul_mat_set_prec(kq, GGML_PREC_F32);

            kq = ggml_tanh(ctx, ggml_scale(ctx, kq, 0.08838834764831845f / 30.0f));
            kq = ggml_scale(ctx, kq, 30);
        }

        kq = ggml_soft_max_ext(ctx, kq, kq_mask, kq_scale, hparams.f_max_alibi_bias);
        cb(kq, "kq_soft_max_ext", il);

        GGML_ASSERT(kv.size == n_ctx);

        // split cached v into n_head heads
        struct ggml_tensor * v = ggml_view_3d(
            ctx,
            kv.v_l[il],
            n_kv,
            n_embd_head_v,
            n_head_kv,
            ggml_element_size(kv.v_l[il]) * n_ctx,
            ggml_element_size(kv.v_l[il]) * n_ctx * n_embd_head_v,
            0);
        cb(v, "v", il);

        struct ggml_tensor * kqv = ggml_mul_mat(ctx, v, kq);
        cb(kqv, "kqv", il);

        struct ggml_tensor * kqv_merged = ggml_permute(ctx, kqv, 0, 2, 1, 3);
        cb(kqv_merged, "kqv_merged", il);

        cur = ggml_cont_2d(ctx, kqv_merged, n_embd_head_v * n_head, n_tokens);
        cb(cur, "kqv_merged_cont", il);
    }

    ggml_build_forward_expand(graph, cur);

    cur = ggml_mul_mat(ctx, wo, cur);
    if (wo_b)
    {
        cb(cur, "kqv_wo", il);
    }

    if (wo_b)
    {
        cur = ggml_add(ctx, cur, wo_b);
    }

    return cur;
}

static struct ggml_tensor * llm_build_kv(
    struct ggml_context * ctx,
    const llama_model & model,
    const llama_hparams & hparams,
    const llama_cparams & cparams,
    const llama_kv_cache & kv,
    struct ggml_cgraph * graph,
    struct ggml_tensor * wo,
    struct ggml_tensor * wo_b,
    struct ggml_tensor * k_cur,
    struct ggml_tensor * v_cur,
    struct ggml_tensor * q_cur,
    struct ggml_tensor * kq_mask,
    int32_t n_tokens,
    int32_t kv_head,
    int32_t n_kv,
    float kq_scale,
    const llm_build_cb & cb,
    int il)
{
    // these nodes are added to the graph together so that they are not reordered
    // by doing so, the number of splits in the graph is reduced
    ggml_build_forward_expand(graph, q_cur);
    ggml_build_forward_expand(graph, k_cur);
    ggml_build_forward_expand(graph, v_cur);

    llm_build_kv_store(ctx, hparams, cparams, kv, graph, k_cur, v_cur, n_tokens, kv_head, cb, il);

    struct ggml_tensor * cur;

    cur = llm_build_kqv(ctx, model, hparams, cparams, kv, graph, wo, wo_b, q_cur, kq_mask, n_tokens, n_kv, kq_scale, cb, il);
    cb(cur, "kqv_out", il);

    return cur;
}

struct llm_build_context
{
    const llama_model & model;
    llama_context & lctx;
    const llama_hparams & hparams;
    const llama_cparams & cparams;
    const llama_batch & batch;
    const llama_kv_cache & kv_self;

    const int64_t n_embd;
    const int64_t n_layer;
    const int64_t n_rot;
    const int64_t n_ctx; // user-specified context size (can be different from n_ctx_train)
    const int64_t n_head;
    const int64_t n_head_kv;
    const int64_t n_embd_head_k;
    const int64_t n_embd_k_gqa;
    const int64_t n_embd_head_v;
    const int64_t n_embd_v_gqa;
    const int64_t n_expert;
    const int64_t n_expert_used;

    const float freq_base;
    const float freq_scale;
    const float ext_factor;
    const float attn_factor;
    const float beta_fast;
    const float beta_slow;
    const float norm_eps;
    const float norm_rms_eps;

    const int32_t n_tokens;
    const int32_t n_kv; // size of KV cache to consider (n_kv <= kv_self.size)
    const int32_t n_outputs;
    const int32_t kv_head; // index of where we store new KV data in the cache
    const int32_t n_orig_ctx;

    const bool flash_attn;

    const enum llama_pooling_type pooling_type;
    const enum llama_rope_type rope_type;

    const llm_build_cb & cb;

    std::vector<uint8_t> & buf_compute_meta;

    struct ggml_context * ctx0 = nullptr;

    // TODO: consider making the entire interface noexcept
    llm_build_context(llama_context & lctx, const llama_batch & batch, const llm_build_cb & cb, bool worst_case)
        : model(lctx.model)
        , lctx(lctx)
        , hparams(model.hparams)
        , cparams(lctx.cparams)
        , batch(batch)
        , kv_self(lctx.kv_self)
        , n_embd(hparams.n_embd)
        , n_layer(hparams.n_layer)
        , n_rot(hparams.n_rot)
        , n_ctx(cparams.n_ctx)
        , n_head(hparams.n_head)
        , n_head_kv(hparams.n_head_kv)
        , n_embd_head_k(hparams.n_embd_head_k)
        , n_embd_k_gqa(hparams.n_embd_k_gqa())
        , n_embd_head_v(hparams.n_embd_head_v)
        , n_embd_v_gqa(hparams.n_embd_v_gqa())
        , n_expert(hparams.n_expert)
        , n_expert_used(hparams.n_expert_used)
        , freq_base(cparams.rope_freq_base)
        , freq_scale(cparams.rope_freq_scale)
        , ext_factor(cparams.yarn_ext_factor)
        , attn_factor(cparams.yarn_attn_factor)
        , beta_fast(cparams.yarn_beta_fast)
        , beta_slow(cparams.yarn_beta_slow)
        , norm_eps(hparams.f_norm_eps)
        , norm_rms_eps(hparams.f_norm_rms_eps)
        , n_tokens(batch.n_tokens)
        , n_kv(worst_case ? kv_self.size : kv_self.n)
        , n_outputs(worst_case ? n_tokens : lctx.n_outputs)
        , kv_head(worst_case ? (kv_self.recurrent ? 0 : kv_self.size - n_tokens) : kv_self.head)
        , n_orig_ctx(cparams.n_yarn_orig_ctx)
        , flash_attn(cparams.flash_attn)
        , pooling_type(cparams.pooling_type)
        , rope_type(hparams.rope_type)
        , cb(cb)
        , buf_compute_meta(lctx.buf_compute_meta)
    {
        // all initializations should be done in init()
    }

    void init()
    {
        ggml_init_params params = {
            .mem_size   =buf_compute_meta.size(),
            .mem_buffer =buf_compute_meta.data(),
            .no_alloc   =true,
        };

        ctx0 = ggml_init(params);

        lctx.inp_tokens = nullptr;
        lctx.inp_embd = nullptr;
        lctx.inp_pos = nullptr;
        lctx.inp_out_ids = nullptr;
        lctx.inp_KQ_mask = nullptr;
        lctx.inp_K_shift = nullptr;
        lctx.inp_mean = nullptr;
        lctx.inp_cls = nullptr;
        lctx.inp_s_copy = nullptr;
        lctx.inp_s_mask = nullptr;
        lctx.inp_s_seq = nullptr;
    }

    void free()
    {
        if (ctx0)
        {
            ggml_free(ctx0);
            ctx0 = nullptr;
        }
    }

    struct ggml_cgraph * build_k_shift()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        GGML_ASSERT(kv_self.size == n_ctx);

        lctx.inp_K_shift = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n_ctx);
        cb(lctx.inp_K_shift, "K_shift", -1);
        ggml_set_input(lctx.inp_K_shift);


        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * rope_factors = build_rope_factors(il);
            struct ggml_tensor * tmp =
                // we rotate only the first n_rot dimensions
                ggml_rope_ext_inplace(
                    ctx0,
                    ggml_view_3d(
                        ctx0,
                        kv_self.k_l[il],
                        n_embd_head_k,
                        n_head_kv,
                        n_ctx,
                        ggml_row_size(kv_self.k_l[il]->type, n_embd_head_k),
                        ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa),
                        0),
                    lctx.inp_K_shift,
                    rope_factors,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);

            cb(tmp, "K_shifted", il);
            ggml_build_forward_expand(gf, tmp);
        }

        return gf;
    }

    struct ggml_cgraph * build_s_copy()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        GGML_ASSERT(kv_self.recurrent);

        struct ggml_tensor * state_copy = build_inp_s_copy();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * conv_states = ggml_reshape_2d(ctx0, kv_self.k_l[il], hparams.n_embd_k_s(), kv_self.size);
            struct ggml_tensor * ssm_states = ggml_reshape_2d(ctx0, kv_self.v_l[il], hparams.n_embd_v_s(), kv_self.size);

            conv_states = ggml_get_rows(ctx0, conv_states, state_copy);
            ssm_states = ggml_get_rows(ctx0, ssm_states, state_copy);

            // TODO: name the intermediate tensors with cb()

            ggml_build_forward_expand(gf, ggml_cpy(ctx0, conv_states, kv_self.k_l[il]));
            ggml_build_forward_expand(gf, ggml_cpy(ctx0, ssm_states, kv_self.v_l[il]));
        }

        return gf;
    }

    struct ggml_cgraph * build_defrag(const std::vector<uint32_t> & ids)
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        for (uint32_t i = 0; i < ids.size(); ++i)
        {
            const uint32_t id = ids[i];

            if (i == id || id == ids.size())
            {
                continue;
            }

            uint32_t nm = 1;

            while (i + nm < ids.size() && ids[i + nm] == id + nm)
            {
                nm++;
            }

            for (int il = 0; il < n_layer; ++il)
            {
                ggml_tensor * view_k_src = ggml_view_2d(
                    ctx0,
                    kv_self.k_l[il],
                    n_embd_k_gqa,
                    nm,
                    ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa),
                    ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa * i));

                ggml_tensor * view_k_dst = ggml_view_2d(
                    ctx0,
                    kv_self.k_l[il],
                    n_embd_k_gqa,
                    nm,
                    ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa),
                    ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa * id));

                ggml_tensor * view_v_src;
                ggml_tensor * view_v_dst;

                if (flash_attn)
                {
                    // NOTE: the V cache is not transposed when using flash attention
                    view_v_src = ggml_view_2d(
                        ctx0,
                        kv_self.v_l[il],
                        n_embd_v_gqa,
                        nm,
                        ggml_row_size(kv_self.v_l[il]->type, n_embd_v_gqa),
                        ggml_row_size(kv_self.v_l[il]->type, n_embd_v_gqa * i));

                    view_v_dst = ggml_view_2d(
                        ctx0,
                        kv_self.v_l[il],
                        n_embd_v_gqa,
                        nm,
                        ggml_row_size(kv_self.v_l[il]->type, n_embd_v_gqa),
                        ggml_row_size(kv_self.v_l[il]->type, n_embd_v_gqa * id));
                }
                else
                {
                    view_v_src = ggml_view_2d(
                        ctx0,
                        kv_self.v_l[il],
                        nm,
                        n_embd_v_gqa,
                        ggml_row_size(kv_self.v_l[il]->type, kv_self.size),
                        ggml_row_size(kv_self.v_l[il]->type, i));

                    view_v_dst = ggml_view_2d(
                        ctx0,
                        kv_self.v_l[il],
                        nm,
                        n_embd_v_gqa,
                        ggml_row_size(kv_self.v_l[il]->type, kv_self.size),
                        ggml_row_size(kv_self.v_l[il]->type, id));
                }

                ggml_build_forward_expand(gf, ggml_cpy(ctx0, view_k_src, view_k_dst));
                ggml_build_forward_expand(gf, ggml_cpy(ctx0, view_v_src, view_v_dst));
            }

            i += nm - 1;
        }

        //LLAMA_LOG_INFO("gf->n_nodes = %d\n", gf->n_nodes);

        return gf;
    }

    struct ggml_tensor * build_inp_pos()
    {
        lctx.inp_pos = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n_tokens);
        cb(lctx.inp_pos, "inp_pos", -1);
        ggml_set_input(lctx.inp_pos);
        return lctx.inp_pos;
    }

    struct ggml_tensor * build_rope_factors(int il)
    {
        // choose long/short freq factors based on the context size
        const auto n_ctx_pre_seq = cparams.n_ctx / cparams.n_seq_max;

        if (n_ctx_pre_seq > hparams.n_yarn_orig_ctx)
        {
            return model.layers[il].rope_long;
        }

        return model.layers[il].rope_short;
    }

    struct ggml_tensor * build_inp_out_ids()
    {
        lctx.inp_out_ids = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n_outputs);
        cb(lctx.inp_out_ids, "inp_out_ids", -1);
        ggml_set_input(lctx.inp_out_ids);
        return lctx.inp_out_ids;
    }

    struct ggml_tensor * build_inp_KQ_mask(bool causal = true)
    {
        if (causal)
        {
            lctx.inp_KQ_mask = ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, n_kv, GGML_PAD(n_tokens, GGML_KQ_MASK_PAD));
        }
        else
        {
            lctx.inp_KQ_mask = ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, n_tokens, GGML_PAD(n_tokens, GGML_KQ_MASK_PAD));
        }
        cb(lctx.inp_KQ_mask, "KQ_mask", -1);
        ggml_set_input(lctx.inp_KQ_mask);
        return flash_attn ? ggml_cast(ctx0, lctx.inp_KQ_mask, GGML_TYPE_F16) : lctx.inp_KQ_mask;
    }

    struct ggml_tensor * build_inp_mean()
    {
        lctx.inp_mean = ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, n_tokens, n_tokens);
        cb(lctx.inp_mean, "inp_mean", -1);
        ggml_set_input(lctx.inp_mean);
        return lctx.inp_mean;
    }

    struct ggml_tensor * build_inp_cls()
    {
        lctx.inp_cls = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n_tokens);
        cb(lctx.inp_cls, "inp_cls", -1);
        ggml_set_input(lctx.inp_cls);
        return lctx.inp_cls;
    }

    struct ggml_tensor * build_inp_s_copy()
    {
        lctx.inp_s_copy = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, kv_self.size);
        cb(lctx.inp_s_copy, "inp_s_copy", -1);
        ggml_set_input(lctx.inp_s_copy);
        return lctx.inp_s_copy;
    }

    struct ggml_tensor * build_inp_s_mask()
    {
        lctx.inp_s_mask = ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, 1, n_kv);
        cb(lctx.inp_s_mask, "inp_s_mask", -1);
        ggml_set_input(lctx.inp_s_mask);
        return lctx.inp_s_mask;
    }

    struct ggml_tensor * build_inp_s_seq()
    {
        lctx.inp_s_seq = ggml_new_tensor_2d(ctx0, GGML_TYPE_I32, n_kv, n_tokens);
        cb(lctx.inp_s_seq, "inp_s_seq", -1);
        ggml_set_input(lctx.inp_s_seq);
        return lctx.inp_s_seq;
    }

    struct ggml_cgraph * build_llama()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        // mutable variable, needed during the last layer of the computation to skip unused tokens
        int32_t n_tokens = this->n_tokens;

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                n_tokens = n_outputs;
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            if (model.layers[il].ffn_gate_inp == nullptr)
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }
            else
            {
                // MoE branch
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_moe_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_gate_inp,
                    model.layers[il].ffn_up_exps,
                    model.layers[il].ffn_gate_exps,
                    model.layers[il].ffn_down_exps,
                    n_expert,
                    n_expert_used,
                    LLM_FFN_SILU,
                    true,
                    cb,
                    il);
                cb(cur, "ffn_moe_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "ffn_out", il);

            ggml_tensor * layer_dir = lctx.cvec.tensor_for(il);
            if (layer_dir != nullptr)
            {
                cur = ggml_add(ctx0, cur, layer_dir);
            }
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_baichuan()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = model.type == MODEL_7B ? build_inp_pos() : nullptr;

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);

                switch (model.type)
                {
                    case MODEL_7B:
                        Qcur = ggml_rope_ext(
                            ctx0,
                            ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                            inp_pos,
                            nullptr,
                            n_rot,
                            rope_type,
                            0,
                            n_orig_ctx,
                            freq_base,
                            freq_scale,
                            ext_factor,
                            attn_factor,
                            beta_fast,
                            beta_slow);
                        Kcur = ggml_rope_ext(
                            ctx0,
                            ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                            inp_pos,
                            nullptr,
                            n_rot,
                            rope_type,
                            0,
                            n_orig_ctx,
                            freq_base,
                            freq_scale,
                            ext_factor,
                            attn_factor,
                            beta_fast,
                            beta_slow);
                        break;
                    case MODEL_13B:
                        Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd / n_head, n_head, n_tokens);
                        Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd / n_head, n_head, n_tokens);
                        break;
                    default:
                        GGML_ASSERT(false);
                }
                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_xverse()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);
                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_falcon()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * attn_norm;

            attn_norm = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(attn_norm, "attn_norm", il);

            // self-attention
            {
                if (model.layers[il].attn_norm_2)
                {
                    // Falcon-40B
                    cur = llm_build_norm(
                        ctx0, inpL, hparams, model.layers[il].attn_norm_2, model.layers[il].attn_norm_2_b, LLM_NORM, cb, il);
                    cb(cur, "attn_norm_2", il);
                }
                else
                {
                    cur = attn_norm;
                }

                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);

                // using mode = 2 for neox mode
                Qcur = ggml_rope_ext(
                    ctx0,
                    Qcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    Kcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
                attn_norm = ggml_get_rows(ctx0, attn_norm, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = cur;

            // feed forward
            {
                cur = llm_build_ffn(
                    ctx0,
                    attn_norm, // !! use the attn norm, not the result
                    model.layers[il].ffn_up,
                    nullptr,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            cur = ggml_add(ctx0, cur, inpL);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        // norm
        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_grok()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        // mutable variable, needed during the last layer of the computation to skip unused tokens
        int32_t n_tokens = this->n_tokens;

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // multiply by embedding_multiplier_scale of 78.38367176906169
        inpL = ggml_scale(ctx0, inpL, 78.38367176906169f);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);


            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f,
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                n_tokens = n_outputs;
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            // Grok
            // if attn_out_norm is present then apply it before adding the input
            if (model.layers[il].attn_out_norm)
            {
                cur = llm_build_norm(ctx0, cur, hparams, model.layers[il].attn_out_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "attn_out_norm", il);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            // MoE branch
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_moe_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_gate_inp,
                model.layers[il].ffn_up_exps,
                model.layers[il].ffn_gate_exps,
                model.layers[il].ffn_down_exps,
                n_expert,
                n_expert_used,
                LLM_FFN_GELU,
                true,
                cb,
                il);
            cb(cur, "ffn_moe_out", il);

            // Grok
            // if layer_out_norm is present then apply it before adding the input
            // Idea: maybe ffn_out_norm is a better name
            if (model.layers[il].layer_out_norm)
            {
                cur = llm_build_norm(ctx0, cur, hparams, model.layers[il].layer_out_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "layer_out_norm", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "ffn_out", il);

            ggml_tensor * layer_dir = lctx.cvec.tensor_for(il);
            if (layer_dir != nullptr)
            {
                cur = ggml_add(ctx0, cur, layer_dir);
            }
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);

        // Grok
        // multiply logits by output_multiplier_scale of 0.5773502691896257

        cur = ggml_scale(ctx0, cur, 0.5773502691896257f);

        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_dbrx()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        // mutable variable, needed during the last layer of the computation to skip unused tokens
        int32_t n_tokens = this->n_tokens;

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                struct ggml_tensor * Qcur = nullptr;
                struct ggml_tensor * Kcur = nullptr;
                struct ggml_tensor * Vcur = nullptr;

                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_clamp(ctx0, cur, -hparams.f_clamp_kqv, hparams.f_clamp_kqv);
                cb(cur, "wqkv_clamped", il);

                Qcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                Kcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                n_tokens = n_outputs;
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            // MoE branch
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].attn_out_norm, nullptr, LLM_NORM, cb, il);
            cb(cur, "attn_out_norm", il);

            cur = llm_build_moe_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_gate_inp,
                model.layers[il].ffn_up_exps,
                model.layers[il].ffn_gate_exps,
                model.layers[il].ffn_down_exps,
                n_expert,
                n_expert_used,
                LLM_FFN_SILU,
                true,
                cb,
                il);
            cb(cur, "ffn_moe_out", il);

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "ffn_out", il);

            ggml_tensor * layer_dir = lctx.cvec.tensor_for(il);
            if (layer_dir != nullptr)
            {
                cur = ggml_add(ctx0, cur, layer_dir);
            }
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);

        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_starcoder()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        struct ggml_tensor * pos = ggml_get_rows(ctx0, model.pos_embd, inp_pos);
        cb(pos, "pos_embd", -1);

        inpL = ggml_add(ctx0, inpL, pos);
        cb(inpL, "inpL", -1);

        for (int il = 0; il < n_layer; ++il)
        {
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                cb(cur, "bqkv", il);

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // add the input
            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
            cb(ffn_inp, "ffn_inp", il);

            // FF
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            inpL = ggml_add(ctx0, cur, ffn_inp);
            cb(inpL, "l_out", il);
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_refact()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);

                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);
                cb(Kcur, "Kcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                cb(Qcur, "Qcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_bert()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();

        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;
        struct ggml_tensor * inp_pos = nullptr;

        if (model.arch != LLM_ARCH_JINA_BERT_V2)
        {
            inp_pos = build_inp_pos();
        }
        struct ggml_tensor * inp_mean = build_inp_mean();
        struct ggml_tensor * inp_cls = build_inp_cls();

        // construct input embeddings (token, type, position)
        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // token types are hardcoded to zero ("Sentence A")
        struct ggml_tensor * type_row0 = ggml_view_1d(ctx0, model.type_embd, n_embd, 0);
        inpL = ggml_add(ctx0, inpL, type_row0);
        if (model.arch == LLM_ARCH_BERT)
        {
            inpL = ggml_add(ctx0, ggml_get_rows(ctx0, model.pos_embd, inp_pos), inpL);
        }
        cb(inpL, "inp_embd", -1);

        // embed layer norm
        inpL = llm_build_norm(ctx0, inpL, hparams, model.tok_norm, model.tok_norm_b, LLM_NORM, cb, -1);
        cb(inpL, "inp_norm", -1);

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask(false);

        // iterate layers
        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * cur = inpL;

            struct ggml_tensor * Qcur;
            struct ggml_tensor * Kcur;
            struct ggml_tensor * Vcur;

            // self-attention
            if (model.arch == LLM_ARCH_BERT || model.arch == LLM_ARCH_JINA_BERT_V2)
            {
                Qcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wq, cur), model.layers[il].bq);
                cb(Qcur, "Qcur", il);

                if (model.layers[il].attn_q_norm)
                {
                    Qcur = llm_build_norm(
                        ctx0, Qcur, hparams, model.layers[il].attn_q_norm, model.layers[il].attn_q_norm_b, LLM_NORM, cb, il);
                }

                Kcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wk, cur), model.layers[il].bk);
                cb(Kcur, "Kcur", il);

                if (model.layers[il].attn_k_norm)
                {
                    Kcur = llm_build_norm(
                        ctx0, Kcur, hparams, model.layers[il].attn_k_norm, model.layers[il].attn_k_norm_b, LLM_NORM, cb, il);
                }
                Vcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wv, cur), model.layers[il].bv);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);
            }
            else
            {
                // compute Q and K and RoPE them
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                Qcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                Kcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);
            }

            struct ggml_tensor * q = ggml_permute(ctx0, Qcur, 0, 2, 1, 3);
            struct ggml_tensor * k = ggml_cont(ctx0, ggml_permute(ctx0, Kcur, 0, 2, 1, 3));

            struct ggml_tensor * kq = ggml_mul_mat(ctx0, k, q);
            cb(kq, "kq", il);

            kq = ggml_soft_max_ext(ctx0, kq, KQ_mask, 1.0f / sqrtf(float(n_embd_head)), hparams.f_max_alibi_bias);
            cb(kq, "kq_soft_max_ext", il);

            struct ggml_tensor * v = ggml_cont(ctx0, ggml_transpose(ctx0, ggml_reshape_2d(ctx0, Vcur, n_embd_gqa, n_tokens)));
            cb(v, "v", il);

            struct ggml_tensor * kqv = ggml_mul_mat(ctx0, ggml_reshape_3d(ctx0, v, n_tokens, n_embd_head, n_head_kv), kq);
            cb(kqv, "kqv", il);

            struct ggml_tensor * kqv_merged = ggml_permute(ctx0, kqv, 0, 2, 1, 3);
            cb(kqv_merged, "kqv_merged", il);

            cur = ggml_cont_2d(ctx0, kqv_merged, n_embd_gqa, n_tokens);
            cb(cur, "kqv_merged_cont", il);

            ggml_build_forward_expand(gf, cur);

            cur = ggml_mul_mat(ctx0, model.layers[il].wo, cur);
            if (model.layers[il].bo)
            {
                cb(cur, "kqv_wo", il);
            }

            if (model.layers[il].bo)
            {
                cur = ggml_add(ctx0, cur, model.layers[il].bo);
            }
            cb(cur, "kqv_out", il);

            if (il == n_layer - 1 && pooling_type == LLAMA_POOLING_TYPE_NONE)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // re-add the layer input
            cur = ggml_add(ctx0, cur, inpL);

            // attention layer norm
            cur = llm_build_norm(ctx0, cur, hparams, model.layers[il].attn_out_norm, model.layers[il].attn_out_norm_b, LLM_NORM, cb, il);

            struct ggml_tensor * ffn_inp = cur;
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            if (model.arch == LLM_ARCH_BERT)
            {
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
            }
            else if (model.arch == LLM_ARCH_JINA_BERT_V2)
            {
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_PAR,
                    cb,
                    il);
            }
            else
            {
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
            }
            cb(cur, "ffn_out", il);

            // attentions bypass the intermediate layer
            cur = ggml_add(ctx0, cur, ffn_inp);

            // output layer norm
            cur = llm_build_norm(ctx0, cur, hparams, model.layers[il].layer_out_norm, model.layers[il].layer_out_norm_b, LLM_NORM, cb, il);

            // input for next layer
            inpL = cur;
        }

        // final output
        cur = inpL;
        cb(cur, "result_embd", -1);

        // pooling layer
        switch (pooling_type)
        {
            case LLAMA_POOLING_TYPE_NONE: {
                // nop
            }
            break;
            case LLAMA_POOLING_TYPE_MEAN: {
                cur = ggml_mul_mat(ctx0, ggml_cont(ctx0, ggml_transpose(ctx0, cur)), inp_mean);
                cb(cur, "result_embd_pooled", -1);
            }
            break;
            case LLAMA_POOLING_TYPE_CLS: {
                cur = ggml_get_rows(ctx0, cur, inp_cls);
                cb(cur, "result_embd_pooled", -1);
            }
            break;
            case LLAMA_POOLING_TYPE_UNSPECIFIED: {
                GGML_ASSERT(false && "Invalid pooling type");
            }
            break;
        }

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_bloom()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        inpL = llm_build_norm(ctx0, inpL, hparams, model.tok_norm, model.tok_norm_b, LLM_NORM, cb, -1);
        cb(inpL, "inp_norm", -1);

        for (int il = 0; il < n_layer; ++il)
        {
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                cb(cur, "bqkv", il);

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // Add the input
            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
            cb(ffn_inp, "ffn_inp", il);

            // FF
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            inpL = ggml_add(ctx0, cur, ffn_inp);
            cb(inpL, "l_out", il);
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_mpt()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * pos;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        if (model.pos_embd)
        {
            // inp_pos - contains the positions
            struct ggml_tensor * inp_pos = build_inp_pos();
            pos = ggml_get_rows(ctx0, model.pos_embd, inp_pos);
            cb(pos, "pos_embd", -1);

            inpL = ggml_add(ctx0, inpL, pos);
            cb(inpL, "inpL", -1);
        }

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * attn_norm;

            attn_norm = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(attn_norm, "attn_norm", il);

            // self-attention
            {
                cur = attn_norm;

                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                if (model.layers[il].bqkv)
                {
                    cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                    cb(cur, "bqkv", il);
                }

                if (hparams.f_clamp_kqv > 0.0f)
                {
                    cur = ggml_clamp(ctx0, cur, -hparams.f_clamp_kqv, hparams.f_clamp_kqv);
                    cb(cur, "wqkv_clamped", il);
                }

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                // Q/K Layernorm
                if (model.layers[il].attn_q_norm)
                {
                    Qcur = llm_build_norm(
                        ctx0, Qcur, hparams, model.layers[il].attn_q_norm, model.layers[il].attn_q_norm_b, LLM_NORM, cb, il);
                    cb(Qcur, "Qcur", il);

                    Kcur = llm_build_norm(
                        ctx0, Kcur, hparams, model.layers[il].attn_k_norm, model.layers[il].attn_k_norm_b, LLM_NORM, cb, il);
                    cb(Kcur, "Kcur", il);

                    Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                    Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);

                    cur = llm_build_kv(
                        ctx0,
                        model,
                        hparams,
                        cparams,
                        kv_self,
                        gf,
                        model.layers[il].wo,
                        model.layers[il].bo,
                        Kcur,
                        Vcur,
                        Qcur,
                        KQ_mask,
                        n_tokens,
                        kv_head,
                        n_kv,
                        1.0f / sqrtf(float(n_embd_head)),
                        cb,
                        il);
                }
                else
                {
                    Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);

                    cur = llm_build_kv(
                        ctx0,
                        model,
                        hparams,
                        cparams,
                        kv_self,
                        gf,
                        model.layers[il].wo,
                        model.layers[il].bo,
                        Kcur,
                        Vcur,
                        Qcur,
                        KQ_mask,
                        n_tokens,
                        kv_head,
                        n_kv,
                        1.0f / sqrtf(float(n_embd_head)),
                        cb,
                        il);
                }
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // Add the input
            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
            cb(ffn_inp, "ffn_inp", il);

            // feed forward
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    model.layers[il].ffn_act,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_stablelm()
    {
        struct ggml_cgraph * gf = ggml_new_graph(ctx0);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            struct ggml_tensor * inpSA = cur;

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                cb(Qcur, "Qcur", il);
                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);
                cb(Kcur, "Kcur", il);

                if (model.layers[il].attn_q_norm)
                {
                    Qcur = llm_build_norm(ctx0, Qcur, hparams, model.layers[il].attn_q_norm, nullptr, LLM_NORM, cb, il);
                    cb(Qcur, "Qcur", il);
                }
                if (model.layers[il].attn_k_norm)
                {
                    Kcur = llm_build_norm(ctx0, Kcur, hparams, model.layers[il].attn_k_norm, nullptr, LLM_NORM, cb, il);
                    cb(Kcur, "Kcur", il);
                }


                Qcur = ggml_rope_ext(
                    ctx0,
                    Qcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    Kcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            {
                if (model.layers[il].ffn_norm)
                {
                    cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                    cb(cur, "ffn_norm", il);
                }
                else
                {
                    // parallel residual
                    cur = inpSA;
                }
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_qwen()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                cb(cur, "bqkv", il);

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 2 * sizeof(float) * (n_embd)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);

                // using mode = 2 for neox mode
                Qcur = ggml_rope_ext(
                    ctx0,
                    Qcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    Kcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward forward
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_qwen2()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_up,
                nullptr,
                model.layers[il].ffn_gate,
                nullptr,
                model.layers[il].ffn_down,
                nullptr,
                nullptr,
                LLM_FFN_SILU,
                LLM_FFN_PAR,
                cb,
                il);
            cb(cur, "ffn_out", il);

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_qwen2moe()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        // mutable variable, needed during the last layer of the computation to skip unused tokens
        int32_t n_tokens = this->n_tokens;

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self_attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                n_tokens = n_outputs;
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // MoE branch
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            ggml_tensor * moe_out = llm_build_moe_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_gate_inp,
                model.layers[il].ffn_up_exps,
                model.layers[il].ffn_gate_exps,
                model.layers[il].ffn_down_exps,
                n_expert,
                n_expert_used,
                LLM_FFN_SILU,
                false,
                cb,
                il);
            cb(cur, "ffn_moe_out", il);

            // FFN shared expert
            {
                ggml_tensor * cur_gate_inp = ggml_mul_mat(ctx0, model.layers[il].ffn_gate_inp_shexp, cur);
                cb(cur_gate_inp, "ffn_shexp_gate_inp", il);

                // sigmoid
                ggml_tensor * cur_gate = ggml_div(ctx0, ggml_silu(ctx0, cur_gate_inp), cur_gate_inp);
                cb(cur_gate, "ffn_shexp_gate", il);

                ggml_tensor * cur_ffn = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up_shexp,
                    nullptr,
                    model.layers[il].ffn_gate_shexp,
                    nullptr,
                    model.layers[il].ffn_down_shexp,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur_ffn, "ffn_shexp", il);

                ggml_tensor * ffn_shexp_out = ggml_mul(ctx0, cur_ffn, cur_gate);
                cb(ffn_shexp_out, "ffn_shexp_out", il);

                moe_out = ggml_add(ctx0, moe_out, ffn_shexp_out);
                cb(moe_out, "ffn_out", il);

                cur = moe_out;
            }

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_phi2()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * attn_norm_output;
        struct ggml_tensor * ffn_output;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            attn_norm_output
                = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(attn_norm_output, "attn_norm", il);

            // self-attention
            {
                struct ggml_tensor * Qcur = nullptr;
                struct ggml_tensor * Kcur = nullptr;
                struct ggml_tensor * Vcur = nullptr;

                if (model.layers[il].wqkv)
                {
                    cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, attn_norm_output);
                    cb(cur, "wqkv", il);

                    cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                    cb(cur, "bqkv", il);

                    Qcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                    Kcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                    Vcur = ggml_cont(
                        ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));
                }
                else
                {
                    Qcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wq, attn_norm_output), model.layers[il].bq);
                    Kcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wk, attn_norm_output), model.layers[il].bk);
                    Vcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wv, attn_norm_output), model.layers[il].bv);
                }

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);

                Qcur = ggml_rope_ext(
                    ctx0,
                    Qcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                // with phi2, we scale the Q to avoid precision issues
                // ref: https://github.com/ml-explore/mlx-examples/blob/08e862336ade809bc37d1035f94b359e7d1a5152/phi2/phi2.py#L64-L66
                Qcur = ggml_scale(ctx0, Qcur, 1.0f / sqrtf(float(n_embd_head)));
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    Kcur,
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f,
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
                attn_norm_output = ggml_get_rows(ctx0, attn_norm_output, inp_out_ids);
            }

            // FF
            {
                ffn_output = llm_build_ffn(
                    ctx0,
                    attn_norm_output,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(ffn_output, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, ffn_output);
            cb(cur, "l_out", il);

            cur = ggml_add(ctx0, cur, inpL);
            cb(cur, "l_out", il);

            inpL = cur;
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output_no_bias", -1);

        cur = ggml_add(ctx0, cur, model.output_b);
        cb(cur, "result_output", -1);
        ggml_build_forward_expand(gf, cur);
        return gf;
    }

    struct ggml_cgraph * build_phi3()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            auto residual = inpL;

            // self-attention
            {
                // rope freq factors for 128k context
                struct ggml_tensor * rope_factors = build_rope_factors(il);

                struct ggml_tensor * attn_norm_output
                    = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(attn_norm_output, "attn_norm", il);

                struct ggml_tensor * Qcur = nullptr;
                struct ggml_tensor * Kcur = nullptr;
                struct ggml_tensor * Vcur = nullptr;

                if (model.layers[il].wqkv)
                {
                    cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, attn_norm_output);
                    cb(cur, "wqkv", il);

                    Qcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                    Kcur = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                    Vcur = ggml_cont(
                        ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));
                }
                else
                {
                    Qcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wq, attn_norm_output), model.layers[il].bq);
                    Kcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wk, attn_norm_output), model.layers[il].bk);
                    Vcur = ggml_add(ctx0, ggml_mul_mat(ctx0, model.layers[il].wv, attn_norm_output), model.layers[il].bv);
                }

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);
                Kcur = ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens);

                Qcur = ggml_rope_ext(
                    ctx0,
                    Qcur,
                    inp_pos,
                    rope_factors,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Qcur = ggml_scale(ctx0, Qcur, 1.0f / sqrtf(float(n_embd_head)));
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    Kcur,
                    inp_pos,
                    rope_factors,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f,
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                residual = ggml_get_rows(ctx0, residual, inp_out_ids);
            }

            cur = ggml_add(ctx0, cur, residual);
            residual = cur;

            cur = llm_build_norm(ctx0, cur, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            // FF
            // special-case: the up and gate tensors are merged into a single tensor
            // TODO: support into llm_build_ffn
            {
                struct ggml_tensor * up = ggml_mul_mat(ctx0, model.layers[il].ffn_up, cur);
                cb(up, "ffn_up", il);

                auto g = ggml_cont(ctx0, ggml_view_2d(ctx0, up, up->ne[0] / 2, up->ne[1], ggml_row_size(up->type, up->ne[0]), 0));
                auto y
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, up, up->ne[0] / 2, up->ne[1], ggml_row_size(up->type, up->ne[0]), up->nb[1] / 2));

                y = ggml_mul(ctx0, y, ggml_silu(ctx0, g));
                cb(y, "ffn_gate", il);

                auto down = ggml_mul_mat(ctx0, model.layers[il].ffn_down, y);
                cb(down, "ffn_down", il);

                cur = down;
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, residual, cur);
            cb(cur, "l_out", il);

            inpL = cur;
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }


    struct ggml_cgraph * build_plamo()
    {
        struct ggml_cgraph * gf = ggml_new_graph(ctx0);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            struct ggml_tensor * attention_norm = cur;

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_rot, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_embd_head,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_rot, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_embd_head,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }
            struct ggml_tensor * sa_out = cur;

            cur = attention_norm;

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                sa_out = ggml_get_rows(ctx0, sa_out, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // feed-forward network
            {
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, sa_out);
            cb(cur, "l_out", il);

            cur = ggml_add(ctx0, cur, inpL);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_gpt2()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * pos;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        pos = ggml_get_rows(ctx0, model.pos_embd, inp_pos);
        cb(pos, "pos_embd", -1);

        inpL = ggml_add(ctx0, inpL, pos);
        cb(inpL, "inpL", -1);

        for (int il = 0; il < n_layer; ++il)
        {
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                cb(cur, "bqkv", il);

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // add the input
            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
            cb(ffn_inp, "ffn_inp", il);

            // FF
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            inpL = ggml_add(ctx0, cur, ffn_inp);
            cb(inpL, "l_out", il);
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_codeshell()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                cb(cur, "bqkv", il);

                struct ggml_tensor * tmpq
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * tmpk
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(tmpq, "tmpq", il);
                cb(tmpk, "tmpk", il);
                cb(Vcur, "Vcur", il);

                struct ggml_tensor * Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, tmpq, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, tmpk, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // add the input
            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
            cb(ffn_inp, "ffn_inp", il);

            // FF
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            inpL = ggml_add(ctx0, cur, ffn_inp);
            cb(inpL, "l_out", il);
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_orion()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                // if (model.layers[il].bq) {
                //     Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                //     cb(Qcur, "Qcur", il);
                // }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                // if (model.layers[il].bk) {
                //     Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                //     cb(Kcur, "Kcur", il);
                // }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                // if (model.layers[il].bv) {
                //     Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                //     cb(Vcur, "Vcur", il);
                // }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_up,
                nullptr,
                model.layers[il].ffn_gate,
                nullptr,
                model.layers[il].ffn_down,
                nullptr,
                nullptr,
                LLM_FFN_SILU,
                LLM_FFN_PAR,
                cb,
                il);
            cb(cur, "ffn_out", il);

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_internlm2()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_up,
                nullptr,
                model.layers[il].ffn_gate,
                nullptr,
                model.layers[il].ffn_down,
                nullptr,
                nullptr,
                LLM_FFN_SILU,
                LLM_FFN_PAR,
                cb,
                il);
            cb(cur, "ffn_out", il);

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    // ref: https://arxiv.org/abs/2203.03466
    //      https://github.com/ggerganov/llama.cpp/issues/5276#issuecomment-1925774738
    // based on the original build_llama() function
    struct ggml_cgraph * build_minicpm()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        const int64_t n_embd = hparams.n_embd;
        //TODO: if the model varies, these parameters need to be read from the model
        const int64_t n_embd_base = 256;
        const float scale_embd = 12.0f;
        const float scale_depth = 1.4f;

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // scale the input embeddings
        inpL = ggml_scale(ctx0, inpL, scale_embd);
        cb(inpL, "inp_scaled", -1);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            // scale_res - scale the hidden states for residual connection
            const float scale_res = scale_depth / sqrtf(float(n_layer));
            cur = ggml_scale(ctx0, cur, scale_res);
            cb(cur, "hidden_scaled", -1);

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            {
                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            // scale the hidden states for residual connection
            cur = ggml_scale(ctx0, cur, scale_res);
            cb(cur, "hidden_scaled_ffn", -1);

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head scaling
        const float scale_lmhead = float(n_embd_base) / float(n_embd);
        cur = ggml_scale(ctx0, cur, scale_lmhead);
        cb(cur, "lmhead_scaling", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.tok_embd, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_gemma()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head_k = hparams.n_embd_head_k;

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        inpL = ggml_scale(ctx0, inpL, sqrtf(n_embd));
        cb(inpL, "inp_scaled", -1);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head_k, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_embd_head_k,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Qcur = ggml_scale(ctx0, Qcur, 1.0f / sqrtf(float(n_embd_head_k)));
                cb(Qcur, "Qcur_scaled", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head_k, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_embd_head_k,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f,
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            struct ggml_tensor * sa_out = ggml_add(ctx0, cur, inpL);
            cb(sa_out, "sa_out", il);

            cur = llm_build_norm(ctx0, sa_out, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            // feed-forward network
            {
                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            cur = ggml_add(ctx0, cur, sa_out);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_starcoder2()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network

            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_up,
                model.layers[il].ffn_up_b,
                nullptr,
                nullptr,
                model.layers[il].ffn_down,
                model.layers[il].ffn_down_b,
                nullptr,
                LLM_FFN_GELU,
                LLM_FFN_SEQ,
                cb,
                il);
            cb(cur, "ffn_out", il);
            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_mamba()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t d_model = n_embd;
        const int64_t d_conv = hparams.ssm_d_conv;
        const int64_t d_inner = hparams.ssm_d_inner;
        GGML_ASSERT(2 * d_model == d_inner);
        const int64_t d_state = hparams.ssm_d_state;
        const int64_t dt_rank = hparams.ssm_dt_rank;

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        // {n_embd, n_tokens}
        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        struct ggml_tensor * state_mask = build_inp_s_mask();
        struct ggml_tensor * state_seq = build_inp_s_seq();

        for (int il = 0; il < n_layer; ++il)
        {
            // (ab)using the KV cache to store the states
            struct ggml_tensor * conv_states = ggml_reshape_2d(ctx0, kv_self.k_l[il], hparams.n_embd_k_s(), kv_self.size);
            struct ggml_tensor * ssm_states = ggml_reshape_2d(ctx0, kv_self.v_l[il], hparams.n_embd_v_s(), kv_self.size);

            // clear states of sequences which are starting at the beginning of this batch
            {
                conv_states = ggml_mul(
                    ctx0,
                    ggml_view_2d(ctx0, conv_states, conv_states->ne[0], n_kv, conv_states->nb[1], kv_head * conv_states->nb[1]),
                    state_mask);
                ssm_states = ggml_mul(
                    ctx0,
                    ggml_view_2d(ctx0, ssm_states, ssm_states->ne[0], n_kv, ssm_states->nb[1], kv_head * ssm_states->nb[1]),
                    state_mask);
            }

            conv_states = ggml_reshape_3d(ctx0, conv_states, d_conv - 1, d_inner, n_kv);
            ssm_states = ggml_reshape_3d(ctx0, ssm_states, d_state, d_inner, n_kv);

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // {n_embd, 2*d_inner} * {n_embd, n_tokens} => {2*d_inner, n_tokens}
            struct ggml_tensor * xz = ggml_mul_mat(ctx0, model.layers[il].ssm_in, cur);
            // split the above in two
            // => {d_inner, n_tokens}
            struct ggml_tensor * x = ggml_view_2d(ctx0, xz, d_inner, xz->ne[1], xz->nb[1], 0);
            struct ggml_tensor * z = ggml_view_2d(ctx0, xz, d_inner, xz->ne[1], xz->nb[1], ggml_element_size(xz) * d_inner);

            // conv
            {
                // Custom operator which is needed only to ease simultaneous sequence processing.
                // For a single sequence, the equivalent is to concatenate the columns of conv_states and x,
                // then make a self-overlapping view of that over d_conv columns at each stride in the 3rd dimension,
                // then element-wise multiply that with the conv1d weight,
                // then sum the elements of each row,
                // (the last two steps are a dot product over rows (also doable with mul_mat))
                // then permute away the ne[0] dimension,
                // and then you're left with the resulting x tensor.
                // The new conv_states is the last (d_conv - 1) columns
                // of the last 3rd dimensional "layer" of the self-overlapping view.
                // For simultaneous sequences, it's more complicated.
                struct ggml_tensor * x_conv = ggml_ssm_conv(ctx0, conv_states, x, model.layers[il].ssm_conv1d, state_seq);

                // store last (d_conv - 1) columns of the conv_state part of x_conv back into the KV cache
                ggml_build_forward_expand(
                    gf,
                    ggml_cpy(
                        ctx0,
                        ggml_view_2d(
                            ctx0,
                            x_conv,
                            d_conv - 1,
                            d_inner * n_kv,
                            d_conv * ggml_element_size(x_conv),
                            (1 + d_inner * n_tokens) * ggml_element_size(x_conv)),
                        ggml_view_1d(
                            ctx0,
                            kv_self.k_l[il],
                            (d_conv - 1) * (d_inner) * (n_kv),
                            kv_head * (d_conv - 1) * (d_inner)*ggml_element_size(x_conv))));

                // extract x from x_conv
                x = ggml_view_2d(ctx0, x_conv, d_inner, n_tokens, d_inner * ggml_element_size(x_conv), 0);

                // bias
                x = ggml_add(ctx0, x, model.layers[il].ssm_conv1d_b);

                x = ggml_silu(ctx0, x);
            }

            // ssm
            {
                // {d_inner, dt_rank + 2*d_state} * {d_inner, n_tokens} => {dt_rank + 2*d_state, n_tokens}
                struct ggml_tensor * x_db = ggml_mul_mat(ctx0, model.layers[il].ssm_x, x);
                // split
                struct ggml_tensor * dt = ggml_view_2d(ctx0, x_db, dt_rank, n_tokens, x_db->nb[1], 0);
                struct ggml_tensor * B = ggml_view_2d(ctx0, x_db, d_state, n_tokens, x_db->nb[1], ggml_element_size(x_db) * dt_rank);
                struct ggml_tensor * C
                    = ggml_view_2d(ctx0, x_db, d_state, n_tokens, x_db->nb[1], ggml_element_size(x_db) * (dt_rank + d_state));

                // {dt_rank, d_inner} * {dt_rank, n_tokens} => {d_inner, n_tokens}
                dt = ggml_mul_mat(ctx0, model.layers[il].ssm_dt, dt);
                dt = ggml_add(ctx0, dt, model.layers[il].ssm_dt_b);

                // Custom operator to optimize the parallel associative scan
                // as described in the Annex D of the Mamba paper.
                // => {d_inner, n_tokens} and {d_state, d_inner, n_kv} combined,
                // because only a single tensor can be returned.
                struct ggml_tensor * y_ssm_states = ggml_ssm_scan(ctx0, ssm_states, x, dt, model.layers[il].ssm_a, B, C, state_seq);

                // store last states (the second part of y_ssm_states)
                ggml_build_forward_expand(
                    gf,
                    ggml_cpy(
                        ctx0,
                        ggml_view_1d(ctx0, y_ssm_states, d_state * d_inner * n_kv, d_inner * n_tokens * ggml_element_size(y_ssm_states)),
                        ggml_view_1d(
                            ctx0, kv_self.v_l[il], d_state * d_inner * n_kv, kv_head * d_state * d_inner * ggml_element_size(ssm_states))));

                struct ggml_tensor * y = ggml_view_2d(ctx0, y_ssm_states, d_inner, n_tokens, d_inner * ggml_element_size(y_ssm_states), 0);

                if (il == n_layer - 1)
                {
                    // skip computing output for unused tokens
                    struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                    x = ggml_get_rows(ctx0, x, inp_out_ids);
                    y = ggml_get_rows(ctx0, y, inp_out_ids);
                    z = ggml_get_rows(ctx0, z, inp_out_ids);
                    inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
                }

                // {d_inner, n_tokens} * {d_inner} => {d_inner, n_tokens}
                y = ggml_add(ctx0, y, ggml_mul(ctx0, x, model.layers[il].ssm_d));
                y = ggml_mul(ctx0, y, ggml_silu(ctx0, z));

                // {d_inner, n_embd} * {d_inner, n_tokens} => {n_embd, n_tokens}
                cur = ggml_mul_mat(ctx0, model.layers[il].ssm_out, y);
            }

            // residual
            cur = ggml_add(ctx0, cur, inpL);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        // final rmsnorm
        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_command_r()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        const float f_logit_scale = hparams.f_logit_scale;

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);
            struct ggml_tensor * ffn_inp = cur;

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (model.layers[il].bq)
                {
                    Qcur = ggml_add(ctx0, Qcur, model.layers[il].bq);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (model.layers[il].bk)
                {
                    Kcur = ggml_add(ctx0, Kcur, model.layers[il].bk);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (model.layers[il].bv)
                {
                    Vcur = ggml_add(ctx0, Vcur, model.layers[il].bv);
                    cb(Vcur, "Vcur", il);
                }

                if (model.layers[il].attn_q_norm)
                {
                    Qcur = ggml_view_3d(
                        ctx0,
                        Qcur,
                        n_embd_head,
                        n_head,
                        n_tokens,
                        ggml_element_size(Qcur) * n_embd_head,
                        ggml_element_size(Qcur) * n_embd_head * n_head,
                        0);
                    cb(Qcur, "Qcur", il);
                    Kcur = ggml_view_3d(
                        ctx0,
                        Kcur,
                        n_embd_head,
                        n_head_kv,
                        n_tokens,
                        ggml_element_size(Kcur) * n_embd_head,
                        ggml_element_size(Kcur) * n_embd_head * n_head_kv,
                        0);
                    cb(Kcur, "Kcur", il);

                    Qcur = llm_build_norm(ctx0, Qcur, hparams, model.layers[il].attn_q_norm, nullptr, LLM_NORM, cb, il);
                    cb(Qcur, "Qcur", il);

                    Kcur = llm_build_norm(ctx0, Kcur, hparams, model.layers[il].attn_k_norm, nullptr, LLM_NORM, cb, il);
                    cb(Kcur, "Kcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
                ffn_inp = ggml_get_rows(ctx0, ffn_inp, inp_out_ids);
            }

            struct ggml_tensor * attn_out = cur;

            // feed-forward network
            {
                cur = llm_build_ffn(
                    ctx0,
                    ffn_inp,
                    model.layers[il].ffn_up,
                    nullptr,
                    model.layers[il].ffn_gate,
                    nullptr,
                    model.layers[il].ffn_down,
                    nullptr,
                    nullptr,
                    LLM_FFN_SILU,
                    LLM_FFN_PAR,
                    cb,
                    il);
                cb(cur, "ffn_out", il);
            }

            // add together residual + FFN + self-attention
            cur = ggml_add(ctx0, cur, inpL);
            cur = ggml_add(ctx0, cur, attn_out);
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);

        if (f_logit_scale)
        {
            cur = ggml_scale(ctx0, cur, f_logit_scale);
        }

        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    // ref: https://allenai.org/olmo
    // based on the original build_llama() function, changes:
    //   * non-parametric layer norm
    //   * clamp qkv
    //   * removed bias
    //   * removed MoE
    struct ggml_cgraph * build_olmo()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        // mutable variable, needed during the last layer of the computation to skip unused tokens
        int32_t n_tokens = this->n_tokens;

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, nullptr, nullptr, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);
                if (hparams.f_clamp_kqv > 0.0f)
                {
                    Qcur = ggml_clamp(ctx0, Qcur, -hparams.f_clamp_kqv, hparams.f_clamp_kqv);
                    cb(Qcur, "Qcur", il);
                }

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);
                if (hparams.f_clamp_kqv > 0.0f)
                {
                    Kcur = ggml_clamp(ctx0, Kcur, -hparams.f_clamp_kqv, hparams.f_clamp_kqv);
                    cb(Kcur, "Kcur", il);
                }

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);
                if (hparams.f_clamp_kqv > 0.0f)
                {
                    Vcur = ggml_clamp(ctx0, Vcur, -hparams.f_clamp_kqv, hparams.f_clamp_kqv);
                    cb(Vcur, "Vcur", il);
                }

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                n_tokens = n_outputs;
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            cur = llm_build_norm(ctx0, ffn_inp, hparams, nullptr, nullptr, LLM_NORM, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_up,
                nullptr,
                model.layers[il].ffn_gate,
                nullptr,
                model.layers[il].ffn_down,
                nullptr,
                nullptr,
                LLM_FFN_SILU,
                LLM_FFN_PAR,
                cb,
                il);
            cb(cur, "ffn_out", il);

            cur = ggml_add(ctx0, cur, ffn_inp);
            cb(cur, "ffn_out", il);

            ggml_tensor * layer_dir = lctx.cvec.tensor_for(il);
            if (layer_dir != nullptr)
            {
                cur = ggml_add(ctx0, cur, layer_dir);
            }
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, nullptr, nullptr, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_gptneox()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        const int64_t n_embd_head = hparams.n_embd_head_v;
        const int64_t n_embd_gqa = hparams.n_embd_v_gqa();
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, model.layers[il].attn_norm_b, LLM_NORM, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                cur = ggml_mul_mat(ctx0, model.layers[il].wqkv, cur);
                cb(cur, "wqkv", il);

                cur = ggml_add(ctx0, cur, model.layers[il].bqkv);
                cb(cur, "bqkv", il);

                struct ggml_tensor * Qcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd, n_tokens, cur->nb[1], 0 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Kcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd)));
                struct ggml_tensor * Vcur
                    = ggml_cont(ctx0, ggml_view_2d(ctx0, cur, n_embd_gqa, n_tokens, cur->nb[1], 1 * sizeof(float) * (n_embd + n_embd_gqa)));

                cb(Qcur, "Qcur", il);
                cb(Kcur, "Kcur", il);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    model.layers[il].bo,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpL = ggml_get_rows(ctx0, inpL, inp_out_ids);
            }

            // ffn
            if (hparams.use_par_res)
            {
                // attention and ffn are computed in parallel
                // x = x + attn(ln1(x)) + ffn(ln2(x))

                struct ggml_tensor * attn_out = cur;

                cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);

                cur = ggml_add(ctx0, cur, inpL);
                cb(cur, "ffn_out", il);

                inpL = ggml_add(ctx0, cur, attn_out);
                cb(inpL, "l_out", il);
            }
            else
            {
                // attention and ffn are computed sequentially
                // x = x + attn(ln1(x))
                // x = x + ffn(ln2(x))

                struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpL);
                cb(ffn_inp, "ffn_inp", il);

                cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, model.layers[il].ffn_norm_b, LLM_NORM, cb, il);
                cb(cur, "ffn_norm", il);

                cur = llm_build_ffn(
                    ctx0,
                    cur,
                    model.layers[il].ffn_up,
                    model.layers[il].ffn_up_b,
                    nullptr,
                    nullptr,
                    model.layers[il].ffn_down,
                    model.layers[il].ffn_down_b,
                    nullptr,
                    LLM_FFN_GELU,
                    LLM_FFN_SEQ,
                    cb,
                    il);
                cb(cur, "ffn_out", il);

                inpL = ggml_add(ctx0, cur, ffn_inp);
                cb(inpL, "l_out", il);
            }
        }

        cur = llm_build_norm(ctx0, inpL, hparams, model.output_norm, model.output_norm_b, LLM_NORM, cb, -1);
        cb(cur, "result_norm", -1);

        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }

    struct ggml_cgraph * build_arctic()
    {
        struct ggml_cgraph * gf = ggml_new_graph_custom(ctx0, LLAMA_MAX_NODES, false);

        // mutable variable, needed during the last layer of the computation to skip unused tokens
        int32_t n_tokens = this->n_tokens;

        const int64_t n_embd_head = hparams.n_embd_head_v;
        GGML_ASSERT(n_embd_head == hparams.n_embd_head_k);
        GGML_ASSERT(n_embd_head == hparams.n_rot);

        struct ggml_tensor * cur;
        struct ggml_tensor * inpL;

        inpL = llm_build_inp_embd(ctx0, lctx, hparams, batch, model.tok_embd, cb);

        // inp_pos - contains the positions
        struct ggml_tensor * inp_pos = build_inp_pos();

        // KQ_mask (mask for 1 head, it will be broadcasted to all heads)
        struct ggml_tensor * KQ_mask = build_inp_KQ_mask();

        for (int il = 0; il < n_layer; ++il)
        {
            struct ggml_tensor * inpSA = inpL;

            // norm
            cur = llm_build_norm(ctx0, inpL, hparams, model.layers[il].attn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "attn_norm", il);

            // self-attention
            {
                // compute Q and K and RoPE them
                struct ggml_tensor * Qcur = ggml_mul_mat(ctx0, model.layers[il].wq, cur);
                cb(Qcur, "Qcur", il);

                struct ggml_tensor * Kcur = ggml_mul_mat(ctx0, model.layers[il].wk, cur);
                cb(Kcur, "Kcur", il);

                struct ggml_tensor * Vcur = ggml_mul_mat(ctx0, model.layers[il].wv, cur);
                cb(Vcur, "Vcur", il);

                Qcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Qcur, n_embd_head, n_head, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Qcur, "Qcur", il);

                Kcur = ggml_rope_ext(
                    ctx0,
                    ggml_reshape_3d(ctx0, Kcur, n_embd_head, n_head_kv, n_tokens),
                    inp_pos,
                    nullptr,
                    n_rot,
                    rope_type,
                    0,
                    n_orig_ctx,
                    freq_base,
                    freq_scale,
                    ext_factor,
                    attn_factor,
                    beta_fast,
                    beta_slow);
                cb(Kcur, "Kcur", il);

                cur = llm_build_kv(
                    ctx0,
                    model,
                    hparams,
                    cparams,
                    kv_self,
                    gf,
                    model.layers[il].wo,
                    nullptr,
                    Kcur,
                    Vcur,
                    Qcur,
                    KQ_mask,
                    n_tokens,
                    kv_head,
                    n_kv,
                    1.0f / sqrtf(float(n_embd_head)),
                    cb,
                    il);
            }

            if (il == n_layer - 1)
            {
                // skip computing output for unused tokens
                struct ggml_tensor * inp_out_ids = build_inp_out_ids();
                n_tokens = n_outputs;
                cur = ggml_get_rows(ctx0, cur, inp_out_ids);
                inpSA = ggml_get_rows(ctx0, inpSA, inp_out_ids);
            }

            struct ggml_tensor * ffn_inp = ggml_add(ctx0, cur, inpSA);
            cb(ffn_inp, "ffn_inp", il);

            // feed-forward network
            cur = llm_build_norm(ctx0, ffn_inp, hparams, model.layers[il].ffn_norm, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm", il);

            cur = llm_build_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_up,
                nullptr,
                model.layers[il].ffn_gate,
                nullptr,
                model.layers[il].ffn_down,
                nullptr,
                nullptr,
                LLM_FFN_SILU,
                LLM_FFN_PAR,
                cb,
                il);
            cb(cur, "ffn_out", il);

            struct ggml_tensor * ffn_out = ggml_add(ctx0, cur, ffn_inp);
            cb(ffn_out, "ffn_out", il);

            // MoE
            cur = llm_build_norm(ctx0, inpSA, hparams, model.layers[il].ffn_norm_exps, nullptr, LLM_NORM_RMS, cb, il);
            cb(cur, "ffn_norm_exps", il);

            cur = llm_build_moe_ffn(
                ctx0,
                cur,
                model.layers[il].ffn_gate_inp,
                model.layers[il].ffn_up_exps,
                model.layers[il].ffn_gate_exps,
                model.layers[il].ffn_down_exps,
                n_expert,
                n_expert_used,
                LLM_FFN_SILU,
                true,
                cb,
                il);
            cb(cur, "ffn_moe_out", il);

            cur = ggml_add(ctx0, cur, ffn_out);
            cb(cur, "ffn_out", il);

            ggml_tensor * layer_dir = lctx.cvec.tensor_for(il);
            if (layer_dir != nullptr)
            {
                cur = ggml_add(ctx0, cur, layer_dir);
            }
            cb(cur, "l_out", il);

            // input for next layer
            inpL = cur;
        }

        cur = inpL;

        cur = llm_build_norm(ctx0, cur, hparams, model.output_norm, nullptr, LLM_NORM_RMS, cb, -1);
        cb(cur, "result_norm", -1);

        // lm_head
        cur = ggml_mul_mat(ctx0, model.output, cur);
        cb(cur, "result_output", -1);

        ggml_build_forward_expand(gf, cur);

        return gf;
    }
};

static struct ggml_cgraph * llama_build_graph_defrag(llama_context & lctx, const std::vector<uint32_t> & ids)
{
    llama_batch dummy;
    dummy.n_tokens = 0;

    llm_build_cb cb = [&](struct ggml_tensor *, const char *, int) { };

    struct llm_build_context llm(lctx, dummy, cb, false);

    llm.init();

    struct ggml_cgraph * result = llm.build_defrag(ids);

    llm.free();

    return result;
}

static struct ggml_cgraph * llama_build_graph_k_shift(llama_context & lctx)
{
    llama_batch dummy;
    dummy.n_tokens = 0;

    llm_build_cb cb = [&](struct ggml_tensor *, const char *, int) { };

    struct llm_build_context llm(lctx, dummy, cb, false);

    llm.init();

    struct ggml_cgraph * result = llm.build_k_shift();

    llm.free();

    return result;
}

static struct ggml_cgraph * llama_build_graph_s_copy(llama_context & lctx)
{
    llama_batch dummy;
    dummy.n_tokens = 0;

    llm_build_cb cb = [&](struct ggml_tensor *, const char *, int) { };

    struct llm_build_context llm(lctx, dummy, cb, false);

    llm.init();

    struct ggml_cgraph * result = llm.build_s_copy();

    llm.free();

    return result;
}

static struct ggml_cgraph * llama_build_graph(llama_context & lctx, const llama_batch & batch, bool worst_case)
{
    const auto & model = lctx.model;

    // this callback allows us to apply custom logic to each tensor (e.g. ggml-alloc, offloading, etc.)
    llm_build_cb cb = [&](struct ggml_tensor * cur, const char * name, int il)
    {
        if (il >= 0)
        {
            ggml_format_name(cur, "%s-%d", name, il);
        }
        else
        {
            ggml_set_name(cur, name);
        }

        if (!lctx.cparams.offload_kqv)
        {
            if (strcmp(name, "kqv_merged_cont") == 0)
            {
                // all nodes between the KV store and the attention output are run on the CPU
                ggml_backend_sched_set_tensor_backend(lctx.sched, cur, lctx.backend_cpu);
            }
        }

        // norm may be automatically assigned to the backend of the previous layer, increasing data transfer between backends
        // FIXME: fix in ggml_backend_sched
        const bool full_offload = lctx.model.n_gpu_layers > (int)lctx.model.hparams.n_layer;
        if (batch.n_tokens < 32 || full_offload)
        {
            if (il != -1 && strcmp(name, "norm") == 0)
            {
                for (auto * backend : lctx.backends)
                {
                    if (ggml_backend_buft_supports_backend(lctx.model.buft_layer[il].buft, backend))
                    {
                        ggml_backend_sched_set_tensor_backend(lctx.sched, cur, backend);
                        break;
                    }
                }
            }
        }
    };

    struct ggml_cgraph * result = nullptr;

    struct llm_build_context llm(lctx, batch, cb, worst_case);

    llm.init();

    switch (model.arch)
    {
        case LLM_ARCH_LLAMA: {
            result = llm.build_llama();
        }
        break;
        case LLM_ARCH_BAICHUAN: {
            result = llm.build_baichuan();
        }
        break;
        case LLM_ARCH_FALCON: {
            result = llm.build_falcon();
        }
        break;
        case LLM_ARCH_GROK: {
            result = llm.build_grok();
        }
        break;
        case LLM_ARCH_STARCODER: {
            result = llm.build_starcoder();
        }
        break;
        case LLM_ARCH_REFACT: {
            result = llm.build_refact();
        }
        break;
        case LLM_ARCH_BERT:
        case LLM_ARCH_JINA_BERT_V2:
        case LLM_ARCH_NOMIC_BERT: {
            result = llm.build_bert();
        }
        break;
        case LLM_ARCH_BLOOM: {
            result = llm.build_bloom();
        }
        break;
        case LLM_ARCH_MPT: {
            result = llm.build_mpt();
        }
        break;
        case LLM_ARCH_STABLELM: {
            result = llm.build_stablelm();
        }
        break;
        case LLM_ARCH_QWEN: {
            result = llm.build_qwen();
        }
        break;
        case LLM_ARCH_QWEN2: {
            result = llm.build_qwen2();
        }
        break;
        case LLM_ARCH_QWEN2MOE: {
            result = llm.build_qwen2moe();
        }
        break;
        case LLM_ARCH_PHI2: {
            result = llm.build_phi2();
        }
        break;
        case LLM_ARCH_PHI3: {
            result = llm.build_phi3();
        }
        break;
        case LLM_ARCH_PLAMO: {
            result = llm.build_plamo();
        }
        break;
        case LLM_ARCH_GPT2: {
            result = llm.build_gpt2();
        }
        break;
        case LLM_ARCH_CODESHELL: {
            result = llm.build_codeshell();
        }
        break;
        case LLM_ARCH_ORION: {
            result = llm.build_orion();
        }
        break;
        case LLM_ARCH_INTERNLM2: {
            result = llm.build_internlm2();
        }
        break;
        case LLM_ARCH_MINICPM: {
            result = llm.build_minicpm();
        }
        break;
        case LLM_ARCH_GEMMA: {
            result = llm.build_gemma();
        }
        break;
        case LLM_ARCH_STARCODER2: {
            result = llm.build_starcoder2();
        }
        break;
        case LLM_ARCH_MAMBA: {
            result = llm.build_mamba();
        }
        break;
        case LLM_ARCH_XVERSE: {
            result = llm.build_xverse();
        }
        break;
        case LLM_ARCH_COMMAND_R: {
            result = llm.build_command_r();
        }
        break;
        case LLM_ARCH_DBRX: {
            result = llm.build_dbrx();
        }
        break;
        case LLM_ARCH_OLMO: {
            result = llm.build_olmo();
        }
        break;
        case LLM_ARCH_GPTNEOX: {
            result = llm.build_gptneox();
        }
        break;
        case LLM_ARCH_ARCTIC: {
            result = llm.build_arctic();
        }
        break;
        default:
            GGML_ASSERT(false);
    }

    llm.free();

    return result;
}

static void llama_set_k_shift(llama_context & lctx)
{
    const int64_t kv_size = lctx.kv_self.size;

    assert(ggml_backend_buffer_is_host(lctx.inp_K_shift->buffer));

    int32_t * data = (int32_t *)lctx.inp_K_shift->data;

    for (int i = 0; i < kv_size; ++i)
    {
        data[i] = lctx.kv_self.cells[i].delta;
    }
}

static void llama_set_s_copy(llama_context & lctx)
{
    const int64_t kv_size = lctx.kv_self.size;

    assert(ggml_backend_buffer_is_host(lctx.inp_s_copy->buffer));

    int32_t * data = (int32_t *)lctx.inp_s_copy->data;

    for (int i = 0; i < kv_size; ++i)
    {
        data[i] = lctx.kv_self.cells[i].src;
    }
}

static void llama_set_inputs(llama_context & lctx, const llama_batch & batch)
{
    //
    // set input data
    //

    const auto & hparams = lctx.model.hparams;
    const auto & cparams = lctx.cparams;
    const auto & kv_self = lctx.kv_self;

    if (batch.token)
    {
        const int64_t n_tokens = batch.n_tokens;

        ggml_backend_tensor_set(lctx.inp_tokens, batch.token, 0, n_tokens * ggml_element_size(lctx.inp_tokens));
    }

    if (batch.embd)
    {
        const int64_t n_embd = hparams.n_embd;
        const int64_t n_tokens = batch.n_tokens;

        ggml_backend_tensor_set(lctx.inp_embd, batch.embd, 0, n_tokens * n_embd * ggml_element_size(lctx.inp_embd));
    }

    if (batch.pos && lctx.inp_pos)
    {
        const int64_t n_tokens = batch.n_tokens;

        ggml_backend_tensor_set(lctx.inp_pos, batch.pos, 0, n_tokens * ggml_element_size(lctx.inp_pos));
    }

    if (hparams.causal_attn || cparams.pooling_type == LLAMA_POOLING_TYPE_NONE)
    {
        GGML_ASSERT(lctx.inp_out_ids && "every model that can must skip unused outputs");
        const int64_t n_tokens = batch.n_tokens;

        GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_out_ids->buffer));
        int32_t * data = (int32_t *)lctx.inp_out_ids->data;

        if (lctx.n_outputs == n_tokens)
        {
            for (int i = 0; i < n_tokens; ++i)
            {
                data[i] = i;
            }
        }
        else if (batch.logits)
        {
            int32_t n_outputs = 0;
            for (int i = 0; i < n_tokens; ++i)
            {
                if (batch.logits[i])
                {
                    data[n_outputs++] = i;
                }
            }
            // the graph needs to have been passed the correct number of outputs
            GGML_ASSERT(lctx.n_outputs == n_outputs);
        }
        else if (lctx.n_outputs == 1)
        {
            // only keep last output
            data[0] = n_tokens - 1;
        }
        else
        {
            GGML_ASSERT(lctx.n_outputs == 0);
        }
    }

    GGML_ASSERT(
        // (!a || b) is a logical implication (a -> b)
        // !hparams.causal_attn -> !cparams.causal_attn
        (hparams.causal_attn || !cparams.causal_attn) && "causal attention with embedding models is not supported");

    if (lctx.inp_KQ_mask)
    {
        // NOTE: hparams.causal_attn indicates the model is capable of generation and uses the kv cache.
        if (cparams.causal_attn)
        {
            const int64_t n_kv = kv_self.n;
            const int64_t n_tokens = batch.n_tokens;

            GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_KQ_mask->buffer));

            float * data = (float *)lctx.inp_KQ_mask->data;

            // For causal attention, use only the previous KV cells
            // of the correct sequence for each token of the batch.
            // It's assumed that if a token in the batch has multiple sequences, they are equivalent.
            for (int h = 0; h < 1; ++h)
            {
                for (int j = 0; j < n_tokens; ++j)
                {
                    const llama_pos pos = batch.pos[j];
                    const llama_seq_id seq_id = batch.seq_id[j][0];

                    for (int i = 0; i < n_kv; ++i)
                    {
                        float f;
                        if (!lctx.kv_self.cells[i].has_seq_id(seq_id) || lctx.kv_self.cells[i].pos > pos)
                        {
                            f = -INFINITY;
                        }
                        else
                        {
                            if (hparams.use_alibi)
                            {
                                f = -fabs(lctx.kv_self.cells[i].pos - pos);
                            }
                            else
                            {
                                f = 0.0f;
                            }
                        }
                        data[h * (n_kv * n_tokens) + j * n_kv + i] = f;
                    }
                }

                for (int i = n_tokens; i < GGML_PAD(n_tokens, GGML_KQ_MASK_PAD); ++i)
                {
                    for (int j = 0; j < n_kv; ++j)
                    {
                        data[h * (n_kv * n_tokens) + i * n_kv + j] = -INFINITY;
                    }
                }
            }
        }
        else
        {
            // when using kv cache, the mask needs to match the kv cache size
            const int64_t n_tokens = batch.n_tokens;
            const int64_t n_stride = hparams.causal_attn ? kv_self.n : n_tokens;

            GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_KQ_mask->buffer));

            float * data = (float *)lctx.inp_KQ_mask->data;

            for (int h = 0; h < 1; ++h)
            {
                for (int j = 0; j < n_tokens; ++j)
                {
                    const llama_seq_id seq_id = batch.seq_id[j][0];

                    for (int i = 0; i < n_tokens; ++i)
                    {
                        float f = -INFINITY;
                        for (int s = 0; s < batch.n_seq_id[i]; ++s)
                        {
                            if (batch.seq_id[i][s] == seq_id)
                            {
                                if (hparams.use_alibi)
                                {
                                    f = -fabs(batch.pos[i] - batch.pos[j]);
                                }
                                else
                                {
                                    f = 0.0f;
                                }
                                break;
                            }
                        }

                        data[h * (n_tokens * n_tokens) + j * n_stride + i] = f;
                    }

                    for (int i = n_tokens; i < n_stride; ++i)
                    {
                        data[h * (n_tokens * n_tokens) + j * n_stride + i] = -INFINITY;
                    }
                }
            }
        }
    }

    if (cparams.pooling_type == LLAMA_POOLING_TYPE_MEAN)
    {
        const int64_t n_tokens = batch.n_tokens;

        GGML_ASSERT(lctx.inp_mean);
        GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_mean->buffer));

        float * data = (float *)lctx.inp_mean->data;
        memset(lctx.inp_mean->data, 0, n_tokens * n_tokens * ggml_element_size(lctx.inp_mean));

        std::vector<uint64_t> sum(n_tokens, 0);
        for (int i = 0; i < n_tokens; ++i)
        {
            const llama_seq_id seq_id = batch.seq_id[i][0];

            GGML_ASSERT(seq_id < n_tokens && "seq_id cannot be larger than n_tokens with pooling_type == MEAN");

            sum[seq_id] += 1;
        }

        std::vector<float> div(n_tokens, 0.0f);
        for (int i = 0; i < n_tokens; ++i)
        {
            const uint64_t s = sum[i];
            if (s > 0)
            {
                div[i] = 1.0f / float(s);
            }
        }

        for (int i = 0; i < n_tokens; ++i)
        {
            const llama_seq_id seq_id = batch.seq_id[i][0];
            data[seq_id * n_tokens + i] = div[seq_id];
        }
    }

    if (cparams.pooling_type == LLAMA_POOLING_TYPE_CLS)
    {
        const int64_t n_tokens = batch.n_tokens;

        GGML_ASSERT(lctx.inp_cls);
        GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_cls->buffer));

        uint32_t * data = (uint32_t *)lctx.inp_cls->data;
        memset(lctx.inp_cls->data, 0, n_tokens * ggml_element_size(lctx.inp_cls));

        for (int i = 0; i < n_tokens; ++i)
        {
            const llama_seq_id seq_id = batch.seq_id[i][0];
            const llama_pos pos = batch.pos[i];

            GGML_ASSERT(seq_id < n_tokens && "seq_id cannot be larger than n_tokens with pooling_type == CLS");

            if (pos == 0)
            {
                data[seq_id] = i;
            }
        }
    }

    if (kv_self.recurrent)
    {
        const int64_t n_kv = kv_self.n;

        if (lctx.inp_s_mask)
        {
            GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_s_mask->buffer));
            float * data = (float *)lctx.inp_s_mask->data;

            // states which are not affected by the current batch are left untouched
            for (int i = 0; i < n_kv; ++i)
            {
                llama_seq_id seq_id = i + lctx.kv_self.head;
                llama_kv_cell & kv_cell = lctx.kv_self.cells[seq_id];
                bool has_self_seq = kv_cell.has_seq_id(seq_id);

                data[i] = (float)has_self_seq;

                // ensure current sequences will be kept
                if (!has_self_seq && kv_cell.pos >= 0)
                {
                    kv_cell.seq_id.insert(seq_id);
                }
            }
        }
        // For Mamba (and other recurrent architectures),
        // update the correct state(s)/sequence(s) for each token of the batch.
        // Like with the KQ_mask, if a token in the batch has multiple sequences,
        // they are assumed to be equivalent (not here, but in ggml_ssm_scan and ggml_ssm_conv).
        if (lctx.inp_s_seq)
        {
            const int64_t n_tokens = batch.n_tokens;

            GGML_ASSERT(ggml_backend_buffer_is_host(lctx.inp_s_seq->buffer));
            int32_t * data = (int32_t *)lctx.inp_s_seq->data;

            for (int j = 0; j < n_tokens; ++j)
            {
                const int32_t n_seq = batch.n_seq_id[j];
                GGML_ASSERT(0 < n_seq); // a token should be part of at least 1 sequence

                for (int i = 0; i < n_kv; ++i)
                {
                    if (i < n_seq)
                    {
                        // for this type of model, the head is the minimum seq_id of the batch
                        data[j * n_kv + i] = batch.seq_id[j][i] - kv_self.head;
                    }
                    else
                    {
                        data[j * n_kv + i] = -1;
                    }
                }
            }
        }
    }
}

// Make sure enough space is available for outputs.
// Returns max number of outputs for which space was reserved.
static size_t llama_output_reserve(llama_context & lctx, size_t n_outputs)
{
    const auto & cparams = lctx.cparams;
    const auto & hparams = lctx.model.hparams;

    const size_t n_outputs_max = std::max(n_outputs, (size_t)cparams.n_seq_max);

    const auto n_batch = cparams.n_batch;
    const auto n_vocab = hparams.n_vocab;
    const auto n_embd = hparams.n_embd;

    // TODO: use a per-batch flag for logits presence instead
    const bool has_logits = cparams.causal_attn;
    const bool has_embd = cparams.embeddings && (hparams.causal_attn || cparams.pooling_type == LLAMA_POOLING_TYPE_NONE);

    const size_t logits_size = has_logits ? n_vocab * n_outputs_max : 0;
    const size_t embd_size = has_embd ? n_embd * n_outputs_max : 0;

    if (lctx.output_ids.empty())
    {
        // init, never resized afterwards
        lctx.output_ids.resize(n_batch);
    }

    const size_t prev_size = lctx.buf_output ? ggml_backend_buffer_get_size(lctx.buf_output) : 0;
    const size_t new_size = (logits_size + embd_size) * sizeof(float);

    // alloc only when more than the current capacity is required
    // TODO: also consider shrinking the buffer
    if (!lctx.buf_output || prev_size < new_size)
    {
        if (lctx.buf_output)
        {
#ifndef NDEBUG
            // This doesn't happen often, but may be annoying in some cases (like the HellaSwag benchmark)
            LLAMA_LOG_INFO(
                "%s: reallocating output buffer from size %.02f MiB to %.02f MiB\n",
                __func__,
                prev_size / 1024.0 / 1024.0,
                new_size / 1024.0 / 1024.0);
#endif
            ggml_backend_buffer_free(lctx.buf_output);
            lctx.buf_output = nullptr;
            lctx.logits = nullptr;
            lctx.embd = nullptr;
        }

        lctx.buf_output = ggml_backend_buft_alloc_buffer(llama_default_buffer_type_cpu(true), new_size);
        if (lctx.buf_output == nullptr)
        {
            LLAMA_LOG_ERROR("%s: failed to allocate output buffer of size %.2f MiB\n", __func__, new_size / (1024.0 * 1024.0));
            return 0;
        }
    }

    float * output_base = (float *)ggml_backend_buffer_get_base(lctx.buf_output);

    lctx.logits = has_logits ? output_base : nullptr;
    lctx.embd = has_embd ? output_base + logits_size : nullptr;

    lctx.output_size = n_outputs_max;
    lctx.logits_size = logits_size;
    lctx.embd_size = embd_size;

    // set all ids as invalid (negative)
    std::fill(lctx.output_ids.begin(), lctx.output_ids.end(), -1);

    ggml_backend_buffer_clear(lctx.buf_output, 0);

    lctx.n_outputs = 0;

    return n_outputs_max;
}


static void llama_graph_compute(llama_context & lctx, ggml_cgraph * gf, int n_threads)
{
#ifdef GGML_USE_METAL
    if (ggml_backend_is_metal(lctx.backend_metal))
    {
        ggml_backend_metal_set_n_cb(lctx.backend_metal, n_threads);
    }
#endif

    if (lctx.backend_cpu != nullptr)
    {
        ggml_backend_cpu_set_n_threads(lctx.backend_cpu, n_threads);
        ggml_backend_cpu_set_abort_callback(lctx.backend_cpu, lctx.abort_callback, lctx.abort_callback_data);
    }

    ggml_backend_sched_graph_compute_async(lctx.sched, gf);

    // fprintf(stderr, "splits: %d\n", ggml_backend_sched_get_n_splits(lctx.sched));
}

// decode a batch of tokens by evaluating the transformer
//
//   - lctx:      llama context
//   - batch:     batch to evaluate
//
// return 0 on success
// return positive int on warning
// return negative int on error
//
static int llama_decode_internal(llama_context & lctx, llama_batch batch_all)
{ // TODO: rename back to batch

    const uint32_t n_tokens_all = batch_all.n_tokens;

    if (n_tokens_all == 0)
    {
        LLAMA_LOG_ERROR("%s: n_tokens == 0", __func__);
        return -1;
    }

    const auto & model = lctx.model;
    const auto & hparams = model.hparams;
    const auto & cparams = lctx.cparams;

    GGML_ASSERT((!batch_all.token && batch_all.embd) || (batch_all.token && !batch_all.embd)); // NOLINT

    GGML_ASSERT(n_tokens_all <= cparams.n_batch);

    GGML_ASSERT((cparams.causal_attn || cparams.n_ubatch >= n_tokens_all) && "non-causal attention requires n_ubatch >= n_tokens");

    if (lctx.t_compute_start_us == 0)
    {
        lctx.t_compute_start_us = ggml_time_us();
    }
    lctx.n_queued_tokens += n_tokens_all;

    auto & kv_self = lctx.kv_self;

    const int64_t n_embd = hparams.n_embd;
    const int64_t n_vocab = hparams.n_vocab;

    uint32_t n_outputs = 0;
    uint32_t n_outputs_prev = 0;

    const auto n_ubatch = cparams.n_ubatch;

    std::vector<llama_pos> pos;
    std::vector<int32_t> n_seq_id;
    std::vector<llama_seq_id *> seq_id_arr;
    std::vector<std::vector<llama_seq_id>> seq_id;

    // count outputs
    if (batch_all.logits)
    {
        for (uint32_t i = 0; i < n_tokens_all; ++i)
        {
            n_outputs += batch_all.logits[i] != 0;
        }
    }
    else if (lctx.logits_all || (cparams.embeddings && cparams.pooling_type != LLAMA_POOLING_TYPE_NONE))
    {
        n_outputs = n_tokens_all;
    }
    else
    {
        // keep last output only
        n_outputs = 1;
    }

    // reserve output buffer
    if (llama_output_reserve(lctx, n_outputs) < n_outputs)
    {
        LLAMA_LOG_ERROR("%s: could not reserve space for batch with %u outputs\n", __func__, n_outputs);
        return -2;
    };

    // set output mappings
    if (batch_all.logits)
    {
        int32_t i_logits = 0;
        for (uint32_t i = 0; i < n_tokens_all; ++i)
        {
            if (batch_all.logits[i])
            {
                lctx.output_ids[i] = i_logits++;
            }
        }
    }
    else
    {
        for (uint32_t i = 0; i < n_outputs; ++i)
        {
            lctx.output_ids[i] = i;
        }
    }

    for (uint32_t cur_token = 0; cur_token < n_tokens_all; cur_token += n_ubatch)
    {
        const uint32_t n_tokens = std::min(n_ubatch, n_tokens_all - cur_token);
        llama_batch u_batch = {
            /* .n_tokens   = */ (int32_t)n_tokens,
            /* .token      = */ batch_all.token ? batch_all.token + cur_token : nullptr,
            /* .embd       = */ batch_all.embd ? batch_all.embd + cur_token * n_embd : nullptr,
            /* .pos        = */ batch_all.pos ? batch_all.pos + cur_token : nullptr,
            /* .n_seq_id   = */ batch_all.n_seq_id ? batch_all.n_seq_id + cur_token : nullptr,
            /* .seq_id     = */ batch_all.seq_id ? batch_all.seq_id + cur_token : nullptr,
            /* .logits     = */ batch_all.logits ? batch_all.logits + cur_token : nullptr,
            /* .all_pos_0  = */ batch_all.all_pos_0 + (llama_pos)cur_token * batch_all.all_pos_1,
            /* .all_pos_1  = */ batch_all.all_pos_1,
            /* .all_seq_id = */ batch_all.all_seq_id,
        };

        // count the outputs in this u_batch
        {
            int32_t n_outputs_new = 0;

            if (u_batch.logits)
            {
                for (uint32_t i = 0; i < n_tokens; i++)
                {
                    n_outputs_new += u_batch.logits[i] != 0;
                }
            }
            else if (n_outputs == n_tokens_all)
            {
                n_outputs_new = n_tokens;
            }
            else
            {
                // keep last output only
                if (cur_token + n_tokens >= n_tokens_all)
                {
                    n_outputs_new = 1;
                }
            }

            // needs to happen before the graph is built
            lctx.n_outputs = n_outputs_new;
        }

        int n_threads = n_tokens == 1 ? cparams.n_threads : cparams.n_threads_batch;
        GGML_ASSERT(n_threads > 0);

        // helpers for smoother batch API transition
        // after deprecating the llama_eval calls, these will be removed
        if (u_batch.pos == nullptr)
        {
            pos.resize(n_tokens);
            for (uint32_t i = 0; i < n_tokens; i++)
            {
                pos[i] = u_batch.all_pos_0 + i * u_batch.all_pos_1;
            }

            u_batch.pos = pos.data();
        }

        if (u_batch.seq_id == nullptr)
        {
            n_seq_id.resize(n_tokens);
            seq_id.resize(n_tokens);
            seq_id_arr.resize(n_tokens);
            for (uint32_t i = 0; i < n_tokens; i++)
            {
                n_seq_id[i] = 1;
                seq_id[i].resize(1);
                seq_id[i][0] = u_batch.all_seq_id;
                seq_id_arr[i] = seq_id[i].data();
            }

            u_batch.n_seq_id = n_seq_id.data();
            u_batch.seq_id = seq_id_arr.data();
        }

        // non-causal masks do not use the KV cache
        if (hparams.causal_attn)
        {
            llama_kv_cache_update(&lctx);

            // if we have enough unused cells before the current head ->
            //   better to start searching from the beginning of the cache, hoping to fill it
            if (kv_self.head > kv_self.used + 2 * n_tokens)
            {
                kv_self.head = 0;
            }

            if (!llama_kv_cache_find_slot(kv_self, u_batch))
            {
                return 1;
            }

            if (!kv_self.recurrent)
            {
                // a heuristic, to avoid attending the full cache if it is not yet utilized
                // after enough generations, the benefit from this heuristic disappears
                // if we start defragmenting the cache, the benefit from this will be more important
                const uint32_t pad = llama_kv_cache_get_padding(cparams);
                kv_self.n = std::min(kv_self.size, std::max(pad, GGML_PAD(llama_kv_cache_cell_max(kv_self), pad)));
                //kv_self.n = llama_kv_cache_cell_max(kv_self);
            }
        }

        //printf("kv_self.n = %5d, kv_self.used = %5d, kv_self.head = %5d\n", kv_self.n, kv_self.used, kv_self.head);

        ggml_backend_sched_reset(lctx.sched);
        ggml_backend_sched_set_eval_callback(lctx.sched, lctx.cparams.cb_eval, lctx.cparams.cb_eval_user_data);

        ggml_cgraph * gf = llama_build_graph(lctx, u_batch, false);

        // the output is always the last tensor in the graph
        struct ggml_tensor * res = gf->nodes[gf->n_nodes - 1];
        struct ggml_tensor * embd = gf->nodes[gf->n_nodes - 2];

        if (lctx.n_outputs == 0)
        {
            // no output
            res = nullptr;
            embd = nullptr;
        }
        else if (!hparams.causal_attn)
        {
            res = nullptr; // do not extract logits for embedding models such as BERT

            // token or sequence embeddings
            embd = gf->nodes[gf->n_nodes - 1];

            GGML_ASSERT(strcmp(embd->name, "result_embd") == 0 || strcmp(embd->name, "result_embd_pooled") == 0);
        }
        else if (cparams.embeddings)
        {
            // the embeddings could be in the second to last tensor, or any of the previous tensors
            int i_embd = gf->n_nodes - 2;
            for (int i = 3; strcmp(embd->name, "result_norm") != 0; ++i)
            {
                i_embd = gf->n_nodes - i;
                if (i_embd < 0)
                {
                    break;
                }
                embd = gf->nodes[i_embd];
            }
            GGML_ASSERT(i_embd >= 0 && "missing result_norm tensor");

            // TODO: use a per-batch flag to know when to skip logits while keeping embeddings
            if (!cparams.causal_attn)
            {
                res = nullptr; // do not extract logits when not needed
                // skip computing logits
                // TODO: is this safe?
                gf->n_nodes = i_embd + 1;
            }
        }
        else
        {
            embd = nullptr; // do not extract embeddings when not needed
            GGML_ASSERT(strcmp(res->name, "result_output") == 0 && "missing result_output tensor");
        }
        // LLAMA_LOG_INFO("graph build time: %.3f ms (%d nodes, %d leafs)\n", (ggml_time_us() - t_start_us)/1000.0, gf->n_nodes, gf->n_leafs);

        // for big prompts, if BLAS is enabled, it is better to use only one thread
        // otherwise, the threads are spin-lock waiting for the BLAS calls and are degrading the performance
        // TODO: this is mostly important for Apple Silicon where CBLAS is still performing very well
        //       we still need some threads to process all non-mul_mat ops, but not too much to avoid interfering
        //       with the BLAS calls. need a better solution
        // MoE Special Case: This logic applies when hparams.n_expert == 0, i.e. the model is NOT an MoE model. When an MoE is
        //                   being processed then Accelerate/BLAS will not be involved, so capping would limit performance.
        if (n_tokens >= 32 && hparams.n_expert == 0 && ggml_cpu_has_blas() && !ggml_cpu_has_gpublas())
        {
            n_threads = std::min(4, n_threads);
        }

        ggml_backend_sched_alloc_graph(lctx.sched, gf);

        llama_set_inputs(lctx, u_batch);

        llama_graph_compute(lctx, gf, n_threads);

        // update the kv ring buffer
        {
            kv_self.head += n_tokens;

            // Ensure kv cache head points to a valid index.
            if (kv_self.head >= kv_self.size)
            {
                kv_self.head = 0;
            }
        }

#ifdef GGML_PERF
        // print timing information per ggml operation (for debugging purposes)
        // requires GGML_PERF to be defined
        ggml_graph_print(gf);
#endif

        // plot the computation graph in dot format (for debugging purposes)
        //if (n_past%100 == 0) {
        //    ggml_graph_dump_dot(gf, nullptr, "llama.dot");
        //}

        // extract logits
        if (res)
        {
            ggml_backend_t backend_res = ggml_backend_sched_get_tensor_backend(lctx.sched, res);
            GGML_ASSERT(backend_res != nullptr);
            GGML_ASSERT(lctx.logits != nullptr);

            float * logits_out = lctx.logits + n_outputs_prev * n_vocab;
            const int32_t n_outputs_new = lctx.n_outputs;

            if (n_outputs_new)
            {
                GGML_ASSERT(n_outputs_prev + n_outputs_new <= n_outputs);
                GGML_ASSERT((n_outputs_prev + n_outputs_new) * n_vocab <= (int64_t)lctx.logits_size);
                ggml_backend_tensor_get_async(backend_res, res, logits_out, 0, n_outputs_new * n_vocab * sizeof(float));
            }
        }

        // extract embeddings
        if (embd)
        {
            ggml_backend_t backend_embd = ggml_backend_sched_get_tensor_backend(lctx.sched, embd);
            GGML_ASSERT(backend_embd != nullptr);

            switch (cparams.pooling_type)
            {
                case LLAMA_POOLING_TYPE_NONE: {
                    // extract token embeddings
                    GGML_ASSERT(lctx.embd != nullptr);
                    float * embd_out = lctx.embd + n_outputs_prev * n_embd;
                    const int32_t n_outputs_new = lctx.n_outputs;

                    if (n_outputs_new)
                    {
                        GGML_ASSERT(n_outputs_prev + n_outputs_new <= n_outputs);
                        GGML_ASSERT((n_outputs_prev + n_outputs_new) * n_embd <= (int64_t)lctx.embd_size);
                        ggml_backend_tensor_get_async(backend_embd, embd, embd_out, 0, n_outputs_new * n_embd * sizeof(float));
                    }
                }
                break;
                case LLAMA_POOLING_TYPE_CLS:
                case LLAMA_POOLING_TYPE_MEAN: {
                    GGML_ASSERT(strcmp(embd->name, "result_embd_pooled") == 0);

                    // extract sequence embeddings
                    auto & embd_seq_out = lctx.embd_seq;
                    embd_seq_out.clear();

                    for (uint32_t i = 0; i < n_tokens; i++)
                    {
                        const llama_seq_id seq_id = u_batch.seq_id[i][0];
                        if (embd_seq_out.find(seq_id) != embd_seq_out.end())
                        {
                            continue;
                        }
                        embd_seq_out[seq_id].resize(n_embd);
                        ggml_backend_tensor_get_async(
                            backend_embd, embd, embd_seq_out[seq_id].data(), (n_embd * seq_id) * sizeof(float), n_embd * sizeof(float));
                    }
                }
                break;
                case LLAMA_POOLING_TYPE_UNSPECIFIED: {
                    GGML_ASSERT(false && "unknown pooling type");
                }
                break;
            }
        }
        n_outputs_prev += lctx.n_outputs;
    }

    // set to total number of outputs in the batch, for use in llama_get_logits_ith
    lctx.n_outputs = n_outputs;

    // wait for the computation to finish (automatically done when obtaining the model output)
    //llama_synchronize(&lctx);

    // decide if we need to defrag the kv cache
    if (cparams.causal_attn && cparams.defrag_thold >= 0.0f)
    {
        const float fragmentation = kv_self.n >= 128 ? 1.0f - float(kv_self.used) / float(kv_self.n) : 0.0f;

        // queue defragmentation for next llama_kv_cache_update
        if (fragmentation > cparams.defrag_thold)
        {
            //LLAMA_LOG_INFO("fragmentation: %.2f\n", fragmentation);

            llama_kv_cache_defrag(kv_self);
        }
    }

    // Reset state for the next token before backend sync, to allow the CPU activities in the reset to
    // overlap with device computation.
    ggml_backend_sched_reset(lctx.sched);

    return 0;
}


// find holes from the beginning of the KV cache and fill them by moving data from the end of the cache
static void llama_kv_cache_defrag_internal(struct llama_context & lctx)
{
    auto & kv_self = lctx.kv_self;

    const auto & hparams = lctx.model.hparams;

    const uint32_t n_layer = hparams.n_layer;

    const uint32_t n_kv = llama_kv_cache_cell_max(kv_self);
    const uint32_t n_used = kv_self.used;

    assert(n_used <= n_kv);

    //const int64_t t_start = ggml_time_us();

    // number of cells moved
    uint32_t n_moves = 0;

    // each move requires 6*n_layer tensors (see build_defrag)
    //   - source view, destination view, copy operation
    //   - x2 for keys and values
    //const uint32_t max_moves = LLAMA_MAX_NODES/(6*n_layer);
    // TODO: tmp fix https://github.com/ggerganov/llama.cpp/issues/6685#issuecomment-2057579516
    const uint32_t max_moves = (LLAMA_MAX_NODES - 2 * n_layer) / (6 * n_layer);

    // determine which KV cells to move where
    //
    //  cell i moves to ids[i]
    //
    //  if ids[i] == i || ids[i] == n_kv, then cell i is not moved
    //
    std::vector<uint32_t> ids(n_kv, n_kv);

    for (uint32_t i0 = 0; i0 < n_used; ++i0)
    {
        const auto & cell0 = kv_self.cells[i0];

        if (!cell0.is_empty())
        {
            ids[i0] = i0;

            continue;
        }

        // found a hole - fill it with data from the end of the cache

        uint32_t nh = 1;

        // determine the size of the hole
        while (i0 + nh < n_used && kv_self.cells[i0 + nh].is_empty())
        {
            nh++;
        }

        uint32_t nf = 0;
        uint32_t is = n_kv - 1;

        // starting from the end, find nh non-empty cells
        for (; is > i0; --is)
        {
            const auto & cell1 = kv_self.cells[is];

            if (cell1.is_empty() || ids[is] != n_kv)
            {
                continue;
            }

            // non-empty cell which is not yet moved
            nf++;

            if (nf == nh)
            {
                break;
            }
        }
        // this can only happen if `n_used` is not accurate, which would be a bug
        GGML_ASSERT(nf == nh && "KV defrag bug: nf != nh");

        nf = 0;

        uint32_t i1 = is;

        // are we moving a continuous block of memory?
        bool cont = false;

        // should we stop searching for the next move?
        bool stop = false;

        // go back and move the nf cells to the hole
        for (; i1 < n_kv; ++i1)
        {
            auto & cell1 = kv_self.cells[i1];

            if (cell1.is_empty() || ids[i1] != n_kv)
            {
                if (n_moves == max_moves)
                {
                    stop = true;
                    break;
                }

                cont = false;
                continue;
            }

            // this cell goes to (i0 + nf)
            ids[i1] = i0 + nf;

            // move the cell meta data
            kv_self.cells[i0 + nf] = cell1;

            // clear the old cell and move the head there
            cell1 = llama_kv_cell();
            kv_self.head = n_used;

            if (!cont)
            {
                n_moves++;
                cont = true;
            }

            nf++;

            if (nf == nh)
            {
                break;
            }
        }

        if (stop || n_moves == max_moves)
        {
            break;
        }

        //LLAMA_LOG_INFO("(tmp log) KV defrag: move [%u, %u) to [%u, %u)\n", is, i1 + 1, i0, i0 + nh);

        i0 += nh - 1;
    }

    if (n_moves == 0)
    {
        return;
    }

    //LLAMA_LOG_INFO("(tmp log) KV defrag cell moves: %u\n", n_moves);

    //LLAMA_LOG_INFO("expected gf nodes: %u\n", 6*n_moves*n_layer);

    // ggml_graph defrag

    ggml_backend_sched_reset(lctx.sched);

    ggml_cgraph * gf = llama_build_graph_defrag(lctx, ids);

    llama_graph_compute(lctx, gf, lctx.cparams.n_threads);

    //const int64_t t_end = ggml_time_us();

    //LLAMA_LOG_INFO("(tmp log) KV defrag time: %.3f ms\n", (t_end - t_start)/1000.0);
}

static void llama_kv_cache_update_internal(struct llama_context & lctx)
{
    bool need_reserve = false;

    // apply K-shift if needed
    if (lctx.model.hparams.rope_type != LLAMA_ROPE_TYPE_NONE && lctx.kv_self.has_shift)
    {
        {
            ggml_backend_sched_reset(lctx.sched);

            ggml_cgraph * gf = llama_build_graph_k_shift(lctx);

            ggml_backend_sched_alloc_graph(lctx.sched, gf);

            llama_set_k_shift(lctx);

            llama_graph_compute(lctx, gf, lctx.cparams.n_threads);

            need_reserve = true;
        }

        {
            auto & kv_self = lctx.kv_self;

            kv_self.has_shift = false;

            for (uint32_t i = 0; i < kv_self.size; ++i)
            {
                kv_self.cells[i].delta = 0;
            }
        }
    }

    if (lctx.kv_self.recurrent && lctx.kv_self.do_copy)
    {
        {
            ggml_backend_sched_reset(lctx.sched);

            ggml_cgraph * gf = llama_build_graph_s_copy(lctx);

            ggml_backend_sched_alloc_graph(lctx.sched, gf);

            llama_set_s_copy(lctx);

            llama_graph_compute(lctx, gf, lctx.cparams.n_threads);

            need_reserve = true;
        }

        {
            auto & kv_self = lctx.kv_self;

            kv_self.do_copy = false;

            for (uint32_t i = 0; i < kv_self.size; ++i)
            {
                kv_self.cells[i].src = i;
            }
        }
    }

    // defragment the KV cache if needed
    if (lctx.kv_self.do_defrag)
    {
        llama_kv_cache_defrag_internal(lctx);

        need_reserve = true;

        lctx.kv_self.do_defrag = false;
    }

    // reserve a worst case graph again
    if (need_reserve)
    {
        // TODO: extract to a function
        // build worst-case graph
        int n_tokens = (int)std::min(lctx.cparams.n_ctx, lctx.cparams.n_ubatch);
        int n_past = lctx.cparams.n_ctx - n_tokens;
        llama_token token = llama_token_bos(
            &lctx.model); // not actually used by llama_build_graph, but required to choose between token and embedding inputs graph
        ggml_cgraph * gf = llama_build_graph(lctx, llama_batch_get_one(&token, n_tokens, n_past, 0), true);

        // initialize scheduler with the worst-case graph
        ggml_backend_sched_reset(lctx.sched);
        if (!ggml_backend_sched_reserve(lctx.sched, gf))
        {
            LLAMA_LOG_ERROR("%s: failed to allocate compute buffers\n", __func__);
        }
    }
}

//
// tokenizer
//

static enum llama_vocab_type llama_vocab_get_type(const llama_vocab & vocab)
{
    return vocab.type;
}

static bool llama_is_normal_token(const llama_vocab & vocab, llama_token id)
{
    GGML_ASSERT(vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return vocab.id_to_token[id].type == LLAMA_TOKEN_TYPE_NORMAL;
}

static bool llama_is_unknown_token(const llama_vocab & vocab, llama_token id)
{
    GGML_ASSERT(vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return vocab.id_to_token[id].type == LLAMA_TOKEN_TYPE_UNKNOWN;
}

static bool llama_is_control_token(const llama_vocab & vocab, llama_token id)
{
    GGML_ASSERT(vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return vocab.id_to_token[id].type == LLAMA_TOKEN_TYPE_CONTROL;
}

static bool llama_is_byte_token(const llama_vocab & vocab, llama_token id)
{
    GGML_ASSERT(vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return vocab.id_to_token[id].type == LLAMA_TOKEN_TYPE_BYTE;
}

static bool llama_is_user_defined_token(const llama_vocab & vocab, llama_token id)
{
    GGML_ASSERT(vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return vocab.id_to_token[id].type == LLAMA_TOKEN_TYPE_USER_DEFINED;
}

static uint8_t llama_token_to_byte(const llama_vocab & vocab, llama_token id)
{
    GGML_ASSERT(llama_vocab_get_type(vocab) != LLAMA_VOCAB_TYPE_NONE);
    GGML_ASSERT(llama_is_byte_token(vocab, id));
    const auto & token_data = vocab.id_to_token.at(id);
    switch (llama_vocab_get_type(vocab))
    {
        case LLAMA_VOCAB_TYPE_SPM: {
            auto buf = token_data.text.substr(3, 2);
            return strtol(buf.c_str(), nullptr, 16);
        }
        case LLAMA_VOCAB_TYPE_BPE: {
            GGML_ASSERT(false);
            return unicode_utf8_to_byte(token_data.text); // TODO: why is this here after GGML_ASSERT?
        }
        case LLAMA_VOCAB_TYPE_WPM: {
            GGML_ASSERT(false);
        }
        default:
            GGML_ASSERT(false);
    }
}

static llama_token llama_byte_to_token(const llama_vocab & vocab, uint8_t ch)
{
    GGML_ASSERT(llama_vocab_get_type(vocab) != LLAMA_VOCAB_TYPE_NONE);
    static const char * hex = "0123456789ABCDEF";
    switch (llama_vocab_get_type(vocab))
    {
        case LLAMA_VOCAB_TYPE_SPM: {
            const char buf[7] = {'<', '0', 'x', hex[ch >> 4], hex[ch & 15], '>', 0};
            auto token = vocab.token_to_id.find(buf);
            if (token != vocab.token_to_id.end())
            {
                return (*token).second;
            }
            // Try to fall back to just the byte as a string
            const char buf2[2] = {(char)ch, 0};
            return vocab.token_to_id.at(buf2);
        }
        case LLAMA_VOCAB_TYPE_WPM:
        case LLAMA_VOCAB_TYPE_BPE: {
            return vocab.token_to_id.at(unicode_byte_to_utf8(ch));
        }
        default:
            GGML_ASSERT(false);
    }
}

static void llama_escape_whitespace(std::string & text)
{
    replace_all(text, " ", "\xe2\x96\x81");
}

static void llama_unescape_whitespace(std::string & word)
{
    replace_all(word, "\xe2\x96\x81", " ");
}

struct llm_symbol
{
    using index = int;
    index prev;
    index next;
    const char * text;
    size_t n;
};

static_assert(std::is_trivially_copyable<llm_symbol>::value, "llm_symbol is not trivially copyable");

// SPM tokenizer
// original implementation:
// https://github.com/ggerganov/llama.cpp/commit/074bea2eb1f1349a0118239c4152914aecaa1be4

struct llm_bigram_spm
{
    struct comparator
    {
        bool operator()(llm_bigram_spm & l, llm_bigram_spm & r) { return (l.score < r.score) || (l.score == r.score && l.left > r.left); }
    };
    using queue_storage = std::vector<llm_bigram_spm>;
    using queue = std::priority_queue<llm_bigram_spm, queue_storage, comparator>;
    llm_symbol::index left;
    llm_symbol::index right;
    float score;
    size_t size;
};

struct llm_tokenizer_spm
{
    llm_tokenizer_spm(const llama_vocab & vocab) : vocab(vocab) { }

    void tokenize(const std::string & text, std::vector<llama_vocab::id> & output)
    {
        // split string into utf8 chars
        int index = 0;
        size_t offs = 0;
        while (offs < text.size())
        {
            llm_symbol sym;
            size_t len = utf8_len(text[offs]);
            sym.text = text.c_str() + offs;
            sym.n = std::min(len, text.size() - offs);
            offs += sym.n;
            sym.prev = index - 1;
            sym.next = offs == text.size() ? -1 : index + 1;
            index++;
            symbols.emplace_back(sym);
        }

        // seed the work queue with all possible 2-character tokens.
        for (size_t i = 1; i < symbols.size(); ++i)
        {
            try_add_bigram(i - 1, i);
        }

        // keep substituting the highest frequency pairs for as long as we can.
        while (!work_queue.empty())
        {
            auto bigram = work_queue.top();
            work_queue.pop();

            auto & left_sym = symbols[bigram.left];
            auto & right_sym = symbols[bigram.right];

            // if one of the symbols already got merged, skip it.
            if (left_sym.n == 0 || right_sym.n == 0 || left_sym.n + right_sym.n != bigram.size)
            {
                continue;
            }

            // merge the right sym into the left one
            left_sym.n += right_sym.n;
            right_sym.n = 0;

            //LLAMA_LOG_INFO("left = '%*s' size = %zu\n", (int) left_sym.n, left_sym.text, bigram.size);

            // remove the right sym from the chain
            left_sym.next = right_sym.next;
            if (right_sym.next >= 0)
            {
                symbols[right_sym.next].prev = bigram.left;
            }

            // find more substitutions
            try_add_bigram(left_sym.prev, bigram.left);
            try_add_bigram(bigram.left, left_sym.next);
        }

        for (int i = 0; i != -1; i = symbols[i].next)
        {
            auto & symbol = symbols[i];
            resegment(symbol, output);
        }
    }

private:
    void resegment(llm_symbol & symbol, std::vector<llama_vocab::id> & output)
    {
        auto text = std::string(symbol.text, symbol.n);
        auto token = vocab.token_to_id.find(text);

        // Do we need to support is_unused?
        if (token != vocab.token_to_id.end())
        {
            output.push_back((*token).second);
            return;
        }

        const auto p = rev_merge.find(text);

        if (p == rev_merge.end())
        {
            // output any symbols that did not form tokens as bytes.
            output.reserve(output.size() + symbol.n);
            for (int j = 0; j < (int)symbol.n; ++j)
            {
                llama_vocab::id token_id = llama_byte_to_token(vocab, symbol.text[j]);
                output.push_back(token_id);
            }
            return;
        }

        resegment(symbols[p->second.first], output);
        resegment(symbols[p->second.second], output);
    }

    void try_add_bigram(int left, int right)
    {
        if (left == -1 || right == -1)
        {
            return;
        }

        const std::string text = std::string(symbols[left].text, symbols[left].n + symbols[right].n);
        auto token = vocab.token_to_id.find(text);

        if (token == vocab.token_to_id.end())
        {
            return;
        }

        if (static_cast<size_t>((*token).second) >= vocab.id_to_token.size())
        {
            return;
        }

        const auto & tok_data = vocab.id_to_token[(*token).second];

        llm_bigram_spm bigram;
        bigram.left = left;
        bigram.right = right;
        bigram.score = tok_data.score;
        bigram.size = text.size();

        work_queue.push(bigram);

        // Do we need to support is_unused?
        rev_merge[text] = std::make_pair(left, right);
    }

    const llama_vocab & vocab;

    std::vector<llm_symbol> symbols;
    llm_bigram_spm::queue work_queue;

    std::map<std::string, std::pair<int, int>> rev_merge;
};

// BPE tokenizer
// adapted from https://github.com/cmp-nct/ggllm.cpp [MIT License]
// tried to simplify unicode stuff, so most likely does not work 100% correctly!

// TODO: there are a lot of common parts between spm and bpe tokenizers, should be refactored and reused

struct llm_bigram_bpe
{
    struct comparator
    {
        bool operator()(const llm_bigram_bpe & l, const llm_bigram_bpe & r) const
        {
            return l.rank > r.rank || (l.rank == r.rank && l.left > r.left);
        }
    };

    using queue_storage = std::vector<llm_bigram_bpe>;
    using queue = std::priority_queue<llm_bigram_bpe, queue_storage, comparator>;
    llm_symbol::index left;
    llm_symbol::index right;
    std::string text;
    int rank;
    size_t size;
};

struct llm_tokenizer_bpe
{
    llm_tokenizer_bpe(const llama_vocab & vocab) : vocab(vocab) { }

    void tokenize([[maybe_unused]] const std::string & text, [[maybe_unused]] std::vector<llama_vocab::id> & output)
    {
        // std::abort();  // not supported
        int final_prev_index = -1;
        bool ignore_merges = false;

        std::vector<std::string> word_collection;
        switch (vocab.type)
        {
            case LLAMA_VOCAB_TYPE_BPE:
                switch (vocab.type_pre)
                {
                    case LLAMA_VOCAB_PRE_TYPE_LLAMA3:
                        ignore_merges = true;
                        word_collection = unicode_regex_split(
                            text,
                            {
                                // original regex from tokenizer.json
                                //"(?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\\r\\n\\p{L}\\p{N}]?\\p{L}+|\\p{N}{1,3}| ?[^\\s\\p{L}\\p{N}]+[\\r\\n]*|\\s*[\\r\\n]+|\\s+(?!\\S)|\\s+",

                                // adapted: https://github.com/ggerganov/llama.cpp/pull/6920#issuecomment-2080233989
                                "(?:'[sS]|'[tT]|'[rR][eE]|'[vV][eE]|'[mM]|'[lL][lL]|'[dD])|[^\\r\\n\\p{L}\\p{N}]?\\p{L}+|\\p{N}{1,3}| "
                                "?[^\\s\\p{L}\\p{N}]+[\\r\\n]*|\\s*[\\r\\n]+|\\s+(?!\\S)|\\s+",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_DBRX:
                    case LLAMA_VOCAB_PRE_TYPE_SMAUG:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                // same as llama3
                                "(?:'[sS]|'[tT]|'[rR][eE]|'[vV][eE]|'[mM]|'[lL][lL]|'[dD])|[^\\r\\n\\p{L}\\p{N}]?\\p{L}+|\\p{N}{1,3}| "
                                "?[^\\s\\p{L}\\p{N}]+[\\r\\n]*|\\s*[\\r\\n]+|\\s+(?!\\S)|\\s+",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_DEEPSEEK_LLM:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "[\r\n]",
                                "\\s?[A-Za-zµÀ-ÖØ-öø-ƺƼ-ƿǄ-ʓʕ-ʯͰ-ͳͶͷͻ-ͽͿΆΈ-ΊΌΎ-ΡΣ-ϵϷ-ҁҊ-ԯԱ-ՖႠ-ჅᎠ-Ᏽᏸ-ᏽᲐ-ᲺᲽ-Ჿᴀ-ᴫᵫ-ᵷᵹ-ᶚḀ-ἕἘ-Ἕἠ-ὅὈ-Ὅὐ-ὗὙὛὝὟ-ώᾀ-"
                                "ᾴᾶ-ᾼιῂ-ῄῆ-ῌῐ-ΐῖ-Ίῠ-Ῥῲ-ῴῶ-ῼℂℇℊ-ℓℕℙ-ℝℤΩℨK-ℭℯ-ℴℹℼ-ℿⅅ-ⅉⅎↃↄⰀ-ⱻⱾ-ⳤⳫ-ⳮⳲⳳꙀ-ꙭꚀ-ꚛꜢ-ꝯꝱ-ꞇꞋ-ꞎꭰ-ꮿﬀ-ﬆﬓ-ﬗＡ-Ｚａ-ｚ𐐀-𐑏𐒰-"
                                "𐓓𐓘-𐓻𐲀-𐲲𐳀-𐳲𑢠-𑣟𞤀-𞥃]+",
                                "\\s?[!-/:-~！-／：-～‘-‟　-。]+",
                                "\\s+$",
                                "[一-龥ࠀ-一가-퟿]+",
                                "\\p{N}+",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_DEEPSEEK_CODER:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "[\r\n]",
                                "\\s?\\p{L}+",
                                "\\s?\\p{P}+",
                                "[一-龥ࠀ-一가-퟿]+",
                                "\\p{N}",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_FALCON:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "[\\p{P}\\$\\+<=>\\^~\\|]+",
                                "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)",
                                "[0-9][0-9][0-9]",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_MPT:
                        // TODO: MPT pre-tokenization regexes are unknown
                        //       the following are close, but not exact. run the following:
                        //       ./bin/test-tokenizer-0 ../models/ggml-vocab-mpt.gguf
                        // GGML_ASSERT("MPT pre-tokenization regexes are unknown - fixes needed");
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "\\s?\\p{L}+",
                                "\\s?\\p{P}+",
                                "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_STARCODER:
                    case LLAMA_VOCAB_PRE_TYPE_REFACT:
                    case LLAMA_VOCAB_PRE_TYPE_COMMAND_R:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "\\p{N}",
                                "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_GPT2:
                    case LLAMA_VOCAB_PRE_TYPE_OLMO:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)",
                            });
                        break;
                    case LLAMA_VOCAB_PRE_TYPE_STABLELM2:
                    case LLAMA_VOCAB_PRE_TYPE_QWEN2:
                        word_collection = unicode_regex_split(
                            text,
                            {
                                // original regex from tokenizer.json
                                // "(?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\\r\\n\\p{L}\\p{N}]?\\p{L}+|\\p{N}| ?[^\\s\\p{L}\\p{N}]+[\\r\\n]*|\\s*[\\r\\n]+|\\s+(?!\\S)|\\s+"
                                "(?:'[sS]|'[tT]|'[rR][eE]|'[vV][eE]|'[mM]|'[lL][lL]|'[dD])|[^\\r\\n\\p{L}\\p{N}]?\\p{L}+|\\p{N}| "
                                "?[^\\s\\p{L}\\p{N}]+[\\r\\n]*|\\s*[\\r\\n]+|\\s+(?!\\S)|\\s+",
                            });
                        break;
                    default:
                        // default regex for BPE tokenization pre-processing
                        word_collection = unicode_regex_split(
                            text,
                            {
                                "[\\p{P}\\$\\+<=>\\^~\\|]+",
                                "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)",
                                "\\p{N}+",
                                "[0-9][0-9][0-9]",
                            });
                        break;
                }
                break;
            default:
                GGML_ASSERT(false);
                break;
        }

        symbols_final.clear();

        for (auto & word : word_collection)
        {
            work_queue = llm_bigram_bpe::queue();
            symbols.clear();

            int index = 0;
            size_t offset = 0;

            if (ignore_merges && vocab.token_to_id.find(word) != vocab.token_to_id.end())
            {
                symbols.emplace_back(llm_symbol{-1, -1, word.c_str(), word.size()});
                offset = word.size();
            }

            while (offset < word.size())
            {
                llm_symbol sym;
                size_t char_len = std::min(word.size() - offset, (size_t)::utf8_len(word[offset]));
                sym.text = word.c_str() + offset;
                sym.n = char_len;
                offset += sym.n;
                sym.prev = index - 1;
                sym.next = offset == word.size() ? -1 : index + 1;
                index++;
                symbols.emplace_back(sym);
            }
            for (size_t i = 1; i < symbols.size(); ++i)
            {
                add_new_bigram(i - 1, i);
            }

            // build token(s)
            while (!work_queue.empty())
            {
                auto bigram = work_queue.top();
                work_queue.pop();

                auto & left_symbol = symbols[bigram.left];
                auto & right_symbol = symbols[bigram.right];

                if (left_symbol.n == 0 || right_symbol.n == 0)
                {
                    continue;
                }
                std::string left_token = std::string(left_symbol.text, left_symbol.n);
                std::string right_token = std::string(right_symbol.text, right_symbol.n);
                if (left_token + right_token != bigram.text)
                {
                    continue; // Skip this bigram if it's outdated
                }

                // merge the right sym into the left one
                left_symbol.n += right_symbol.n;
                right_symbol.n = 0;

                // remove the right sym from the chain
                left_symbol.next = right_symbol.next;
                if (right_symbol.next >= 0)
                {
                    symbols[right_symbol.next].prev = bigram.left;
                }

                add_new_bigram(left_symbol.prev, bigram.left); // left side of current symbol
                add_new_bigram(bigram.left, left_symbol.next); // right side of current symbol
            }

            // add the finished tokens to the final list keeping correct order for next and prev
            for (auto & sym : symbols)
            {
                if (sym.n > 0)
                {
                    sym.prev = final_prev_index;
                    sym.next = -1;
                    if (final_prev_index != -1)
                    {
                        symbols_final[final_prev_index].next = symbols_final.size();
                    }
                    symbols_final.emplace_back(sym);
                    final_prev_index = symbols_final.size() - 1;
                }
            }
        }

        symbols = symbols_final;

        if (!symbols.empty())
        {
            for (int i = 0; i != -1; i = symbols[i].next)
            {
                auto & symbol = symbols[i];
                if (symbol.n == 0)
                {
                    continue;
                }

                const std::string str = std::string(symbol.text, symbol.n);
                const auto token = vocab.token_to_id.find(str);

                if (token == vocab.token_to_id.end())
                {
                    for (auto j = str.begin(); j != str.end(); ++j)
                    {
                        std::string byte_str(1, *j);
                        auto token_multibyte = vocab.token_to_id.find(byte_str);
                        if (token_multibyte == vocab.token_to_id.end())
                        {
                            throw std::runtime_error("ERROR: byte not found in vocab");
                        }
                        output.push_back((*token_multibyte).second);
                    }
                }
                else
                {
                    output.push_back((*token).second);
                }
            }
        }
    }

private:
    void add_new_bigram(int left, int right)
    {
        if (left == -1 || right == -1)
        {
            return;
        }

        std::string left_token = std::string(symbols[left].text, symbols[left].n);
        std::string right_token = std::string(symbols[right].text, symbols[right].n);

        int rank_found = -1;

        rank_found = vocab.find_bpe_rank(left_token, right_token);

        if (rank_found < 0)
        {
            return;
        }

        llm_bigram_bpe bigram;

        bigram.left = left;
        bigram.right = right;
        bigram.text = left_token + right_token;
        bigram.size = left_token.size() + right_token.size();
        bigram.rank = rank_found;

        work_queue.push(bigram);
    }

    const llama_vocab & vocab;

    std::vector<llm_symbol> symbols;
    std::vector<llm_symbol> symbols_final;

    llm_bigram_bpe::queue work_queue;
};

struct llm_tokenizer_wpm
{
    llm_tokenizer_wpm(const llama_vocab & vocab) : vocab(vocab) { }

    void tokenize(const std::string & text, std::vector<llama_vocab::id> & output)
    {
        auto * token_map = &vocab.token_to_id;

        // normalize and split by whitespace
        std::vector<std::string> words = preprocess(text);

        // bos token prepended already

        // find the longest tokens that form the words
        for (const std::string & word : words)
        {
            // skip empty words
            if (word.size() == 0)
            {
                continue;
            }

            // prepend phantom space
            std::string word1 = "\xe2\x96\x81" + word;
            int n = word1.size();

            // we're at the start of a new word
            int i = 0;
            bool match_any = false;

            // move through character position in word
            while (i < n)
            {
                // loop through possible match length
                bool match = false;
                for (int j = n; j > i; j--)
                {
                    auto it = token_map->find(word1.substr(i, j - i));
                    if (it != token_map->end())
                    {
                        output.push_back(it->second);
                        match = true;
                        match_any = true;
                        i = j;
                        break;
                    }
                }

                // must be an unknown character
                if (!match)
                {
                    i++;
                }
            }

            // we didn't find any matches for this word
            if (!match_any)
            {
                output.push_back(vocab.special_unk_id);
            }
        }
    }

    std::vector<std::string> preprocess(const std::string & text)
    {
        std::vector<uint32_t> cpts_nfd = unicode_cpts_normalize_nfd(unicode_cpts_from_utf8(text));

        // strip accents, strip control, uniformize whitespace,
        // to lowercase, pad chinese characters, pad punctuation
        std::string new_str = "";
        for (uint32_t code : cpts_nfd)
        {
            const codepoint_flags flags = unicode_cpt_flags(code);
            if (flags.is_accent_mark || flags.is_control)
            {
                continue;
            }
            code = unicode_tolower(code);
            if (flags.is_separator || flags.is_whitespace)
            { //####FIXME: is_separator ?
                code = ' ';
            }
            std::string s = unicode_cpt_to_utf8(code);
            if (flags.is_punctuation || is_ascii_punct(code) || is_chinese_char(code))
            {
                new_str += " ";
                new_str += s;
                new_str += " ";
            }
            else
            {
                new_str += s;
            }
        }

        // split by whitespace
        uint64_t l = 0;
        uint64_t r = 0;
        std::vector<std::string> words;
        while (r < new_str.size())
        {
            // if is whitespace
            if (isspace(new_str[r], std::locale::classic()))
            {
                if (r > l)
                    words.push_back(new_str.substr(l, (r - l)));
                l = r + 1;
                r = l;
            }
            else
            {
                r += 1;
            }
        }
        if (r > l)
        {
            words.push_back(new_str.substr(l, (r - l)));
        }
        return words;
    }

    bool is_ascii_punct(uint32_t code)
    {
        if (code > 0xFF)
        {
            return false;
        }
        auto c = char(static_cast<unsigned char>(code));
        return ispunct(c, std::locale::classic());
    }

    bool is_chinese_char(uint32_t cpt)
    {
        if ((cpt >= 0x4E00 && cpt <= 0x9FFF) || (cpt >= 0x3400 && cpt <= 0x4DBF) || (cpt >= 0x20000 && cpt <= 0x2A6DF)
            || (cpt >= 0x2A700 && cpt <= 0x2B73F) || (cpt >= 0x2B740 && cpt <= 0x2B81F) || (cpt >= 0x2B920 && cpt <= 0x2CEAF)
            || // this should be 0x2B820 but in hf rust code it is 0x2B920
            (cpt >= 0xF900 && cpt <= 0xFAFF) || (cpt >= 0x2F800 && cpt <= 0x2FA1F) || (cpt >= 0x3000 && cpt <= 0x303F)
            || (cpt >= 0xFF00 && cpt <= 0xFFEF))
        {
            return true; // NOLINT
        }
        return false;
    }

    const llama_vocab & vocab;
};

typedef enum FRAGMENT_BUFFER_VARIANT_TYPE
{
    FRAGMENT_BUFFER_VARIANT_TYPE_TOKEN,
    FRAGMENT_BUFFER_VARIANT_TYPE_RAW_TEXT
} FRAGMENT_BUFFER_VARIANT_TYPE;

struct fragment_buffer_variant
{
    fragment_buffer_variant(llama_vocab::id _token)
        : type(FRAGMENT_BUFFER_VARIANT_TYPE_TOKEN), token(_token), raw_text(_dummy), offset(0), length(0)
    {
    }

    fragment_buffer_variant(const std::string & _raw_text, int64_t _offset, int64_t _length)
        : type(FRAGMENT_BUFFER_VARIANT_TYPE_RAW_TEXT), token((llama_vocab::id)-1), raw_text(_raw_text), offset(_offset), length(_length)
    {
        GGML_ASSERT(_offset >= 0);
        GGML_ASSERT(_length >= 1);
        GGML_ASSERT(offset + length <= raw_text.length());
    }

    const FRAGMENT_BUFFER_VARIANT_TYPE type;
    const llama_vocab::id token;
    const std::string _dummy;
    const std::string & raw_text;
    const uint64_t offset;
    const uint64_t length;
};

// #define PRETOKENIZERDEBUG

static void tokenizer_st_partition(const llama_vocab & vocab, std::forward_list<fragment_buffer_variant> & buffer)
{
    // for each special token
    for (const auto & st : vocab.special_tokens_cache)
    {
        const auto & special_token = st.first;
        const auto & special_id = st.second;

        // for each text fragment
        std::forward_list<fragment_buffer_variant>::iterator it = buffer.begin();
        while (it != buffer.end())
        {
            auto & fragment = (*it);

            // if a fragment is text ( not yet processed )
            if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_RAW_TEXT)
            {
                auto * raw_text = &(fragment.raw_text);

                auto raw_text_base_offset = fragment.offset;
                auto raw_text_base_length = fragment.length;

                // loop over the text
                while (true)
                {
                    // find the first occurrence of a given special token in this fragment
                    //  passing offset argument only limit the "search area" but match coordinates
                    //  are still relative to the source full raw_text
                    auto match = raw_text->find(special_token, raw_text_base_offset);

                    // no occurrences found, stop processing this fragment for a given special token
                    if (match == std::string::npos)
                        break;

                    // check if match is within bounds of offset <-> length
                    if (match + special_token.length() > raw_text_base_offset + raw_text_base_length)
                        break;

#ifdef PRETOKENIZERDEBUG
                    LLAMA_LOG_WARN(
                        "FF: (%ld %ld %ld) '%s'\n",
                        raw_text->length(),
                        raw_text_base_offset,
                        raw_text_base_length,
                        raw_text->substr(raw_text_base_offset, raw_text_base_length).c_str());
#endif
                    auto source = std::distance(buffer.begin(), it);

                    // if match is further than base offset
                    //  then we have some text to the left of it
                    if (match > raw_text_base_offset)
                    {
                        // left
                        const int64_t left_reminder_offset = raw_text_base_offset + 0;
                        const int64_t left_reminder_length = match - raw_text_base_offset;
                        buffer.emplace_after(it, (*raw_text), left_reminder_offset, left_reminder_length);

#ifdef PRETOKENIZERDEBUG
                        LLAMA_LOG_WARN(
                            "FL: (%ld %ld) '%s'\n",
                            left_reminder_offset,
                            left_reminder_length,
                            raw_text->substr(left_reminder_offset, left_reminder_length).c_str());
#endif
                        it++;
                    }

                    // special token
                    buffer.emplace_after(it, special_id);
                    it++;

                    // right
                    if (match + special_token.length() < raw_text_base_offset + raw_text_base_length)
                    {
                        const int64_t right_reminder_offset = match + special_token.length();
                        const int64_t right_reminder_length
                            = raw_text_base_length - ((match - raw_text_base_offset) + special_token.length());
                        buffer.emplace_after(it, (*raw_text), right_reminder_offset, right_reminder_length);

#ifdef PRETOKENIZERDEBUG
                        LLAMA_LOG_WARN(
                            "FR: (%ld %ld) '%s'\n",
                            right_reminder_offset,
                            right_reminder_length,
                            raw_text->substr(right_reminder_offset, right_reminder_length).c_str());
#endif

                        it++;

                        if (source == 0)
                        {
                            buffer.erase_after(buffer.before_begin());
                        }
                        else
                        {
                            buffer.erase_after(std::next(buffer.begin(), (source - 1)));
                        }

                        // repeat for the right side
                        raw_text_base_offset = right_reminder_offset;
                        raw_text_base_length = right_reminder_length;

#ifdef PRETOKENIZERDEBUG
                        LLAMA_LOG_WARN(
                            "RR: (%ld %ld) '%s'\n",
                            raw_text_base_offset,
                            raw_text_base_length,
                            raw_text->substr(raw_text_base_offset, raw_text_base_length).c_str());
#endif
                    }
                    else
                    {
                        if (source == 0)
                        {
                            buffer.erase_after(buffer.before_begin());
                        }
                        else
                        {
                            buffer.erase_after(std::next(buffer.begin(), (source - 1)));
                        }
                        break;
                    }
                }
            }
            it++;
        }
    }
}

static std::vector<llama_vocab::id>
llama_tokenize_internal(const llama_vocab & vocab, std::string raw_text, bool add_special, bool parse_special)
{
    std::vector<llama_vocab::id> output;
    std::forward_list<fragment_buffer_variant> fragment_buffer;

    if (!raw_text.empty())
    {
        fragment_buffer.emplace_front(raw_text, 0, raw_text.length());
        if (parse_special)
            tokenizer_st_partition(vocab, fragment_buffer);
    }
    switch (vocab.type)
    {
        case LLAMA_VOCAB_TYPE_SPM: {
            // OG tokenizer behavior:
            //
            // tokenizer.encode('', add_special_tokens=True)  returns [1]
            // tokenizer.encode('', add_special_tokens=False) returns []

            static const bool rtrim = true; //TODO: as param
            bool is_prev_special = false;
            bool special_token_rtrim = false;

            if (add_special && vocab.special_add_bos != 0)
            {
                GGML_ASSERT(vocab.special_bos_id != -1);
                output.push_back(vocab.special_bos_id);
                is_prev_special = true;
            }

            for (const auto & fragment : fragment_buffer)
            {
                if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_RAW_TEXT)
                {
                    // without adding this leading whitespace, we do not get the same results as the original tokenizer

                    // TODO: It's likely possible to get rid of this string copy entirely
                    //  by modifying llm_tokenizer_x to operate with string offsets like pre-tokenizer
                    //  and passing 'add space prefix' as bool argument
                    //
                    auto raw_text = fragment.raw_text.substr(fragment.offset, fragment.length);

                    if (special_token_rtrim)
                    {
                        size_t num_whitespaces = 0;
                        while (isspace(raw_text[num_whitespaces]))
                        {
                            num_whitespaces++;
                        }
                        if (num_whitespaces == raw_text.size())
                        {
                            continue; // skip if all whitespaces
                        }
                        raw_text = raw_text.substr(num_whitespaces);
                    }

                    if (vocab.add_space_prefix)
                    {
                        if (!output.size() || is_prev_special)
                        { // prefix with space if first token
                            raw_text = " " + raw_text;
                        }
                    }

#ifdef PRETOKENIZERDEBUG
                    LLAMA_LOG_WARN("TT: (%ld %ld %ld) '%s'\n", raw_text.length(), fragment.offset, fragment.length, raw_text.c_str());
#endif
                    llm_tokenizer_spm tokenizer(vocab);
                    llama_escape_whitespace(raw_text);
                    tokenizer.tokenize(raw_text, output);
                }
                else
                { // if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_TOKEN)
                    output.push_back(fragment.token);
                    is_prev_special = true;
                    // phi-3 special tokens without rtrim, works fine for llama-spm too
                    special_token_rtrim = rtrim && fragment.token != vocab.special_bos_id && fragment.token != vocab.special_unk_id
                        && fragment.token != vocab.special_eos_id;
                }
            }

            if (add_special && vocab.special_add_bos != 0 && output.size() >= 2 && output[1] == vocab.special_bos_id)
            {
                LLAMA_LOG_WARN(
                    "%s: Added a BOS token to the prompt as specified by the model but the prompt "
                    "also starts with a BOS token. So now the final prompt starts with 2 BOS tokens. "
                    "Are you sure this is what you want?\n",
                    __FUNCTION__);
            }

            if (add_special && vocab.special_add_eos == 1)
            {
                GGML_ASSERT(vocab.special_eos_id != -1);
                output.push_back(vocab.special_eos_id);
            }
        }
        break;
        case LLAMA_VOCAB_TYPE_BPE: {
            if (add_special && vocab.special_add_bos != 0)
            {
                GGML_ASSERT(vocab.special_bos_id != -1);
                output.push_back(vocab.special_bos_id);
            }

            for (const auto & fragment : fragment_buffer)
            {
                if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_RAW_TEXT)
                {
                    auto raw_text = fragment.raw_text.substr(fragment.offset, fragment.length);

#ifdef PRETOKENIZERDEBUG
                    LLAMA_LOG_WARN("TT: (%ld %ld %ld) '%s'\n", raw_text.length(), fragment.offset, fragment.length, raw_text.c_str());
#endif
                    llm_tokenizer_bpe tokenizer(vocab);
                    tokenizer.tokenize(raw_text, output);
                }
                else
                { // if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_TOKEN)
                    output.push_back(fragment.token);
                }
            }

            if (add_special && vocab.special_add_bos != 0 && output.size() >= 2 && output[1] == vocab.special_bos_id)
            {
                LLAMA_LOG_WARN(
                    "%s: Added a BOS token to the prompt as specified by the model but the prompt "
                    "also starts with a BOS token. So now the final prompt starts with 2 BOS tokens. "
                    "Are you sure this is what you want?\n",
                    __FUNCTION__);
            }

            if (add_special && vocab.special_add_eos == 1)
            {
                GGML_ASSERT(vocab.special_add_eos != -1);
                output.push_back(vocab.special_eos_id);
            }
        }
        break;
        case LLAMA_VOCAB_TYPE_WPM: {
            if (add_special)
            {
                GGML_ASSERT(vocab.special_cls_id != -1);
                output.push_back(vocab.special_cls_id);
            }

            for (const auto & fragment : fragment_buffer)
            {
                if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_RAW_TEXT)
                {
                    auto raw_text = fragment.raw_text.substr(fragment.offset, fragment.length);

#ifdef PRETOKENIZERDEBUG
                    LLAMA_LOG_WARN("TT: (%ld %ld %ld) '%s'\n", raw_text.length(), fragment.offset, fragment.length, raw_text.c_str());
#endif
                    llm_tokenizer_wpm tokenizer(vocab);
                    tokenizer.tokenize(raw_text, output);
                }
                else
                { // if (fragment.type == FRAGMENT_BUFFER_VARIANT_TYPE_TOKEN)
                    output.push_back(fragment.token);
                }
            }

            if (add_special)
            {
                GGML_ASSERT(vocab.special_sep_id != -1);
                output.push_back(vocab.special_sep_id);
            }
        }
        break;
        case LLAMA_VOCAB_TYPE_NONE:
            GGML_ASSERT(false);
    }

    return output;
}

//
// grammar - internal
//


// Decodes a UTF-8 string which may end in an incomplete sequence. Adds a terminating 0 for use as
// pointer. If an invalid sequence is encountered, returns `llama_partial_utf8.n_remain == -1`.
std::pair<std::vector<uint32_t>, llama_partial_utf8> decode_utf8(const std::string & src, llama_partial_utf8 partial_start)
{
    static const int lookup[] = {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 2, 2, 3, 4};
    const char * pos = src.c_str();
    std::vector<uint32_t> code_points;
    // common english strings have the same number of codepoints and bytes. `+ 1` for the terminating 0.
    code_points.reserve(src.size() + 1);
    uint32_t value = partial_start.value;
    int n_remain = partial_start.n_remain;

    // continue previous decode, if applicable
    while (*pos != 0 && n_remain > 0)
    {
        uint8_t next_byte = static_cast<uint8_t>(*pos);
        if ((next_byte >> 6) != 2)
        {
            // invalid sequence, abort
            code_points.push_back(0);
            return std::make_pair(std::move(code_points), llama_partial_utf8{0, -1});
        }
        value = (value << 6) + (next_byte & 0x3F);
        ++pos;
        --n_remain;
    }

    if (partial_start.n_remain > 0 && n_remain == 0)
    {
        code_points.push_back(value);
    }

    // decode any subsequent utf-8 sequences, which may end in an incomplete one
    while (*pos != 0)
    {
        uint8_t first_byte = static_cast<uint8_t>(*pos);
        uint8_t highbits = first_byte >> 4;
        n_remain = lookup[highbits] - 1;

        if (n_remain < 0)
        {
            // invalid sequence, abort
            code_points.clear();
            code_points.push_back(0);
            return std::make_pair(std::move(code_points), llama_partial_utf8{0, n_remain});
        }

        uint8_t mask = (1 << (7 - n_remain)) - 1;
        value = first_byte & mask;
        ++pos;
        while (*pos != 0 && n_remain > 0)
        {
            value = (value << 6) + (static_cast<uint8_t>(*pos) & 0x3F);
            ++pos;
            --n_remain;
        }
        if (n_remain == 0)
        {
            code_points.push_back(value);
        }
    }
    code_points.push_back(0);

    return std::make_pair(std::move(code_points), llama_partial_utf8{value, n_remain});
}

// returns true iff pos points to the end of one of the definitions of a rule
static bool llama_grammar_is_end_of_sequence(const llama_grammar_element * pos)
{
    switch (pos->type)
    {
        case LLAMA_GRETYPE_END:
            return true; // NOLINT
        case LLAMA_GRETYPE_ALT:
            return true; // NOLINT
        default:
            return false;
    }
}

// returns true iff chr satisfies the char range at pos (regular or inverse range)
// asserts that pos is pointing to a char range element
static std::pair<bool, const llama_grammar_element *> llama_grammar_match_char(const llama_grammar_element * pos, const uint32_t chr)
{
    bool found = false;
    bool is_positive_char = pos->type == LLAMA_GRETYPE_CHAR;

    GGML_ASSERT(is_positive_char || pos->type == LLAMA_GRETYPE_CHAR_NOT); // NOLINT

    do
    {
        if (pos[1].type == LLAMA_GRETYPE_CHAR_RNG_UPPER)
        {
            // inclusive range, e.g. [a-z]
            found = found || (pos->value <= chr && chr <= pos[1].value);
            pos += 2;
        }
        else
        {
            // exact char match, e.g. [a] or "a"
            found = found || pos->value == chr;
            pos += 1;
        }
    } while (pos->type == LLAMA_GRETYPE_CHAR_ALT);

    return std::make_pair(found == is_positive_char, pos);
}

// returns true iff some continuation of the given partial UTF-8 sequence could satisfy the char
// range at pos (regular or inverse range)
// asserts that pos is pointing to a char range element
static bool llama_grammar_match_partial_char(const llama_grammar_element * pos, const llama_partial_utf8 partial_utf8)
{
    bool is_positive_char = pos->type == LLAMA_GRETYPE_CHAR;
    GGML_ASSERT(is_positive_char || pos->type == LLAMA_GRETYPE_CHAR_NOT);

    uint32_t partial_value = partial_utf8.value;
    int n_remain = partial_utf8.n_remain;

    // invalid sequence or 7-bit char split across 2 bytes (overlong)
    if (n_remain < 0 || (n_remain == 1 && partial_value < 2))
    {
        return false;
    }

    // range of possible code points this partial UTF-8 sequence could complete to
    uint32_t low = partial_value << (n_remain * 6);
    uint32_t high = low | ((1 << (n_remain * 6)) - 1);

    if (low == 0)
    {
        if (n_remain == 2)
        {
            low = 1 << 11;
        }
        else if (n_remain == 3)
        {
            low = 1 << 16;
        }
    }

    do
    {
        if (pos[1].type == LLAMA_GRETYPE_CHAR_RNG_UPPER)
        {
            // inclusive range, e.g. [a-z]
            if (pos->value <= high && low <= pos[1].value)
            {
                return is_positive_char;
            }
            pos += 2;
        }
        else
        {
            // exact char match, e.g. [a] or "a"
            if (low <= pos->value && pos->value <= high)
            {
                return is_positive_char;
            }
            pos += 1;
        }
    } while (pos->type == LLAMA_GRETYPE_CHAR_ALT);

    return !is_positive_char;
}


// transforms a grammar pushdown stack into N possible stacks, all ending
// at a character range (terminal element)
static void llama_grammar_advance_stack(
    const std::vector<std::vector<llama_grammar_element>> & rules,
    const std::vector<const llama_grammar_element *> & stack,
    std::vector<std::vector<const llama_grammar_element *>> & new_stacks)
{
    if (stack.empty())
    {
        if (std::find(new_stacks.begin(), new_stacks.end(), stack) == new_stacks.end())
        {
            new_stacks.emplace_back(stack);
        }
        return;
    }

    const llama_grammar_element * pos = stack.back();

    switch (pos->type)
    {
        case LLAMA_GRETYPE_RULE_REF: {
            const size_t rule_id = static_cast<size_t>(pos->value);
            const llama_grammar_element * subpos = rules[rule_id].data();
            do
            {
                // init new stack without the top (pos)
                std::vector<const llama_grammar_element *> new_stack(stack.begin(), stack.end() - 1);
                if (!llama_grammar_is_end_of_sequence(pos + 1))
                {
                    // if this rule ref is followed by another element, add that to stack
                    new_stack.push_back(pos + 1);
                }
                if (!llama_grammar_is_end_of_sequence(subpos))
                {
                    // if alternate is nonempty, add to stack
                    new_stack.push_back(subpos);
                }
                llama_grammar_advance_stack(rules, new_stack, new_stacks);
                while (!llama_grammar_is_end_of_sequence(subpos))
                {
                    // scan to end of alternate def
                    subpos++;
                }
                if (subpos->type == LLAMA_GRETYPE_ALT)
                {
                    // there's another alternate def of this rule to process
                    subpos++;
                }
                else
                {
                    break;
                }
            } while (true);
            break;
        }
        case LLAMA_GRETYPE_CHAR:
        case LLAMA_GRETYPE_CHAR_NOT:
            if (std::find(new_stacks.begin(), new_stacks.end(), stack) == new_stacks.end())
            {
                // only add the stack if it's not a duplicate of one we already have
                new_stacks.emplace_back(stack);
            }
            break;
        default:
            // end of alternate (LLAMA_GRETYPE_END, LLAMA_GRETYPE_ALT) or middle of char range
            // (LLAMA_GRETYPE_CHAR_ALT, LLAMA_GRETYPE_CHAR_RNG_UPPER); stack should never be left on
            // those
            GGML_ASSERT(false);
    }
}

// takes a set of possible pushdown stacks on a grammar, which are required to
// be positioned at a character range (see `llama_grammar_advance_stack`), and
// produces the N possible stacks if the given char is accepted at those
// positions
void llama_grammar_accept(
    const std::vector<std::vector<llama_grammar_element>> & rules,
    const std::vector<std::vector<const llama_grammar_element *>> & stacks,
    const uint32_t chr,
    std::vector<std::vector<const llama_grammar_element *>> & new_stacks)
{
    new_stacks.clear();

    for (const auto & stack : stacks)
    {
        if (stack.empty())
        {
            continue;
        }

        auto match = llama_grammar_match_char(stack.back(), chr);
        if (match.first)
        {
            const llama_grammar_element * pos = match.second;

            // update top of stack to next element, if any
            std::vector<const llama_grammar_element *> new_stack(stack.begin(), stack.end() - 1);
            if (!llama_grammar_is_end_of_sequence(pos))
            {
                new_stack.push_back(pos);
            }
            llama_grammar_advance_stack(rules, new_stack, new_stacks);
        }
    }
}

static std::vector<llama_grammar_candidate> llama_grammar_reject_candidates(
    const std::vector<std::vector<llama_grammar_element>> & rules,
    const std::vector<std::vector<const llama_grammar_element *>> & stacks,
    const std::vector<llama_grammar_candidate> & candidates);

static std::vector<llama_grammar_candidate> llama_grammar_reject_candidates_for_stack(
    const std::vector<std::vector<llama_grammar_element>> & rules,
    const std::vector<const llama_grammar_element *> & stack,
    const std::vector<llama_grammar_candidate> & candidates)
{
    std::vector<llama_grammar_candidate> rejects;
    rejects.reserve(candidates.size());

    if (stack.empty())
    {
        for (const auto & tok : candidates)
        {
            if (*tok.code_points != 0 || tok.partial_utf8.n_remain != 0)
            {
                rejects.push_back(tok);
            }
        }
        return rejects;
    }

    const llama_grammar_element * stack_pos = stack.back();

    std::vector<llama_grammar_candidate> next_candidates;
    next_candidates.reserve(candidates.size());

    for (const auto & tok : candidates)
    {
        if (*tok.code_points == 0)
        {
            // reached end of full codepoints in token, reject iff it ended in a partial sequence
            // that cannot satisfy this position in grammar
            if (tok.partial_utf8.n_remain != 0 && !llama_grammar_match_partial_char(stack_pos, tok.partial_utf8))
            {
                rejects.push_back(tok);
            }
        }
        else if (llama_grammar_match_char(stack_pos, *tok.code_points).first)
        {
            next_candidates.push_back({tok.index, tok.code_points + 1, tok.partial_utf8});
        }
        else
        {
            rejects.push_back(tok);
        }
    }

    const auto * stack_pos_after = llama_grammar_match_char(stack_pos, 0).second;

    // update top of stack to next element, if any
    std::vector<const llama_grammar_element *> stack_after(stack.begin(), stack.end() - 1);
    if (!llama_grammar_is_end_of_sequence(stack_pos_after))
    {
        stack_after.push_back(stack_pos_after);
    }
    std::vector<std::vector<const llama_grammar_element *>> next_stacks;
    llama_grammar_advance_stack(rules, stack_after, next_stacks);

    auto next_rejects = llama_grammar_reject_candidates(rules, next_stacks, next_candidates);
    for (const auto & tok : next_rejects)
    {
        rejects.push_back({tok.index, tok.code_points - 1, tok.partial_utf8});
    }

    return rejects;
}

static std::vector<llama_grammar_candidate> llama_grammar_reject_candidates(
    const std::vector<std::vector<llama_grammar_element>> & rules,
    const std::vector<std::vector<const llama_grammar_element *>> & stacks,
    const std::vector<llama_grammar_candidate> & candidates)
{
    GGML_ASSERT(!stacks.empty()); // REVIEW

    if (candidates.empty())
    {
        return std::vector<llama_grammar_candidate>();
    }

    auto rejects = llama_grammar_reject_candidates_for_stack(rules, stacks.front(), candidates);

    for (size_t i = 1, size = stacks.size(); i < size; ++i)
    {
        rejects = llama_grammar_reject_candidates_for_stack(rules, stacks[i], rejects);
    }
    return rejects;
}

static bool llama_grammar_detect_left_recursion(
    const std::vector<std::vector<llama_grammar_element>> & rules,
    size_t rule_index,
    std::vector<bool> * rules_visited,
    std::vector<bool> * rules_in_progress,
    std::vector<bool> * rules_may_be_empty)
{
    if ((*rules_in_progress)[rule_index])
    {
        return true;
    }

    (*rules_in_progress)[rule_index] = true;

    const std::vector<llama_grammar_element> & rule = rules[rule_index];

    // First check if the rule might produce the empty string. This could be done combined with the second
    // step but it's more readable as two steps.
    bool at_rule_start = true;
    for (size_t i = 0; i < rule.size(); i++)
    {
        if (llama_grammar_is_end_of_sequence(&rule[i]))
        {
            if (at_rule_start)
            {
                (*rules_may_be_empty)[rule_index] = true;
                break;
            }
            at_rule_start = true;
        }
        else
        {
            at_rule_start = false;
        }
    }

    // Second, recurse into leftmost nonterminals (or next-leftmost as long as the previous nonterminal may
    // be empty)
    bool recurse_into_nonterminal = true;
    for (size_t i = 0; i < rule.size(); i++)
    {
        if (rule[i].type == LLAMA_GRETYPE_RULE_REF && recurse_into_nonterminal)
        {
            if (llama_grammar_detect_left_recursion(rules, (size_t)rule[i].value, rules_visited, rules_in_progress, rules_may_be_empty))
            {
                return true;
            }
            if (!((*rules_may_be_empty)[(size_t)rule[i].value]))
            {
                recurse_into_nonterminal = false;
            }
        }
        else if (llama_grammar_is_end_of_sequence(&rule[i]))
        {
            recurse_into_nonterminal = true;
        }
        else
        {
            recurse_into_nonterminal = false;
        }
    }

    (*rules_in_progress)[rule_index] = false;
    (*rules_visited)[rule_index] = true;
    return false;
}

//
// grammar - external
//

struct llama_grammar * llama_grammar_init(const llama_grammar_element ** rules, size_t n_rules, size_t start_rule_index)
{
    const llama_grammar_element * pos;

    // copy rule definitions into vectors
    std::vector<std::vector<llama_grammar_element>> vec_rules(n_rules);
    for (size_t i = 0; i < n_rules; i++)
    {
        for (pos = rules[i]; pos->type != LLAMA_GRETYPE_END; pos++)
        {
            vec_rules[i].push_back(*pos);
        }
        vec_rules[i].push_back({LLAMA_GRETYPE_END, 0});
    }

    // Check for left recursion
    std::vector<bool> rules_visited(n_rules);
    std::vector<bool> rules_in_progress(n_rules);
    std::vector<bool> rules_may_be_empty(n_rules);
    for (size_t i = 0; i < n_rules; i++)
    {
        if (rules_visited[i])
        {
            continue;
        }
        if (llama_grammar_detect_left_recursion(vec_rules, i, &rules_visited, &rules_in_progress, &rules_may_be_empty))
        {
            throw std::runtime_error(format("unsupported grammar, left recursion detected for nonterminal at index %zu", i));
        }
    }

    // loop over alternates of start rule to build initial stacks
    std::vector<std::vector<const llama_grammar_element *>> stacks;
    pos = vec_rules[start_rule_index].data();
    do
    {
        std::vector<const llama_grammar_element *> stack;
        if (!llama_grammar_is_end_of_sequence(pos))
        {
            // if alternate is nonempty, add to stack
            stack.push_back(pos);
        }
        llama_grammar_advance_stack(vec_rules, stack, stacks);
        while (!llama_grammar_is_end_of_sequence(pos))
        {
            // scan to end of alternate def
            pos++;
        }
        if (pos->type == LLAMA_GRETYPE_ALT)
        {
            // there's another alternate def of this rule to process
            pos++;
        }
        else
        {
            break;
        }
    } while (true);

    // Important: vec_rules has to be moved here, not copied, because stacks contains
    // pointers to elements of vec_rules. If vec_rules were copied into llama_grammar
    // then the pointers would be invalidated when the local vec_rules goes out of scope.
    return new llama_grammar{std::move(vec_rules), std::move(stacks), {}};
}

void llama_grammar_free(struct llama_grammar * grammar)
{
    delete grammar;
}

struct llama_grammar * llama_grammar_copy(const struct llama_grammar * grammar)
{
    llama_grammar * result = new llama_grammar{grammar->rules, grammar->stacks, grammar->partial_utf8};

    // redirect elements in stacks to point to new rules
    for (size_t is = 0; is < result->stacks.size(); is++)
    {
        for (size_t ie = 0; ie < result->stacks[is].size(); ie++)
        {
            for (size_t ir0 = 0; ir0 < grammar->rules.size(); ir0++)
            {
                for (size_t ir1 = 0; ir1 < grammar->rules[ir0].size(); ir1++)
                {
                    if (grammar->stacks[is][ie] == &grammar->rules[ir0][ir1])
                    {
                        result->stacks[is][ie] = &result->rules[ir0][ir1];
                    }
                }
            }
        }
    }

    return result;
}

//
// sampling
//

void llama_set_rng_seed(struct llama_context * ctx, uint32_t seed)
{
    if (seed == LLAMA_DEFAULT_SEED)
    {
        seed = time(nullptr);
    }
    ctx->rng.seed(seed);
}

void llama_sample_softmax(struct llama_context * ctx, llama_token_data_array * candidates)
{
    GGML_ASSERT(candidates->size > 0);

    const int64_t t_start_sample_us = ggml_time_us();

    // Sort the logits in descending order
    if (!candidates->sorted)
    {
        std::sort(
            candidates->data,
            candidates->data + candidates->size,
            [](const llama_token_data & a, const llama_token_data & b) { return a.logit > b.logit; });
        candidates->sorted = true;
    }

    float max_l = candidates->data[0].logit;
    float cum_sum = 0.0f;
    for (size_t i = 0; i < candidates->size; ++i)
    {
        float p = expf(candidates->data[i].logit - max_l);
        candidates->data[i].p = p;
        cum_sum += p;
    }
    for (size_t i = 0; i < candidates->size; ++i)
    {
        candidates->data[i].p /= cum_sum;
    }

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_top_k(struct llama_context * ctx, llama_token_data_array * candidates, int32_t k, size_t min_keep)
{
    // TODO: move bucket sort to separate function so that top_p/tail_free/typical/softmax first is equally fast
    // if (k >= (int32_t)candidates->size) {
    //     return;
    // }

    const int64_t t_start_sample_us = ggml_time_us();

    if (k <= 0)
    {
        k = candidates->size;
    }

    k = std::max(k, (int)min_keep);
    k = std::min(k, (int)candidates->size);

    // Sort scores in descending order
    if (!candidates->sorted)
    {
        auto comp = [](const llama_token_data & a, const llama_token_data & b) { return a.logit > b.logit; };
        if (k <= 128)
        {
            std::partial_sort(candidates->data, candidates->data + k, candidates->data + candidates->size, comp);
        }
        else
        {
            constexpr int nbuckets = 128;
            constexpr float bucket_low = -10.0f;
            constexpr float bucket_high = 10.0f;
            constexpr float bucket_scale = nbuckets / (bucket_high - bucket_low);
            constexpr float bucker_inter = -bucket_low * bucket_scale;

            std::vector<int> bucket_idx(candidates->size);
            std::vector<int> histo(nbuckets, 0);

            for (int i = 0; i < (int)candidates->size; ++i)
            {
                const float val = candidates->data[i].logit;
                int ib = int(bucket_scale * val + bucker_inter); //nbuckets * (val - bucket_low) / (bucket_high - bucket_low);
                ib = std::max(0, std::min(nbuckets - 1, ib));
                bucket_idx[i] = ib;
                ++histo[ib];
            }
            int nhave = 0;
            int ib = nbuckets - 1;
            for (; ib >= 0; --ib)
            {
                nhave += histo[ib];
                if (nhave >= k)
                    break;
            }
            std::vector<llama_token_data> tmp_tokens(nhave);
            auto ptr = tmp_tokens.data();
            std::vector<llama_token_data *> bucket_ptrs;
            bucket_ptrs.reserve(nbuckets - ib);
            for (int j = nbuckets - 1; j >= ib; --j)
            {
                bucket_ptrs.push_back(ptr);
                ptr += histo[j];
            }
            for (int i = 0; i < (int)candidates->size; ++i)
            {
                int j = bucket_idx[i];
                if (j >= ib)
                {
                    *bucket_ptrs[nbuckets - 1 - j]++ = candidates->data[i];
                }
            }

            ptr = tmp_tokens.data();
            int ndone = 0;
            for (int j = nbuckets - 1; j > ib; --j)
            {
                std::sort(ptr, ptr + histo[j], comp);
                ptr += histo[j];
                ndone += histo[j];
            }
            std::partial_sort(ptr, ptr + k - ndone, ptr + histo[ib], comp);

            std::memcpy(candidates->data, tmp_tokens.data(), k * sizeof(llama_token_data));
        }
        candidates->sorted = true;
    }
    candidates->size = k;

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_top_p(struct llama_context * ctx, llama_token_data_array * candidates, float p, size_t min_keep)
{
    if (p >= 1.0f)
    {
        return;
    }

    llama_sample_softmax(ctx, candidates);

    const int64_t t_start_sample_us = ggml_time_us();

    // Compute the cumulative probabilities
    float cum_sum = 0.0f;
    size_t last_idx = candidates->size;

    for (size_t i = 0; i < candidates->size; ++i)
    {
        cum_sum += candidates->data[i].p;

        // Check if the running sum is at least p or if we have kept at least min_keep tokens
        // we set the last index to i+1 to indicate that the current iterate should be included in the set
        if (cum_sum >= p && i + 1 >= min_keep)
        {
            last_idx = i + 1;
            break;
        }
    }

    // Resize the output vector to keep only the top-p tokens
    candidates->size = last_idx;

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_min_p(struct llama_context * ctx, llama_token_data_array * candidates, float p, size_t min_keep)
{
    if (p <= 0.0f || !candidates->size)
    {
        return;
    }

    const int64_t t_start_sample_us = ggml_time_us();

    bool min_p_applied = false;

    // if the candidates aren't sorted, try the unsorted implementation first
    if (!candidates->sorted)
    {
        std::vector<llama_token_data> filtered_tokens;

        float max_logit = -FLT_MAX;
        for (size_t i = 0; i < candidates->size; ++i)
        {
            max_logit = std::max(max_logit, candidates->data[i].logit);
        }
        const float min_logit = max_logit + logf(p); // min logit for p_i >= p * p_max

        for (size_t i = 0; i < candidates->size; ++i)
        {
            if (candidates->data[i].logit >= min_logit)
            {
                filtered_tokens.push_back(candidates->data[i]);
            }
        }

        // if we have enough values the operation was a success
        if (filtered_tokens.size() >= min_keep)
        {
            memcpy(candidates->data, filtered_tokens.data(), filtered_tokens.size() * sizeof(llama_token_data));
            candidates->size = filtered_tokens.size();
            min_p_applied = true;
        }
    }

    // if the candidates are sorted or the unsorted implementation failed, use this implementation
    if (!min_p_applied)
    {
        // Sort the logits in descending order
        if (!candidates->sorted)
        {
            std::sort(
                candidates->data,
                candidates->data + candidates->size,
                [](const llama_token_data & a, const llama_token_data & b) { return a.logit > b.logit; });
            candidates->sorted = true;
        }

        const float min_logit = candidates->data[0].logit + logf(p); // min logit for p_i >= p * p_max
        size_t i = 1; // first token always matches

        for (; i < candidates->size; ++i)
        {
            if (candidates->data[i].logit < min_logit && i >= min_keep)
            {
                break; // prob too small
            }
        }

        // Resize the output vector to keep only the matching tokens
        candidates->size = i;
    }

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_tail_free(struct llama_context * ctx, llama_token_data_array * candidates, float z, size_t min_keep)
{
    if (z >= 1.0f || candidates->size <= 2)
    {
        return;
    }

    llama_sample_softmax(nullptr, candidates);
    const int64_t t_start_sample_us = ggml_time_us();

    // Compute the first and second derivatives
    std::vector<float> first_derivatives(candidates->size - 1);
    std::vector<float> second_derivatives(candidates->size - 2);

    for (size_t i = 0; i < first_derivatives.size(); ++i)
    {
        first_derivatives[i] = candidates->data[i].p - candidates->data[i + 1].p;
    }
    for (size_t i = 0; i < second_derivatives.size(); ++i)
    {
        second_derivatives[i] = first_derivatives[i] - first_derivatives[i + 1];
    }

    // Calculate absolute value of second derivatives
    for (size_t i = 0; i < second_derivatives.size(); ++i)
    {
        second_derivatives[i] = std::abs(second_derivatives[i]);
    }

    // Normalize the second derivatives
    {
        const float second_derivatives_sum = std::accumulate(second_derivatives.begin(), second_derivatives.end(), 0.0f);

        if (second_derivatives_sum > 1e-6f)
        {
            for (float & value : second_derivatives)
            {
                value /= second_derivatives_sum;
            }
        }
        else
        {
            for (float & value : second_derivatives)
            {
                value = 1.0f / second_derivatives.size();
            }
        }
    }

    float cum_sum = 0.0f;
    size_t last_idx = candidates->size;
    for (size_t i = 0; i < second_derivatives.size(); ++i)
    {
        cum_sum += second_derivatives[i];

        // Check if the running sum is greater than z or if we have kept at least min_keep tokens
        if (cum_sum > z && i >= min_keep)
        {
            last_idx = i;
            break;
        }
    }

    // Resize the output vector to keep only the tokens above the tail location
    candidates->size = last_idx;

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_typical(struct llama_context * ctx, llama_token_data_array * candidates, float p, size_t min_keep)
{
    // Reference implementation:
    // https://github.com/huggingface/transformers/compare/main...cimeister:typical-sampling:typical-pr
    if (p >= 1.0f)
    {
        return;
    }

    // Compute the softmax of logits and calculate entropy
    llama_sample_softmax(nullptr, candidates);

    const int64_t t_start_sample_us = ggml_time_us();

    float entropy = 0.0f;
    for (size_t i = 0; i < candidates->size; ++i)
    {
        entropy += -candidates->data[i].p * logf(candidates->data[i].p);
    }

    // Compute the absolute difference between negative log probability and entropy for each candidate
    std::vector<float> shifted_scores;
    for (size_t i = 0; i < candidates->size; ++i)
    {
        float shifted_score = fabsf(-logf(candidates->data[i].p) - entropy);
        shifted_scores.push_back(shifted_score);
    }

    // Sort tokens based on the shifted_scores and their corresponding indices
    std::vector<size_t> indices(candidates->size);
    std::iota(indices.begin(), indices.end(), 0);

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) { return shifted_scores[a] < shifted_scores[b]; });

    // Compute the cumulative probabilities
    float cum_sum = 0.0f;
    size_t last_idx = indices.size();

    for (size_t i = 0; i < indices.size(); ++i)
    {
        size_t idx = indices[i];
        cum_sum += candidates->data[idx].p;

        // Check if the running sum is greater than typical or if we have kept at least min_keep tokens
        if (cum_sum > p && i >= min_keep - 1)
        {
            last_idx = i + 1;
            break;
        }
    }

    // Resize the output vector to keep only the locally typical tokens
    std::vector<llama_token_data> new_candidates;
    for (size_t i = 0; i < last_idx; ++i)
    {
        size_t idx = indices[i];
        new_candidates.push_back(candidates->data[idx]);
    }

    // Replace the data in candidates with the new_candidates data
    std::copy(new_candidates.begin(), new_candidates.end(), candidates->data);
    candidates->size = new_candidates.size();
    candidates->sorted = false;

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_entropy(
    struct llama_context * ctx, llama_token_data_array * candidates_p, float min_temp, float max_temp, float exponent_val)
{
    const int64_t t_start_sample_us = ggml_time_us();

    // no need to do anything if there is only one (or zero) candidates
    if (candidates_p->size <= 1)
    {
        return;
    }

    // Calculate maximum possible entropy
    float max_entropy = -logf(1.0f / candidates_p->size);

    llama_sample_softmax(nullptr, candidates_p);

    // Calculate entropy of the softmax probabilities
    float entropy = 0.0f;
    for (size_t i = 0; i < candidates_p->size; ++i)
    {
        float prob = candidates_p->data[i].p;
        if (prob > 0.0f)
        { // Ensure no log(0)
            entropy -= prob * logf(prob);
        }
    }

    // Normalize the entropy (max_entropy cannot be 0 here because we checked candidates_p->size != 1 above)
    float normalized_entropy = entropy / max_entropy;

    // Map the normalized entropy to the desired temperature range using the power function
    float dyn_temp = min_temp + (max_temp - min_temp) * powf(normalized_entropy, exponent_val);

#ifdef DEBUG
    LLAMA_LOG_INFO("Your text maxtemp value is: %f\n", max_temp);
    LLAMA_LOG_INFO("Entropy: %f\n", entropy);
    LLAMA_LOG_INFO("Max Possible Entropy: %f\n", max_entropy);
    LLAMA_LOG_INFO("Normalized Entropy: %f\n", normalized_entropy);
    LLAMA_LOG_INFO("Exponent: %f\n", exponent_val);
    LLAMA_LOG_INFO("Dynamic Temperature (dyn_temp): %f\n", dyn_temp);
#endif

    // Apply the dynamically calculated temperature scaling
    for (size_t i = 0; i < candidates_p->size; ++i)
    {
        candidates_p->data[i].logit /= dyn_temp;
    }

    // Re-compute softmax probabilities after scaling logits with dynamic temperature
    double max_l_double = candidates_p->data[0].logit;
    double cum_sum_double = 0.0;
    for (size_t i = 0; i < candidates_p->size; ++i)
    {
        double p = exp(candidates_p->data[i].logit - max_l_double);
        candidates_p->data[i].p = p; // Store the scaled probability
        cum_sum_double += p;
    }
    for (size_t i = 0; i < candidates_p->size; ++i)
    {
        candidates_p->data[i].p /= cum_sum_double; // Re-normalize the probabilities
    }

#ifdef DEBUG
    // Print the updated top 25 probabilities after temperature scaling
    LLAMA_LOG_INFO("\nUpdated Top 25 Probabilities After Dynamic Temperature Scaling (in percentages):\n");
    for (size_t i = 0; i < 25 && i < candidates_p->size; ++i)
    {
        LLAMA_LOG_INFO("Token %zu: %f%%\n", i + 1, candidates_p->data[i].p * 100.0f);
    }
#endif

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_temp(struct llama_context * ctx, llama_token_data_array * candidates_p, float temp)
{
    const int64_t t_start_sample_us = ggml_time_us();

    for (size_t i = 0; i < candidates_p->size; ++i)
    {
        candidates_p->data[i].logit /= temp;
    }

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_repetition_penalties(
    struct llama_context * ctx,
    llama_token_data_array * candidates,
    const llama_token * last_tokens,
    size_t penalty_last_n,
    float penalty_repeat,
    float penalty_freq,
    float penalty_present)
{
    if (penalty_last_n == 0 || (penalty_repeat == 1.0f && penalty_freq == 0.0f && penalty_present == 0.0f))
    {
        return;
    }

    const int64_t t_start_sample_us = ggml_time_us();

    // Create a frequency map to count occurrences of each token in last_tokens
    std::unordered_map<llama_token, int> token_count;
    for (size_t i = 0; i < penalty_last_n; ++i)
    {
        token_count[last_tokens[i]]++;
    }

    // Apply frequency and presence penalties to the candidates
    for (size_t i = 0; i < candidates->size; ++i)
    {
        const auto token_iter = token_count.find(candidates->data[i].id);
        if (token_iter == token_count.end())
        {
            continue;
        }

        const int count = token_iter->second;

        // The academic publication that described this technique actually just only divided, but that would cause tokens with negative logits to become more likely, which is obviously wrong.
        // This is common fix for this problem, which is to multiply by the penalty instead of dividing.
        if (candidates->data[i].logit <= 0)
        {
            candidates->data[i].logit *= penalty_repeat;
        }
        else
        {
            candidates->data[i].logit /= penalty_repeat;
        }

        candidates->data[i].logit -= float(count) * penalty_freq + float(count > 0) * penalty_present;
    }

    candidates->sorted = false;

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
}

void llama_sample_grammar(struct llama_context * ctx, llama_token_data_array * candidates, const struct llama_grammar * grammar)
{
    GGML_ASSERT(ctx);
    const int64_t t_start_sample_us = ggml_time_us();

    bool allow_eog = false;
    for (const auto & stack : grammar->stacks)
    {
        if (stack.empty())
        {
            allow_eog = true;
            break;
        }
    }

    std::vector<std::pair<std::vector<uint32_t>, llama_partial_utf8>> candidates_decoded;
    candidates_decoded.reserve(candidates->size);
    std::vector<llama_grammar_candidate> candidates_grammar;
    candidates_grammar.reserve(candidates->size);

    for (size_t i = 0; i < candidates->size; ++i)
    {
        const llama_token id = candidates->data[i].id;
        const std::string piece = llama_token_to_piece(ctx, id, false);

        if (llama_token_is_eog(&ctx->model, id))
        {
            if (!allow_eog)
            {
                candidates->data[i].logit = -INFINITY;
            }
        }
        else if (piece.empty() || piece[0] == 0)
        {
            candidates->data[i].logit = -INFINITY;
        }
        else
        {
            candidates_decoded.push_back(decode_utf8(piece, grammar->partial_utf8));
            candidates_grammar.push_back({i, candidates_decoded.back().first.data(), candidates_decoded.back().second});
        }
    }

    const auto rejects = llama_grammar_reject_candidates(grammar->rules, grammar->stacks, candidates_grammar);
    for (const auto & reject : rejects)
    {
        candidates->data[reject.index].logit = -INFINITY;
    }

    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
}

static void llama_log_softmax(float * array, size_t size)
{
    float max_l = *std::max_element(array, array + size);
    float sum = 0.f;
    for (size_t i = 0; i < size; ++i)
    {
        float p = expf(array[i] - max_l);
        sum += p;
        array[i] = p;
    }

    for (size_t i = 0; i < size; ++i)
    {
        array[i] = logf(array[i] / sum);
    }
}

void llama_sample_apply_guidance(struct llama_context * ctx, float * logits, float * logits_guidance, float scale)
{
    GGML_ASSERT(ctx);

    const auto t_start_sample_us = ggml_time_us();
    const auto n_vocab = llama_n_vocab(llama_get_model(ctx));

    llama_log_softmax(logits, n_vocab);
    llama_log_softmax(logits_guidance, n_vocab);

    for (int i = 0; i < n_vocab; ++i)
    {
        auto & l = logits[i];
        const auto & g = logits_guidance[i];

        l = scale * (l - g) + g;
    }

    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
}

llama_token
llama_sample_token_mirostat(struct llama_context * ctx, llama_token_data_array * candidates, float tau, float eta, int32_t m, float * mu)
{
    GGML_ASSERT(ctx);

    auto N = float(llama_n_vocab(llama_get_model(ctx)));
    int64_t t_start_sample_us;
    t_start_sample_us = ggml_time_us();

    llama_sample_softmax(nullptr, candidates);

    // Estimate s_hat using the most probable m tokens
    float s_hat = 0.0;
    float sum_ti_bi = 0.0;
    float sum_ti_sq = 0.0;
    for (size_t i = 0; i < size_t(m - 1) && i < candidates->size - 1; ++i)
    {
        float t_i = logf(float(i + 2) / float(i + 1));
        float b_i = logf(candidates->data[i].p / candidates->data[i + 1].p);
        sum_ti_bi += t_i * b_i;
        sum_ti_sq += t_i * t_i;
    }
    s_hat = sum_ti_bi / sum_ti_sq;

    // Compute k from the estimated s_hat and target surprise value
    float epsilon_hat = s_hat - 1;
    float k = powf((epsilon_hat * powf(2, *mu)) / (1 - powf(N, -epsilon_hat)), 1 / s_hat);

    // Sample the next word X using top-k sampling
    llama_sample_top_k(nullptr, candidates, int(k), 1);
    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    llama_token X = llama_sample_token(ctx, candidates);
    t_start_sample_us = ggml_time_us();

    // Compute error as the difference between observed surprise and target surprise value
    size_t X_idx = std::distance(
        candidates->data,
        std::find_if(
            candidates->data, candidates->data + candidates->size, [&](const llama_token_data & candidate) { return candidate.id == X; }));
    float observed_surprise = -log2f(candidates->data[X_idx].p);
    float e = observed_surprise - tau;

    // Update mu using the learning rate and error
    *mu = *mu - eta * e;

    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    return X;
}

llama_token
llama_sample_token_mirostat_v2(struct llama_context * ctx, llama_token_data_array * candidates, float tau, float eta, float * mu)
{
    int64_t t_start_sample_us;
    t_start_sample_us = ggml_time_us();

    llama_sample_softmax(ctx, candidates);

    // Truncate the words with surprise values greater than mu
    candidates->size = std::distance(
        candidates->data,
        std::find_if(
            candidates->data,
            candidates->data + candidates->size,
            [&](const llama_token_data & candidate) { return -log2f(candidate.p) > *mu; }));

    if (candidates->size == 0)
    {
        candidates->size = 1;
    }

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }

    // Normalize the probabilities of the remaining words
    llama_sample_softmax(ctx, candidates);

    // Sample the next word X from the remaining words
    llama_token X = llama_sample_token(ctx, candidates);
    t_start_sample_us = ggml_time_us();

    // Compute error as the difference between observed surprise and target surprise value
    size_t X_idx = std::distance(
        candidates->data,
        std::find_if(
            candidates->data, candidates->data + candidates->size, [&](const llama_token_data & candidate) { return candidate.id == X; }));
    float observed_surprise = -log2f(candidates->data[X_idx].p);
    float e = observed_surprise - tau;

    // Update mu using the learning rate and error
    *mu = *mu - eta * e;

    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    }
    return X;
}

llama_token llama_sample_token_greedy(struct llama_context * ctx, llama_token_data_array * candidates)
{
    const int64_t t_start_sample_us = ggml_time_us();

    // Find max element
    auto * max_iter = std::max_element(
        candidates->data,
        candidates->data + candidates->size,
        [](const llama_token_data & a, const llama_token_data & b) { return a.logit < b.logit; });

    llama_token result = max_iter->id;
    if (ctx)
    {
        ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
        ctx->n_sample++;
    }
    return result;
}

llama_token llama_sample_token_with_rng(struct llama_context * ctx, llama_token_data_array * candidates, pcg64_fast & rng)
{
    GGML_ASSERT(ctx);

    const int64_t t_start_sample_us = ggml_time_us();
    llama_sample_softmax(nullptr, candidates);

    std::vector<float> probs;
    probs.reserve(candidates->size);
    for (size_t i = 0; i < candidates->size; ++i)
    {
        probs.push_back(candidates->data[i].p);
    }

    std::discrete_distribution<> dist(probs.begin(), probs.end());
    int idx = dist(rng);

    llama_token result = candidates->data[idx].id;

    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    ctx->n_sample++;
    return result;
}

llama_token llama_sample_token(struct llama_context * ctx, llama_token_data_array * candidates)
{
    return llama_sample_token_with_rng(ctx, candidates, ctx->rng);
}

void llama_grammar_accept_token(struct llama_context * ctx, struct llama_grammar * grammar, llama_token token)
{
    const int64_t t_start_sample_us = ggml_time_us();

    if (llama_token_is_eog(&ctx->model, token))
    {
        for (const auto & stack : grammar->stacks)
        {
            if (stack.empty())
            {
                return;
            }
        }
        GGML_ASSERT(false);
    }

    const std::string piece = llama_token_to_piece(ctx, token, false);

    // Note terminating 0 in decoded string
    const auto decoded = decode_utf8(piece, grammar->partial_utf8);
    const auto & code_points = decoded.first;
    std::vector<std::vector<const llama_grammar_element *>> tmp_new_stacks;
    for (auto it = code_points.begin(), end = code_points.end() - 1; it != end; ++it)
    {
        llama_grammar_accept(grammar->rules, grammar->stacks, *it, tmp_new_stacks);
        grammar->stacks = tmp_new_stacks;
    }
    grammar->partial_utf8 = decoded.second;
    GGML_ASSERT(!grammar->stacks.empty());

    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
}

//
// Beam search
//

struct llama_beam
{
    std::vector<llama_token> tokens;
    float p; // Cumulative beam probability (renormalized relative to all beams)
    bool eob; // Initialize end-of-beam to false. Callback sets this to true.
    // Sort beams by probability. In case of ties, prefer beams at eob.
    bool operator<(const llama_beam & rhs) const { return std::make_pair(p, eob) < std::make_pair(rhs.p, rhs.eob); }
    // Shift off first n tokens and discard them.
    void shift_tokens(const size_t n)
    {
        if (n)
        {
            std::copy(tokens.begin() + n, tokens.end(), tokens.begin());
            tokens.resize(tokens.size() - n);
        }
    }
    llama_beam_view view() const { return {tokens.data(), tokens.size(), p, eob}; }
};

// A struct for calculating logit-related info.
struct llama_logit_info
{
    const float * const logits;
    const int n_vocab;
    const float max_l;
    const float normalizer;
    struct sum_exp
    {
        float max_l;
        float operator()(float sum, float l) const { return sum + std::exp(l - max_l); }
    };
    llama_logit_info(llama_context * ctx)
        : logits(llama_get_logits(ctx))
        , n_vocab(llama_n_vocab(llama_get_model(ctx)))
        , max_l(*std::max_element(logits, logits + n_vocab))
        , normalizer(1.0f / std::accumulate(logits, logits + n_vocab, 0.0f, sum_exp{max_l}))
    {
    }
    llama_token_data get_token_data(const llama_token token_id) const
    {
        constexpr auto p = std::numeric_limits<float>::quiet_NaN(); // never used
        return {token_id, logits[token_id], p};
    }
    // Return top k token_data by logit.
    std::vector<llama_token_data> top_k(size_t k)
    {
        std::vector<llama_token_data> min_heap; // min-heap by logit
        const llama_token k_min = std::min(static_cast<llama_token>(k), n_vocab);
        min_heap.reserve(k_min);
        for (llama_token token_id = 0; token_id < k_min; ++token_id)
        {
            min_heap.push_back(get_token_data(token_id));
        }
        auto comp = [](const llama_token_data & a, const llama_token_data & b) { return a.logit > b.logit; };
        std::make_heap(min_heap.begin(), min_heap.end(), comp);
        for (llama_token token_id = k_min; token_id < n_vocab; ++token_id)
        {
            if (min_heap.front().logit < logits[token_id])
            {
                std::pop_heap(min_heap.begin(), min_heap.end(), comp);
                min_heap.back().id = token_id;
                min_heap.back().logit = logits[token_id];
                std::push_heap(min_heap.begin(), min_heap.end(), comp);
            }
        }
        return min_heap;
    }
    float probability_from_logit(float logit) const { return normalizer * std::exp(logit - max_l); }
};

struct llama_beam_search_data
{
    llama_context * ctx;
    size_t n_beams;
    int n_past;
    int n_predict;
    std::vector<llama_beam> beams;
    std::vector<llama_beam> next_beams;

    // Re-calculated on each loop iteration
    size_t common_prefix_length;

    // Used to communicate to/from callback on beams state.
    std::vector<llama_beam_view> beam_views;

    llama_beam_search_data(llama_context * ctx, size_t n_beams, int n_past, int n_predict)
        : ctx(ctx), n_beams(n_beams), n_past(n_past), n_predict(n_predict), beam_views(n_beams)
    {
        beams.reserve(n_beams);
        next_beams.reserve(n_beams);
    }

    // Collapse beams to a single beam given by index.
    void collapse_beams(const size_t beam_idx)
    {
        if (0u < beam_idx)
        {
            std::swap(beams[0], beams[beam_idx]);
        }
        beams.resize(1);
    }

    // Min-heaps are used to efficiently collect the top-k elements (k=n_beams).
    // The repetitive patterns below reflect the 2 stages of heaps:
    //  * Gather elements until the vector is full, then call std::make_heap() on it.
    //  * If the heap is full and a new element is found that should be included, pop the
    //    least element to the back(), replace it with the new, then push it into the heap.
    void fill_next_beams_by_top_probabilities(llama_beam & beam)
    {
        // Min-heaps use a greater-than comparator.
        const auto comp = [](const llama_beam & a, const llama_beam & b) { return a.p > b.p; };
        if (beam.eob)
        {
            // beam is at end-of-sentence, so just copy it to next_beams if its probability is high enough.
            if (next_beams.size() < n_beams)
            {
                next_beams.push_back(std::move(beam));
                if (next_beams.size() == n_beams)
                {
                    std::make_heap(next_beams.begin(), next_beams.end(), comp);
                }
            }
            else if (next_beams.front().p < beam.p)
            {
                std::pop_heap(next_beams.begin(), next_beams.end(), comp);
                next_beams.back() = std::move(beam);
                std::push_heap(next_beams.begin(), next_beams.end(), comp);
            }
        }
        else
        {
            // beam is not at end-of-sentence, so branch with next top_k tokens.
            if (!beam.tokens.empty())
            {
                llama_decode(ctx, llama_batch_get_one(beam.tokens.data(), beam.tokens.size(), n_past, 0));
            }
            llama_logit_info logit_info(ctx);
            std::vector<llama_token_data> next_tokens = logit_info.top_k(n_beams);

            // Clear the kv slot so that other beams may try different tokens at this position. The llama_decode()
            // call in loop() will conclusively fill in the kv slot once the beams converge at this position.
            llama_kv_cache_seq_rm(ctx, 0, n_past, -1);

            size_t i = 0;
            if (next_beams.size() < n_beams)
            {
                for (; next_beams.size() < n_beams; ++i)
                {
                    llama_beam next_beam = beam;
                    next_beam.tokens.push_back(next_tokens[i].id);
                    next_beam.p *= logit_info.probability_from_logit(next_tokens[i].logit);
                    next_beams.push_back(std::move(next_beam));
                }
                std::make_heap(next_beams.begin(), next_beams.end(), comp);
            }
            else
            {
                for (; next_beams.front().p == 0.0f; ++i)
                {
                    std::pop_heap(next_beams.begin(), next_beams.end(), comp);
                    next_beams.back() = beam;
                    next_beams.back().tokens.push_back(next_tokens[i].id);
                    next_beams.back().p *= logit_info.probability_from_logit(next_tokens[i].logit);
                    std::push_heap(next_beams.begin(), next_beams.end(), comp);
                }
            }
            for (; i < n_beams; ++i)
            {
                const float next_p = beam.p * logit_info.probability_from_logit(next_tokens[i].logit);
                if (next_beams.front().p < next_p)
                {
                    std::pop_heap(next_beams.begin(), next_beams.end(), comp);
                    next_beams.back() = beam;
                    next_beams.back().tokens.push_back(next_tokens[i].id);
                    next_beams.back().p = next_p;
                    std::push_heap(next_beams.begin(), next_beams.end(), comp);
                }
            }
        }
    }

    // Find common_prefix_length based on beams.
    // Requires beams is not empty.
    size_t find_common_prefix_length()
    {
        size_t common_prefix_length = beams[0].tokens.size();
        for (size_t i = 1; i < beams.size(); ++i)
        {
            common_prefix_length = std::min(common_prefix_length, beams[i].tokens.size());
            for (size_t j = 0; j < common_prefix_length; ++j)
            {
                if (beams[0].tokens[j] != beams[i].tokens[j])
                {
                    common_prefix_length = j;
                    break;
                }
            }
        }
        return common_prefix_length;
    }

    // Construct beams_state to send back to caller via the callback function.
    // Side effect: set common_prefix_length = find_common_prefix_length();
    llama_beams_state get_beams_state(const bool last_call)
    {
        for (size_t i = 0; i < beams.size(); ++i)
        {
            beam_views[i] = beams[i].view();
        }
        common_prefix_length = find_common_prefix_length();
        return {beam_views.data(), beams.size(), common_prefix_length, last_call};
    }

    // Loop:
    //  * while i < n_predict, AND
    //  * any of the beams have not yet reached end-of-beam (eob), AND
    //  * the highest probability beam(s) (plural in case of ties) are not at end-of-sentence
    //    (since all other beam probabilities can only decrease)
    void loop(const llama_beam_search_callback_fn_t callback, void * const callback_data)
    {
        beams.push_back({{}, 1.0f, false}); // Start with one empty beam w/ probability = 1.0 and !eob.
        const auto not_eob = [](const llama_beam & beam) { return !beam.eob; };
        for (int i = 0; i < n_predict && std::any_of(beams.begin(), beams.end(), not_eob) && !beams[top_beam_index()].eob; ++i)
        {
            callback(callback_data, get_beams_state(false)); // Sets common_prefix_length
            update_beams_from_beam_views(); // Update values (p,eob) that callback may have changed.
            if (common_prefix_length)
            {
                llama_decode(ctx, llama_batch_get_one(beams[0].tokens.data(), common_prefix_length, n_past, 0));
                n_past += common_prefix_length;
            }
            // Zero-out next_beam probabilities to place them last in following min-heap.
            std::for_each(next_beams.begin(), next_beams.end(), [](llama_beam & beam) { beam.p = 0.0f; });
            for (llama_beam & beam : beams)
            {
                beam.shift_tokens(common_prefix_length);
                fill_next_beams_by_top_probabilities(beam);
            }
            // next_beams become the beams of next/final iteration. Swap them to re-use memory.
            beams.swap(next_beams);
            renormalize_beam_probabilities(beams);
        }
        collapse_beams(top_beam_index());
        callback(callback_data, get_beams_state(true));
    }

    // As beams grow, the cumulative probabilities decrease.
    // Renormalize them to avoid floating point underflow.
    static void renormalize_beam_probabilities(std::vector<llama_beam> & beams)
    {
        const auto sum_p = [](float sum, llama_beam & beam) { return sum + beam.p; };
        const float inv_sum = 1.0f / std::accumulate(beams.begin(), beams.end(), 0.0f, sum_p);
        std::for_each(beams.begin(), beams.end(), [=](llama_beam & beam) { beam.p *= inv_sum; });
    }

    // Assumes beams is non-empty.  Uses llama_beam::operator<() for ordering.
    size_t top_beam_index() { return std::max_element(beams.begin(), beams.end()) - beams.begin(); }

    // Copy (p,eob) for each beam which may have been changed by the callback.
    void update_beams_from_beam_views()
    {
        for (size_t i = 0; i < beams.size(); ++i)
        {
            beams[i].p = beam_views[i].p;
            beams[i].eob = beam_views[i].eob;
        }
    }
};

void llama_beam_search(
    llama_context * ctx, llama_beam_search_callback_fn_t callback, void * callback_data, size_t n_beams, int n_past, int n_predict)
{
    assert(ctx);
    const int64_t t_start_sample_us = ggml_time_us();

    llama_beam_search_data beam_search_data(ctx, n_beams, n_past, n_predict);

    beam_search_data.loop(callback, callback_data);

    ctx->t_sample_us += ggml_time_us() - t_start_sample_us;
    ctx->n_sample++;
}

//
// quantization
//

struct quantize_state_internal
{
    const llama_model & model;
    const llama_model_quantize_params * params;

    int n_attention_wv = 0;
    int n_ffn_down = 0;
    int n_ffn_gate = 0;
    int n_ffn_up = 0;
    int i_attention_wv = 0;
    int i_ffn_down = 0;
    int i_ffn_gate = 0;
    int i_ffn_up = 0;

    int n_k_quantized = 0;
    int n_fallback = 0;

    bool has_imatrix = false;

    // used to figure out if a model shares tok_embd with the output weight
    bool has_output = false;

    quantize_state_internal(const llama_model & model, const llama_model_quantize_params * params) : model(model), params(params) { }
};

static void llama_tensor_dequantize_internal(
    struct ggml_tensor * tensor,
    std::vector<no_init<float>> & output,
    std::vector<std::thread> & workers,
    const size_t nelements,
    const int nthread)
{
    if (output.size() < nelements)
    {
        output.resize(nelements);
    }
    float * f32_output = (float *)output.data();

    ggml_type_traits_t qtype;
    if (ggml_is_quantized(tensor->type))
    {
        qtype = ggml_internal_get_type_traits(tensor->type);
        if (qtype.to_float == nullptr)
        {
            throw std::runtime_error(
                format("type %s unsupported for integer quantization: no dequantization available", ggml_type_name(tensor->type)));
        }
    }
    else if (tensor->type != GGML_TYPE_F16 && tensor->type != GGML_TYPE_BF16)
    {
        throw std::runtime_error(format("cannot dequantize/convert tensor type %s", ggml_type_name(tensor->type)));
    }

    if (nthread < 2)
    {
        if (tensor->type == GGML_TYPE_F16)
        {
            ggml_fp16_to_fp32_row((ggml_fp16_t *)tensor->data, f32_output, nelements);
        }
        else if (tensor->type == GGML_TYPE_BF16)
        {
            ggml_bf16_to_fp32_row((ggml_bf16_t *)tensor->data, f32_output, nelements);
        }
        else if (ggml_is_quantized(tensor->type))
        {
            qtype.to_float(tensor->data, f32_output, nelements);
        }
        else
        {
            GGML_ASSERT(false); // unreachable
        }
        return;
    }

    size_t block_size;
    if (tensor->type == GGML_TYPE_F16 || tensor->type == GGML_TYPE_BF16)
    {
        block_size = 1;
    }
    else
    {
        block_size = (size_t)ggml_blck_size(tensor->type);
    }

    size_t block_size_bytes = ggml_type_size(tensor->type);

    GGML_ASSERT(nelements % block_size == 0);
    size_t nblocks = nelements / block_size;
    size_t blocks_per_thread = nblocks / nthread;
    size_t spare_blocks = nblocks - (blocks_per_thread * nthread); // if blocks aren't divisible by thread count

    size_t in_buff_offs = 0;
    size_t out_buff_offs = 0;

    for (int tnum = 0; tnum < nthread; tnum++)
    {
        size_t thr_blocks = blocks_per_thread + (tnum == nthread - 1 ? spare_blocks : 0); // num blocks for this thread
        size_t thr_elems = thr_blocks * block_size; // number of elements for this thread
        size_t thr_block_bytes = thr_blocks * block_size_bytes; // number of input bytes for this thread

        auto compute = [qtype](ggml_type typ, uint8_t * inbuf, float * outbuf, int nels)
        {
            if (typ == GGML_TYPE_F16)
            {
                ggml_fp16_to_fp32_row((ggml_fp16_t *)inbuf, outbuf, nels);
            }
            else if (typ == GGML_TYPE_BF16)
            {
                ggml_bf16_to_fp32_row((ggml_bf16_t *)inbuf, outbuf, nels);
            }
            else
            {
                qtype.to_float(inbuf, outbuf, nels);
            }
        };
        workers.emplace_back(compute, tensor->type, (uint8_t *)tensor->data + in_buff_offs, f32_output + out_buff_offs, thr_elems);
        in_buff_offs += thr_block_bytes;
        out_buff_offs += thr_elems;
    }
    for (auto & w : workers)
    {
        w.join();
    }
    workers.clear();
}

static ggml_type llama_tensor_get_type(quantize_state_internal & qs, ggml_type new_type, const ggml_tensor * tensor, llama_ftype ftype)
{
    const std::string name = ggml_get_name(tensor);

    // TODO: avoid hardcoded tensor names - use the TN_* constants
    const llm_arch arch = qs.model.arch;
    const auto tn = LLM_TN(arch);

    auto use_more_bits = [](int i_layer, int num_layers) -> bool
    { return i_layer < num_layers / 8 || i_layer >= 7 * num_layers / 8 || (i_layer - num_layers / 8) % 3 == 2; };
    const int n_expert = std::max(1, (int)qs.model.hparams.n_expert);
    auto layer_info = [n_expert](int i_layer, int n_layer, const char * name)
    {
        if (n_expert > 1)
        {
            // Believe it or not, "experts" in the FFN of Mixtral-8x7B are not consecutive, but iccasionally randomly
            // sprinkled in the model. Hence, simply dividing i_ffn_down by n_expert does not work
            // for getting the current layer as I initially thought, and we need to resort to parsing the
            // tensor name.
            if (sscanf(name, "blk.%d.", &i_layer) != 1)
            {
                throw std::runtime_error(format("Failed to determine layer for tensor %s", name));
            }
            if (i_layer < 0 || i_layer >= n_layer)
            {
                throw std::runtime_error(format("Bad layer %d for tensor %s. Must be in [0, %d)", i_layer, name, n_layer));
            }
        }
        return std::make_pair(i_layer, n_layer);
    };

    // for arches that share the same tensor between the token embeddings and the output, we quantize the token embeddings
    // with the quantization of the output tensor
    if (name == tn(LLM_TENSOR_OUTPUT, "weight") || (!qs.has_output && name == tn(LLM_TENSOR_TOKEN_EMBD, "weight")))
    {
        if (qs.params->output_tensor_type < GGML_TYPE_COUNT)
        {
            new_type = qs.params->output_tensor_type;
        }
        else
        {
            int nx = tensor->ne[0];
            if (arch == LLM_ARCH_FALCON || nx % QK_K != 0)
            {
                new_type = GGML_TYPE_Q8_0;
            }
            else if (
                ftype == LLAMA_FTYPE_MOSTLY_IQ2_XXS || ftype == LLAMA_FTYPE_MOSTLY_IQ2_XS || ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS
                || ftype == LLAMA_FTYPE_MOSTLY_IQ1_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_M
                || ftype == LLAMA_FTYPE_MOSTLY_IQ1_M)
            {
                new_type = GGML_TYPE_Q5_K;
            }
            else if (new_type != GGML_TYPE_Q8_0)
            {
                new_type = GGML_TYPE_Q6_K;
            }
        }
    }
    else if (name == "token_embd.weight")
    {
        if (qs.params->token_embedding_type < GGML_TYPE_COUNT)
        {
            new_type = qs.params->token_embedding_type;
        }
        else
        {
            if (ftype == LLAMA_FTYPE_MOSTLY_IQ2_XXS || ftype == LLAMA_FTYPE_MOSTLY_IQ2_XS || ftype == LLAMA_FTYPE_MOSTLY_IQ1_S
                || ftype == LLAMA_FTYPE_MOSTLY_IQ1_M)
            {
                new_type = GGML_TYPE_Q2_K;
            }
            else if (ftype == LLAMA_FTYPE_MOSTLY_IQ2_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_M)
            {
                new_type = GGML_TYPE_IQ3_S;
            }
            else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS)
            {
                new_type = GGML_TYPE_IQ3_S;
            }
        }
    }
    else if (
        ftype == LLAMA_FTYPE_MOSTLY_IQ2_XXS || ftype == LLAMA_FTYPE_MOSTLY_IQ2_XS || ftype == LLAMA_FTYPE_MOSTLY_IQ1_S
        || ftype == LLAMA_FTYPE_MOSTLY_IQ2_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_M || ftype == LLAMA_FTYPE_MOSTLY_IQ1_M)
    {
        if (name.find("attn_v.weight") != std::string::npos)
        {
            if (qs.model.hparams.n_gqa() >= 4 || qs.model.hparams.n_expert >= 4)
                new_type = GGML_TYPE_Q4_K;
            else
                new_type = ftype == LLAMA_FTYPE_MOSTLY_IQ2_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_M ? GGML_TYPE_IQ3_S : GGML_TYPE_Q2_K;
            ++qs.i_attention_wv;
        }
        else if (qs.model.hparams.n_expert == 8 && name.find("attn_k.weight") != std::string::npos)
        {
            new_type = GGML_TYPE_Q4_K;
        }
        else if (name.find("ffn_down") != std::string::npos)
        {
            if (qs.i_ffn_down < qs.n_ffn_down / 8)
            {
                new_type = ftype == LLAMA_FTYPE_MOSTLY_IQ2_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_M ? GGML_TYPE_IQ3_S : GGML_TYPE_Q2_K;
            }
            ++qs.i_ffn_down;
        }
        else if (name.find("attn_output.weight") != std::string::npos)
        {
            if (qs.model.hparams.n_expert == 8)
            {
                new_type = GGML_TYPE_Q5_K;
            }
            else
            {
                if (ftype == LLAMA_FTYPE_MOSTLY_IQ1_S || ftype == LLAMA_FTYPE_MOSTLY_IQ1_M)
                    new_type = GGML_TYPE_IQ2_XXS;
                else if (ftype == LLAMA_FTYPE_MOSTLY_IQ2_S || ftype == LLAMA_FTYPE_MOSTLY_IQ2_M)
                    new_type = GGML_TYPE_IQ3_S;
            }
        }
    }
    else if (name.find("attn_v.weight") != std::string::npos)
    {
        if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K)
        {
            new_type = qs.model.hparams.n_gqa() >= 4 ? GGML_TYPE_Q4_K : GGML_TYPE_Q3_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K_S && qs.model.hparams.n_gqa() >= 4)
        {
            new_type = GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS)
        {
            new_type = qs.model.hparams.n_gqa() >= 4 ? GGML_TYPE_Q4_K : !qs.has_imatrix ? GGML_TYPE_IQ3_S : GGML_TYPE_IQ3_XXS;
        }
        else if ((ftype == LLAMA_FTYPE_MOSTLY_IQ3_XS || ftype == LLAMA_FTYPE_MOSTLY_IQ3_S) && qs.model.hparams.n_gqa() >= 4)
        {
            new_type = GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_M)
        {
            new_type = GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_M)
        {
            new_type = qs.i_attention_wv < 2 ? GGML_TYPE_Q5_K : GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_L)
            new_type = GGML_TYPE_Q5_K;
        else if ((ftype == LLAMA_FTYPE_MOSTLY_IQ4_NL || ftype == LLAMA_FTYPE_MOSTLY_IQ4_XS) && qs.model.hparams.n_gqa() >= 4)
        {
            new_type = GGML_TYPE_Q5_K;
        }
        else if (
            (ftype == LLAMA_FTYPE_MOSTLY_Q4_K_M || ftype == LLAMA_FTYPE_MOSTLY_Q5_K_M)
            && use_more_bits(qs.i_attention_wv, qs.n_attention_wv))
            new_type = GGML_TYPE_Q6_K;
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q4_K_S && qs.i_attention_wv < 4)
            new_type = GGML_TYPE_Q5_K;
        if (qs.model.type == MODEL_70B)
        {
            // In the 70B model we have 8 heads sharing the same attn_v weights. As a result, the attn_v.weight tensor is
            // 8x smaller compared to attn_q.weight. Hence, we can get a nice boost in quantization accuracy with
            // nearly negligible increase in model size by quantizing this tensor with more bits:
            if (new_type == GGML_TYPE_Q3_K || new_type == GGML_TYPE_Q4_K)
                new_type = GGML_TYPE_Q5_K;
        }
        if (qs.model.hparams.n_expert == 8)
        {
            // for the 8-expert model, bumping this to Q8_0 trades just ~128MB
            // TODO: explore better strategies
            new_type = GGML_TYPE_Q8_0;
        }
        ++qs.i_attention_wv;
    }
    else if (name.find("attn_k.weight") != std::string::npos)
    {
        if (qs.model.hparams.n_expert == 8)
        {
            // for the 8-expert model, bumping this to Q8_0 trades just ~128MB
            // TODO: explore better strategies
            new_type = GGML_TYPE_Q8_0;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XS)
        {
            new_type = GGML_TYPE_IQ3_XXS;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS)
        {
            new_type = GGML_TYPE_IQ2_S;
        }
    }
    else if (name.find("attn_q.weight") != std::string::npos)
    {
        if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XS)
        {
            new_type = GGML_TYPE_IQ3_XXS;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS)
        {
            new_type = GGML_TYPE_IQ2_S;
        }
    }
    else if (name.find("ffn_down") != std::string::npos)
    {
        auto info = layer_info(qs.i_ffn_down, qs.n_ffn_down, name.c_str());
        int i_layer = info.first, n_layer = info.second;
        if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K)
            new_type = GGML_TYPE_Q3_K;
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K_S)
        {
            if (i_layer < n_layer / 8)
                new_type = GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS && !qs.has_imatrix)
        {
            new_type = i_layer < n_layer / 8 ? GGML_TYPE_Q4_K : GGML_TYPE_Q3_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_M)
        {
            new_type = i_layer < n_layer / 16                                ? GGML_TYPE_Q5_K
                : arch != LLM_ARCH_FALCON || use_more_bits(i_layer, n_layer) ? GGML_TYPE_Q4_K
                                                                             : GGML_TYPE_Q3_K;
        }
        else if (
            ftype == LLAMA_FTYPE_MOSTLY_IQ3_M
            && (i_layer < n_layer / 8 || (qs.model.hparams.n_expert == 8 && use_more_bits(i_layer, n_layer))))
        {
            new_type = GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_L)
        {
            new_type = arch == LLM_ARCH_FALCON ? GGML_TYPE_Q4_K : GGML_TYPE_Q5_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q4_K_M)
        {
            if (arch == LLM_ARCH_FALCON)
            {
                new_type = i_layer < n_layer / 16 ? GGML_TYPE_Q6_K : use_more_bits(i_layer, n_layer) ? GGML_TYPE_Q5_K : GGML_TYPE_Q4_K;
            }
            else
            {
                if (use_more_bits(i_layer, n_layer))
                    new_type = GGML_TYPE_Q6_K;
            }
        }
        else if (i_layer < n_layer / 8 && (ftype == LLAMA_FTYPE_MOSTLY_IQ4_NL || ftype == LLAMA_FTYPE_MOSTLY_IQ4_XS) && !qs.has_imatrix)
        {
            new_type = GGML_TYPE_Q5_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q5_K_M && use_more_bits(i_layer, n_layer))
            new_type = GGML_TYPE_Q6_K;
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q4_K_S && arch != LLM_ARCH_FALCON && i_layer < n_layer / 8)
        {
            new_type = GGML_TYPE_Q5_K;
        }
        else if ((ftype == LLAMA_FTYPE_MOSTLY_Q4_0 || ftype == LLAMA_FTYPE_MOSTLY_Q5_0) && qs.has_imatrix && i_layer < n_layer / 8)
        {
            // Guard against craziness in the first few ffn_down layers that can happen even with imatrix for Q4_0/Q5_0.
            // We only do it when an imatrix is provided because a) we want to make sure that one can always get the
            // same quantization as before imatrix stuff, and b) Q4_1/Q5_1 do go crazy on ffn_down without an imatrix.
            new_type = ftype == LLAMA_FTYPE_MOSTLY_Q4_0 ? GGML_TYPE_Q4_1 : GGML_TYPE_Q5_1;
        }
        ++qs.i_ffn_down;
    }
    else if (name.find("attn_output.weight") != std::string::npos)
    {
        if (arch != LLM_ARCH_FALCON)
        {
            if (qs.model.hparams.n_expert == 8)
            {
                if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K || ftype == LLAMA_FTYPE_MOSTLY_IQ3_XS || ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS
                    || ftype == LLAMA_FTYPE_MOSTLY_Q3_K_S || ftype == LLAMA_FTYPE_MOSTLY_Q3_K_M || ftype == LLAMA_FTYPE_MOSTLY_IQ4_NL
                    || ftype == LLAMA_FTYPE_MOSTLY_Q4_K_S || ftype == LLAMA_FTYPE_MOSTLY_Q4_K_M || ftype == LLAMA_FTYPE_MOSTLY_IQ3_S
                    || ftype == LLAMA_FTYPE_MOSTLY_IQ3_M || ftype == LLAMA_FTYPE_MOSTLY_IQ4_XS)
                {
                    new_type = GGML_TYPE_Q5_K;
                }
            }
            else
            {
                if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K)
                    new_type = GGML_TYPE_Q3_K;
                else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XXS)
                    new_type = GGML_TYPE_IQ3_S;
                else if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_M)
                    new_type = GGML_TYPE_Q4_K;
                else if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_L)
                    new_type = GGML_TYPE_Q5_K;
                else if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_M)
                    new_type = GGML_TYPE_Q4_K;
            }
        }
        else
        {
            if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_L)
                new_type = GGML_TYPE_Q4_K;
        }
    }
    else if (name.find("attn_qkv.weight") != std::string::npos)
    {
        if (ftype == LLAMA_FTYPE_MOSTLY_Q3_K_M || ftype == LLAMA_FTYPE_MOSTLY_Q3_K_L || ftype == LLAMA_FTYPE_MOSTLY_IQ3_M)
        {
            new_type = GGML_TYPE_Q4_K;
        }
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q4_K_M)
            new_type = GGML_TYPE_Q5_K;
        else if (ftype == LLAMA_FTYPE_MOSTLY_Q5_K_M)
            new_type = GGML_TYPE_Q6_K;
    }
    else if (name.find("ffn_gate") != std::string::npos)
    {
        auto info = layer_info(qs.i_ffn_gate, qs.n_ffn_gate, name.c_str());
        int i_layer = info.first, n_layer = info.second;
        if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XS && (i_layer >= n_layer / 8 && i_layer < 7 * n_layer / 8))
        {
            new_type = GGML_TYPE_IQ3_XXS;
        }
        ++qs.i_ffn_gate;
    }
    else if (name.find("ffn_up") != std::string::npos)
    {
        auto info = layer_info(qs.i_ffn_up, qs.n_ffn_up, name.c_str());
        int i_layer = info.first, n_layer = info.second;
        if (ftype == LLAMA_FTYPE_MOSTLY_IQ3_XS && (i_layer >= n_layer / 8 && i_layer < 7 * n_layer / 8))
        {
            new_type = GGML_TYPE_IQ3_XXS;
        }
        ++qs.i_ffn_up;
    }

    //    if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K) new_type = GGML_TYPE_Q3_K;
    //}
    // IK: let's remove this, else Q2_K is almost the same as Q3_K_S
    //else if (name.find("ffn_gate") != std::string::npos || name.find("ffn_up") != std::string::npos) {
    //    if (ftype == LLAMA_FTYPE_MOSTLY_Q2_K) new_type = GGML_TYPE_Q3_K;
    //}
    // This can be used to reduce the size of the Q5_K_S model.
    // The associated PPL increase is fully in line with the size reduction
    //else {
    //    if (ftype == LLAMA_FTYPE_MOSTLY_Q5_K_S) new_type = GGML_TYPE_Q4_K;
    //}
    bool convert_incompatible_tensor = false;
    if (new_type == GGML_TYPE_Q2_K || new_type == GGML_TYPE_Q3_K || new_type == GGML_TYPE_Q4_K || new_type == GGML_TYPE_Q5_K
        || new_type == GGML_TYPE_Q6_K || new_type == GGML_TYPE_IQ4_XS || new_type == GGML_TYPE_IQ2_XS || new_type == GGML_TYPE_IQ2_XXS
        || new_type == GGML_TYPE_IQ2_S || new_type == GGML_TYPE_IQ3_XXS || new_type == GGML_TYPE_IQ1_S || new_type == GGML_TYPE_IQ3_S
        || new_type == GGML_TYPE_IQ1_M)
    {
        int nx = tensor->ne[0];
        int ny = tensor->ne[1];
        if (nx % QK_K != 0)
        {
            LLAMA_LOG_WARN(
                "\n\n%s : tensor cols %d x %d are not divisible by %d, required for %s", __func__, nx, ny, QK_K, ggml_type_name(new_type));
            convert_incompatible_tensor = true;
        }
        else
        {
            ++qs.n_k_quantized;
        }
    }
    if (convert_incompatible_tensor)
    {
        switch (new_type)
        {
            case GGML_TYPE_IQ2_XXS:
            case GGML_TYPE_IQ2_XS:
            case GGML_TYPE_IQ2_S:
            case GGML_TYPE_IQ3_XXS:
            case GGML_TYPE_IQ3_S:
            case GGML_TYPE_IQ1_S:
            case GGML_TYPE_IQ1_M:
            case GGML_TYPE_Q2_K:
            case GGML_TYPE_Q3_K:
            case GGML_TYPE_IQ4_XS:
                new_type = GGML_TYPE_IQ4_NL;
                break;
            case GGML_TYPE_Q4_K:
                new_type = GGML_TYPE_Q5_0;
                break;
            case GGML_TYPE_Q5_K:
                new_type = GGML_TYPE_Q5_1;
                break;
            case GGML_TYPE_Q6_K:
                new_type = GGML_TYPE_Q8_0;
                break;
            default:
                throw std::runtime_error("\nUnsupported tensor size encountered\n");
        }
        LLAMA_LOG_WARN(" - using fallback quantization %s\n", ggml_type_name(new_type));
        ++qs.n_fallback;
    }

    return new_type;
}

static size_t llama_tensor_quantize_internal(
    enum ggml_type new_type,
    const float * f32_data,
    void * new_data,
    const int64_t chunk_size,
    int64_t nrows,
    int64_t n_per_row,
    const float * imatrix,
    std::vector<std::thread> & workers,
    const int nthread)
{
    if (nthread < 2)
    {
        // single-thread
        size_t new_size = ggml_quantize_chunk(new_type, f32_data, new_data, 0, nrows, n_per_row, imatrix);
        if (!ggml_validate_row_data(new_type, new_data, new_size))
        {
            throw std::runtime_error("quantized data validation failed");
        }
        return new_size;
    }

    std::mutex mutex;
    int64_t counter = 0;
    size_t new_size = 0;
    bool valid = true;
    auto compute = [&mutex, &counter, &new_size, &valid, new_type, f32_data, new_data, chunk_size, nrows, n_per_row, imatrix]()
    {
        const int64_t nrows_per_chunk = chunk_size / n_per_row;
        size_t local_size = 0;
        while (true)
        {
            std::unique_lock<std::mutex> lock(mutex);
            int64_t first_row = counter;
            counter += nrows_per_chunk;
            if (first_row >= nrows)
            {
                if (local_size > 0)
                {
                    new_size += local_size;
                }
                break;
            }
            lock.unlock();
            const int64_t this_nrow = std::min(nrows - first_row, nrows_per_chunk);
            size_t this_size = ggml_quantize_chunk(new_type, f32_data, new_data, first_row * n_per_row, this_nrow, n_per_row, imatrix);
            local_size += this_size;

            // validate the quantized data
            const size_t row_size = ggml_row_size(new_type, n_per_row);
            void * this_data = (char *)new_data + first_row * row_size;
            if (!ggml_validate_row_data(new_type, this_data, this_size))
            {
                std::unique_lock<std::mutex> lock(mutex);
                valid = false;
                break;
            }
        }
    };
    for (int it = 0; it < nthread - 1; ++it)
    {
        workers.emplace_back(compute);
    }
    compute();
    for (auto & w : workers)
    {
        w.join();
    }
    workers.clear();
    if (!valid)
    {
        throw std::runtime_error("quantized data validation failed");
    }
    return new_size;
}

static void
llama_model_quantize_internal(const std::string & fname_inp, const std::string & fname_out, const llama_model_quantize_params * params)
{
    ggml_type default_type;
    llama_ftype ftype = params->ftype;

    switch (params->ftype)
    {
        case LLAMA_FTYPE_MOSTLY_Q4_0:
            default_type = GGML_TYPE_Q4_0;
            break;
        case LLAMA_FTYPE_MOSTLY_Q4_1:
            default_type = GGML_TYPE_Q4_1;
            break;
        case LLAMA_FTYPE_MOSTLY_Q5_0:
            default_type = GGML_TYPE_Q5_0;
            break;
        case LLAMA_FTYPE_MOSTLY_Q5_1:
            default_type = GGML_TYPE_Q5_1;
            break;
        case LLAMA_FTYPE_MOSTLY_Q8_0:
            default_type = GGML_TYPE_Q8_0;
            break;
        case LLAMA_FTYPE_MOSTLY_F16:
            default_type = GGML_TYPE_F16;
            break;
        case LLAMA_FTYPE_MOSTLY_BF16:
            default_type = GGML_TYPE_BF16;
            break;
        case LLAMA_FTYPE_ALL_F32:
            default_type = GGML_TYPE_F32;
            break;

        // K-quants
        case LLAMA_FTYPE_MOSTLY_Q2_K_S:
        case LLAMA_FTYPE_MOSTLY_Q2_K:
            default_type = GGML_TYPE_Q2_K;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ3_XS:
            default_type = GGML_TYPE_IQ3_S;
            break;
        case LLAMA_FTYPE_MOSTLY_Q3_K_S:
        case LLAMA_FTYPE_MOSTLY_Q3_K_M:
        case LLAMA_FTYPE_MOSTLY_Q3_K_L:
            default_type = GGML_TYPE_Q3_K;
            break;
        case LLAMA_FTYPE_MOSTLY_Q4_K_S:
        case LLAMA_FTYPE_MOSTLY_Q4_K_M:
            default_type = GGML_TYPE_Q4_K;
            break;
        case LLAMA_FTYPE_MOSTLY_Q5_K_S:
        case LLAMA_FTYPE_MOSTLY_Q5_K_M:
            default_type = GGML_TYPE_Q5_K;
            break;
        case LLAMA_FTYPE_MOSTLY_Q6_K:
            default_type = GGML_TYPE_Q6_K;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ2_XXS:
            default_type = GGML_TYPE_IQ2_XXS;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ2_XS:
            default_type = GGML_TYPE_IQ2_XS;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ2_S:
            default_type = GGML_TYPE_IQ2_XS;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ2_M:
            default_type = GGML_TYPE_IQ2_S;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ3_XXS:
            default_type = GGML_TYPE_IQ3_XXS;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ1_S:
            default_type = GGML_TYPE_IQ1_S;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ1_M:
            default_type = GGML_TYPE_IQ1_M;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ4_NL:
            default_type = GGML_TYPE_IQ4_NL;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ4_XS:
            default_type = GGML_TYPE_IQ4_XS;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ3_S:
            default_type = GGML_TYPE_IQ3_S;
            break;
        case LLAMA_FTYPE_MOSTLY_IQ3_M:
            default_type = GGML_TYPE_IQ3_S;
            break;

        default:
            throw std::runtime_error(format("invalid output file type %d\n", ftype));
    }

    int nthread = params->nthread;

    if (nthread <= 0)
    {
        nthread = std::thread::hardware_concurrency();
    }

    // mmap consistently increases speed Linux, and also increases speed on Windows with
    // hot cache. It may cause a slowdown on macOS, possibly related to free memory.
#if defined(__linux__) || defined(_WIN32)
    constexpr bool use_mmap = true;
#else
    constexpr bool use_mmap = false;
#endif

    llama_model_kv_override * kv_overrides = nullptr;
    if (params->kv_overrides)
    {
        auto v = (std::vector<llama_model_kv_override> *)params->kv_overrides;
        kv_overrides = v->data();
    }
    llama_model_loader ml(fname_inp, use_mmap, /*check_tensors*/ true, kv_overrides);
    ml.init_mappings(false); // no prefetching

    llama_model model;
    llm_load_arch(ml, model);
    llm_load_hparams(ml, model);

    struct quantize_state_internal qs(model, params);

    if (params->only_copy)
    {
        ftype = model.ftype;
    }
    const std::unordered_map<std::string, std::vector<float>> * imatrix_data = nullptr;
    if (params->imatrix)
    {
        imatrix_data = static_cast<const std::unordered_map<std::string, std::vector<float>> *>(params->imatrix);
        if (imatrix_data)
        {
            LLAMA_LOG_INFO("================================ Have weights data with %d entries\n", int(imatrix_data->size()));
            qs.has_imatrix = true;
        }
    }

    const size_t align = GGUF_DEFAULT_ALIGNMENT;
    struct gguf_context * ctx_out = gguf_init_empty();

    // copy the KV pairs from the input file
    gguf_set_kv(ctx_out, ml.meta);
    gguf_set_val_u32(ctx_out, "general.quantization_version", GGML_QNT_VERSION);
    gguf_set_val_u32(ctx_out, "general.file_type", ftype);
    // Remove split metadata
    gguf_remove_key(ctx_out, ml.llm_kv(LLM_KV_SPLIT_NO).c_str());
    gguf_remove_key(ctx_out, ml.llm_kv(LLM_KV_SPLIT_COUNT).c_str());
    gguf_remove_key(ctx_out, ml.llm_kv(LLM_KV_SPLIT_TENSORS_COUNT).c_str());

    if (params->kv_overrides)
    {
        const std::vector<llama_model_kv_override> & overrides = *(const std::vector<llama_model_kv_override> *)params->kv_overrides;
        for (auto & o : overrides)
        {
            if (o.key[0] == 0)
                break;
            if (o.tag == LLAMA_KV_OVERRIDE_TYPE_FLOAT)
            {
                gguf_set_val_f32(ctx_out, o.key, o.val_f64);
            }
            else if (o.tag == LLAMA_KV_OVERRIDE_TYPE_INT)
            {
                gguf_set_val_i32(ctx_out, o.key, o.val_i64);
            }
            else if (o.tag == LLAMA_KV_OVERRIDE_TYPE_BOOL)
            {
                gguf_set_val_bool(ctx_out, o.key, o.val_bool);
            }
            else if (o.tag == LLAMA_KV_OVERRIDE_TYPE_STR)
            {
                gguf_set_val_str(ctx_out, o.key, o.val_str);
            }
            else
            {
                LLAMA_LOG_WARN("%s: unknown KV override type for key %s\n", __func__, o.key);
            }
        }
    }

    for (int i = 0; i < ml.n_tensors; ++i)
    {
        const struct ggml_tensor * meta = ml.get_tensor_meta(i);

        const std::string name = ggml_get_name(meta);

        // TODO: avoid hardcoded tensor names - use the TN_* constants
        if (name.find("attn_v.weight") != std::string::npos || name.find("attn_qkv.weight") != std::string::npos)
        {
            ++qs.n_attention_wv;
        }
        else if (name == LLM_TN(model.arch)(LLM_TENSOR_OUTPUT, "weight"))
        {
            qs.has_output = true;
        }
    }

    qs.n_ffn_down = qs.n_ffn_gate = qs.n_ffn_up = (int)model.hparams.n_layer;

    // sanity checks
    //
    //  - qs.n_attention_wv == 0                     for Mamba       models
    //  - qs.n_attention_wv == model.hparams.n_layer for Transformer models
    //
    GGML_ASSERT((qs.n_attention_wv == 0 || qs.n_attention_wv == (int)model.hparams.n_layer) && "n_attention_wv is unexpected");

    size_t total_size_org = 0;
    size_t total_size_new = 0;

    std::vector<std::thread> workers;
    workers.reserve(nthread);

    int idx = 0;

    std::vector<no_init<uint8_t>> read_data;
    std::vector<no_init<uint8_t>> work;
    std::vector<no_init<float>> f32_conv_buf;

    uint16_t n_split = 1;
    // Assume split index is continuous
    if (params->keep_split)
    {
        for (int i = 0; i < ml.n_tensors; ++i)
        {
            n_split = std::max(uint16_t(ml.get_weight(i)->idx + 1), n_split);
        }
    }
    std::vector<gguf_context *> ctx_outs(n_split, nullptr);
    ctx_outs[0] = ctx_out;

    // populate the original tensors so we get an initial meta data
    for (int i = 0; i < ml.n_tensors; ++i)
    {
        auto weight = ml.get_weight(i);
        uint16_t i_split = params->keep_split ? weight->idx : 0;
        struct ggml_tensor * tensor = weight->tensor;
        if (ctx_outs[i_split] == nullptr)
        {
            ctx_outs[i_split] = gguf_init_empty();
        }
        gguf_add_tensor(ctx_outs[i_split], tensor);
    }

    // Set split info if needed
    if (n_split > 1)
    {
        for (size_t i = 0; i < ctx_outs.size(); ++i)
        {
            gguf_set_val_u16(ctx_outs[i], ml.llm_kv(LLM_KV_SPLIT_NO).c_str(), i);
            gguf_set_val_u16(ctx_outs[i], ml.llm_kv(LLM_KV_SPLIT_COUNT).c_str(), n_split);
            gguf_set_val_i32(ctx_outs[i], ml.llm_kv(LLM_KV_SPLIT_TENSORS_COUNT).c_str(), ml.n_tensors);
        }
    }

    int cur_split = -1;
    std::ofstream fout;
    auto close_ofstream = [&]()
    {
        // Write metadata and close file handler
        if (fout.is_open())
        {
            fout.seekp(0);
            std::vector<uint8_t> data(gguf_get_meta_size(ctx_outs[cur_split]));
            gguf_get_meta_data(ctx_outs[cur_split], data.data());
            fout.write((const char *)data.data(), data.size());
            fout.close();
        }
    };
    auto new_ofstream = [&](int index)
    {
        cur_split = index;
        GGML_ASSERT(ctx_outs[cur_split] && "Find uninitialized gguf_context");
        std::string fname = fname_out;
        if (params->keep_split)
        {
            char split_path[PATH_MAX] = {0};
            llama_split_path(split_path, sizeof(split_path), fname_out.c_str(), cur_split, n_split);
            fname = std::string(split_path);
        }

        fout = std::ofstream(fname, std::ios::binary);
        fout.exceptions(std::ofstream::failbit); // fail fast on write errors
        const size_t meta_size = gguf_get_meta_size(ctx_outs[cur_split]);
        // placeholder for the meta data
        ::zeros(fout, meta_size);
    };

    const auto tn = LLM_TN(model.arch);
    new_ofstream(0);
    for (int i = 0; i < ml.n_tensors; ++i)
    {
        auto weight = ml.get_weight(i);
        struct ggml_tensor * tensor = weight->tensor;
        if (weight->idx != cur_split && params->keep_split)
        {
            close_ofstream();
            new_ofstream(weight->idx);
        }

        const std::string name = ggml_get_name(tensor);

        if (!ml.use_mmap)
        {
            if (read_data.size() < ggml_nbytes(tensor))
            {
                read_data.resize(ggml_nbytes(tensor));
            }
            tensor->data = read_data.data();
        }
        ml.load_data_for(tensor);

        LLAMA_LOG_INFO(
            "[%4d/%4d] %36s - [%s], type = %6s, ",
            ++idx,
            ml.n_tensors,
            ggml_get_name(tensor),
            llama_format_tensor_shape(tensor).c_str(),
            ggml_type_name(tensor->type));

        // This used to be a regex, but <regex> has an extreme cost to compile times.
        bool quantize = name.rfind("weight") == name.size() - 6; // ends with 'weight'?

        // quantize only 2D and 3D tensors (experts)
        quantize &= (ggml_n_dims(tensor) >= 2);

        // do not quantize norm tensors
        quantize &= name.find("_norm.weight") == std::string::npos;

        quantize &= params->quantize_output_tensor || name != "output.weight";
        quantize &= !params->only_copy;

        // do not quantize expert gating tensors
        // NOTE: can't use LLM_TN here because the layer number is not known
        quantize &= name.find("ffn_gate_inp.weight") == std::string::npos;

        // do not quantize positional embeddings and token types (BERT)
        quantize &= name != LLM_TN(model.arch)(LLM_TENSOR_POS_EMBD, "weight");
        quantize &= name != LLM_TN(model.arch)(LLM_TENSOR_TOKEN_TYPES, "weight");

        // do not quantize Mamba's small yet 2D weights
        // NOTE: can't use LLM_TN here because the layer number is not known
        quantize &= name.find("ssm_conv1d.weight") == std::string::npos;
        quantize &= name.find("ssm_x.weight") == std::string::npos;
        quantize &= name.find("ssm_dt.weight") == std::string::npos;

        enum ggml_type new_type;
        void * new_data;
        size_t new_size;

        if (quantize)
        {
            new_type = default_type;

            // get more optimal quantization type based on the tensor shape, layer, etc.
            if (!params->pure && ggml_is_quantized(default_type))
            {
                new_type = llama_tensor_get_type(qs, new_type, tensor, ftype);
            }
            if (params->token_embedding_type < GGML_TYPE_COUNT && strcmp(tensor->name, "token_embd.weight") == 0)
            {
                new_type = params->token_embedding_type;
            }
            if (params->output_tensor_type < GGML_TYPE_COUNT && strcmp(tensor->name, "output.weight") == 0)
            {
                new_type = params->output_tensor_type;
            }

            // If we've decided to quantize to the same type the tensor is already
            // in then there's nothing to do.
            quantize = tensor->type != new_type;
        }

        if (!quantize)
        {
            new_type = tensor->type;
            new_data = tensor->data;
            new_size = ggml_nbytes(tensor);
            LLAMA_LOG_INFO("size = %8.3f MB\n", ggml_nbytes(tensor) / 1024.0 / 1024.0);
        }
        else
        {
            const int64_t nelements = ggml_nelements(tensor);

            const float * imatrix = nullptr;
            if (imatrix_data)
            {
                auto it = imatrix_data->find(tensor->name);
                if (it == imatrix_data->end())
                {
                    LLAMA_LOG_INFO("\n====== %s: did not find weights for %s\n", __func__, tensor->name);
                }
                else
                {
                    if (it->second.size() == (size_t)tensor->ne[0] * tensor->ne[2])
                    {
                        imatrix = it->second.data();
                    }
                    else
                    {
                        LLAMA_LOG_INFO(
                            "\n====== %s: imatrix size %d is different from tensor size %d for %s\n",
                            __func__,
                            int(it->second.size()),
                            int(tensor->ne[0] * tensor->ne[2]),
                            tensor->name);

                        // this can happen when quantizing an old mixtral model with split tensors with a new incompatible imatrix
                        // this is a significant error and it may be good idea to abort the process if this happens,
                        // since many people will miss the error and not realize that most of the model is being quantized without an imatrix
                        // tok_embd should be ignored in this case, since it always causes this warning
                        if (name != tn(LLM_TENSOR_TOKEN_EMBD, "weight"))
                        {
                            throw std::runtime_error(format(
                                "imatrix size %d is different from tensor size %d for %s",
                                int(it->second.size()),
                                int(tensor->ne[0] * tensor->ne[2]),
                                tensor->name));
                        }
                    }
                }
            }
            if ((new_type == GGML_TYPE_IQ2_XXS || new_type == GGML_TYPE_IQ2_XS || new_type == GGML_TYPE_IQ2_S || new_type == GGML_TYPE_IQ1_S
                 || (new_type == GGML_TYPE_IQ1_M && strcmp(tensor->name, "token_embd.weight") && strcmp(tensor->name, "output.weight"))
                 || (new_type == GGML_TYPE_Q2_K && params->ftype == LLAMA_FTYPE_MOSTLY_Q2_K_S
                     && strcmp(tensor->name, "token_embd.weight") != 0))
                && !imatrix)
            {
                LLAMA_LOG_ERROR("\n\n============================================================\n");
                LLAMA_LOG_ERROR("Missing importance matrix for tensor %s in a very low-bit quantization\n", tensor->name);
                LLAMA_LOG_ERROR("The result will be garbage, so bailing out\n");
                LLAMA_LOG_ERROR("============================================================\n\n");
                throw std::runtime_error(format("Missing importance matrix for tensor %s in a very low-bit quantization", tensor->name));
            }

            float * f32_data;

            if (tensor->type == GGML_TYPE_F32)
            {
                f32_data = (float *)tensor->data;
            }
            else if (ggml_is_quantized(tensor->type) && !params->allow_requantize)
            {
                throw std::runtime_error(format("requantizing from type %s is disabled", ggml_type_name(tensor->type)));
            }
            else
            {
                llama_tensor_dequantize_internal(tensor, f32_conv_buf, workers, nelements, nthread);
                f32_data = (float *)f32_conv_buf.data();
            }

            LLAMA_LOG_INFO("converting to %s .. ", ggml_type_name(new_type));
            fflush(stdout);

            if (work.size() < (size_t)nelements * 4)
            {
                work.resize(nelements * 4); // upper bound on size
            }
            new_data = work.data();

            const int64_t n_per_row = tensor->ne[0];
            const int64_t nrows = tensor->ne[1];

            static const int64_t min_chunk_size = 32 * 512;
            const int64_t chunk_size = n_per_row >= min_chunk_size ? n_per_row : n_per_row * ((min_chunk_size + n_per_row - 1) / n_per_row);

            const int64_t nelements_matrix = tensor->ne[0] * tensor->ne[1];
            const int64_t nchunk = (nelements_matrix + chunk_size - 1) / chunk_size;
            const int64_t nthread_use = nthread > 1 ? std::max((int64_t)1, std::min((int64_t)nthread, nchunk)) : 1;

            // quantize each expert separately since they have different importance matrices
            new_size = 0;
            for (int64_t i03 = 0; i03 < tensor->ne[2]; ++i03)
            {
                const float * f32_data_03 = f32_data + i03 * nelements_matrix;
                void * new_data_03 = (char *)new_data + ggml_row_size(new_type, n_per_row) * i03 * nrows;
                const float * imatrix_03 = imatrix ? imatrix + i03 * n_per_row : nullptr;

                new_size += llama_tensor_quantize_internal(
                    new_type, f32_data_03, new_data_03, chunk_size, nrows, n_per_row, imatrix_03, workers, nthread_use);
            }
            LLAMA_LOG_INFO("size = %8.2f MiB -> %8.2f MiB\n", ggml_nbytes(tensor) / 1024.0 / 1024.0, new_size / 1024.0 / 1024.0);
        }
        total_size_org += ggml_nbytes(tensor);
        total_size_new += new_size;

        // update the gguf meta data as we go
        gguf_set_tensor_type(ctx_outs[cur_split], name.c_str(), new_type);
        gguf_set_tensor_data(ctx_outs[cur_split], name.c_str(), new_data, new_size);

        // write tensor data + padding
        fout.write((const char *)new_data, new_size);
        zeros(fout, GGML_PAD(new_size, align) - new_size);
    }
    close_ofstream();
    for (auto & c : ctx_outs)
    {
        gguf_free(c);
    }

    LLAMA_LOG_INFO("%s: model size  = %8.2f MB\n", __func__, total_size_org / 1024.0 / 1024.0);
    LLAMA_LOG_INFO("%s: quant size  = %8.2f MB\n", __func__, total_size_new / 1024.0 / 1024.0);

    if (qs.n_fallback > 0)
    {
        LLAMA_LOG_WARN(
            "%s: WARNING: %d of %d tensor(s) required fallback quantization\n", __func__, qs.n_fallback, qs.n_k_quantized + qs.n_fallback);
    }
}

static int llama_apply_lora_from_file_internal(
    const struct llama_model & model, const char * path_lora, float scale, const char * path_base_model, int n_threads)
{
    LLAMA_LOG_INFO("%s: applying lora adapter from '%s' - please wait ...\n", __func__, path_lora);

    const int64_t t_start_lora_us = ggml_time_us();

    llama_file fin(path_lora, "rb");

    // verify magic and version
    {
        uint32_t magic = fin.read_u32();
        if (magic != LLAMA_FILE_MAGIC_GGLA)
        {
            LLAMA_LOG_ERROR("%s: bad file magic\n", __func__);
            return 1;
        }

        uint32_t format_version = fin.read_u32();
        if (format_version != 1)
        {
            LLAMA_LOG_ERROR("%s: unsupported file version\n", __func__);
            return 1;
        }
    }

    int32_t lora_r = fin.read_u32();
    int32_t lora_alpha = fin.read_u32();
    float scaling = scale * (float)lora_alpha / (float)lora_r;

    LLAMA_LOG_INFO("%s: r = %d, alpha = %d, scaling = %.2f\n", __func__, lora_r, lora_alpha, scaling);

    // load base model
    std::unique_ptr<llama_model_loader> ml;
    if (path_base_model)
    {
        LLAMA_LOG_INFO("%s: loading base model from '%s'\n", __func__, path_base_model);
        ml.reset(new llama_model_loader(path_base_model, /*use_mmap*/ true, /*check_tensors*/ false, /*kv_overrides*/ nullptr));
        ml->init_mappings(/*prefetch*/ false); // no prefetching
    }

    struct tensor_meta
    {
        std::string name;
        ggml_type type;
        int32_t ne[2];
        size_t offset;
    };
    std::map<std::string, tensor_meta> tensor_meta_map;

    // load all tensor meta
    while (true)
    {
        if (fin.tell() == fin.size)
        {
            // eof
            break;
        }

        int32_t n_dims;
        int32_t name_len;
        int32_t ftype;

        fin.read_raw(&n_dims, sizeof(n_dims));
        fin.read_raw(&name_len, sizeof(name_len));
        fin.read_raw(&ftype, sizeof(ftype));

        if (n_dims != 1 && n_dims != 2)
        {
            LLAMA_LOG_ERROR("%s: unsupported tensor dimension %d\n", __func__, n_dims);
            return 1;
        }

        int32_t ne[2] = {1, 1};
        for (int i = 0; i < n_dims; ++i)
        {
            fin.read_raw(&ne[i], sizeof(ne[i]));
        }

        std::string name;
        {
            GGML_ASSERT(name_len < GGML_MAX_NAME);
            char buf[GGML_MAX_NAME];
            fin.read_raw(buf, name_len);
            name = std::string(buf, name_len);
        }

        // check for lora suffix
        std::string lora_suffix;
        if (name.length() > 6)
        {
            lora_suffix = name.substr(name.length() - 6);
        }
        if (lora_suffix != ".loraA" && lora_suffix != ".loraB")
        {
            LLAMA_LOG_ERROR("%s: error: '%s' is not a lora tensor\n", __func__, name.c_str());
            return 1;
        }

        // tensor type
        ggml_type wtype;
        switch (ftype)
        {
            case 0:
                wtype = GGML_TYPE_F32;
                break;
            case 1:
                wtype = GGML_TYPE_F16;
                break;
            default: {
                LLAMA_LOG_ERROR("%s: invalid tensor data type '%d'\n", __func__, ftype);
                return 1;
            }
        }

        // data offset
        size_t offset = fin.tell();
        offset = (offset + 31) & -32;

        // skip tensor data
        fin.seek(offset + ggml_row_size(wtype, ne[0]) * ne[1], SEEK_SET);

        tensor_meta_map.emplace(name, tensor_meta{name, wtype, {ne[0], ne[1]}, offset});
    }

    bool warned = false;
    int n_tensors = 0;

    // apply
    ggml_backend_t backend_cpu = ggml_backend_cpu_init();
    if (backend_cpu == nullptr)
    {
        LLAMA_LOG_ERROR("%s: error: failed to initialize cpu backend\n", __func__);
        return 1;
    }
    ggml_backend_cpu_set_n_threads(backend_cpu, n_threads);

    std::vector<no_init<uint8_t>> read_buf;
    for (const auto & it : model.tensors_by_name)
    {
        const std::string & base_name = it.first;
        ggml_tensor * model_t = it.second;

        if (tensor_meta_map.find(base_name + ".loraA") == tensor_meta_map.end()
            || tensor_meta_map.find(base_name + ".loraB") == tensor_meta_map.end())
        {
            continue;
        }

        tensor_meta & metaA = tensor_meta_map.at(base_name + ".loraA");
        tensor_meta & metaB = tensor_meta_map.at(base_name + ".loraB");

        ggml_init_params lora_init_params = {
            /* .mem_size   */ ggml_tensor_overhead() * 128 + ggml_graph_overhead(),
            /* .mem_buffer */ nullptr,
            /* .no_alloc   */ true,
        };
        ggml_context * lora_ctx = ggml_init(lora_init_params);
        if (lora_ctx == nullptr)
        {
            LLAMA_LOG_ERROR("%s: error: failed to initialize lora context\n", __func__);
            ggml_backend_free(backend_cpu);
            return 1;
        }

        // create tensors
        ggml_tensor * loraA = ggml_new_tensor_2d(lora_ctx, metaA.type, metaA.ne[0], metaA.ne[1]);
        ggml_tensor * loraB = ggml_new_tensor_2d(lora_ctx, metaB.type, metaB.ne[0], metaB.ne[1]);
        ggml_set_name(loraA, metaA.name.c_str());
        ggml_set_name(loraB, metaB.name.c_str());

        ggml_tensor * base_t;
        if (ml)
        {
            if (!ml->get_tensor_meta(base_name.c_str()))
            {
                LLAMA_LOG_ERROR("%s: error: tensor '%s' not found in base model\n", __func__, base_name.c_str());
                return 1;
            }
            base_t = ggml_dup_tensor(lora_ctx, ml->get_tensor_meta(base_name.c_str()));
        }
        else
        {
            base_t = ggml_dup_tensor(lora_ctx, model_t);
        }
        ggml_set_name(base_t, base_name.c_str());

        // allocate in backend buffer
        ggml_backend_buffer_t lora_buf = ggml_backend_alloc_ctx_tensors_from_buft(lora_ctx, ggml_backend_cpu_buffer_type());
        if (lora_buf == nullptr)
        {
            LLAMA_LOG_ERROR("%s: error: failed to allocate lora tensors\n", __func__);
            return 1;
        }

        // load tensor data
        auto load_tensor = [&read_buf, &fin](const tensor_meta & tensor_meta, ggml_tensor * tensor)
        {
            read_buf.resize(ggml_nbytes(tensor));
            fin.seek(tensor_meta.offset, SEEK_SET);
            fin.read_raw(read_buf.data(), ggml_nbytes(tensor));
            ggml_backend_tensor_set(tensor, read_buf.data(), 0, read_buf.size());
        };
        load_tensor(metaA, loraA);
        load_tensor(metaB, loraB);

        // load base model tensor data
        if (ml)
        {
            ml->load_data_for(base_t);
        }
        else
        {
            ggml_backend_tensor_copy(model_t, base_t);
        }

        if (ggml_is_quantized(base_t->type) && !warned)
        {
            LLAMA_LOG_WARN(
                "%s: warning: using a lora adapter with a quantized model may result in poor quality, "
                "use a f16 or f32 base model with --lora-base\n",
                __func__);
            warned = true;
        }

        if (base_t->ne[0] != loraA->ne[1] || base_t->ne[1] != loraB->ne[1])
        {
            LLAMA_LOG_ERROR(
                "%s: incompatible tensor dimensions (%" PRId64 " and %" PRId64 ");"
                " are you sure that this adapter is for this model?\n",
                __func__,
                base_t->ne[0],
                loraA->ne[1]);
            ggml_free(lora_ctx);
            ggml_backend_buffer_free(lora_buf);
            ggml_backend_free(backend_cpu);
            return 1;
        }

        auto build_lora_graph = [&]()
        {
            // w = w + BA*s
            ggml_tensor * BA = ggml_mul_mat(lora_ctx, loraA, loraB);
            ggml_set_name(BA, "BA");

            if (scaling != 1.0f)
            {
                BA = ggml_scale(lora_ctx, BA, scaling);
                ggml_set_name(BA, "BA_scaled");
            }

            ggml_tensor * r;
            r = ggml_add_inplace(lora_ctx, base_t, BA);
            ggml_set_name(r, "r_add");

            if (base_t->type != model_t->type)
            {
                // convert the result to the model type
                r = ggml_cast(lora_ctx, r, model_t->type);
                ggml_set_name(r, "r_cast");
            }

            return r;
        };

        ggml_cgraph * gf = ggml_new_graph(lora_ctx);
        ggml_tensor * r = build_lora_graph();
        ggml_build_forward_expand(gf, r);

        ggml_backend_buffer_t graph_buf = ggml_backend_alloc_ctx_tensors_from_buft(lora_ctx, ggml_backend_cpu_buffer_type());
        if (graph_buf == nullptr)
        {
            LLAMA_LOG_ERROR("%s: error: failed to allocate graph tensors\n", __func__);
            ggml_free(lora_ctx);
            ggml_backend_buffer_free(lora_buf);
            ggml_backend_free(backend_cpu);
            return 1;
        }

        ggml_backend_graph_compute(backend_cpu, gf);

        ggml_backend_tensor_set(model_t, r->data, 0, ggml_nbytes(r));

#if 0
        // TODO: use scheduler with fallback to CPU for less copies between CPU and GPU
        //ggml_backend_sched_t sched = ggml_backend_sched_new(backends.data(), backends.size(), GGML_DEFAULT_GRAPH_SIZE);

        // sched compute
        ggml_build_forward_expand(gf, build_graph());
        ggml_backend_sched_init_measure(sched, gf);

        // create the graph again, since the previous one was destroyed by the measure
        ggml_graph_clear(gf);
        ggml_build_forward_expand(gf, build_graph());
        ggml_backend_sched_graph_compute(sched, gf);
        ggml_backend_sched_free(sched);
#endif

        ggml_backend_buffer_free(lora_buf);
        ggml_backend_buffer_free(graph_buf);
        ggml_free(lora_ctx);

        n_tensors++;
        if (n_tensors % 4 == 0)
        {
            LLAMA_LOG_INFO(".");
        }
    }

    ggml_backend_free(backend_cpu);

    const int64_t t_lora_us = ggml_time_us() - t_start_lora_us;
    LLAMA_LOG_INFO(" done (%.2f ms)\n", t_lora_us / 1000.0);

    return 0;
}

//
// interface implementation
//
struct llama_model_params llama_model_default_params()
{
    llama_model_params result = {
        .n_gpu_layers                =0,
        .split_mode                  =LLAMA_SPLIT_MODE_LAYER,
        .main_gpu                    =0,
        .tensor_split                =nullptr,
        .rpc_servers                 =nullptr,
        .progress_callback           =nullptr,
        .progress_callback_user_data =nullptr,
        .kv_overrides                =nullptr,
        .vocab_only                  =false,
        .use_mmap                    =true,
        .use_mlock                   =false,
        .check_tensors               =false,
    };

#ifdef GGML_USE_METAL
    // note: we usually have plenty of VRAM, so by default offload all layers to the GPU
    result.n_gpu_layers = 999;
#endif

    return result;
}

struct llama_context_params llama_context_default_params()
{
    llama_context_params result = {
        .seed                        =LLAMA_DEFAULT_SEED,
        .n_ctx                       =512,
        .n_batch                     =2048,
        .n_ubatch                    =512,
        .n_seq_max                   =1,
        .n_threads                   =GGML_DEFAULT_N_THREADS, // TODO: better default
        .n_threads_batch             =GGML_DEFAULT_N_THREADS,
        .rope_scaling_type           =LLAMA_ROPE_SCALING_TYPE_UNSPECIFIED,
        .pooling_type                =LLAMA_POOLING_TYPE_UNSPECIFIED,
        .rope_freq_base              =0.0f,
        .rope_freq_scale             =0.0f,
        .yarn_ext_factor             =-1.0f,
        .yarn_attn_factor            =1.0f,
        .yarn_beta_fast              =32.0f,
        .yarn_beta_slow              =1.0f,
        .yarn_orig_ctx               =0,
        .defrag_thold                =-1.0f,
        .cb_eval                     =nullptr,
        .cb_eval_user_data           =nullptr,
        .type_k                      =GGML_TYPE_F16,
        .type_v                      =GGML_TYPE_F16,
        .logits_all                  =false,
        .embeddings                  =false,
        .offload_kqv                 =true,
        .flash_attn                  =false,
        .abort_callback              =nullptr,
        .abort_callback_data         =nullptr,
    };

    return result;
}

struct llama_model_quantize_params llama_model_quantize_default_params()
{
    llama_model_quantize_params result = {
        .nthread                     =0,
        .ftype                       =LLAMA_FTYPE_MOSTLY_Q5_1,
        .output_tensor_type          =GGML_TYPE_COUNT,
        .token_embedding_type        =GGML_TYPE_COUNT,
        .allow_requantize            =false,
        .quantize_output_tensor      =true,
        .only_copy                   =false,
        .pure                        =false,
        .keep_split                  =false,
        .imatrix                     =nullptr,
        .kv_overrides                =nullptr,
    };

    return result;
}

size_t llama_max_devices(void)
{
#if defined(GGML_USE_RPC)
    return GGML_RPC_MAX_SERVERS;
#elif defined(GGML_USE_METAL)
    return 1;
#elif defined(GGML_USE_CUDA)
    return GGML_CUDA_MAX_DEVICES;
#elif defined(GGML_USE_SYCL)
    return GGML_SYCL_MAX_DEVICES;
#elif defined(GGML_USE_VULKAN)
    return GGML_VK_MAX_DEVICES;
#else
    return 1;
#endif
}

bool llama_supports_mmap(void)
{
    return llama_mmap::SUPPORTED;
}

bool llama_supports_mlock(void)
{
    return llama_mlock::SUPPORTED;
}

bool llama_supports_gpu_offload(void)
{
#if defined(GGML_USE_CUDA) || defined(GGML_USE_CLBLAST) || defined(GGML_USE_METAL) || defined(GGML_USE_VULKAN) || defined(GGML_USE_SYCL) \
    || defined(GGML_USE_KOMPUTE) || defined(GGML_USE_RPC)
    // Defined when llama.cpp is compiled with support for offloading model layers to GPU.
    return true;
#else
    return false;
#endif
}

void llama_backend_init(void)
{
    ggml_time_init();

    // needed to initialize f16 tables
    {
        struct ggml_init_params params = {0, nullptr, false};
        struct ggml_context * ctx = ggml_init(params);
        ggml_free(ctx);
    }
}

void llama_numa_init(enum ggml_numa_strategy numa)
{
    if (numa != GGML_NUMA_STRATEGY_DISABLED)
    {
        ggml_numa_init(numa);
    }
}

void llama_backend_free(void)
{
    ggml_quantize_free();
}

int64_t llama_time_us(void)
{
    return ggml_time_us();
}

struct llama_model * llama_load_model_from_file(const char * path_model, struct llama_model_params params)
{
    ggml_time_init();

    llama_model * model = new llama_model;

    unsigned cur_percentage = 0;
    if (params.progress_callback == nullptr)
    {
        params.progress_callback_user_data = &cur_percentage;
        params.progress_callback = [](float progress, void * ctx)
        {
            unsigned * cur_percentage_p = (unsigned *)ctx;
            unsigned percentage = (unsigned)(100 * progress);
            while (percentage > *cur_percentage_p)
            {
                *cur_percentage_p = percentage;
                LLAMA_LOG_INFO(".");
                if (percentage >= 100)
                {
                    LLAMA_LOG_INFO("\n");
                }
            }
            return true;
        };
    }
    if (params.rpc_servers != nullptr)
    {
        // split the servers set them into model->rpc_servers
        std::string servers(params.rpc_servers);
        size_t pos = 0;
        while ((pos = servers.find(",")) != std::string::npos)
        {
            std::string server = servers.substr(0, pos);
            model->rpc_servers.push_back(server);
            servers.erase(0, pos + 1);
        }
        model->rpc_servers.push_back(servers);
    }
    int status = llama_model_load(path_model, *model, params);
    GGML_ASSERT(status <= 0);
    if (status < 0)
    {
        if (status == -1)
        {
            LLAMA_LOG_ERROR("%s: failed to load model\n", __func__);
        }
        else if (status == -2)
        {
            LLAMA_LOG_INFO("%s: cancelled model load\n", __func__);
        }
        delete model;
        return nullptr;
    }

    return model;
}

void llama_free_model(struct llama_model * model)
{
    delete model;
}

struct llama_context * llama_new_context_with_model(struct llama_model * model, struct llama_context_params params)
{
    if (!model)
    {
        LLAMA_LOG_ERROR("%s: model cannot be nullptr\n", __func__);
        return nullptr;
    }

    if (params.n_batch == 0 && params.n_ubatch == 0)
    {
        LLAMA_LOG_ERROR("%s: n_batch and n_ubatch cannot both be zero\n", __func__);
        return nullptr;
    }

    if (params.n_ctx == 0 && model->hparams.n_ctx_train == 0)
    {
        LLAMA_LOG_ERROR("%s: n_ctx and model->hparams.n_ctx_train cannot both be zero\n", __func__);
        return nullptr;
    }

    if (params.flash_attn && model->arch == LLM_ARCH_GROK)
    {
        LLAMA_LOG_WARN("%s: flash_attn is not compatible with Grok - forcing off\n", __func__);
        params.flash_attn = false;
    }

    llama_context * ctx = new llama_context(*model);

    const auto & hparams = model->hparams;
    auto & cparams = ctx->cparams;

    cparams.n_seq_max = std::max(1u, params.n_seq_max);
    cparams.n_threads = params.n_threads;
    cparams.n_threads_batch = params.n_threads_batch;
    cparams.yarn_ext_factor = params.yarn_ext_factor;
    cparams.yarn_attn_factor = params.yarn_attn_factor;
    cparams.yarn_beta_fast = params.yarn_beta_fast;
    cparams.yarn_beta_slow = params.yarn_beta_slow;
    cparams.defrag_thold = params.defrag_thold;
    cparams.embeddings = params.embeddings;
    cparams.offload_kqv = params.offload_kqv;
    cparams.flash_attn = params.flash_attn;
    cparams.pooling_type = params.pooling_type;

    cparams.n_ctx = params.n_ctx == 0 ? hparams.n_ctx_train : params.n_ctx;
    cparams.rope_freq_base = params.rope_freq_base == 0.0f ? hparams.rope_freq_base_train : params.rope_freq_base;
    cparams.rope_freq_scale = params.rope_freq_scale == 0.0f ? hparams.rope_freq_scale_train : params.rope_freq_scale;

    // this is necessary due to kv_self.n being padded later during inference
    cparams.n_ctx = GGML_PAD(cparams.n_ctx, llama_kv_cache_get_padding(cparams));

    // with causal attention, the batch size is limited by the context size
    cparams.n_batch = hparams.causal_attn ? std::min(cparams.n_ctx, params.n_batch) : params.n_batch;

    // the batch has to be at least GGML_KQ_MASK_PAD because we will be padding the KQ_mask
    // this is required by GPU kernels in order to avoid out-of-bounds accesses (e.g. ggml_flash_attn_ext)
    // ref: https://github.com/ggerganov/llama.cpp/pull/5021
    if (cparams.n_batch < GGML_KQ_MASK_PAD)
    {
        LLAMA_LOG_WARN("%s: n_batch is less than GGML_KQ_MASK_PAD - increasing to %d\n", __func__, GGML_KQ_MASK_PAD);
        cparams.n_batch = GGML_KQ_MASK_PAD;
    }

    cparams.n_ubatch = std::min(cparams.n_batch, params.n_ubatch == 0 ? params.n_batch : params.n_ubatch);

    cparams.n_yarn_orig_ctx = params.yarn_orig_ctx != 0 ? params.yarn_orig_ctx
        : hparams.n_yarn_orig_ctx != 0                  ? hparams.n_yarn_orig_ctx
                                                        : hparams.n_ctx_train;

    cparams.cb_eval = params.cb_eval;
    cparams.cb_eval_user_data = params.cb_eval_user_data;

    auto rope_scaling_type = params.rope_scaling_type;
    if (rope_scaling_type == LLAMA_ROPE_SCALING_TYPE_UNSPECIFIED)
    {
        rope_scaling_type = hparams.rope_scaling_type_train;
    }

    if (rope_scaling_type == LLAMA_ROPE_SCALING_TYPE_NONE)
    {
        cparams.rope_freq_scale = 1.0f; // never scale if scaling type is none
    }

    if (cparams.yarn_ext_factor < 0.0f)
    { // negative indicates 'not set'
        cparams.yarn_ext_factor = rope_scaling_type == LLAMA_ROPE_SCALING_TYPE_YARN ? 1.0f : 0.0f;
    }

    cparams.yarn_attn_factor *= hparams.rope_attn_factor;
    cparams.causal_attn = hparams.causal_attn;

    if (cparams.pooling_type == LLAMA_POOLING_TYPE_UNSPECIFIED)
    {
        if (hparams.pooling_type == LLAMA_POOLING_TYPE_UNSPECIFIED)
        {
            cparams.pooling_type = LLAMA_POOLING_TYPE_NONE;
        }
        else
        {
            cparams.pooling_type = hparams.pooling_type;
        }
    }

    if (params.seed == LLAMA_DEFAULT_SEED)
    {
        params.seed = time(nullptr);
    }

    LLAMA_LOG_INFO("%s: n_ctx      = %u\n", __func__, cparams.n_ctx);
    LLAMA_LOG_INFO("%s: n_batch    = %u\n", __func__, cparams.n_batch);
    LLAMA_LOG_INFO("%s: n_ubatch   = %u\n", __func__, cparams.n_ubatch);
    LLAMA_LOG_INFO("%s: flash_attn = %d\n", __func__, cparams.flash_attn);
    LLAMA_LOG_INFO("%s: freq_base  = %.1f\n", __func__, cparams.rope_freq_base);
    LLAMA_LOG_INFO("%s: freq_scale = %g\n", __func__, cparams.rope_freq_scale);

    ctx->abort_callback = params.abort_callback;
    ctx->abort_callback_data = params.abort_callback_data;

    ctx->rng = pcg64_fast();
    ctx->rng.seed(params.seed);
    ctx->logits_all = params.logits_all;

    uint32_t kv_size = cparams.n_ctx;
    ggml_type type_k = params.type_k;
    ggml_type type_v = params.type_v;

    // Mamba only needs a constant number of KV cache cells per sequence
    if (model->arch == LLM_ARCH_MAMBA)
    {
        // Mamba needs at least as many KV cells as there are sequences kept at any time
        kv_size = std::max((uint32_t)1, params.n_seq_max);
        // it's probably best to keep as much precision as possible for the states
        type_k = GGML_TYPE_F32; // required by ggml_ssm_conv for Mamba's conv_states
        type_v = GGML_TYPE_F32; // required by ggml_ssm_scan for Mamba's ssm_states
    }

    GGML_ASSERT(hparams.n_embd_head_k % ggml_blck_size(type_k) == 0);
    GGML_ASSERT(hparams.n_embd_head_v % ggml_blck_size(type_v) == 0);

    if (!hparams.vocab_only)
    {
        // initialize backends
#if defined(GGML_USE_RPC)
        for (auto & server : model->rpc_servers)
        {
            ggml_backend_t backend = ggml_backend_rpc_init(server.c_str());
            if (backend == nullptr)
            {
                LLAMA_LOG_ERROR("%s: failed to connect RPC backend to %s\n", __func__, server.c_str());
                llama_free(ctx);
                return nullptr;
            }
            ctx->backends.push_back(backend);
        }
#elif defined(GGML_USE_METAL)
        if (model->n_gpu_layers > 0)
        {
            ctx->backend_metal = ggml_backend_metal_init();
            if (ctx->backend_metal == nullptr)
            {
                LLAMA_LOG_ERROR("%s: failed to initialize Metal backend\n", __func__);
                llama_free(ctx);
                return nullptr;
            }
            ctx->backends.push_back(ctx->backend_metal);
        }
#elif defined(GGML_USE_CUDA)
        if (model->split_mode == LLAMA_SPLIT_MODE_NONE || model->split_mode == LLAMA_SPLIT_MODE_ROW)
        {
            // with split_mode LLAMA_SPLIT_MODE_NONE or LLAMA_SPLIT_MODE_ROW, only the main GPU backend is used
            ggml_backend_t backend = ggml_backend_cuda_init(model->main_gpu);
            if (backend == nullptr)
            {
                LLAMA_LOG_ERROR("%s: failed to initialize CUDA%d backend\n", __func__, model->main_gpu);
                llama_free(ctx);
                return nullptr;
            }
            ctx->backends.push_back(backend);
        }
        else
        {
            // LLAMA_SPLIT_MODE_LAYER requires a backend for each GPU
            for (int device = 0; device < ggml_backend_cuda_get_device_count(); ++device)
            {
                ggml_backend_t backend = ggml_backend_cuda_init(device);
                if (backend == nullptr)
                {
                    LLAMA_LOG_ERROR("%s: failed to initialize CUDA%d backend\n", __func__, device);
                    llama_free(ctx);
                    return nullptr;
                }
                ctx->backends.push_back(backend);
            }
        }
#elif defined(GGML_USE_VULKAN)
        if (model->split_mode == LLAMA_SPLIT_MODE_ROW)
        {
            LLAMA_LOG_ERROR("%s: Row split not supported. Failed to initialize Vulkan backend\n", __func__);
            llama_free(ctx);
            return nullptr;
        }
        if (model->split_mode == LLAMA_SPLIT_MODE_NONE)
        {
            ggml_backend_t backend = ggml_backend_vk_init(0);
            if (backend == nullptr)
            {
                LLAMA_LOG_ERROR("%s: failed to initialize Vulkan backend\n", __func__);
                llama_free(ctx);
                return nullptr;
            }
            ctx->backends.push_back(backend);
        }
        else
        {
            for (int device = 0; device < ggml_backend_vk_get_device_count(); ++device)
            {
                ggml_backend_t backend = ggml_backend_vk_init(device);
                if (backend == nullptr)
                {
                    LLAMA_LOG_ERROR("%s: failed to initialize Vulkan%d backend\n", __func__, device);
                    llama_free(ctx);
                    return nullptr;
                }
                ctx->backends.push_back(backend);
            }
        }
#elif defined(GGML_USE_SYCL)
        // with split_mode LLAMA_SPLIT_MODE_NONE or LLAMA_SPLIT_MODE_ROW, only the main GPU backend is used
        if (model->split_mode == LLAMA_SPLIT_MODE_NONE || model->split_mode == LLAMA_SPLIT_MODE_ROW)
        {
            ggml_backend_t backend = ggml_backend_sycl_init(model->main_gpu);
            if (backend == nullptr)
            {
                int main_gpu_id = ggml_backend_sycl_get_device_id(model->main_gpu);
                LLAMA_LOG_ERROR("%s: failed to initialize SYCL%d (index %d) backend\n", __func__, main_gpu_id, model->main_gpu);
                llama_free(ctx);
                return nullptr;
            }
            ctx->backends.push_back(backend);
        }
        else
        {
            // LLAMA_SPLIT_LAYER requires a backend for each GPU
            for (int i = 0; i < ggml_backend_sycl_get_device_count(); ++i)
            {
                ggml_backend_t backend = ggml_backend_sycl_init(i);
                if (backend == nullptr)
                {
                    int id_list[GGML_SYCL_MAX_DEVICES];
                    ggml_sycl_get_gpu_list(id_list, GGML_SYCL_MAX_DEVICES);
                    LLAMA_LOG_ERROR("%s: failed to initialize SYCL%d (index %d) backend\n", __func__, id_list[i], i);
                    llama_free(ctx);
                    return nullptr;
                }
                ctx->backends.push_back(backend);
            }
        }
#elif defined(GGML_USE_KOMPUTE)
        if (model->n_gpu_layers > 0)
        {
            auto * backend = ggml_backend_kompute_init(model->main_gpu);
            if (backend == nullptr)
            {
                LLAMA_LOG_ERROR("%s: failed to initialize Kompute backend\n", __func__);
                llama_free(ctx);
                return nullptr;
            }
            ctx->backends.push_back(backend);
        }
#endif
        ctx->backend_cpu = ggml_backend_cpu_init();
        if (ctx->backend_cpu == nullptr)
        {
            LLAMA_LOG_ERROR("%s: failed to initialize CPU backend\n", __func__);
            llama_free(ctx);
            return nullptr;
        }
        ctx->backends.push_back(ctx->backend_cpu);

        if (!llama_kv_cache_init(ctx->kv_self, ctx, type_k, type_v, kv_size, cparams.offload_kqv))
        {
            LLAMA_LOG_ERROR("%s: llama_kv_cache_init() failed for self-attention cache\n", __func__);
            llama_free(ctx);
            return nullptr;
        }

        {
            size_t memory_size_k = 0;
            size_t memory_size_v = 0;

            for (auto & k : ctx->kv_self.k_l)
            {
                memory_size_k += ggml_nbytes(k);
            }

            for (auto & v : ctx->kv_self.v_l)
            {
                memory_size_v += ggml_nbytes(v);
            }

            LLAMA_LOG_INFO(
                "%s: KV self size  = %7.2f MiB, K (%s): %7.2f MiB, V (%s): %7.2f MiB\n",
                __func__,
                (float)(memory_size_k + memory_size_v) / (1024.0f * 1024.0f),
                ggml_type_name(type_k),
                (float)memory_size_k / (1024.0f * 1024.0f),
                ggml_type_name(type_v),
                (float)memory_size_v / (1024.0f * 1024.0f));
        }

        // graph outputs buffer
        {
            // resized during inference when a batch uses more outputs
            if (llama_output_reserve(*ctx, params.n_seq_max) < params.n_seq_max)
            {
                LLAMA_LOG_ERROR("%s: failed to reserve initial output buffer\n", __func__);
                llama_free(ctx);
                return nullptr;
            }

            LLAMA_LOG_INFO(
                "%s: %10s  output buffer size = %8.2f MiB\n",
                __func__,
                ggml_backend_buffer_name(ctx->buf_output),
                ggml_backend_buffer_get_size(ctx->buf_output) / 1024.0 / 1024.0);
        }

        // scheduler and compute buffers
        {
            // buffer types used for the compute buffer of each backend
            std::vector<ggml_backend_buffer_type_t> backend_buft;
            for (auto * backend : ctx->backends)
            {
                if (ggml_backend_is_cpu(backend))
                {
                    // use host buffers for the CPU backend compute buffer
                    backend_buft.push_back(llama_default_buffer_type_cpu(true));
                }
                else
                {
                    backend_buft.push_back(ggml_backend_get_default_buffer_type(backend));
                }
            }

            // buffer used to store the computation graph and the tensor meta data
            ctx->buf_compute_meta.resize(ggml_tensor_overhead() * LLAMA_MAX_NODES + ggml_graph_overhead_custom(LLAMA_MAX_NODES, false));

            // enabling pipeline parallelism in the scheduler increases memory usage, so it is only done when necessary
            bool pipeline_parallel = llama_get_device_count(*model) > 1 && model->n_gpu_layers > (int)model->hparams.n_layer
                && model->split_mode == LLAMA_SPLIT_MODE_LAYER && params.offload_kqv;
#ifndef GGML_USE_CUDA
            // pipeline parallelism requires support for async compute and events
            // currently this is only implemented in the CUDA backend
            pipeline_parallel = false;
#endif
            ctx->sched = ggml_backend_sched_new(
                ctx->backends.data(), backend_buft.data(), ctx->backends.size(), LLAMA_MAX_NODES, pipeline_parallel);

            if (pipeline_parallel)
            {
                LLAMA_LOG_INFO("%s: pipeline parallelism enabled (n_copies=%d)\n", __func__, ggml_backend_sched_get_n_copies(ctx->sched));
            }

            // build worst-case graph
            int n_tokens = (int)std::min(cparams.n_ctx, cparams.n_ubatch);
            int n_past = cparams.n_ctx - n_tokens;
            llama_token token = llama_token_bos(
                &ctx->model); // not actually used by llama_build_graph, but required to choose between token and embedding inputs graph
            ggml_cgraph * gf = llama_build_graph(*ctx, llama_batch_get_one(&token, n_tokens, n_past, 0), true);

            // initialize scheduler with the worst-case graph
            if (!ggml_backend_sched_reserve(ctx->sched, gf))
            {
                LLAMA_LOG_ERROR("%s: failed to allocate compute buffers\n", __func__);
                llama_free(ctx);
                return nullptr;
            }

            for (size_t i = 0; i < ctx->backends.size(); i++)
            {
                ggml_backend_t backend = ctx->backends[i];
                ggml_backend_buffer_type_t buft = backend_buft[i];
                size_t size = ggml_backend_sched_get_buffer_size(ctx->sched, backend);
                if (size > 1)
                {
                    LLAMA_LOG_INFO(
                        "%s: %10s compute buffer size = %8.2f MiB\n", __func__, ggml_backend_buft_name(buft), size / 1024.0 / 1024.0);
                }
            }

            // note: the number of splits during measure is higher than during inference due to the kv shift
            int n_splits = ggml_backend_sched_get_n_splits(ctx->sched);
            LLAMA_LOG_INFO("%s: graph nodes  = %d\n", __func__, gf->n_nodes);
            LLAMA_LOG_INFO("%s: graph splits = %d\n", __func__, n_splits);
        }
    }

    return ctx;
}

void llama_free(struct llama_context * ctx)
{
    delete ctx;
}

const llama_model * llama_get_model(const struct llama_context * ctx)
{
    return &ctx->model;
}

uint32_t llama_n_ctx(const struct llama_context * ctx)
{
    return ctx->cparams.n_ctx;
}

uint32_t llama_n_batch(const struct llama_context * ctx)
{
    return ctx->cparams.n_batch;
}

uint32_t llama_n_ubatch(const struct llama_context * ctx)
{
    return ctx->cparams.n_ubatch;
}

uint32_t llama_n_seq_max(const struct llama_context * ctx)
{
    return ctx->kv_self.size;
}

enum llama_vocab_type llama_vocab_type(const struct llama_model * model)
{
    return model->vocab.type;
}

enum llama_rope_type llama_rope_type(const struct llama_model * model)
{
    switch (model->arch)
    {
        // these models do not use RoPE
        case LLM_ARCH_GPT2:
        case LLM_ARCH_GPTJ:
        case LLM_ARCH_MPT:
        case LLM_ARCH_REFACT:
        case LLM_ARCH_BLOOM:
        case LLM_ARCH_MAMBA:
        case LLM_ARCH_JINA_BERT_V2:
            return LLAMA_ROPE_TYPE_NONE;

        // use what we call a normal RoPE, operating on pairs of consecutive head values
        case LLM_ARCH_LLAMA:
        case LLM_ARCH_BAICHUAN:
        case LLM_ARCH_STARCODER:
        case LLM_ARCH_PLAMO:
        case LLM_ARCH_CODESHELL:
        case LLM_ARCH_ORION:
        case LLM_ARCH_INTERNLM2:
        case LLM_ARCH_MINICPM:
        case LLM_ARCH_XVERSE:
        case LLM_ARCH_COMMAND_R:
        case LLM_ARCH_OLMO:
        case LLM_ARCH_ARCTIC:
            return LLAMA_ROPE_TYPE_NORM;

        // the pairs of head values are offset by n_rot/2
        case LLM_ARCH_FALCON:
        case LLM_ARCH_GROK:
        case LLM_ARCH_DBRX:
        case LLM_ARCH_BERT:
        case LLM_ARCH_NOMIC_BERT:
        case LLM_ARCH_STABLELM:
        case LLM_ARCH_QWEN:
        case LLM_ARCH_QWEN2:
        case LLM_ARCH_QWEN2MOE:
        case LLM_ARCH_PHI2:
        case LLM_ARCH_PHI3:
        case LLM_ARCH_GEMMA:
        case LLM_ARCH_STARCODER2:
        case LLM_ARCH_GPTNEOX:
            return LLAMA_ROPE_TYPE_NEOX;

        // all model arches should be listed explicitly here
        case LLM_ARCH_UNKNOWN:
            GGML_ASSERT(false && "unknown architecture");
            break;
    }

    return LLAMA_ROPE_TYPE_NONE;
}

enum llama_pooling_type llama_pooling_type(const struct llama_context * ctx)
{
    return ctx->cparams.pooling_type;
}

int32_t llama_n_vocab(const struct llama_model * model)
{
    return model->hparams.n_vocab;
}

int32_t llama_n_ctx_train(const struct llama_model * model)
{
    return model->hparams.n_ctx_train;
}

int32_t llama_n_embd(const struct llama_model * model)
{
    return model->hparams.n_embd;
}

int32_t llama_n_layer(const struct llama_model * model)
{
    return model->hparams.n_layer;
}

float llama_rope_freq_scale_train(const struct llama_model * model)
{
    return model->hparams.rope_freq_scale_train;
}

int32_t llama_model_meta_val_str(const struct llama_model * model, const char * key, char * buf, size_t buf_size)
{
    const auto & it = model->gguf_kv.find(key);
    if (it == model->gguf_kv.end())
    {
        if (buf_size > 0)
        {
            buf[0] = '\0';
        }
        return -1;
    }
    return snprintf(buf, buf_size, "%s", it->second.c_str());
}

int32_t llama_model_meta_count(const struct llama_model * model)
{
    return (int)model->gguf_kv.size();
}

int32_t llama_model_meta_key_by_index(const struct llama_model * model, int i, char * buf, size_t buf_size)
{
    if (i < 0 || i >= (int)model->gguf_kv.size())
    {
        if (buf_size > 0)
        {
            buf[0] = '\0';
        }
        return -1;
    }
    auto it = model->gguf_kv.begin();
    std::advance(it, i);
    return snprintf(buf, buf_size, "%s", it->first.c_str());
}

int32_t llama_model_meta_val_str_by_index(const struct llama_model * model, int32_t i, char * buf, size_t buf_size)
{
    if (i < 0 || i >= (int)model->gguf_kv.size())
    {
        if (buf_size > 0)
        {
            buf[0] = '\0';
        }
        return -1;
    }
    auto it = model->gguf_kv.begin();
    std::advance(it, i);
    return snprintf(buf, buf_size, "%s", it->second.c_str());
}

int32_t llama_model_desc(const struct llama_model * model, char * buf, size_t buf_size)
{
    return snprintf(
        buf,
        buf_size,
        "%s %s %s",
        llama_model_arch_name(model->arch),
        llama_model_type_name(model->type),
        llama_model_ftype_name(model->ftype).c_str());
}

uint64_t llama_model_size(const struct llama_model * model)
{
    uint64_t size = 0;
    for (const auto & it : model->tensors_by_name)
    {
        size += ggml_nbytes(it.second);
    }
    return size;
}

uint64_t llama_model_n_params(const struct llama_model * model)
{
    uint64_t nparams = 0;
    for (const auto & it : model->tensors_by_name)
    {
        nparams += ggml_nelements(it.second);
    }
    return nparams;
}

struct ggml_tensor * llama_get_model_tensor(struct llama_model * model, const char * name)
{
    auto it = std::find_if(
        model->tensors_by_name.begin(),
        model->tensors_by_name.end(),
        [name](const std::pair<std::string, struct ggml_tensor *> & it) { return it.first == name; });
    if (it == model->tensors_by_name.end())
    {
        return nullptr;
    }
    return it->second;
}

uint32_t llama_model_quantize(const char * fname_inp, const char * fname_out, const llama_model_quantize_params * params)
{
    try
    {
        llama_model_quantize_internal(fname_inp, fname_out, params);
        return 0;
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("%s: failed to quantize: %s\n", __func__, err.what());
        return 1;
    }
}

int32_t llama_model_apply_lora_from_file(
    const struct llama_model * model, const char * path_lora, float scale, const char * path_base_model, int32_t n_threads)
{
    try
    {
        return llama_apply_lora_from_file_internal(*model, path_lora, scale, path_base_model, n_threads);
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("%s: failed to apply lora adapter: %s\n", __func__, err.what());
        return 1;
    }
}

static bool llama_control_vector_init(struct llama_control_vector & cvec, const llama_model & model)
{
    GGML_ASSERT(cvec.tensors.empty());
    GGML_ASSERT(cvec.ctxs.empty());
    GGML_ASSERT(cvec.bufs.empty());

    // count layer buffer types
    std::map<ggml_backend_buffer_type_t, int> buft_layer_count;
    for (int64_t i = 0; i < model.hparams.n_layer; i++)
    {
        buft_layer_count[model.buft_layer[i].buft]++;
    }

    // allocate contexts
    std::map<ggml_backend_buffer_type_t, ggml_context *> ctx_map;
    for (auto & it : buft_layer_count)
    {
        int n_layers = it.second;
        ggml_init_params params = {
            .mem_size   =n_layers * ggml_tensor_overhead(),
            .mem_buffer =nullptr,
            .no_alloc   =true,
        };
        ggml_context * ctx = ggml_init(params);
        if (!ctx)
        {
            LLAMA_LOG_ERROR("%s: failed to allocate context for control vector\n", __func__);
            return 1;
        }
        ctx_map[it.first] = ctx;
    }

    // make tensors
    cvec.tensors.reserve(model.hparams.n_layer);
    cvec.tensors.push_back(nullptr); // there's never a tensor for layer 0
    for (size_t il = 1; il < model.hparams.n_layer; il++)
    {
        struct ggml_context * ctx = ctx_map.at(model.buft_layer[il].buft);
        ggml_tensor * tensor = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, model.hparams.n_embd);
        cvec.tensors.push_back(tensor);
    }

    // allocate tensors / buffers and zero
    cvec.ctxs.reserve(ctx_map.size());
    cvec.bufs.reserve(ctx_map.size());
    for (auto it : ctx_map)
    {
        ggml_backend_buffer_type_t buft = it.first;
        ggml_context * ctx = it.second;
        ggml_backend_buffer_t buf = ggml_backend_alloc_ctx_tensors_from_buft(ctx, buft);
        if (!buf)
        {
            LLAMA_LOG_ERROR("%s: failed to allocate buffer for control vector\n", __func__);
            return false;
        }
        ggml_backend_buffer_clear(buf, 0);
        cvec.ctxs.push_back(ctx);
        cvec.bufs.push_back(buf);
    }

    return true;
}

int32_t
llama_control_vector_apply(struct llama_context * lctx, const float * data, size_t len, int32_t n_embd, int32_t il_start, int32_t il_end)
{
    const llama_model & model = lctx->model;
    llama_control_vector & cvec = lctx->cvec;

    if (data == nullptr)
    {
        // disable the current control vector (but leave allocated for later)
        cvec.layer_start = -1;
        cvec.layer_end = -1;
        return 0;
    }

    if (n_embd != (int)model.hparams.n_embd)
    {
        LLAMA_LOG_ERROR("%s: control vector n_embd does not match model\n", __func__);
        return 1;
    }

    if (cvec.tensors.empty())
    {
        if (!llama_control_vector_init(cvec, model))
        {
            return 1;
        }
    }

    cvec.layer_start = il_start;
    cvec.layer_end = il_end;

    for (size_t il = 1; il < model.hparams.n_layer; il++)
    {
        assert(cvec.tensors[il] != nullptr);

        const size_t off = n_embd * (il - 1); // buffer doesn't have data for layer 0, since it's never present
        if (off + n_embd <= len)
        {
            ggml_backend_tensor_set(cvec.tensors[il], data + off, 0, n_embd * ggml_element_size(cvec.tensors[il]));
        }
    }

    return 0;
}

struct llama_kv_cache_view llama_kv_cache_view_init(const struct llama_context * ctx, int32_t n_seq_max)
{
    llama_kv_cache_view result = {
        .n_cells            = 0,
        .n_seq_max          = n_seq_max,
        .token_count        = 0,
        .used_cells         = llama_get_kv_cache_used_cells(ctx),
        .max_contiguous     = 0,
        .max_contiguous_idx = -1,
        .cells              = nullptr,
        .cells_sequences    = nullptr,
    };
    return result;
}

void llama_kv_cache_view_free(struct llama_kv_cache_view * view)
{
    if (view->cells != nullptr)
    {
        free(view->cells);
        view->cells = nullptr;
    }
    if (view->cells_sequences != nullptr)
    {
        free(view->cells_sequences);
        view->cells_sequences = nullptr;
    }
}

void llama_kv_cache_view_update(const struct llama_context * ctx, struct llama_kv_cache_view * view)
{
    if (uint32_t(view->n_cells) < ctx->kv_self.size || view->cells == nullptr)
    {
        view->n_cells = int32_t(ctx->kv_self.size);
        void * p = realloc(view->cells, sizeof(struct llama_kv_cache_view_cell) * view->n_cells);
        GGML_ASSERT(p != nullptr && "Failed to alloc kv_cache_view cells");
        view->cells = (struct llama_kv_cache_view_cell *)p;
        p = realloc(view->cells_sequences, sizeof(llama_seq_id) * view->n_seq_max * view->n_cells);
        GGML_ASSERT(p != nullptr && "Failed to alloc kv_cache_view cells sequences");
        view->cells_sequences = (llama_seq_id *)p;
    }

    const std::vector<llama_kv_cell> & kv_cells = ctx->kv_self.cells;
    llama_kv_cache_view_cell * c_curr = view->cells;
    llama_seq_id * cs_curr = view->cells_sequences;
    int32_t used_cells = 0;
    int32_t token_count = 0;
    int32_t curr_contig_idx = -1;
    uint32_t max_contig = 0;
    int32_t max_contig_idx = -1;

    for (int32_t i = 0; i < int32_t(ctx->kv_self.size); i++, c_curr++, cs_curr += view->n_seq_max)
    {
        const size_t curr_size = kv_cells[i].seq_id.size();
        token_count += curr_size;
        c_curr->pos = kv_cells[i].pos + kv_cells[i].delta;

        if (curr_size > 0)
        {
            if (curr_contig_idx >= 0 && uint32_t(i - curr_contig_idx) > max_contig)
            {
                max_contig = i - curr_contig_idx;
                max_contig_idx = curr_contig_idx;
            }
            curr_contig_idx = -1;
        }
        else if (curr_contig_idx < 0)
        {
            curr_contig_idx = i;
        }

        int seq_idx = 0;
        for (const llama_seq_id it : kv_cells[i].seq_id)
        {
            if (seq_idx >= view->n_seq_max)
            {
                break;
            }
            cs_curr[seq_idx] = it;
            seq_idx++;
        }
        if (seq_idx != 0)
        {
            used_cells++;
        }
        for (; seq_idx < view->n_seq_max; seq_idx++)
        {
            cs_curr[seq_idx] = -1;
        }
    }
    if (curr_contig_idx >= 0 && kv_cells.size() - curr_contig_idx > max_contig)
    {
        max_contig_idx = curr_contig_idx;
        max_contig = kv_cells.size() - curr_contig_idx;
    }
    view->max_contiguous = max_contig;
    view->max_contiguous_idx = max_contig_idx;
    view->token_count = token_count;
    view->used_cells = used_cells;
    if (uint32_t(used_cells) != ctx->kv_self.used)
    {
        LLAMA_LOG_ERROR("%s: used cells mismatch. kv_cache says %d but we calculated %d\n", __func__, ctx->kv_self.used, used_cells);
    }
}

int32_t llama_get_kv_cache_token_count(const struct llama_context * ctx)
{
    int result = 0;

    for (uint32_t i = 0; i < ctx->kv_self.size; i++)
    {
        result += ctx->kv_self.cells[i].seq_id.size();
    }

    return result;
}

int32_t llama_get_kv_cache_used_cells(const struct llama_context * ctx)
{
    return ctx->kv_self.used;
}

void llama_kv_cache_clear(struct llama_context * ctx)
{
    llama_kv_cache_clear(ctx->kv_self);
}

bool llama_kv_cache_seq_rm(struct llama_context * ctx, llama_seq_id seq_id, llama_pos p0, llama_pos p1)
{
    return llama_kv_cache_seq_rm(ctx->kv_self, seq_id, p0, p1);
}

void llama_kv_cache_seq_cp(struct llama_context * ctx, llama_seq_id seq_id_src, llama_seq_id seq_id_dst, llama_pos p0, llama_pos p1)
{
    if (seq_id_src == seq_id_dst)
    {
        return;
    }
    llama_kv_cache_seq_cp(ctx->kv_self, seq_id_src, seq_id_dst, p0, p1);
}

void llama_kv_cache_seq_keep(struct llama_context * ctx, llama_seq_id seq_id)
{
    llama_kv_cache_seq_keep(ctx->kv_self, seq_id);
}

void llama_kv_cache_seq_add(struct llama_context * ctx, llama_seq_id seq_id, llama_pos p0, llama_pos p1, llama_pos delta)
{
    if (delta == 0)
    {
        return;
    }

    llama_kv_cache_seq_add(ctx->kv_self, seq_id, p0, p1, delta);
}

void llama_kv_cache_seq_div(struct llama_context * ctx, llama_seq_id seq_id, llama_pos p0, llama_pos p1, int d)
{
    if (d == 1)
    {
        return;
    }

    llama_kv_cache_seq_div(ctx->kv_self, seq_id, p0, p1, d);
}

llama_pos llama_kv_cache_seq_pos_max(struct llama_context * ctx, llama_seq_id seq_id)
{
    return llama_kv_cache_seq_pos_max(ctx->kv_self, seq_id);
}

void llama_kv_cache_defrag(struct llama_context * ctx)
{
    llama_kv_cache_defrag(ctx->kv_self);
}

void llama_kv_cache_update(struct llama_context * ctx)
{
    llama_kv_cache_update_internal(*ctx);
}

// llama_context_data
struct llama_data_context
{
    virtual void write(const void * src, size_t size) = 0;
    virtual size_t get_size_written() = 0;
    virtual ~llama_data_context() = default;
};

struct llama_data_buffer_context : llama_data_context
{
    uint8_t * ptr;
    size_t size_written = 0;

    llama_data_buffer_context(uint8_t * p) : ptr(p) { }

    void write(const void * src, size_t size) override
    {
        memcpy(ptr, src, size);
        ptr += size;
        size_written += size;
    }

    size_t get_size_written() override { return size_written; }
};

struct llama_data_file_context : llama_data_context
{
    llama_file * file;
    size_t size_written = 0;

    llama_data_file_context(llama_file * f) : file(f) { }

    void write(const void * src, size_t size) override
    {
        file->write_raw(src, size);
        size_written += size;
    }

    size_t get_size_written() override { return size_written; }
};

size_t llama_state_seq_get_size(struct llama_context * ctx, llama_seq_id seq_id)
{
    // save the size of size_t as a uint32_t for safety check
    const size_t size_t_size_size = sizeof(uint32_t);

    // other values
    const size_t s_cell_count_size = sizeof(uint32_t);
    const size_t s_layer_count_size = sizeof(uint32_t);
    const size_t n_embd_v_gqa_size = sizeof(uint32_t);

    size_t s_cell_count = 0;
    size_t s_cell_data_size = 0;
    const auto & kv_self = ctx->kv_self;
    const auto & hparams = ctx->model.hparams;

    const uint32_t n_layer = hparams.n_layer;
    const uint32_t n_embd_k_gqa = hparams.n_embd_k_gqa() + hparams.n_embd_k_s();
    const uint32_t n_embd_v_gqa = hparams.n_embd_v_gqa() + hparams.n_embd_v_s();

    for (uint32_t i = 0; i < kv_self.size; ++i)
    {
        const auto & cell = kv_self.cells[i];
        if (cell.seq_id.count(seq_id) > 0)
        {
            ++s_cell_count;
            s_cell_data_size += sizeof(llama_pos);
        }
    }

    for (int il = 0; il < (int)n_layer; ++il)
    {
        // types of keys and values
        s_cell_data_size += sizeof(int32_t) * 2;
        // k_size_row and v_size_el values of layer
        s_cell_data_size += sizeof(size_t) * 2;

        // keys
        const size_t k_size_row = ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa);
        s_cell_data_size += k_size_row * s_cell_count;

        // values (transposed)
        const size_t v_size_el = ggml_type_size(kv_self.v_l[il]->type);
        s_cell_data_size += v_size_el * s_cell_count * n_embd_v_gqa;
    }

    const size_t s_total = (size_t_size_size + s_cell_count_size + s_layer_count_size + n_embd_v_gqa_size + s_cell_data_size);

    return s_total;
}

static size_t llama_state_seq_get_data_internal(struct llama_context * ctx, llama_data_context & data_ctx, llama_seq_id seq_id)
{
    llama_synchronize(ctx);

    const auto & kv_self = ctx->kv_self;
    GGML_ASSERT(!kv_self.recurrent); // not implemented

    // Save the size of size_t as a uint32_t for safety check
    const uint32_t size_t_size = sizeof(size_t);
    data_ctx.write(&size_t_size, sizeof(size_t_size));

    std::vector<std::pair<uint32_t, uint32_t>> cell_ranges; // ranges, from inclusive, to exclusive
    uint32_t cell_count = 0;

    // Count the number of cells with the specified seq_id
    // Find all the ranges of cells with this seq id
    {
        uint32_t cell_range_begin = kv_self.size;
        for (uint32_t i = 0; i < kv_self.size; ++i)
        {
            const auto & cell = kv_self.cells[i];
            if (cell.has_seq_id(seq_id))
            {
                ++cell_count;
                if (cell_range_begin == kv_self.size)
                {
                    cell_range_begin = i;
                }
            }
            else
            {
                if (cell_range_begin != kv_self.size)
                {
                    cell_ranges.emplace_back(cell_range_begin, i);
                    cell_range_begin = kv_self.size;
                }
            }
        }
        if (cell_range_begin != kv_self.size)
        {
            cell_ranges.emplace_back(cell_range_begin, kv_self.size);
        }

        // DEBUG CHECK: Sum of cell counts in ranges should equal the total cell count
        uint32_t cell_count_check = 0;
        for (const auto & range : cell_ranges)
        {
            cell_count_check += range.second - range.first;
        }
        GGML_ASSERT(cell_count == cell_count_check);
    }

    // Write the cell count
    data_ctx.write(&cell_count, sizeof(cell_count));

    const auto & hparams = ctx->model.hparams;
    const uint32_t n_layer = hparams.n_layer;
    const uint32_t n_embd_k_gqa = hparams.n_embd_k_gqa() + hparams.n_embd_k_s();
    const uint32_t n_embd_v_gqa = hparams.n_embd_v_gqa() + hparams.n_embd_v_s();

    // Write the layer count
    data_ctx.write(&n_layer, sizeof(n_layer));

    // Write n_embd_v_gqa
    data_ctx.write(&n_embd_v_gqa, sizeof(n_embd_v_gqa));

    // Iterate the ranges and write all the pos (this is the token position in the prompt)
    for (const auto & range : cell_ranges)
    {
        for (uint32_t i = range.first; i < range.second; ++i)
        {
            const auto & cell = kv_self.cells[i];
            data_ctx.write(&cell.pos, sizeof(cell.pos));
        }
    }

    // Iterate and write all the keys first, each row is a cell
    // Get whole range at a time
    std::vector<uint8_t> tmp_buf;
    for (int il = 0; il < (int)n_layer; ++il)
    {
        // Write key type
        const int32_t k_type_i = (int32_t)kv_self.k_l[il]->type;
        data_ctx.write(&k_type_i, sizeof(k_type_i));

        // Write row size of key
        const size_t k_size_row = ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa);
        data_ctx.write(&k_size_row, sizeof(k_size_row));

        // Read each range of cells of k_size length each into tmp_buf and write out
        for (const auto & range : cell_ranges)
        {
            const size_t range_size = range.second - range.first;
            tmp_buf.resize(range_size * k_size_row);
            ggml_backend_tensor_get(kv_self.k_l[il], tmp_buf.data(), range.first * k_size_row, range_size * k_size_row);
            data_ctx.write(tmp_buf.data(), tmp_buf.size());
        }
    }

    // TODO: simplify, reduce copy-paste
    if (!kv_self.v_trans)
    {
        for (int il = 0; il < (int)n_layer; ++il)
        {
            // Write value type
            const int32_t v_type_i = (int32_t)kv_self.v_l[il]->type;
            data_ctx.write(&v_type_i, sizeof(v_type_i));

            // Write row size of value
            const size_t v_size_row = ggml_row_size(kv_self.v_l[il]->type, n_embd_v_gqa);
            data_ctx.write(&v_size_row, sizeof(v_size_row));

            // Read each range of cells of v_size length each into tmp_buf and write out
            for (const auto & range : cell_ranges)
            {
                const size_t range_size = range.second - range.first;
                tmp_buf.resize(range_size * v_size_row);
                ggml_backend_tensor_get(kv_self.v_l[il], tmp_buf.data(), range.first * v_size_row, range_size * v_size_row);
                data_ctx.write(tmp_buf.data(), tmp_buf.size());
            }
        }
    }
    else
    {
        // For the values, they are transposed, so we also need the element size and get the element ranges from each row
        const uint32_t kv_size = kv_self.size;
        for (int il = 0; il < (int)n_layer; ++il)
        {
            // Write value type
            const int32_t v_type_i = (int32_t)kv_self.v_l[il]->type;
            data_ctx.write(&v_type_i, sizeof(v_type_i));

            // Write element size
            const size_t v_size_el = ggml_type_size(kv_self.v_l[il]->type);
            data_ctx.write(&v_size_el, sizeof(v_size_el));

            // For each row, we get the element values of each cell
            for (uint32_t j = 0; j < n_embd_v_gqa; ++j)
            {
                // Read each range of cells of v_size_el length each into tmp_buf and write out
                for (const auto & range : cell_ranges)
                {
                    const size_t range_size = range.second - range.first;
                    const size_t src_offset = (range.first + j * kv_size) * v_size_el;
                    tmp_buf.resize(range_size * v_size_el);
                    ggml_backend_tensor_get(kv_self.v_l[il], tmp_buf.data(), src_offset, tmp_buf.size());
                    data_ctx.write(tmp_buf.data(), tmp_buf.size());
                }
            }
        }
    }

    return data_ctx.get_size_written();
}

size_t llama_state_seq_get_data(struct llama_context * ctx, uint8_t * dst, llama_seq_id seq_id)
{
    llama_data_buffer_context data_ctx(dst);
    return llama_state_seq_get_data_internal(ctx, data_ctx, seq_id);
}

size_t llama_state_seq_set_data(struct llama_context * ctx, const uint8_t * src, llama_seq_id dest_seq_id)
{
    llama_synchronize(ctx);

    auto & kv_self = ctx->kv_self;
    GGML_ASSERT(!kv_self.recurrent); // not implemented

    // Wipe the slot
    llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);

    const uint8_t * inp = src;

    // Read size of size_t
    uint32_t size_t_size;
    memcpy(&size_t_size, inp, sizeof(size_t_size));
    inp += sizeof(size_t_size);
    if (size_t_size != sizeof(size_t))
    {
        LLAMA_LOG_ERROR("%s: size_t size mismatch\n", __func__);
        return 0;
    }

    // Read the cell count
    uint32_t cell_count;
    memcpy(&cell_count, inp, sizeof(cell_count));
    inp += sizeof(cell_count);

    // Read the layer count
    uint32_t n_layer_ref;
    memcpy(&n_layer_ref, inp, sizeof(n_layer_ref));
    inp += sizeof(n_layer_ref);

    // Read n_embd_v_gqa
    uint32_t n_embd_v_gqa_ref;
    memcpy(&n_embd_v_gqa_ref, inp, sizeof(n_embd_v_gqa_ref));
    inp += sizeof(n_embd_v_gqa_ref);

    // Sanity check model compatibility
    const auto & hparams = ctx->model.hparams;
    const uint32_t n_layer = hparams.n_layer;
    const uint32_t n_embd_k_gqa = hparams.n_embd_k_gqa() + hparams.n_embd_k_s();
    const uint32_t n_embd_v_gqa = hparams.n_embd_v_gqa() + hparams.n_embd_v_s();
    if (n_layer != n_layer_ref)
    {
        LLAMA_LOG_ERROR("%s: mismatched n_layer (%d != %d)\n", __func__, n_layer, n_layer_ref);
        return 0;
    }
    if (n_embd_v_gqa != n_embd_v_gqa_ref)
    {
        LLAMA_LOG_ERROR("%s: mismatched n_embd_v_gqa (%d != %d)\n", __func__, n_embd_v_gqa, n_embd_v_gqa_ref);
        return 0;
    }

    // Allocate the new cells for the slot
    if (cell_count)
    {
        llama_batch batch = llama_batch_init(cell_count, 0, 1);
        batch.n_tokens = cell_count;
        for (uint32_t i = 0; i < cell_count; ++i)
        {
            llama_pos pos;
            memcpy(&pos, inp, sizeof(pos));
            inp += sizeof(pos);

            batch.pos[i] = pos;
            batch.n_seq_id[i] = 1;
            batch.seq_id[i][0] = dest_seq_id;
        }
        if (!llama_kv_cache_find_slot(kv_self, batch))
        {
            llama_batch_free(batch);
            LLAMA_LOG_ERROR("%s: failed to find available cells in kv cache\n", __func__);
            return 0;
        }

        // DEBUG CHECK: kv_self.head should be our first cell, kv_self.head + cell_count - 1 should be our last cell (verify seq_id and pos values)
        // Assume that this is one contiguous block of cells
        GGML_ASSERT(kv_self.head + cell_count <= kv_self.size);
        GGML_ASSERT(kv_self.cells[kv_self.head].pos == batch.pos[0]);
        GGML_ASSERT(kv_self.cells[kv_self.head + cell_count - 1].pos == batch.pos[cell_count - 1]);
        GGML_ASSERT(kv_self.cells[kv_self.head].has_seq_id(dest_seq_id));
        GGML_ASSERT(kv_self.cells[kv_self.head + cell_count - 1].has_seq_id(dest_seq_id));

        // Cleanup
        llama_batch_free(batch);
    }

    const uint32_t kv_size = kv_self.size;
    const uint32_t kv_head = kv_self.head;

    // For each layer, read the keys for each cell, one row is one cell, read as one contiguous blo
    for (int il = 0; il < (int)n_layer; ++il)
    {
        // Read type of key
        int32_t k_type_i_ref;
        memcpy(&k_type_i_ref, inp, sizeof(k_type_i_ref));
        inp += sizeof(k_type_i_ref);
        const int32_t k_type_i = (int32_t)kv_self.k_l[il]->type;
        if (k_type_i != k_type_i_ref)
        {
            llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);
            LLAMA_LOG_ERROR("%s: mismatched key type (%d != %d, layer %d)\n", __func__, k_type_i, k_type_i_ref, il);
            return 0;
        }

        // Read row size of key
        size_t k_size_row_ref;
        memcpy(&k_size_row_ref, inp, sizeof(k_size_row_ref));
        inp += sizeof(k_size_row_ref);
        const size_t k_size_row = ggml_row_size(kv_self.k_l[il]->type, n_embd_k_gqa);
        if (k_size_row != k_size_row_ref)
        {
            llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);
            LLAMA_LOG_ERROR("%s: mismatched key row size (%zu != %zu, layer %d)\n", __func__, k_size_row, k_size_row_ref, il);
            return 0;
        }

        if (cell_count)
        {
            // Read and set the keys for the whole cell range
            ggml_backend_tensor_set(kv_self.k_l[il], inp, kv_head * k_size_row, cell_count * k_size_row);
            inp += cell_count * k_size_row;
        }
    }

    // TODO: simplify, reduce copy-paste
    if (!kv_self.v_trans)
    {
        for (int il = 0; il < (int)n_layer; ++il)
        {
            // Read type of value
            int32_t v_type_i_ref;
            memcpy(&v_type_i_ref, inp, sizeof(v_type_i_ref));
            inp += sizeof(v_type_i_ref);
            const int32_t v_type_i = (int32_t)kv_self.v_l[il]->type;
            if (v_type_i != v_type_i_ref)
            {
                llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);
                LLAMA_LOG_ERROR("%s: mismatched value type (%d != %d, layer %d)\n", __func__, v_type_i, v_type_i_ref, il);
                return 0;
            }

            // Read row size of value
            size_t v_size_row_ref;
            memcpy(&v_size_row_ref, inp, sizeof(v_size_row_ref));
            inp += sizeof(v_size_row_ref);
            const size_t v_size_row = ggml_row_size(kv_self.v_l[il]->type, n_embd_v_gqa);
            if (v_size_row != v_size_row_ref)
            {
                llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);
                LLAMA_LOG_ERROR("%s: mismatched value row size (%zu != %zu, layer %d)\n", __func__, v_size_row, v_size_row_ref, il);
                return 0;
            }

            if (cell_count)
            {
                // Read and set the values for the whole cell range
                ggml_backend_tensor_set(kv_self.v_l[il], inp, kv_head * v_size_row, cell_count * v_size_row);
                inp += cell_count * v_size_row;
            }
        }
    }
    else
    {
        // For each layer, read the values for each cell (transposed)
        for (int il = 0; il < (int)n_layer; ++il)
        {
            // Read type of value
            int32_t v_type_i_ref;
            memcpy(&v_type_i_ref, inp, sizeof(v_type_i_ref));
            inp += sizeof(v_type_i_ref);
            const int32_t v_type_i = (int32_t)kv_self.v_l[il]->type;
            if (v_type_i != v_type_i_ref)
            {
                llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);
                LLAMA_LOG_ERROR("%s: mismatched value type (%d != %d, layer %d)\n", __func__, v_type_i, v_type_i_ref, il);
                return 0;
            }

            // Read element size of value
            size_t v_size_el_ref;
            memcpy(&v_size_el_ref, inp, sizeof(v_size_el_ref));
            inp += sizeof(v_size_el_ref);
            const size_t v_size_el = ggml_type_size(kv_self.v_l[il]->type);
            if (v_size_el != v_size_el_ref)
            {
                llama_kv_cache_seq_rm(kv_self, dest_seq_id, -1, -1);
                LLAMA_LOG_ERROR("%s: mismatched value element size (%zu != %zu, layer %d)\n", __func__, v_size_el, v_size_el_ref, il);
                return 0;
            }

            if (cell_count)
            {
                // For each row in the transposed matrix, read the values for the whole cell range
                for (uint32_t j = 0; j < n_embd_v_gqa; ++j)
                {
                    const size_t dst_offset = (kv_head + j * kv_size) * v_size_el;
                    ggml_backend_tensor_set(kv_self.v_l[il], inp, dst_offset, cell_count * v_size_el);
                    inp += cell_count * v_size_el;
                }
            }
        }
    }

    const size_t nread = inp - src;

    return nread;
}

static size_t llama_state_seq_save_file_internal(
    struct llama_context * ctx, const char * filepath, llama_seq_id seq_id, const llama_token * tokens, size_t n_token_count)
{
    llama_file file(filepath, "wb");

    file.write_u32(LLAMA_STATE_SEQ_MAGIC);
    file.write_u32(LLAMA_STATE_SEQ_VERSION);

    // save the prompt
    file.write_u32((uint32_t)n_token_count);
    file.write_raw(tokens, sizeof(llama_token) * n_token_count);

    // save the context state using stream saving
    llama_data_file_context data_ctx(&file);
    llama_state_seq_get_data_internal(ctx, data_ctx, seq_id);

    const size_t res = file.tell();
    GGML_ASSERT(res == sizeof(uint32_t) * 3 + sizeof(llama_token) * n_token_count + data_ctx.get_size_written());
    return res;
}

static size_t llama_state_seq_load_file_internal(
    struct llama_context * ctx,
    const char * filepath,
    llama_seq_id dest_seq_id,
    llama_token * tokens_out,
    size_t n_token_capacity,
    size_t * n_token_count_out)
{
    llama_file file(filepath, "rb");

    // version checks
    {
        const uint32_t magic = file.read_u32();
        const uint32_t version = file.read_u32();

        if (magic != LLAMA_STATE_SEQ_MAGIC || version != LLAMA_STATE_SEQ_VERSION)
        {
            LLAMA_LOG_ERROR("%s: unknown (magic, version) for sequence state file: %08x, %08x\n", __func__, magic, version);
            return 0;
        }
    }

    // load the prompt
    {
        const uint32_t n_token_count = file.read_u32();

        if (n_token_count > n_token_capacity)
        {
            LLAMA_LOG_ERROR(
                "%s: token count in sequence state file exceeded capacity! %u > %zu\n", __func__, n_token_count, n_token_capacity);
            return 0;
        }

        file.read_raw(tokens_out, sizeof(llama_token) * n_token_count);
        *n_token_count_out = n_token_count;
    }

    // restore the context state
    {
        const size_t state_size = file.size - file.tell();
        std::vector<uint8_t> state_data(state_size);
        file.read_raw(state_data.data(), state_size);
        const size_t nread = llama_state_seq_set_data(ctx, state_data.data(), dest_seq_id);
        if (!nread)
        {
            LLAMA_LOG_ERROR("%s: failed to restore sequence state\n", __func__);
            return 0;
        }
        GGML_ASSERT(nread <= state_size);
        GGML_ASSERT(nread + sizeof(uint32_t) * 3 + sizeof(llama_token) * *n_token_count_out == file.tell());
    }

    return file.tell();
}

size_t llama_state_seq_save_file(
    struct llama_context * ctx, const char * filepath, llama_seq_id seq_id, const llama_token * tokens, size_t n_token_count)
{
    try
    {
        return llama_state_seq_save_file_internal(ctx, filepath, seq_id, tokens, n_token_count);
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("error saving sequence state file: %s\n", err.what());
        return 0;
    }
}

size_t llama_state_seq_load_file(
    struct llama_context * ctx,
    const char * filepath,
    llama_seq_id dest_seq_id,
    llama_token * tokens_out,
    size_t n_token_capacity,
    size_t * n_token_count_out)
{
    try
    {
        return llama_state_seq_load_file_internal(ctx, filepath, dest_seq_id, tokens_out, n_token_capacity, n_token_count_out);
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("error loading sequence state file: %s\n", err.what());
        return 0;
    }
}

void llama_set_n_threads(struct llama_context * ctx, uint32_t n_threads, uint32_t n_threads_batch)
{
    ctx->cparams.n_threads = n_threads;
    ctx->cparams.n_threads_batch = n_threads_batch;
}

uint32_t llama_n_threads(struct llama_context * ctx)
{
    return ctx->cparams.n_threads;
}

uint32_t llama_n_threads_batch(struct llama_context * ctx)
{
    return ctx->cparams.n_threads_batch;
}

void llama_set_abort_callback(struct llama_context * ctx, bool (*abort_callback)(void * data), void * abort_callback_data)
{
    ctx->abort_callback = abort_callback;
    ctx->abort_callback_data = abort_callback_data;
}

void llama_set_causal_attn(struct llama_context * ctx, bool causal_attn)
{
    ctx->cparams.causal_attn = causal_attn;
}

struct llama_batch llama_batch_get_one(llama_token * tokens, int32_t n_tokens, llama_pos pos_0, llama_seq_id seq_id)
{
    return {
        .n_tokens       =n_tokens,
        .token          =tokens,
        .embd           =nullptr,
        .pos            =nullptr,
        .n_seq_id       =nullptr,
        .seq_id         =nullptr,
        .logits         =nullptr,
        .all_pos_0      =pos_0,
        .all_pos_1      =1,
        .all_seq_id     =seq_id,
    };
}

struct llama_batch llama_batch_init(int32_t n_tokens_alloc, int32_t embd, int32_t n_seq_max)
{
    llama_batch batch = {
        0,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        0,
        0,
        0,
    };

    if (embd)
    {
        batch.embd = (float *)malloc(sizeof(float) * n_tokens_alloc * embd);
    }
    else
    {
        batch.token = (llama_token *)malloc(sizeof(llama_token) * n_tokens_alloc);
    }

    batch.pos = (llama_pos *)malloc(sizeof(llama_pos) * n_tokens_alloc);
    batch.n_seq_id = (int32_t *)malloc(sizeof(int32_t) * n_tokens_alloc);
    batch.seq_id = (llama_seq_id **)malloc(sizeof(llama_seq_id *) * (n_tokens_alloc + 1));
    for (int i = 0; i < n_tokens_alloc; ++i)
    {
        batch.seq_id[i] = (llama_seq_id *)malloc(sizeof(llama_seq_id) * n_seq_max);
    }
    batch.seq_id[n_tokens_alloc] = nullptr;

    batch.logits = (int8_t *)malloc(sizeof(int8_t) * n_tokens_alloc);

    return batch;
}

void llama_batch_free(struct llama_batch batch)
{
    if (batch.token)
        free(batch.token);
    if (batch.embd)
        free(batch.embd);
    if (batch.pos)
        free(batch.pos);
    if (batch.n_seq_id)
        free(batch.n_seq_id);
    if (batch.seq_id)
    {
        for (int i = 0; batch.seq_id[i] != nullptr; ++i)
        {
            free(batch.seq_id[i]);
        }
        free(batch.seq_id);
    }
    if (batch.logits)
        free(batch.logits);
}

int32_t llama_decode(struct llama_context * ctx, struct llama_batch batch)
{
    const int ret = llama_decode_internal(*ctx, batch);
    if (ret < 0)
    {
        LLAMA_LOG_ERROR("%s: failed to decode, ret = %d\n", __func__, ret);
    }

    return ret;
}

void llama_synchronize(struct llama_context * ctx)
{
    ggml_backend_sched_synchronize(ctx->sched);

    // FIXME: if multiple single tokens are evaluated without a synchronization,
    // the stats will be added to the prompt evaluation stats
    // this should only happen when using batch size 1 to evaluate a batch

    // add the evaluation to the stats
    if (ctx->n_queued_tokens == 1)
    {
        ctx->t_eval_us += ggml_time_us() - ctx->t_compute_start_us;
        ctx->n_eval++;
    }
    else if (ctx->n_queued_tokens > 1)
    {
        ctx->t_p_eval_us += ggml_time_us() - ctx->t_compute_start_us;
        ctx->n_p_eval += ctx->n_queued_tokens;
    }

    // get a more accurate load time, upon first eval
    if (ctx->n_queued_tokens > 0 && !ctx->has_evaluated_once)
    {
        ctx->t_load_us = ggml_time_us() - ctx->t_start_us;
        ctx->has_evaluated_once = true;
    }

    ctx->n_queued_tokens = 0;
    ctx->t_compute_start_us = 0;
}

float * llama_get_logits(struct llama_context * ctx)
{
    llama_synchronize(ctx);

    return ctx->logits;
}

float * llama_get_logits_ith(struct llama_context * ctx, int32_t i)
{
    int32_t j = -1;
    llama_synchronize(ctx);

    try
    {
        if (ctx->logits == nullptr)
        {
            throw std::runtime_error("no logits");
        }

        if (i < 0)
        {
            j = ctx->n_outputs + i;
            if (j < 0)
            {
                throw std::runtime_error(format("negative index out of range [0, %d)", ctx->n_outputs));
            }
        }
        else if ((size_t)i >= ctx->output_ids.size())
        {
            throw std::runtime_error(format("out of range [0, %lu)", ctx->output_ids.size()));
        }
        else
        {
            j = ctx->output_ids[i];
        }

        if (j < 0)
        {
            throw std::runtime_error(format("batch.logits[%d] != true", i));
        }
        if (j >= ctx->n_outputs)
        {
            // This should not happen
            throw std::runtime_error(format("corrupt output buffer (j=%d, n_outputs=%d)", j, ctx->n_outputs));
        }

        return ctx->logits + j * ctx->model.hparams.n_vocab;
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("%s: invalid logits id %d, reason: %s\n", __func__, i, err.what());
#ifndef NDEBUG
        GGML_ASSERT(false);
#endif
        return nullptr;
    }
}

float * llama_get_embeddings(struct llama_context * ctx)
{
    llama_synchronize(ctx);

    return ctx->embd;
}

float * llama_get_embeddings_ith(struct llama_context * ctx, int32_t i)
{
    int32_t j = -1;

    llama_synchronize(ctx);

    try
    {
        if (ctx->embd == nullptr)
        {
            throw std::runtime_error("no embeddings");
        }

        if (i < 0)
        {
            j = ctx->n_outputs + i;
            if (j < 0)
            {
                throw std::runtime_error(format("negative index out of range [0, %d)", ctx->n_outputs));
            }
        }
        else if ((size_t)i >= ctx->output_ids.size())
        {
            throw std::runtime_error(format("out of range [0, %lu)", ctx->output_ids.size()));
        }
        else
        {
            j = ctx->output_ids[i];
        }

        if (j < 0)
        {
            throw std::runtime_error(format("batch.logits[%d] != true", i));
        }
        if (j >= ctx->n_outputs)
        {
            // This should not happen
            throw std::runtime_error(format("corrupt output buffer (j=%d, n_outputs=%d)", j, ctx->n_outputs));
        }

        return ctx->embd + j * ctx->model.hparams.n_embd;
    }
    catch (const std::exception & err)
    {
        LLAMA_LOG_ERROR("%s: invalid embeddings id %d, reason: %s\n", __func__, i, err.what());
#ifndef NDEBUG
        GGML_ASSERT(false);
#endif
        return nullptr;
    }
}

float * llama_get_embeddings_seq(struct llama_context * ctx, llama_seq_id seq_id)
{
    llama_synchronize(ctx);

    auto it = ctx->embd_seq.find(seq_id);
    if (it == ctx->embd_seq.end())
    {
        return nullptr;
    }

    return it->second.data();
}

const char * llama_token_get_text(const struct llama_model * model, llama_token token)
{
    GGML_ASSERT(model->vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return model->vocab.id_to_token[token].text.c_str();
}

float llama_token_get_score(const struct llama_model * model, llama_token token)
{
    GGML_ASSERT(model->vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return model->vocab.id_to_token[token].score;
}

llama_token_type llama_token_get_type(const struct llama_model * model, llama_token token)
{
    GGML_ASSERT(model->vocab.type != LLAMA_VOCAB_TYPE_NONE);
    return model->vocab.id_to_token[token].type;
}

bool llama_token_is_eog(const struct llama_model * model, llama_token token)
{
    return token != -1 && (token == llama_token_eos(model) || token == llama_token_eot(model));
}

bool llama_token_is_control(const struct llama_model * model, llama_token token)
{
    return llama_is_control_token(model->vocab, token);
}

llama_token llama_token_bos(const struct llama_model * model)
{
    return model->vocab.special_bos_id;
}

llama_token llama_token_eos(const struct llama_model * model)
{
    return model->vocab.special_eos_id;
}

llama_token llama_token_cls(const struct llama_model * model)
{
    return model->vocab.special_cls_id;
}

llama_token llama_token_sep(const struct llama_model * model)
{
    return model->vocab.special_sep_id;
}

llama_token llama_token_nl(const struct llama_model * model)
{
    return model->vocab.linefeed_id;
}

int32_t llama_add_bos_token(const struct llama_model * model)
{
    return model->vocab.special_add_bos;
}

int32_t llama_add_eos_token(const struct llama_model * model)
{
    return model->vocab.special_add_eos;
}

llama_token llama_token_prefix(const struct llama_model * model)
{
    return model->vocab.special_prefix_id;
}

llama_token llama_token_middle(const struct llama_model * model)
{
    return model->vocab.special_middle_id;
}

llama_token llama_token_suffix(const struct llama_model * model)
{
    return model->vocab.special_suffix_id;
}

llama_token llama_token_eot(const struct llama_model * model)
{
    return model->vocab.special_eot_id;
}

int32_t llama_tokenize(
    const struct llama_model * model,
    const char * text,
    int32_t text_len,
    llama_token * tokens,
    int32_t n_tokens_max,
    bool add_special,
    bool parse_special)
{
    auto res = llama_tokenize_internal(model->vocab, std::string(text, text_len), add_special, parse_special);

    if (n_tokens_max < (int)res.size())
    {
        // LLAMA_LOG_ERROR("%s: too many tokens\n", __func__);
        return -((int)res.size());
    }

    for (size_t i = 0; i < res.size(); i++)
    {
        tokens[i] = res[i];
    }

    return res.size();
}

static std::string llama_decode_text(const std::string & text)
{
    std::string decoded_text;

    const auto cpts = unicode_cpts_from_utf8(text);
    for (const auto cpt : cpts)
    {
        decoded_text += unicode_utf8_to_byte(unicode_cpt_to_utf8(cpt));
    }

    return decoded_text;
}

// does not write null-terminator to buf
int32_t llama_token_to_piece(const struct llama_model * model, llama_token token, char * buf, int32_t length, bool special)
{
    if (0 <= token && token < llama_n_vocab(model))
    {
        switch (llama_vocab_get_type(model->vocab))
        {
            case LLAMA_VOCAB_TYPE_WPM:
            case LLAMA_VOCAB_TYPE_SPM: {
                // NOTE: we accept all unsupported token types,
                // suppressing them like CONTROL tokens.
                if (llama_is_normal_token(model->vocab, token))
                {
                    std::string result = model->vocab.id_to_token[token].text;
                    llama_unescape_whitespace(result);
                    if (length < (int)result.length())
                    {
                        return -(int)result.length();
                    }
                    memcpy(buf, result.c_str(), result.length());
                    return result.length();
                }
                else if ((llama_is_user_defined_token(model->vocab, token)) || (llama_is_control_token(model->vocab, token) && special))
                {
                    std::string result = model->vocab.id_to_token[token].text;
                    if (length < (int)result.length())
                    {
                        return -(int)result.length();
                    }
                    memcpy(buf, result.c_str(), result.length());
                    return result.length();
                }
                else if (llama_is_unknown_token(model->vocab, token))
                { // NOLINT
                    if (length < 3)
                    {
                        return -3;
                    }
                    memcpy(buf, "\xe2\x96\x85", 3);
                    return 3;
                }
                else if (llama_is_byte_token(model->vocab, token))
                {
                    if (length < 1)
                    {
                        return -1;
                    }
                    buf[0] = llama_token_to_byte(model->vocab, token);
                    return 1;
                }
                break;
            }
            case LLAMA_VOCAB_TYPE_BPE: {
                // NOTE: we accept all unsupported token types,
                // suppressing them like CONTROL tokens.
                if (llama_is_normal_token(model->vocab, token))
                {
                    std::string result = model->vocab.id_to_token[token].text;
                    result = llama_decode_text(result);
                    if (length < (int)result.length())
                    {
                        return -(int)result.length();
                    }
                    memcpy(buf, result.c_str(), result.length());
                    return result.length();
                }
                else if ((llama_is_user_defined_token(model->vocab, token)) || (llama_is_control_token(model->vocab, token) && special))
                {
                    std::string result = model->vocab.id_to_token[token].text;
                    if (length < (int)result.length())
                    {
                        return -(int)result.length();
                    }
                    memcpy(buf, result.c_str(), result.length());
                    return result.length();
                }
                break;
            }
            default:
                GGML_ASSERT(false);
        }
    }
    return 0;
}

// trim whitespace from the beginning and end of a string
static std::string trim(const std::string & str)
{
    size_t start = 0;
    size_t end = str.size();
    while (start < end && isspace(str[start]))
    {
        start += 1;
    }
    while (end > start && isspace(str[end - 1]))
    {
        end -= 1;
    }
    return str.substr(start, end - start);
}

LLAMA_API int llama_split_path(char * split_path, size_t maxlen, const char * path_prefix, int split_no, int split_count)
{
    static const char * const SPLIT_PATH_FORMAT = "%s-%05d-of-%05d.gguf";
    if (snprintf(split_path, maxlen, SPLIT_PATH_FORMAT, path_prefix, split_no + 1, split_count))
    {
        return strlen(split_path);
    }
    return 0;
}

int llama_split_prefix(char * split_prefix, size_t maxlen, const char * split_path, int split_no, int split_count)
{
    std::string str_split_path(split_path);
    char postfix[32];
    snprintf(postfix, 32, "-%05d-of-%05d.gguf", split_no + 1, split_count);
    std::string str_postfix(postfix);

    // check if dest ends with postfix
    int size_prefix = str_split_path.size() - str_postfix.size();
    if (size_prefix > 0 && str_split_path.find(str_postfix, size_prefix) != std::string::npos)
    {
        snprintf(split_prefix, std::min((size_t)size_prefix + 1, maxlen), "%s", split_path);
        return size_prefix;
    }

    return 0;
}

struct llama_timings llama_get_timings(struct llama_context * ctx)
{
    llama_timings result = {
        .t_start_ms  =1e-3 * ctx->t_start_us,
        .t_end_ms    =1.00 * ggml_time_ms(),
        .t_load_ms   =1e-3 * ctx->t_load_us,
        .t_sample_ms =1e-3 * ctx->t_sample_us,
        .t_p_eval_ms =1e-3 * ctx->t_p_eval_us,
        .t_eval_ms   =1e-3 * ctx->t_eval_us,

        .n_sample =std::max(1, ctx->n_sample),
        .n_p_eval =std::max(0, ctx->n_p_eval),
        .n_eval   =std::max(1, ctx->n_eval),
    };

    return result;
}

void llama_print_timings(struct llama_context * ctx)
{
    const llama_timings timings = llama_get_timings(ctx);

    LLAMA_LOG_INFO("\n");
    LLAMA_LOG_INFO("%s:        load time = %10.2f ms\n", __func__, timings.t_load_ms);
    LLAMA_LOG_INFO(
        "%s:      sample time = %10.2f ms / %5d runs   (%8.2f ms per token, %8.2f tokens per second)\n",
        __func__,
        timings.t_sample_ms,
        timings.n_sample,
        timings.t_sample_ms / timings.n_sample,
        1e3 / timings.t_sample_ms * timings.n_sample);
    LLAMA_LOG_INFO(
        "%s: prompt eval time = %10.2f ms / %5d tokens (%8.2f ms per token, %8.2f tokens per second)\n",
        __func__,
        timings.t_p_eval_ms,
        timings.n_p_eval,
        timings.t_p_eval_ms / timings.n_p_eval,
        1e3 / timings.t_p_eval_ms * timings.n_p_eval);
    LLAMA_LOG_INFO(
        "%s:        eval time = %10.2f ms / %5d runs   (%8.2f ms per token, %8.2f tokens per second)\n",
        __func__,
        timings.t_eval_ms,
        timings.n_eval,
        timings.t_eval_ms / timings.n_eval,
        1e3 / timings.t_eval_ms * timings.n_eval);
    LLAMA_LOG_INFO(
        "%s:       total time = %10.2f ms / %5d tokens\n",
        __func__,
        (timings.t_end_ms - timings.t_start_ms),
        (timings.n_p_eval + timings.n_eval));
}

void llama_reset_timings(struct llama_context * ctx)
{
    ctx->t_start_us = ggml_time_us();
    ctx->t_sample_us = ctx->n_sample = 0;
    ctx->t_eval_us = ctx->n_eval = 0;
    ctx->t_p_eval_us = ctx->n_p_eval = 0;
}

const char * llama_print_system_info(void)
{
    static std::string s;

    s = "";
    s += "AVX = " + std::to_string(ggml_cpu_has_avx()) + " | ";
    s += "AVX_VNNI = " + std::to_string(ggml_cpu_has_avx_vnni()) + " | ";
    s += "AVX2 = " + std::to_string(ggml_cpu_has_avx2()) + " | ";
    s += "AVX512 = " + std::to_string(ggml_cpu_has_avx512()) + " | ";
    s += "AVX512_VBMI = " + std::to_string(ggml_cpu_has_avx512_vbmi()) + " | ";
    s += "AVX512_VNNI = " + std::to_string(ggml_cpu_has_avx512_vnni()) + " | ";
    s += "AVX512_BF16 = " + std::to_string(ggml_cpu_has_avx512_bf16()) + " | ";
    s += "FMA = " + std::to_string(ggml_cpu_has_fma()) + " | ";
    s += "NEON = " + std::to_string(ggml_cpu_has_neon()) + " | ";
    s += "SVE = " + std::to_string(ggml_cpu_has_sve()) + " | ";
    s += "ARM_FMA = " + std::to_string(ggml_cpu_has_arm_fma()) + " | ";
    s += "F16C = " + std::to_string(ggml_cpu_has_f16c()) + " | ";
    s += "FP16_VA = " + std::to_string(ggml_cpu_has_fp16_va()) + " | ";
    s += "WASM_SIMD = " + std::to_string(ggml_cpu_has_wasm_simd()) + " | ";
    s += "BLAS = " + std::to_string(ggml_cpu_has_blas()) + " | ";
    s += "SSE3 = " + std::to_string(ggml_cpu_has_sse3()) + " | ";
    s += "SSSE3 = " + std::to_string(ggml_cpu_has_ssse3()) + " | ";
    s += "VSX = " + std::to_string(ggml_cpu_has_vsx()) + " | ";
    s += "MATMUL_INT8 = " + std::to_string(ggml_cpu_has_matmul_int8()) + " | ";
#ifdef GGML_USE_LLAMAFILE
    s += "LLAMAFILE = 1 | ";
#else
    s += "LLAMAFILE = 0 | ";
#endif

    return s.c_str();
}

void llama_dump_timing_info_yaml(FILE * stream, const llama_context * ctx)
{
    fprintf(stream, "\n");
    fprintf(stream, "###########\n");
    fprintf(stream, "# Timings #\n");
    fprintf(stream, "###########\n");
    fprintf(stream, "\n");

    fprintf(stream, "mst_eval: %.2f  # ms / token during generation\n", 1.0e-3 * ctx->t_eval_us / ctx->n_eval);
    fprintf(stream, "mst_p_eval: %.2f  # ms / token during prompt processing\n", 1.0e-3 * ctx->t_p_eval_us / ctx->n_p_eval);
    fprintf(stream, "mst_sample: %.2f  # ms / token during sampling\n", 1.0e-3 * ctx->t_sample_us / ctx->n_sample);
    fprintf(stream, "n_eval: %d  # number of tokens generated (excluding the first one)\n", ctx->n_eval);
    fprintf(stream, "n_p_eval: %d  # number of tokens processed in batches at the beginning\n", ctx->n_p_eval);
    fprintf(stream, "n_sample: %d  # number of sampled tokens\n", ctx->n_sample);
    fprintf(stream, "t_eval_us: %" PRId64 "  # total microseconds spent generating tokens\n", ctx->t_eval_us);
    fprintf(stream, "t_load_us: %" PRId64 "  # total microseconds spent loading the model\n", ctx->t_load_us);
    fprintf(stream, "t_p_eval_us: %" PRId64 "  # total microseconds spent prompt processing\n", ctx->t_p_eval_us);
    fprintf(stream, "t_sample_us: %" PRId64 "  # total microseconds spent sampling\n", ctx->t_sample_us);
    fprintf(stream, "ts_eval: %.2f  # tokens / second during generation\n", 1.0e6 * ctx->n_eval / ctx->t_eval_us);
    fprintf(stream, "ts_p_eval: %.2f  # tokens / second during prompt processing\n", 1.0e6 * ctx->n_p_eval / ctx->t_p_eval_us);
    fprintf(stream, "ts_sample: %.2f  # tokens / second during sampling\n", 1.0e6 * ctx->n_sample / ctx->t_sample_us);
}

// For internal test use
const std::vector<std::pair<std::string, struct ggml_tensor *>> & llama_internal_get_tensor_map(struct llama_context * ctx)
{
    return ctx->model.tensors_by_name;
}

void llama_log_set(ggml_log_callback log_callback, void * user_data)
{
    g_state.log_callback = log_callback ? log_callback : llama_log_callback_default;
    g_state.log_callback_user_data = user_data;
#ifdef GGML_USE_METAL
    ggml_backend_metal_log_set_callback(g_state.log_callback, g_state.log_callback_user_data);
#elif defined(GGML_USE_CUDA)
    ggml_backend_cuda_log_set_callback(g_state.log_callback, g_state.log_callback_user_data);
#endif
}

static void llama_log_internal_v(ggml_log_level level, const char * format, va_list args)
{
    va_list args_copy;
    va_copy(args_copy, args);
    char buffer[128];
    int len = vsnprintf(buffer, 128, format, args);
    if (len < 128)
    {
        g_state.log_callback(level, buffer, g_state.log_callback_user_data);
    }
    else
    {
        char * buffer2 = new char[len + 1];
        vsnprintf(buffer2, len + 1, format, args_copy);
        buffer2[len] = 0;
        g_state.log_callback(level, buffer2, g_state.log_callback_user_data);
        delete[] buffer2;
    }
    va_end(args_copy);
}

static void llama_log_internal(ggml_log_level level, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    llama_log_internal_v(level, format, args);
    va_end(args);
}

static void llama_log_callback_default(ggml_log_level level, const char * text, void * user_data)
{
    (void)level;
    (void)user_data;
    fputs(text, stderr);
    fflush(stderr);
}

#pragma clang diagnostic pop
// NOLINTEND
