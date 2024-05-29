// NOLINTBEGIN

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-macros"

#include "common.h"
// Change JSON_ASSERT from assert() to GGML_ASSERT:
#define JSON_ASSERT GGML_ASSERT
// #include "json.hpp"
// #include "json-schema-to-grammar.h"
#include "llama.h"

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <codecvt>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if defined(__APPLE__) && defined(__MACH__)
#    include <sys/sysctl.h>
#    include <sys/types.h>
#endif

#if defined(_WIN32)
#    define WIN32_LEAN_AND_MEAN
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    include <locale>
#    include <fcntl.h>
#    include <io.h>
#    include <windows.h>
#else
#    include <unistd.h>
#    include <sys/ioctl.h>
#    include <sys/stat.h>
#endif

#if defined(_MSC_VER)
#    pragma warning(disable : 4244 4267) // possible loss of data
#endif

#if (defined(GGML_USE_CUDA) || defined(GGML_USE_SYCL))
#    define GGML_USE_CUDA_SYCL
#endif

#if (defined(GGML_USE_CUDA) || defined(GGML_USE_SYCL)) || defined(GGML_USE_VULKAN)
#    define GGML_USE_CUDA_SYCL_VULKAN
#endif

//
// CPU utils
//

int32_t cpu_get_num_physical_cores()
{
#ifdef __linux__
    // enumerate the set of thread siblings, num entries is num cores
    std::unordered_set<std::string> siblings;
    for (uint32_t cpu = 0; cpu < UINT32_MAX; ++cpu)
    {
        std::ifstream thread_siblings("/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/thread_siblings");
        if (!thread_siblings.is_open())
        {
            break; // no more cpus
        }
        std::string line;
        if (std::getline(thread_siblings, line))
        {
            siblings.insert(line);
        }
    }
    if (!siblings.empty())
    {
        return static_cast<int32_t>(siblings.size());
    }
#elif defined(__APPLE__) && defined(__MACH__)
    int32_t num_physical_cores;
    size_t len = sizeof(num_physical_cores);
    int result = sysctlbyname("hw.perflevel0.physicalcpu", &num_physical_cores, &len, nullptr, 0);
    if (result == 0)
    {
        return num_physical_cores;
    }
    result = sysctlbyname("hw.physicalcpu", &num_physical_cores, &len, nullptr, 0);
    if (result == 0)
    {
        return num_physical_cores;
    }
#elif defined(_WIN32)
    //TODO: Implement
#endif
    unsigned int n_threads = std::thread::hardware_concurrency();
    return n_threads > 0 ? (n_threads <= 4 ? n_threads : n_threads / 2) : 4;
}

#if defined(__x86_64__) && defined(__linux__) && !defined(__ANDROID__)
#    include <pthread.h>

static void cpuid(unsigned leaf, unsigned subleaf, unsigned * eax, unsigned * ebx, unsigned * ecx, unsigned * edx)
{
    __asm__("movq\t%%rbx,%%rsi\n\t"
            "cpuid\n\t"
            "xchgq\t%%rbx,%%rsi"
            : "=a"(*eax), "=S"(*ebx), "=c"(*ecx), "=d"(*edx)
            : "0"(leaf), "2"(subleaf));
}

static int pin_cpu(int cpu)
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    return pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
}

static bool is_hybrid_cpu(void)
{
    unsigned eax, ebx, ecx, edx;
    cpuid(7, 0, &eax, &ebx, &ecx, &edx);
    return !!(edx & (1u << 15));
}

static bool is_running_on_efficiency_core(void)
{
    unsigned eax, ebx, ecx, edx;
    cpuid(0x1a, 0, &eax, &ebx, &ecx, &edx);
    int intel_atom = 0x20;
    int core_type = (eax & 0xff000000u) >> 24;
    return core_type == intel_atom;
}

static int cpu_count_math_cpus(int n_cpu)
{
    int result = 0;
    for (int cpu = 0; cpu < n_cpu; ++cpu)
    {
        if (pin_cpu(cpu))
        {
            return -1;
        }
        if (is_running_on_efficiency_core())
        {
            continue; // efficiency cores harm lockstep threading
        }
        ++cpu; // hyperthreading isn't useful for linear algebra
        ++result;
    }
    return result;
}

#endif // __x86_64__ && __linux__

/**
 * Returns number of CPUs on system that are useful for math.
 */
int32_t cpu_get_num_math()
{
#if defined(__x86_64__) && defined(__linux__) && !defined(__ANDROID__)
    int n_cpu = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
    if (n_cpu < 1)
    {
        return cpu_get_num_physical_cores();
    }
    if (is_hybrid_cpu())
    {
        cpu_set_t affinity;
        if (!pthread_getaffinity_np(pthread_self(), sizeof(affinity), &affinity))
        {
            int result = cpu_count_math_cpus(n_cpu);
            pthread_setaffinity_np(pthread_self(), sizeof(affinity), &affinity);
            if (result > 0)
            {
                return result;
            }
        }
    }
#endif
    return cpu_get_num_physical_cores();
}

//
// Model utils
//

std::tuple<struct llama_model *, struct llama_context *> llama_init_from_gpt_params(gpt_params & params)
{
    auto mparams = llama_model_params_from_gpt_params(params);

    llama_model * model = llama_load_model_from_file(params.model.c_str(), mparams);

    if (model == nullptr)
    {
        // fprintf(stderr, "%s: error: failed to load model '%s'\n", __func__, params.model.c_str());
        return std::make_tuple(nullptr, nullptr);
    }

    auto cparams = llama_context_params_from_gpt_params(params);

    llama_context * lctx = llama_new_context_with_model(model, cparams);
    if (lctx == nullptr)
    {
        // fprintf(stderr, "%s: error: failed to create context with model '%s'\n", __func__, params.model.c_str());
        llama_free_model(model);
        return std::make_tuple(nullptr, nullptr);
    }

    if (!params.control_vectors.empty())
    {
        if (params.control_vector_layer_start <= 0)
            params.control_vector_layer_start = 1;
        if (params.control_vector_layer_end <= 0)
            params.control_vector_layer_end = llama_n_layer(model);

        const auto cvec = llama_control_vector_load(params.control_vectors);
        if (cvec.n_embd == -1)
        {
            llama_free(lctx);
            llama_free_model(model);
            return std::make_tuple(nullptr, nullptr);
        }

        int err = llama_control_vector_apply(
            lctx, cvec.data.data(), cvec.data.size(), cvec.n_embd, params.control_vector_layer_start, params.control_vector_layer_end);
        if (err)
        {
            llama_free(lctx);
            llama_free_model(model);
            return std::make_tuple(nullptr, nullptr);
        }
    }

    for (unsigned int i = 0; i < params.lora_adapter.size(); ++i)
    {
        const std::string & lora_adapter = std::get<0>(params.lora_adapter[i]);
        float lora_scale = std::get<1>(params.lora_adapter[i]);
        int err = llama_model_apply_lora_from_file(
            model,
            lora_adapter.c_str(),
            lora_scale,
            ((i > 0) || params.lora_base.empty()) ? nullptr : params.lora_base.c_str(),
            params.n_threads);
        if (err != 0)
        {
            fprintf(stderr, "%s: error: failed to apply lora adapter\n", __func__);
            llama_free(lctx);
            llama_free_model(model);
            return std::make_tuple(nullptr, nullptr);
        }
    }

    if (params.ignore_eos)
    {
        params.sparams.logit_bias[llama_token_eos(model)] = -INFINITY;
    }

    if (params.warmup)
    {
        // LOG("warming up the model with an empty run\n");

        std::vector<llama_token> tmp = {
            llama_token_bos(model),
            llama_token_eos(model),
        };
        llama_decode(lctx, llama_batch_get_one(tmp.data(), std::min<int32_t>(static_cast<int32_t>(tmp.size()), params.n_batch), 0, 0));
        llama_kv_cache_clear(lctx);
        llama_synchronize(lctx);
        llama_reset_timings(lctx);
    }

    return std::make_tuple(model, lctx);
}

struct llama_model_params llama_model_params_from_gpt_params(const gpt_params & params)
{
    auto mparams = llama_model_default_params();

    if (params.n_gpu_layers != -1)
    {
        mparams.n_gpu_layers = params.n_gpu_layers;
    }
    mparams.rpc_servers = params.rpc_servers.c_str();
    mparams.main_gpu = params.main_gpu;
    mparams.split_mode = params.split_mode;
    mparams.tensor_split = params.tensor_split;
    mparams.use_mmap = params.use_mmap;
    mparams.use_mlock = params.use_mlock;
    mparams.check_tensors = params.check_tensors;
    if (params.kv_overrides.empty())
    {
        mparams.kv_overrides = nullptr;
    }
    else
    {
        // GGML_ASSERT(params.kv_overrides.back().key[0] == 0 && "KV overrides not terminated with empty key");
        mparams.kv_overrides = params.kv_overrides.data();
    }

    return mparams;
}

static ggml_type kv_cache_type_from_str(const std::string & s)
{
    if (s == "f32")
    {
        return GGML_TYPE_F32;
    }
    if (s == "f16")
    {
        return GGML_TYPE_F16;
    }
    if (s == "q8_0")
    {
        return GGML_TYPE_Q8_0;
    }
    if (s == "q4_0")
    {
        return GGML_TYPE_Q4_0;
    }
    if (s == "q4_1")
    {
        return GGML_TYPE_Q4_1;
    }
    if (s == "iq4_nl")
    {
        return GGML_TYPE_IQ4_NL;
    }
    if (s == "q5_0")
    {
        return GGML_TYPE_Q5_0;
    }
    if (s == "q5_1")
    {
        return GGML_TYPE_Q5_1;
    }

    throw std::runtime_error("Invalid cache type: " + s);
}

struct llama_context_params llama_context_params_from_gpt_params(const gpt_params & params)
{
    auto cparams = llama_context_default_params();

    cparams.n_ctx = params.n_ctx;
    cparams.n_seq_max = params.n_parallel;
    cparams.n_batch = params.n_batch;
    cparams.n_ubatch = params.n_ubatch;
    cparams.n_threads = params.n_threads;
    cparams.n_threads_batch = params.n_threads_batch == -1 ? params.n_threads : params.n_threads_batch;
    cparams.seed = params.seed;
    cparams.logits_all = params.logits_all;
    cparams.embeddings = params.embedding;
    cparams.rope_scaling_type = params.rope_scaling_type;
    cparams.rope_freq_base = params.rope_freq_base;
    cparams.rope_freq_scale = params.rope_freq_scale;
    cparams.yarn_ext_factor = params.yarn_ext_factor;
    cparams.yarn_attn_factor = params.yarn_attn_factor;
    cparams.yarn_beta_fast = params.yarn_beta_fast;
    cparams.yarn_beta_slow = params.yarn_beta_slow;
    cparams.yarn_orig_ctx = params.yarn_orig_ctx;
    cparams.pooling_type = params.pooling_type;
    cparams.defrag_thold = params.defrag_thold;
    cparams.cb_eval = params.cb_eval;
    cparams.cb_eval_user_data = params.cb_eval_user_data;
    cparams.offload_kqv = !params.no_kv_offload;
    cparams.flash_attn = params.flash_attn;

    cparams.type_k = kv_cache_type_from_str(params.cache_type_k);
    cparams.type_v = kv_cache_type_from_str(params.cache_type_v);

    return cparams;
}

//
// Batch utils
//

void llama_batch_clear(struct llama_batch & batch)
{
    batch.n_tokens = 0;
}

void llama_batch_add(struct llama_batch & batch, llama_token id, llama_pos pos, const std::vector<llama_seq_id> & seq_ids, bool logits)
{
    batch.token[batch.n_tokens] = id;
    batch.pos[batch.n_tokens] = pos;
    batch.n_seq_id[batch.n_tokens] = static_cast<int>(seq_ids.size());
    for (size_t i = 0; i < seq_ids.size(); ++i)
    {
        batch.seq_id[batch.n_tokens][i] = seq_ids[i];
    }
    batch.logits[batch.n_tokens] = logits;

    batch.n_tokens++;
}

//
// Vocab utils
//

std::vector<llama_token> llama_tokenize(const struct llama_context * ctx, const std::string & text, bool add_special, bool parse_special)
{
    return llama_tokenize(llama_get_model(ctx), text, add_special, parse_special);
}

std::vector<llama_token> llama_tokenize(const struct llama_model * model, const std::string & text, bool add_special, bool parse_special)
{
    // upper limit for the number of tokens
    int n_tokens = static_cast<int>(text.length()) + 2 * add_special;
    std::vector<llama_token> result(n_tokens);
    n_tokens = llama_tokenize(
        model, text.data(), static_cast<int>(text.length()), result.data(), static_cast<int>(result.size()), add_special, parse_special);
    if (n_tokens < 0)
    {
        result.resize(-n_tokens);
        int check = llama_tokenize(
            model,
            text.data(),
            static_cast<int>(text.length()),
            result.data(),
            static_cast<int>(result.size()),
            add_special,
            parse_special);
        GGML_ASSERT(check == -n_tokens);
    }
    else
    {
        result.resize(n_tokens);
    }
    return result;
}

std::string llama_token_to_piece(const struct llama_context * ctx, llama_token token, bool special)
{
    std::vector<char> result(8, 0);
    const int n_tokens = llama_token_to_piece(llama_get_model(ctx), token, result.data(), static_cast<int>(result.size()), special);
    if (n_tokens < 0)
    {
        result.resize(-n_tokens);
        int check = llama_token_to_piece(llama_get_model(ctx), token, result.data(), static_cast<int>(result.size()), special);
        GGML_ASSERT(check == -n_tokens);
    }
    else
    {
        result.resize(n_tokens);
    }

    return std::string(result.data(), result.size());
}

std::string llama_detokenize_spm(llama_context * ctx, const std::vector<llama_token> & tokens)
{
    const llama_token bos_id = llama_token_bos(llama_get_model(ctx));

    std::string piece;
    std::string result;

    for (size_t i = 0; i < tokens.size(); ++i)
    {
        piece = llama_token_to_piece(ctx, tokens[i]);

        // remove the leading space of the first non-BOS token
        if (((tokens[0] == bos_id && i == 1) || (tokens[0] != bos_id && i == 0)) && piece[0] == ' ')
        {
            piece = piece.substr(1);
        }

        result += piece;
    }

    return result;
}

std::string llama_detokenize_bpe(llama_context * ctx, const std::vector<llama_token> & tokens)
{
    std::string piece;
    std::string result;

    for (size_t i = 0; i < tokens.size(); ++i)
    {
        piece = llama_token_to_piece(ctx, tokens[i]);

        result += piece;
    }

    // NOTE: the original tokenizer decodes bytes after collecting the pieces.
    return result;
}

bool llama_should_add_bos_token(const llama_model * model)
{
    const int add_bos = llama_add_bos_token(model);

    return add_bos != -1 ? bool(add_bos) : (llama_vocab_type(model) == LLAMA_VOCAB_TYPE_SPM);
}

//
// KV cache utils
//

void llama_kv_cache_dump_view(const llama_kv_cache_view & view, int row_size)
{
    static const char slot_chars[] = ".123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+";

    printf(
        "=== Dumping KV cache. total cells %d, max sequences per cell %d, populated cells %d, total tokens in cache %d, largest empty "
        "slot=%d @ %d",
        view.n_cells,
        view.n_seq_max,
        view.used_cells,
        view.token_count,
        view.max_contiguous,
        view.max_contiguous_idx);

    llama_kv_cache_view_cell * c_curr = view.cells;
    llama_seq_id * cs_curr = view.cells_sequences;

    for (int i = 0; i < view.n_cells; i++, c_curr++, cs_curr += view.n_seq_max)
    {
        if (i % row_size == 0)
        {
            printf("\n%5d: ", i);
        }
        int seq_count = 0;
        for (int j = 0; j < view.n_seq_max; j++)
        {
            if (cs_curr[j] >= 0)
            {
                seq_count++;
            }
        }
        putchar(slot_chars[std::min(sizeof(slot_chars) - 2, size_t(seq_count))]);
    }

    printf("\n=== Done dumping\n");
}

void llama_kv_cache_dump_view_seqs(const llama_kv_cache_view & view, int row_size)
{
    static const char slot_chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    printf(
        "=== Dumping KV cache. total cells %d, max sequences per cell %d, populated cells %d, total tokens in cache %d, largest empty "
        "slot=%d @ %d\n",
        view.n_cells,
        view.n_seq_max,
        view.used_cells,
        view.token_count,
        view.max_contiguous,
        view.max_contiguous_idx);

    std::unordered_map<llama_seq_id, size_t> seqs;
    llama_kv_cache_view_cell * c_curr = view.cells;
    llama_seq_id * cs_curr = view.cells_sequences;

    for (int i = 0; i < view.n_cells; i++, c_curr++, cs_curr += view.n_seq_max)
    {
        for (int j = 0; j < view.n_seq_max; j++)
        {
            if (cs_curr[j] < 0)
            {
                continue;
            }
            if (seqs.find(cs_curr[j]) == seqs.end())
            {
                if (seqs.size() + 1 >= sizeof(slot_chars))
                {
                    break;
                }
                const size_t sz = seqs.size();
                seqs[cs_curr[j]] = sz;
            }
        }
        if (seqs.size() + 1 >= sizeof(slot_chars))
        {
            break;
        }
    }

    printf("=== Sequence legend: ");
    for (const auto & it : seqs)
    {
        printf("%zu=%d, ", it.second, it.first);
    }
    printf("'+'=other sequence ids");

    c_curr = view.cells;
    cs_curr = view.cells_sequences;
    for (int i = 0; i < view.n_cells; i++, c_curr++, cs_curr += view.n_seq_max)
    {
        if (i % row_size == 0)
        {
            printf("\n%5d: ", i);
        }
        for (int j = 0; j < view.n_seq_max; j++)
        {
            if (cs_curr[j] >= 0)
            {
                const auto & it = seqs.find(cs_curr[j]);
                putchar(it != seqs.end() ? int(slot_chars[it->second]) : '+');
            }
            else
            {
                putchar('.');
            }
        }
        putchar(' ');
    }

    printf("\n=== Done dumping\n");
}

//
// Embedding utils
//

void llama_embd_normalize(const float * inp, float * out, int n)
{
    double sum = 0.0;
    for (int i = 0; i < n; i++)
    {
        sum += inp[i] * inp[i];
    }
    sum = sqrt(sum);

    const float norm = static_cast<float>(sum > 0.0 ? 1.0f / sum : 0.0f);

    for (int i = 0; i < n; i++)
    {
        out[i] = inp[i] * norm;
    }
}

float llama_embd_similarity_cos(const float * embd1, const float * embd2, int n)
{
    double sum = 0.0;
    double sum1 = 0.0;
    double sum2 = 0.0;

    for (int i = 0; i < n; i++)
    {
        sum += embd1[i] * embd2[i];
        sum1 += embd1[i] * embd1[i];
        sum2 += embd2[i] * embd2[i];
    }

    return static_cast<float>(sum / (sqrt(sum1) * sqrt(sum2)));
}

//
// Control vector utils
//

static llama_control_vector_data llama_control_vector_load_one(const llama_control_vector_load_info & load_info)
{
    int32_t n_tensors;

    size_t n_bytes = 0;

    uint32_t max_direction_layer = 0;

    llama_control_vector_data result = {-1, {}};

    // calculate size of ctx needed for tensors, ensure tensors are f32, and find max layer
    {
        struct ggml_init_params meta_params = {
            /* .mem_size   = */ ggml_tensor_overhead() * 128 + ggml_graph_overhead(),
            /* .mem_buffer = */ nullptr,
            /* .no_alloc   = */ true,
        };
        ggml_context * meta_ctx = ggml_init(meta_params);
        struct gguf_init_params meta_gguf_params = {
            /* .no_alloc = */ true,
            /* .ctx      = */ &meta_ctx,
        };
        struct gguf_context * meta_ctx_gguf = gguf_init_from_file(load_info.fname.c_str(), meta_gguf_params);
        if (!meta_ctx_gguf)
        {
            fprintf(stderr, "%s: failed to load control vector from %s\n", __func__, load_info.fname.c_str());
            ggml_free(meta_ctx);
            return result;
        }

        n_tensors = gguf_get_n_tensors(meta_ctx_gguf);
        for (int i = 0; i < n_tensors; i++)
        {
            std::string name = gguf_get_tensor_name(meta_ctx_gguf, i);

            // split on '.'
            size_t dotpos = name.find('.');
            if (dotpos != std::string::npos && name.substr(0, dotpos) == "direction")
            {
                try
                {
                    uint32_t layer = std::stoi(name.substr(dotpos + 1));
                    if (layer == 0)
                    {
                        fprintf(stderr, "%s: direction tensor invalid in %s\n", __func__, load_info.fname.c_str());
                        ggml_free(meta_ctx);
                        gguf_free(meta_ctx_gguf);
                        return result;
                    }
                    if (layer > max_direction_layer)
                    {
                        max_direction_layer = layer;
                    }
                }
                catch (...)
                {
                    fprintf(stderr, "%s: direction tensor invalid in %s\n", __func__, load_info.fname.c_str());
                    ggml_free(meta_ctx);
                    gguf_free(meta_ctx_gguf);
                    return result;
                }
            }

            struct ggml_tensor * tensor_meta = ggml_get_tensor(meta_ctx, name.c_str());
            if (tensor_meta->type != GGML_TYPE_F32 || ggml_n_dims(tensor_meta) != 1)
            {
                fprintf(stderr, "%s: direction tensor invalid in %s\n", __func__, load_info.fname.c_str());
                ggml_free(meta_ctx);
                gguf_free(meta_ctx_gguf);
                return result;
            }
            if (result.n_embd == -1)
            {
                result.n_embd = static_cast<int>(ggml_nelements(tensor_meta));
            }
            else if (ggml_nelements(tensor_meta) != result.n_embd)
            {
                fprintf(stderr, "%s: direction tensor sizes mismatched in %s\n", __func__, load_info.fname.c_str());
                ggml_free(meta_ctx);
                gguf_free(meta_ctx_gguf);
                return result;
            }
            n_bytes += ggml_nbytes(tensor_meta);
        }
        ggml_free(meta_ctx);
        gguf_free(meta_ctx_gguf);
    }

    if (n_tensors == 0)
    {
        fprintf(stderr, "%s: no direction tensors found in %s\n", __func__, load_info.fname.c_str());
        return result;
    }

    // load and scale tensors into final control vector context
    struct ggml_init_params ggml_params = {
        /* .mem_size   = */ ggml_tensor_overhead() * n_tensors + n_bytes,
        /* .mem_buffer = */ nullptr,
        /* .no_alloc   = */ false,
    };
    struct ggml_context * ctx = ggml_init(ggml_params);

    struct gguf_init_params params = {
        /*.no_alloc = */ false,
        /*.ctx      = */ &ctx,
    };
    struct gguf_context * ctx_gguf = gguf_init_from_file(load_info.fname.c_str(), params);
    if (!ctx_gguf)
    {
        fprintf(stderr, "%s: failed to load control vector from %s\n", __func__, load_info.fname.c_str());
        ggml_free(ctx);
        return result;
    }

    // do not store data for layer 0 (it's not used)
    result.data.resize(result.n_embd * max_direction_layer);

    for (uint32_t il = 1; il <= max_direction_layer; il++)
    {
        const std::string name = "direction." + std::to_string(il);
        const ggml_tensor * tensor = ggml_get_tensor(ctx, name.c_str());

        float * dst = result.data.data() + result.n_embd * (il - 1);

        if (tensor)
        {
            const float * src = static_cast<const float *>(tensor->data);
            for (int j = 0; j < result.n_embd; j++)
            {
                dst[j] = src[j] * load_info.strength;
            }
        }
        else
        {
            for (int j = 0; j < result.n_embd; j++)
            {
                dst[j] = 0.0f;
            }
        }
    }

    return result;
}

llama_control_vector_data llama_control_vector_load(const std::vector<llama_control_vector_load_info> & load_infos)
{
    llama_control_vector_data result = {-1, {}};

    for (const auto & info : load_infos)
    {
        auto cur = llama_control_vector_load_one(info);

        if (cur.n_embd == -1)
        {
            return result;
        }
        if (result.n_embd != -1 && (result.n_embd != cur.n_embd || result.data.size() != cur.data.size()))
        {
            fprintf(stderr, "%s: control vector in %s does not match previous vector dimensions\n", __func__, info.fname.c_str());
            return result;
        }

        if (result.n_embd == -1)
        {
            result = std::move(cur);
        }
        else
        {
            for (size_t i = 0; i < cur.data.size(); i++)
            {
                result.data[i] += cur.data[i];
            }
        }
    }

    if (result.n_embd == -1)
    {
        fprintf(stderr, "%s: no vectors passed\n", __func__);
    }

    return result;
}

#pragma clang diagnostic pop
// NOLINTEND
