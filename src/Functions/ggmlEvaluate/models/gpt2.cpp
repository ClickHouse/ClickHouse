#include "gpt2.h"

#include "Functions/ggmlEvaluate/model_storage.h"
#include "Functions/ggmlEvaluate/models/gpt_common.h"
#include "ggml/ggml-alloc.h"
#include "ggml/ggml-backend.h"
#include "ggml/ggml.h"

// #ifdef GGML_USE_CUDA
// #include "ggml-cuda.h"
// #endif

// #ifdef GGML_USE_METAL
// #include "ggml-metal.h"
// #endif

#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <string>
#include <vector>
#include "Common/Exception.h"

// #if defined(_MSC_VER)
// #pragma warning(disable: 4244 4267) // possible loss of data
// #endif

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
extern const int INCORRECT_DATA;
extern const int RECEIVED_EMPTY_DATA;
extern const int SYNTAX_ERROR;
}

constexpr Int32 GPT2_MAX_NODES = 4096;
static const std::string ModelArchName = "gpt2";

void Gpt2Model::loadImpl(ConfigPtr config)
{
    //  bool gpt2_model_load(const std::string & fname, gpt2_model & model, gpt_vocab & vocab, int n_ctx, int n_gpu_layers) {
    auto fname = getPathFromConfig(config, ModelArchName);

    auto fin = std::ifstream(fname, std::ios::binary);
    if (!fin)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Unable to open file at '{}'", fname);

    // verify magic
    {
        uint32_t magic;
        fin.read(reinterpret_cast<char *>(&magic), sizeof(magic));
        if (magic != GGML_FILE_MAGIC)
        {
            throw Exception(
                ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT, "File at '{}' does not match the required format (bad magic)", fname);
        }
    }

    // load hparams
    {
        fin.read(reinterpret_cast<char *>(&state.hparams.n_vocab), sizeof(state.hparams.n_vocab));
        fin.read(reinterpret_cast<char *>(&state.hparams.n_ctx), sizeof(state.hparams.n_ctx));
        fin.read(reinterpret_cast<char *>(&state.hparams.n_embd), sizeof(state.hparams.n_embd));
        fin.read(reinterpret_cast<char *>(&state.hparams.n_head), sizeof(state.hparams.n_head));
        fin.read(reinterpret_cast<char *>(&state.hparams.n_layer), sizeof(state.hparams.n_layer));
        fin.read(reinterpret_cast<char *>(&state.hparams.ftype), sizeof(state.hparams.ftype));

        state.hparams.ftype %= GGML_QNT_VERSION_FACTOR;
    }

    // load vocab
    {
        int32_t n_vocab = 0;
        fin.read(reinterpret_cast<char *>(&n_vocab), sizeof(n_vocab));

        if (n_vocab != state.hparams.n_vocab)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Invalid model file '{}': bad vocab size ({} != {})", fname, n_vocab, state.hparams.n_vocab);
        }

        std::string word;
        std::vector<char> buf(128);

        for (int i = 0; i < n_vocab; i++)
        {
            uint32_t len;
            fin.read(reinterpret_cast<char *>(&len), sizeof(len));

            buf.resize(len);
            fin.read(reinterpret_cast<char *>(buf.data()), len);
            word.assign(buf.data(), len);

            gpt_vocab.token_to_id[word] = i;
            gpt_vocab.id_to_token[i] = word;
        }
    }

    // for the big tensors, we have the option to store the data in 16-bit floats or quantized
    // in order to save memory and also to speed up the computation
    ggml_type wtype = ggml_ftype_to_ggml_type(static_cast<ggml_ftype>(state.hparams.ftype));
    if (wtype == GGML_TYPE_COUNT)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid model file '{}' : bad ftype value {}", fname, GGML_TYPE_COUNT);

    auto & ctx = state.ctx_w;

    // create the ggml context
    {
        size_t n_tensors = 2 + 6 + 12 * state.hparams.n_layer;
        ggml_init_params params = {
            .mem_size = ggml_tensor_overhead() * n_tensors,
            .mem_buffer = nullptr,
            .no_alloc = true,
        };

        ctx = ggml_init(params);
        if (!ctx)
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "ggml_init() failed");
    }

    // initialize the backend
#ifdef GGML_USE_CUDA
    if (n_gpu_layers > 0)
    {
        fprintf(stderr, "%s: using CUDA backend\n", __func__);
        model.backend = ggml_backend_cuda_init(0);
        if (!model.backend)
            fprintf(stderr, "%s: ggml_backend_cuda_init() failed\n", __func__);
    }
#endif

#ifdef GGML_USE_METAL
    if (n_gpu_layers > 0)
    {
        fprintf(stderr, "%s: using Metal backend\n", __func__);
        ggml_backend_metal_log_set_callback(ggml_log_callback_default, nullptr);
        model.backend = ggml_backend_metal_init();
        if (!model.backend)
            fprintf(stderr, "%s: ggml_backend_metal_init() failed\n", __func__);
    }
#endif

    if (!state.backend)
        state.backend = ggml_backend_cpu_init();

    if (!state.backend)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "ggml_backend_cpu_init() failed");

    {
        const int n_embd = state.hparams.n_embd;
        const int n_layer = state.hparams.n_layer;
        const int n_ctx = state.hparams.n_ctx;
        const int n_vocab = state.hparams.n_vocab;

        state.layers.resize(n_layer);

        state.ln_f_g = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);
        state.ln_f_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

        state.wte = ggml_new_tensor_2d(ctx, wtype, n_embd, n_vocab);
        state.wpe = ggml_new_tensor_2d(ctx, GGML_TYPE_F32, n_embd, n_ctx);
        state.lm_head = ggml_new_tensor_2d(ctx, wtype, n_embd, n_vocab);

        // map by name
        state.tensors["model/ln_f/g"] = state.ln_f_g;
        state.tensors["model/ln_f/b"] = state.ln_f_b;

        state.tensors["model/wte"] = state.wte;
        state.tensors["model/wpe"] = state.wpe;
        state.tensors["model/lm_head"] = state.lm_head;

        for (int i = 0; i < n_layer; ++i)
        {
            auto & layer = state.layers[i];

            layer.ln_1_g = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);
            layer.ln_1_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

            layer.ln_2_g = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);
            layer.ln_2_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

            layer.c_attn_attn_w = ggml_new_tensor_2d(ctx, wtype, n_embd, 3 * n_embd);
            layer.c_attn_attn_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, 3 * n_embd);

            layer.c_attn_proj_w = ggml_new_tensor_2d(ctx, wtype, n_embd, n_embd);
            layer.c_attn_proj_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

            layer.c_mlp_fc_w = ggml_new_tensor_2d(ctx, wtype, n_embd, 4 * n_embd);
            layer.c_mlp_fc_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, 4 * n_embd);

            layer.c_mlp_proj_w = ggml_new_tensor_2d(ctx, wtype, 4 * n_embd, n_embd);
            layer.c_mlp_proj_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

            // map by name
            state.tensors["model/h" + std::to_string(i) + "/ln_1/g"] = layer.ln_1_g;
            state.tensors["model/h" + std::to_string(i) + "/ln_1/b"] = layer.ln_1_b;

            state.tensors["model/h" + std::to_string(i) + "/ln_2/g"] = layer.ln_2_g;
            state.tensors["model/h" + std::to_string(i) + "/ln_2/b"] = layer.ln_2_b;

            state.tensors["model/h" + std::to_string(i) + "/attn/c_attn/w"] = layer.c_attn_attn_w;
            state.tensors["model/h" + std::to_string(i) + "/attn/c_attn/b"] = layer.c_attn_attn_b;

            state.tensors["model/h" + std::to_string(i) + "/attn/c_proj/w"] = layer.c_attn_proj_w;
            state.tensors["model/h" + std::to_string(i) + "/attn/c_proj/b"] = layer.c_attn_proj_b;

            state.tensors["model/h" + std::to_string(i) + "/mlp/c_fc/w"] = layer.c_mlp_fc_w;
            state.tensors["model/h" + std::to_string(i) + "/mlp/c_fc/b"] = layer.c_mlp_fc_b;

            state.tensors["model/h" + std::to_string(i) + "/mlp/c_proj/w"] = layer.c_mlp_proj_w;
            state.tensors["model/h" + std::to_string(i) + "/mlp/c_proj/b"] = layer.c_mlp_proj_b;
        }
    }

    // allocate the model tensors in a backend buffer
    state.buffer_w = ggml_backend_alloc_ctx_tensors(ctx, state.backend);

    // override the default training context with the user-provided
    // state.hparams.n_ctx = n_ctx; // GGMLTODO : wtf?

    // key + value memory
    {
        auto * context = state.ctx_kv;

        // create the ggml context
        {
            size_t n_tensors = 2;
            ggml_init_params params = {
                .mem_size = ggml_tensor_overhead() * n_tensors,
                .mem_buffer = nullptr,
                .no_alloc = true,
            };

            context = ggml_init(params);
            if (!context)
                throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "ggml_init() failed");
        }

        const auto & hparams = state.hparams;

        const int n_embd = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_ctx = hparams.n_ctx;

        const int n_mem = n_layer * n_ctx;
        const int n_elements = n_embd * n_mem;

        state.memory_k = ggml_new_tensor_1d(context, GGML_TYPE_F32, n_elements);
        state.memory_v = ggml_new_tensor_1d(context, GGML_TYPE_F32, n_elements);

        // allocate the KV memory in a backend buffer
        state.buffer_kv = ggml_backend_alloc_ctx_tensors(context, state.backend);
    }

    // load weights
    {
        bool has_lm_head = false;

        std::vector<char> read_buf;

        while (true)
        {
            int32_t n_dims;
            int32_t length;
            int32_t ttype;

            fin.read(reinterpret_cast<char *>(&n_dims), sizeof(n_dims));
            fin.read(reinterpret_cast<char *>(&length), sizeof(length));
            fin.read(reinterpret_cast<char *>(&ttype), sizeof(ttype));

            if (fin.eof())
                break;

            int32_t nelements = 1;
            int32_t ne[2] = {1, 1};
            for (int i = 0; i < n_dims; ++i)
            {
                fin.read(reinterpret_cast<char *>(&ne[i]), sizeof(ne[i]));
                nelements *= ne[i];
            }

            std::string name(length, 0);
            fin.read(name.data(), length);

            if (state.tensors.find(name) == state.tensors.end())
                throw Exception(ErrorCodes::INCORRECT_DATA, "In model file '{}' : unknown tensor name {}", fname, name);

            auto * tensor = state.tensors[name];
            ggml_set_name(tensor, name.c_str());
            if (ggml_nelements(tensor) != nelements)
                throw Exception(ErrorCodes::INCORRECT_DATA, "In model file '{}' : tensor {} has wrong size in model file", fname, name);

            if (tensor->ne[0] != ne[0] || tensor->ne[1] != ne[1])
                throw Exception(ErrorCodes::INCORRECT_DATA, "In model file '{}' : tensor {} has wrong shape in model file", fname, name);

            const size_t bpe = ggml_type_size(ggml_type(ttype));

            if ((nelements * bpe) / ggml_blck_size(tensor->type) != ggml_nbytes(tensor))
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "In model file '{}' : tensor {} has wrong size in model file : got {}, expected {}",
                    fname,
                    name,
                    ggml_nbytes(tensor),
                    nelements * bpe);
            }

            if (ggml_backend_buffer_is_host(state.buffer_w))
            {
                // for some backends such as CPU and Metal, the tensor data is in system memory and we can read directly into it
                fin.read(reinterpret_cast<char *>(tensor->data), ggml_nbytes(tensor));
            }
            else
            {
                // read into a temporary buffer first, then copy to device memory
                read_buf.resize(ggml_nbytes(tensor));
                fin.read(read_buf.data(), ggml_nbytes(tensor));
                ggml_backend_tensor_set(tensor, read_buf.data(), 0, ggml_nbytes(tensor));
            }

            // GPT-2 models share the WTE tensor as the LM head
            if (name == "model/wte" && has_lm_head == false)
            {
                //ggml_backend_tensor_copy(tensor, model.lm_head);
                state.lm_head = tensor;
            }

            if (name == "model/lm_head")
                has_lm_head = true;
        }
    }

    fin.close();

    // allocate the compute buffer
    {
        // create a graph allocator with the backend's default buffer type
        allocr = ggml_gallocr_new(ggml_backend_get_default_buffer_type(state.backend));

        // create the worst case graph for memory usage estimation
        // int n_tokens = std::min(state.hparams.n_ctx, params.n_batch); // GGMLTODO : no clue what it means
        int n_tokens = std::min(state.hparams.n_ctx, 8);
        int n_past = state.hparams.n_ctx - n_tokens;
        ggml_cgraph * gf = gpt2_graph(n_past, n_tokens);

        // pre-allocate the compute buffer for the worst case (optional)
        ggml_gallocr_reserve(allocr, gf);
    }
}

std::string Gpt2Model::evalImpl(std::tuple<Int32> user_params, const std::string & input)
{
    int n_past = 0;

    std::vector<float> logits;

    // tokenize the prompt
    std::vector<GptVocab::id> embd_inp = gpt_tokenize(gpt_vocab, input);

    int n_predict = std::min(std::get<0>(user_params), state.hparams.n_ctx - static_cast<int>(embd_inp.size()));

    // submit the input prompt token-by-token
    // this reduces the memory usage during inference, at the cost of a bit of speed at the beginning
    std::vector<GptVocab::id> embd;

    std::string result;
    std::mt19937 random_number_generator;

    for (size_t i = embd.size(); i < embd_inp.size() + n_predict; i++)
    {
        // predict
        if (!embd.empty())
        {
            if (!gpt2_eval(gpt_params.n_threads, n_past, embd, logits))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to predict");
        }

        n_past += embd.size();
        embd.clear();

        if (i >= embd_inp.size())
        {
            // sample next token
            const int top_k = 40; // params.top_k;
            const float top_p = 0.9f; // params.top_p;
            const float temp = 0.9f; // params.temp; // GGMLTODO

            const int n_vocab = state.hparams.n_vocab;

            GptVocab::id id
                = gpt_sample_top_k_top_p(gpt_vocab, logits.data() + (logits.size() - n_vocab), top_k, top_p, temp, random_number_generator);

            // add it to the context
            embd.push_back(id);
        }
        else
        {
            // if here, it means we are still processing the input prompt
            for (size_t k = i; k < embd_inp.size(); k++)
            {
                embd.push_back(embd_inp[k]);
                if (int32_t(embd.size()) >= /* params.n_batch */ 8)
                    break;
            }
            i += embd.size() - 1;
        }

        // display text
        for (auto id : embd)
            result += gpt_vocab.id_to_token[id];

        // end of text token
        if (embd.back() == 50256)
            break;
    }

    return result;
}

// NOLINTBEGIN
ggml_cgraph * Gpt2Model::gpt2_graph(const int n_past, const int n_tokens)
{
    const int N = n_tokens;

    const auto & hparams = state.hparams;

    const int n_embd = hparams.n_embd;
    const int n_layer = hparams.n_layer;
    const int n_ctx = hparams.n_ctx;
    const int n_head = hparams.n_head;

    // since we are using ggml-alloc, this buffer only needs enough space to hold the ggml_tensor and ggml_cgraph structs, but not the tensor data
    static size_t buf_size = ggml_tensor_overhead() * GPT2_MAX_NODES + ggml_graph_overhead_custom(GPT2_MAX_NODES, false);
    static std::vector<uint8_t> buf(buf_size);

    ggml_init_params params = {
        .mem_size = buf_size,
        .mem_buffer = buf.data(),
        .no_alloc = true, // the tensors will be allocated later by ggml_gallocr_alloc_graph()
    };

    struct ggml_context * ctx = ggml_init(params);

    struct ggml_cgraph * gf = ggml_new_graph_custom(ctx, GPT2_MAX_NODES, false);

    struct ggml_tensor * embd = ggml_new_tensor_1d(ctx, GGML_TYPE_I32, N);
    // at this point, the tensor data is not allocated yet and cannot be set
    // we will find the tensor after the graph is allocated by its name, and set the data then
    ggml_set_name(embd, "embd");
    // setting a tensor as an input will ensure that it is allocated at the beginning of the graph
    // this is important to ensure that the input tensors are not overwritten before they are used
    ggml_set_input(embd);

    struct ggml_tensor * position = ggml_new_tensor_1d(ctx, GGML_TYPE_I32, N);
    ggml_set_name(position, "position");
    ggml_set_input(position);

    // wte + wpe
    struct ggml_tensor * inpL = ggml_add(ctx, ggml_get_rows(ctx, state.wte, embd), ggml_get_rows(ctx, state.wpe, position));

    for (int il = 0; il < n_layer; ++il)
    {
        struct ggml_tensor * cur;

        // norm
        {
            // [ 768, N]
            cur = ggml_norm(ctx, inpL, hparams.eps);

            // cur = ln_1_g*cur + ln_1_b
            // [ 768, N]
            cur = ggml_add(ctx, ggml_mul(ctx, cur, state.layers[il].ln_1_g), state.layers[il].ln_1_b);
        }

        // attn
        // [2304, 768] - model.layers[il].c_attn_attn_w
        // [2304,   1] - model.layers[il].c_attn_attn_b
        // [ 768,   N] - cur (in)
        // [2304,   N] - cur (out)
        //
        // cur = attn_w*cur + attn_b
        // [2304, N]
        {
            cur = ggml_mul_mat(ctx, state.layers[il].c_attn_attn_w, cur);

            cur = ggml_add(ctx, cur, state.layers[il].c_attn_attn_b);
        }

        // self-attention
        {
            struct ggml_tensor * Qcur = ggml_view_2d(ctx, cur, n_embd, N, cur->nb[1], 0 * sizeof(float) * n_embd);
            struct ggml_tensor * Kcur = ggml_view_2d(ctx, cur, n_embd, N, cur->nb[1], 1 * sizeof(float) * n_embd);
            struct ggml_tensor * Vcur = ggml_view_2d(ctx, cur, n_embd, N, cur->nb[1], 2 * sizeof(float) * n_embd);

            // store key and value to memory
            if (N >= 1)
            {
                struct ggml_tensor * k
                    = ggml_view_1d(ctx, state.memory_k, N * n_embd, (ggml_element_size(state.memory_k) * n_embd) * (il * n_ctx + n_past));
                struct ggml_tensor * v
                    = ggml_view_1d(ctx, state.memory_v, N * n_embd, (ggml_element_size(state.memory_v) * n_embd) * (il * n_ctx + n_past));

                ggml_build_forward_expand(gf, ggml_cpy(ctx, Kcur, k));
                ggml_build_forward_expand(gf, ggml_cpy(ctx, Vcur, v));
            }

            // Q = Qcur.contiguous().view(n_embd/n_head, n_head, N).permute(0, 2, 1, 3)
            // [64, N, 12]
            struct ggml_tensor * Q = ggml_permute(ctx, ggml_cont_3d(ctx, Qcur, n_embd / n_head, n_head, N), 0, 2, 1, 3);

            // K = Kmem.view(n_embd/n_head, n_head, n_past + N).permute(0, 2, 1, 3)
            // [64, n_past + N, 12]
            struct ggml_tensor * K = ggml_permute(
                ctx,
                ggml_reshape_3d(
                    ctx,
                    ggml_view_1d(ctx, state.memory_k, (n_past + N) * n_embd, il * n_ctx * ggml_element_size(state.memory_k) * n_embd),
                    n_embd / n_head,
                    n_head,
                    n_past + N),
                0,
                2,
                1,
                3);

            // GG: flash attention
            //struct ggml_tensor * V =
            //    ggml_cpy(ctx0,
            //            ggml_permute(ctx0,
            //                ggml_reshape_3d(ctx0,
            //                    ggml_view_1d(ctx0, model.memory_v, (n_past + N)*n_embd, il*n_ctx*ggml_element_size(model.memory_v)*n_embd),
            //                    n_embd/n_head, n_head, n_past + N),
            //                1, 2, 0, 3),
            //            ggml_new_tensor_3d(ctx0, GGML_TYPE_F32, n_past + N, n_embd/n_head, n_head));

            //struct ggml_tensor * KQV = ggml_flash_attn(ctx0, Q, K, V, true);

            // K * Q
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ = ggml_mul_mat(ctx, K, Q);

            // KQ_scaled = KQ / sqrt(n_embd/n_head)
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ_scaled = ggml_scale(ctx, KQ, 1.0f / sqrtf(float(n_embd) / n_head));

            // KQ_masked = mask_past(KQ_scaled)
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ_masked = ggml_diag_mask_inf(ctx, KQ_scaled, n_past);

            // KQ = soft_max(KQ_masked)
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ_soft_max = ggml_soft_max(ctx, KQ_masked);

            // V_trans = Vmem.view(n_embd/n_head, n_head, n_past + N).permute(1, 2, 0, 3).contiguous()
            // [n_past + N, 64, 12]
            struct ggml_tensor * V_trans = ggml_cont_3d(
                ctx,
                ggml_permute(
                    ctx,
                    ggml_reshape_3d(
                        ctx,
                        ggml_view_1d(ctx, state.memory_v, (n_past + N) * n_embd, il * n_ctx * ggml_element_size(state.memory_v) * n_embd),
                        n_embd / n_head,
                        n_head,
                        n_past + N),
                    1,
                    2,
                    0,
                    3),
                n_past + N,
                n_embd / n_head,
                n_head);

            // KQV = transpose(V) * KQ_soft_max
            // [64, N, 12]
            struct ggml_tensor * KQV = ggml_mul_mat(ctx, V_trans, KQ_soft_max);

            // KQV_merged = KQV.permute(0, 2, 1, 3)
            // [64, 12, N]
            struct ggml_tensor * KQV_merged = ggml_permute(ctx, KQV, 0, 2, 1, 3);

            // cur = KQV_merged.contiguous().view(n_embd, N)
            // [768, N]
            cur = ggml_cont_2d(ctx, KQV_merged, n_embd, N);
        }

        // projection
        // [ 768, 768] - model.layers[il].c_attn_proj_w
        // [ 768,   1] - model.layers[il].c_attn_proj_b
        // [ 768,   N] - cur (in)
        // [ 768,   N] - cur (out)
        //
        // cur = proj_w*cur + proj_b
        // [768, N]
        {
            cur = ggml_mul_mat(ctx, state.layers[il].c_attn_proj_w, cur);

            cur = ggml_add(ctx, cur, state.layers[il].c_attn_proj_b);
        }

        // add the input
        cur = ggml_add(ctx, cur, inpL);

        struct ggml_tensor * inpFF = cur;

        // feed-forward network
        {
            // norm
            {
                cur = ggml_norm(ctx, inpFF, hparams.eps);

                // cur = ln_2_g*cur + ln_2_b
                // [ 768, N]
                cur = ggml_add(ctx, ggml_mul(ctx, cur, state.layers[il].ln_2_g), state.layers[il].ln_2_b);
            }

            // fully connected
            // [3072, 768] - model.layers[il].c_mlp_fc_w
            // [3072,   1] - model.layers[il].c_mlp_fc_b
            // [ 768,   N] - cur (in)
            // [3072,   N] - cur (out)
            //
            // cur = fc_w*cur + fc_b
            // [3072, N]
            cur = ggml_mul_mat(ctx, state.layers[il].c_mlp_fc_w, cur);

            cur = ggml_add(ctx, cur, state.layers[il].c_mlp_fc_b);

            // GELU activation
            // [3072, N]
            cur = ggml_gelu(ctx, cur);

            // projection
            // [ 768, 3072] - model.layers[il].c_mlp_proj_w
            // [ 768,    1] - model.layers[il].c_mlp_proj_b
            // [3072,    N] - cur (in)
            // [ 768,    N] - cur (out)
            //
            // cur = proj_w*cur + proj_b
            // [768, N]
            cur = ggml_mul_mat(ctx, state.layers[il].c_mlp_proj_w, cur);

            cur = ggml_add(ctx, cur, state.layers[il].c_mlp_proj_b);
        }

        // input for next layer
        inpL = ggml_add(ctx, cur, inpFF);
    }

    // norm
    {
        // [ 768, N]
        inpL = ggml_norm(ctx, inpL, hparams.eps);

        // inpL = ln_f_g*inpL + ln_f_b
        // [ 768, N]
        inpL = ggml_add(ctx, ggml_mul(ctx, inpL, state.ln_f_g), state.ln_f_b);
    }

    // inpL = WTE * inpL
    // [ 768, 50257] - model.lm_head
    // [ 768, N]     - inpL
    inpL = ggml_mul_mat(ctx, state.lm_head, inpL);
    ggml_set_name(inpL, "logits");
    // setting a tensor as the output will ensure that it is not overwritten by subsequent operations
    ggml_set_output(inpL);

    // logits -> probs
    //inpL = ggml_soft_max(ctx0, inpL);

    ggml_build_forward_expand(gf, inpL);

    ggml_free(ctx);

    return gf;
}
// NOLINTEND

bool Gpt2Model::gpt2_eval(int /*n_threads_*/, int n_past, std::vector<GptVocab::id> & embd_inp, std::vector<float> & embd_w)
{
    const int n_input_elements = static_cast<int>(embd_inp.size());

    const auto & hparams = state.hparams;

    const int n_vocab = hparams.n_vocab;

    struct ggml_cgraph * gf = gpt2_graph(n_past, n_input_elements);

    // allocate the graph tensors
    ggml_gallocr_alloc_graph(allocr, gf);

    // set the graph inputs
    struct ggml_tensor * embd = ggml_graph_get_tensor(gf, "embd");
    ggml_backend_tensor_set(embd, embd_inp.data(), 0, n_input_elements * ggml_element_size(embd));

    struct ggml_tensor * position = ggml_graph_get_tensor(gf, "position");
    for (int i = 0; i < n_input_elements; ++i)
    {
        int32_t v = n_past + i;
        ggml_backend_tensor_set(position, &v, i * sizeof(int32_t), sizeof(v));
    }

    // set backend options
    if (ggml_backend_is_cpu(state.backend))
        ggml_backend_cpu_set_n_threads(state.backend, gpt_params.n_threads);

#ifdef GGML_USE_METAL
    if (ggml_backend_is_metal(model.backend))
        ggml_backend_metal_set_n_cb(model.backend, n_threads);
#endif

    // run the computation
    ggml_backend_graph_compute(state.backend, gf);

    //if (n_past%100 == 0) {
    //    ggml_graph_print   (&gf);
    //    ggml_graph_dump_dot(&gf, NULL, "gpt-2.dot");
    //}

    // get the graph outputs
    struct ggml_tensor * logits = ggml_graph_get_tensor(gf, "logits");

    //embd_w.resize(n_vocab*N);
    //ggml_backend_tensor_get(logits, embd_w.data(), 0, sizeof(float)*n_vocab*N);

    // return result just for the last token
    embd_w.resize(n_vocab);
    ggml_backend_tensor_get(logits, embd_w.data(), (n_vocab * (n_input_elements - 1)) * sizeof(float), sizeof(float) * n_vocab);

    return true;
}

static GgmlModelRegister<Gpt2Model> RegGpt2(ModelArchName);

}
