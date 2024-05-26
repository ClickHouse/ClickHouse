#include "gpt2.h"

#include "Functions/ggmlEvaluate/IGgmlModel.h"
#include "ggml/ggml.h"

#include <Common/Exception.h>

#include <Functions/ggmlEvaluate/model_storage.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <string>
#include <vector>

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

    size_t ctx_size = 0;

    {
        const auto & hparams = state.hparams;

        const int n_embd = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_ctx = hparams.n_ctx;
        const int n_vocab = hparams.n_vocab;

        ctx_size += ggml_row_size(GGML_TYPE_F32, n_embd); // ln_f_g
        ctx_size += ggml_row_size(GGML_TYPE_F32, n_embd); // ln_f_b

        ctx_size += ggml_row_size(wtype, n_vocab * n_embd); // wte
        ctx_size += ggml_row_size(GGML_TYPE_F32, n_ctx * n_embd); // wpe
        ctx_size += ggml_row_size(wtype, n_vocab * n_embd); // lm_head

        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_1_g
        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_1_b

        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_2_g
        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_2_b

        ctx_size += n_layer * (ggml_row_size(wtype, 3 * n_embd * n_embd)); // c_attn_attn_w
        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, 3 * n_embd)); // c_attn_attn_b

        ctx_size += n_layer * (ggml_row_size(wtype, n_embd * n_embd)); // c_attn_proj_w
        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, n_embd)); // c_attn_proj_b

        ctx_size += n_layer * (ggml_row_size(wtype, 4 * n_embd * n_embd)); // c_mlp_fc_w
        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, 4 * n_embd)); // c_mlp_fc_b

        ctx_size += n_layer * (ggml_row_size(wtype, 4 * n_embd * n_embd)); // c_mlp_proj_w
        ctx_size += n_layer * (ggml_row_size(GGML_TYPE_F32, 4 * n_embd)); // c_mlp_proj_b

        ctx_size += n_ctx * n_layer * ggml_row_size(GGML_TYPE_F32, n_embd); // memory_k
        ctx_size += n_ctx * n_layer * ggml_row_size(GGML_TYPE_F32, n_embd); // memory_v

        ctx_size += (6 + 12 * n_layer) * 512; // object overhead
    }

    // create the ggml context
    {
        ggml_init_params params = {
            .mem_size = ctx_size,
            .mem_buffer = nullptr,
            .no_alloc = false,
        };

        ctx = ggml_init(params);
        if (!ctx)
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "ggml_init() failed");
    }

    // prepare memory for the weights
    {
        const auto & hparams = state.hparams;

        const int n_embd = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_ctx = hparams.n_ctx;
        const int n_vocab = hparams.n_vocab;

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

    // key + value memory
    {
        const auto & hparams = state.hparams;

        const int n_embd = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_ctx = hparams.n_ctx;

        const int n_mem = n_layer * n_ctx;
        const int n_elements = n_embd * n_mem;

        state.memory_k = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_elements);
        state.memory_v = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_elements);
    }

    {
        bool has_lm_head = false;

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

            fin.read(reinterpret_cast<char *>(tensor->data), ggml_nbytes(tensor));

            // GPT-2 models share the WTE tensor as the LM head
            if (name == "model/wte" && has_lm_head == false)
                memcpy(state.lm_head->data, tensor->data, ggml_nbytes(tensor));

            if (name == "model/lm_head")
                has_lm_head = true;
        }
    }

    fin.close();
}

std::string Gpt2Model::evalImpl(const std::string & input, const GgmlModelParams & user_params)
{
    int n_past = 0;

    std::vector<float> logits;

    // tokenize the prompt
    std::vector<GptVocab::id> embd_inp = gpt_tokenize(gpt_vocab, input);

    Int64 n_predict = gpt_params.n_predict;
    if (auto it = user_params.find("n_predict"); it != user_params.end())
        n_predict = it->second.safeGet<Int64>();
    n_predict = std::min(n_predict, state.hparams.n_ctx - static_cast<Int64>(embd_inp.size()));

    // submit the input prompt token-by-token
    // this reduces the memory usage during inference, at the cost of a bit of speed at the beginning
    size_t mem_per_token = 0;
    gpt2_eval(gpt_params.n_threads, 0, {0, 1, 2, 3}, logits, mem_per_token);

    std::vector<GptVocab::id> embd;

    std::string result;
    pcg64_fast random_number_generator;

    for (size_t i = embd.size(); i < embd_inp.size() + n_predict; i++)
    {
        // predict
        if (!embd.empty())
        {
            if (!gpt2_eval(gpt_params.n_threads, n_past, embd, logits, mem_per_token))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to predict");
        }

        n_past += embd.size();
        embd.clear();

        if (i >= embd_inp.size())
        {
            // sample next token
            const int top_k = gpt_params.top_k;
            const float top_p = gpt_params.top_p;
            const float temp = gpt_params.temp;

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
bool Gpt2Model::gpt2_eval(
    const int n_threads, const int n_past, const std::vector<GptVocab::id> & embd_inp, std::vector<float> & embd_w, size_t & mem_per_token)
{
    const int n = static_cast<int>(embd_inp.size());

    const auto & hparams = state.hparams;

    const int n_embd = hparams.n_embd;
    const int n_layer = hparams.n_layer;
    const int n_ctx = hparams.n_ctx;
    const int n_head = hparams.n_head;
    const int n_vocab = hparams.n_vocab;

    static size_t buf_size = 256u * 1024 * 1024;
    static thread_local void * buf = malloc(buf_size);

    if (mem_per_token > 0 && mem_per_token * n > buf_size)
    {
        const size_t buf_size_new = static_cast<size_t>(1.1 * (mem_per_token * n)); // add 10% to account for ggml object overhead
        //printf("\n%s: reallocating buffer from %zu to %zu bytes\n", __func__, buf_size, buf_size_new);

        // reallocate
        buf_size = buf_size_new;
        buf = realloc(buf, buf_size);
        if (buf == nullptr)
            return false;
    }

    ggml_init_params params = {
        .mem_size = buf_size,
        .mem_buffer = buf,
        .no_alloc = false,
    };

    struct ggml_context * ctx0 = ggml_init(params);
    struct ggml_cgraph * gf = ggml_new_graph(ctx0);

    struct ggml_tensor * embd = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n);
    memcpy(embd->data, embd_inp.data(), n * ggml_element_size(embd));

    struct ggml_tensor * position = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n);
    for (int i = 0; i < n; ++i)
        reinterpret_cast<int32_t *>(position->data)[i] = n_past + i;

    // wte + wpe
    struct ggml_tensor * inpL = ggml_add(ctx0, ggml_get_rows(ctx0, state.wte, embd), ggml_get_rows(ctx0, state.wpe, position));

    for (int il = 0; il < n_layer; ++il)
    {
        struct ggml_tensor * cur;

        // norm
        {
            // [ 768, N]
            cur = ggml_norm(ctx0, inpL, hparams.eps);

            // cur = ln_1_g*cur + ln_1_b
            // [ 768, N]
            cur = ggml_add(
                ctx0,
                ggml_mul(ctx0, ggml_repeat(ctx0, state.layers[il].ln_1_g, cur), cur),
                ggml_repeat(ctx0, state.layers[il].ln_1_b, cur));
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
            cur = ggml_mul_mat(ctx0, state.layers[il].c_attn_attn_w, cur);

            cur = ggml_add(ctx0, ggml_repeat(ctx0, state.layers[il].c_attn_attn_b, cur), cur);
        }

        // self-attention
        {
            struct ggml_tensor * Qcur = ggml_view_2d(ctx0, cur, n_embd, n, cur->nb[1], 0 * sizeof(float) * n_embd);
            struct ggml_tensor * Kcur = ggml_view_2d(ctx0, cur, n_embd, n, cur->nb[1], 1 * sizeof(float) * n_embd);
            struct ggml_tensor * Vcur = ggml_view_2d(ctx0, cur, n_embd, n, cur->nb[1], 2 * sizeof(float) * n_embd);

            // store key and value to memory
            if (n >= 1)
            {
                struct ggml_tensor * k
                    = ggml_view_1d(ctx0, state.memory_k, n * n_embd, (ggml_element_size(state.memory_k) * n_embd) * (il * n_ctx + n_past));
                struct ggml_tensor * v
                    = ggml_view_1d(ctx0, state.memory_v, n * n_embd, (ggml_element_size(state.memory_v) * n_embd) * (il * n_ctx + n_past));

                ggml_build_forward_expand(gf, ggml_cpy(ctx0, Kcur, k));
                ggml_build_forward_expand(gf, ggml_cpy(ctx0, Vcur, v));
            }

            // Q = Qcur.contiguous().view(n_embd/n_head, n_head, N).permute(0, 2, 1, 3)
            // [64, N, 12]
            struct ggml_tensor * Q
                = ggml_permute(ctx0, ggml_cpy(ctx0, Qcur, ggml_new_tensor_3d(ctx0, GGML_TYPE_F32, n_embd / n_head, n_head, n)), 0, 2, 1, 3);

            // K = Kmem.view(n_embd/n_head, n_head, n_past + N).permute(0, 2, 1, 3)
            // [64, n_past + N, 12]
            struct ggml_tensor * K = ggml_permute(
                ctx0,
                ggml_reshape_3d(
                    ctx0,
                    ggml_view_1d(ctx0, state.memory_k, (n_past + n) * n_embd, il * n_ctx * ggml_element_size(state.memory_k) * n_embd),
                    n_embd / n_head,
                    n_head,
                    n_past + n),
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
            struct ggml_tensor * KQ = ggml_mul_mat(ctx0, K, Q);

            // KQ_scaled = KQ / sqrt(n_embd/n_head)
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ_scaled = ggml_scale_inplace(ctx0, KQ, 1.0f / sqrt(float(n_embd) / n_head));

            // KQ_masked = mask_past(KQ_scaled)
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ_masked = ggml_diag_mask_inf_inplace(ctx0, KQ_scaled, n_past);

            // KQ = soft_max(KQ_masked)
            // [n_past + N, N, 12]
            struct ggml_tensor * KQ_soft_max = ggml_soft_max_inplace(ctx0, KQ_masked);

            // V_trans = Vmem.view(n_embd/n_head, n_head, n_past + N).permute(1, 2, 0, 3).contiguous()
            // [n_past + N, 64, 12]
            struct ggml_tensor * V_trans = ggml_cpy(
                ctx0,
                ggml_permute(
                    ctx0,
                    ggml_reshape_3d(
                        ctx0,
                        ggml_view_1d(ctx0, state.memory_v, (n_past + n) * n_embd, il * n_ctx * ggml_element_size(state.memory_v) * n_embd),
                        n_embd / n_head,
                        n_head,
                        n_past + n),
                    1,
                    2,
                    0,
                    3),
                ggml_new_tensor_3d(ctx0, state.memory_v->type, n_past + n, n_embd / n_head, n_head));

            // KQV = transpose(V) * KQ_soft_max
            // [64, N, 12]
            struct ggml_tensor * KQV = ggml_mul_mat(ctx0, V_trans, KQ_soft_max);

            // KQV_merged = KQV.permute(0, 2, 1, 3)
            // [64, 12, N]
            struct ggml_tensor * KQV_merged = ggml_permute(ctx0, KQV, 0, 2, 1, 3);

            // cur = KQV_merged.contiguous().view(n_embd, N)
            // [768, N]
            cur = ggml_cpy(ctx0, KQV_merged, ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, n_embd, n));
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
            cur = ggml_mul_mat(ctx0, state.layers[il].c_attn_proj_w, cur);

            cur = ggml_add(ctx0, ggml_repeat(ctx0, state.layers[il].c_attn_proj_b, cur), cur);
        }

        // add the input
        cur = ggml_add(ctx0, cur, inpL);

        struct ggml_tensor * inpFF = cur;

        // feed-forward network
        {
            // norm
            {
                cur = ggml_norm(ctx0, inpFF, hparams.eps);

                // cur = ln_2_g*cur + ln_2_b
                // [ 768, N]
                cur = ggml_add(
                    ctx0,
                    ggml_mul(ctx0, ggml_repeat(ctx0, state.layers[il].ln_2_g, cur), cur),
                    ggml_repeat(ctx0, state.layers[il].ln_2_b, cur));
            }

            // fully connected
            // [3072, 768] - model.layers[il].c_mlp_fc_w
            // [3072,   1] - model.layers[il].c_mlp_fc_b
            // [ 768,   N] - cur (in)
            // [3072,   N] - cur (out)
            //
            // cur = fc_w*cur + fc_b
            // [3072, N]
            cur = ggml_mul_mat(ctx0, state.layers[il].c_mlp_fc_w, cur);

            cur = ggml_add(ctx0, ggml_repeat(ctx0, state.layers[il].c_mlp_fc_b, cur), cur);

            // GELU activation
            // [3072, N]
            cur = ggml_gelu(ctx0, cur);

            // projection
            // [ 768, 3072] - model.layers[il].c_mlp_proj_w
            // [ 768,    1] - model.layers[il].c_mlp_proj_b
            // [3072,    N] - cur (in)
            // [ 768,    N] - cur (out)
            //
            // cur = proj_w*cur + proj_b
            // [768, N]
            cur = ggml_mul_mat(ctx0, state.layers[il].c_mlp_proj_w, cur);

            cur = ggml_add(ctx0, ggml_repeat(ctx0, state.layers[il].c_mlp_proj_b, cur), cur);
        }

        // input for next layer
        inpL = ggml_add(ctx0, cur, inpFF);
    }

    // norm
    {
        // [ 768, N]
        inpL = ggml_norm(ctx0, inpL, hparams.eps);

        // inpL = ln_f_g*inpL + ln_f_b
        // [ 768, N]
        inpL = ggml_add(ctx0, ggml_mul(ctx0, ggml_repeat(ctx0, state.ln_f_g, inpL), inpL), ggml_repeat(ctx0, state.ln_f_b, inpL));
    }

    // inpL = WTE * inpL
    // [ 768, 50257] - model.lm_head
    // [ 768, N]     - inpL
    inpL = ggml_mul_mat(ctx0, state.lm_head, inpL);

    // logits -> probs
    //inpL = ggml_soft_max_inplace(ctx0, inpL);

    // run the computation
    ggml_build_forward_expand(gf, inpL);
    ggml_graph_compute_with_ctx(ctx0, gf, n_threads);

    //if (n_past%100 == 0) {
    //    ggml_graph_print   (&gf);
    //    ggml_graph_dump_dot(&gf, NULL, "gpt-2.dot");
    //}

    //embd_w.resize(n_vocab*N);
    //memcpy(embd_w.data(), ggml_get_data(inpL), sizeof(float)*n_vocab*N);

    // return result just for the last token
    embd_w.resize(n_vocab);
    memcpy(embd_w.data(), reinterpret_cast<float *>(ggml_get_data(inpL)) + (n_vocab * (n - 1)), sizeof(float) * n_vocab);

    if (mem_per_token == 0)
        mem_per_token = ggml_used_mem(ctx0) / n;
    //printf("used_mem = %zu\n", ggml_used_mem(ctx0));

    ggml_free(ctx0);
    return true;
}
// NOLINTEND

static GgmlModelRegister<Gpt2Model> RegGpt2(ModelArchName);

}
