#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include "base/types.h"
#include "ggml.h"
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

#include <DataTypes/DataTypeString.h>

#include <fstream>
#include <string>
#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
    extern const int SYNTAX_ERROR;
}

/// Evaluate CatBoost model.
/// - Arguments: float features first, then categorical features.
/// - Result: Float64.
class FunctionGGMLEvaluate final : public IFunction, WithContext
{
private:
    mutable std::unique_ptr<CatBoostLibraryBridgeHelper> bridge_helper;

    // default hparams (GPT-J 6B)
    struct gptj_hparams {
        int32_t n_vocab = 50400;
        int32_t n_ctx   = 2048;
        int32_t n_embd  = 4096;
        int32_t n_head  = 16;
        int32_t n_layer = 28;
        int32_t n_rot   = 64;
        int32_t ftype   = 1;
        float   eps     = 1e-5f;
    };

    struct gptj_layer {
        // normalization
        struct ggml_tensor * ln_1_g;
        struct ggml_tensor * ln_1_b;

        // attention
        struct ggml_tensor * c_attn_q_proj_w;
        struct ggml_tensor * c_attn_k_proj_w;
        struct ggml_tensor * c_attn_v_proj_w;

        struct ggml_tensor * c_attn_proj_w;

        // ff
        struct ggml_tensor * c_mlp_fc_w;
        struct ggml_tensor * c_mlp_fc_b;

        struct ggml_tensor * c_mlp_proj_w;
        struct ggml_tensor * c_mlp_proj_b;
    };

    struct gptj_model {
        gptj_hparams hparams;

        // normalization
        struct ggml_tensor * ln_f_g;
        struct ggml_tensor * ln_f_b;

        struct ggml_tensor * wte; // position embedding

        struct ggml_tensor * lmh_g; // language model head
        struct ggml_tensor * lmh_b; // language model bias

        std::vector<gptj_layer> layers;

        // key + value memory
        struct ggml_tensor * memory_k;
        struct ggml_tensor * memory_v;

        //
        struct ggml_context * ctx;
        std::map<std::string, struct ggml_tensor *> tensors;
    };

    struct gpt_vocab {
        using id    = int32_t;
        using token = std::string;

        std::map<token, id> token_to_id;
        std::map<id, token> id_to_token;
        std::vector<std::string> special_tokens;

        void add_special_token(const std::string & token) {
            special_tokens.push_back(token);
        }
    };

    struct gpt_params {
        int32_t seed         = -1;   // RNG seed
        int32_t n_threads    = std::min(4, static_cast<int32_t>(std::thread::hardware_concurrency()));
        int32_t n_predict    = 200;  // new tokens to predict
        int32_t n_parallel   = 1;    // number of parallel streams
        int32_t n_batch      = 8;    // batch size for prompt processing
        int32_t n_ctx        = 2048; // context size (this is the KV cache max size)
        int32_t n_gpu_layers = 0;    // number of layers to offlload to the GPU

        bool ignore_eos = false; // ignore EOS token when generating text

        // sampling parameters
        int32_t top_k          = 40;
        float   top_p          = 0.9f;
        float   temp           = 0.9f;
        int32_t repeat_last_n  = 64;
        float   repeat_penalty = 1.00f;

        std::string model      = "~/ggml-model.bin"; // model path
        std::string prompt;
        std::string token_test;

        bool    interactive      = false;
        int32_t interactive_port = -1;
    };

    // load the model's weights from a file
    bool gptj_model_load(const std::string & fname, gptj_model & model, gpt_vocab & vocab) const {
        auto fin = std::ifstream(fname, std::ios::binary);
        if (!fin) {
            return false;
        }

        // verify magic
        {
            uint32_t magic;
            fin.read(reinterpret_cast<char*>(&magic), sizeof(magic));
            if (magic != GGML_FILE_MAGIC) {
                return false;
            }
        }

        // load hparams
        {
            auto & hparams = model.hparams;

            fin.read(reinterpret_cast<char*>(&hparams.n_vocab), sizeof(hparams.n_vocab));
            fin.read(reinterpret_cast<char*>(&hparams.n_ctx),   sizeof(hparams.n_ctx));
            fin.read(reinterpret_cast<char*>(&hparams.n_embd),  sizeof(hparams.n_embd));
            fin.read(reinterpret_cast<char*>(&hparams.n_head),  sizeof(hparams.n_head));
            fin.read(reinterpret_cast<char*>(&hparams.n_layer), sizeof(hparams.n_layer));
            fin.read(reinterpret_cast<char*>(&hparams.n_rot),   sizeof(hparams.n_rot));
            fin.read(reinterpret_cast<char*>(&hparams.ftype),   sizeof(hparams.ftype));

            hparams.ftype %= GGML_QNT_VERSION_FACTOR;
        }

        // load vocab
        {
            int32_t n_vocab = 0;
            fin.read(reinterpret_cast<char*>(&n_vocab), sizeof(n_vocab));

            if (n_vocab != model.hparams.n_vocab) {
                return false;
            }

            std::string word;
            std::vector<char> buf(128);

            for (int i = 0; i < n_vocab; i++) {
                uint32_t len;
                fin.read(reinterpret_cast<char*>(&len), sizeof(len));

                buf.resize(len);
                fin.read(reinterpret_cast<char*>(buf.data()), len);
                word.assign(buf.data(), len);

                vocab.token_to_id[word] = i;
                vocab.id_to_token[i] = word;
            }
        }

        // for the big tensors, we have the option to store the data in 16-bit floats or quantized
        // in order to save memory and also to speed up the computation
        ggml_type wtype = ggml_ftype_to_ggml_type(static_cast<enum ggml_ftype>(model.hparams.ftype));
        if (wtype == GGML_TYPE_COUNT) {
            return false;
        }

        auto & ctx = model.ctx;

        size_t ctx_size = 0;

        {
            const auto & hparams = model.hparams;

            const int n_embd  = hparams.n_embd;
            const int n_layer = hparams.n_layer;
            const int n_ctx   = hparams.n_ctx;
            const int n_vocab = hparams.n_vocab;

            ctx_size += ggml_row_size(GGML_TYPE_F32, n_embd); // ln_f_g
            ctx_size += ggml_row_size(GGML_TYPE_F32, n_embd); // ln_f_b

            ctx_size += ggml_row_size(wtype, n_embd*n_vocab); // wte

            ctx_size += ggml_row_size(wtype,         n_embd*n_vocab); // lmh_g
            ctx_size += ggml_row_size(GGML_TYPE_F32,        n_vocab); // lmh_b

            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_1_g
            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_1_b

            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_q_proj_w
            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_k_proj_w
            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_v_proj_w

            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_proj_w

            ctx_size += n_layer*(ggml_row_size(wtype,         4*n_embd*n_embd)); // c_mlp_fc_w
            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32, 4*n_embd));        // c_mlp_fc_b

            ctx_size += n_layer*(ggml_row_size(wtype,         4*n_embd*n_embd)); // c_mlp_proj_w
            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32,   n_embd));        // c_mlp_proj_b

            ctx_size += n_ctx*n_layer*ggml_row_size(GGML_TYPE_F16, n_embd); // memory_k
            ctx_size += n_ctx*n_layer*ggml_row_size(GGML_TYPE_F16, n_embd); // memory_v

            ctx_size += (5 + 10*n_layer)*512; // object overhead
        }

        // create the ggml context
        {
            struct ggml_init_params params = {
                /*.mem_size   =*/ ctx_size,
                /*.mem_buffer =*/ nullptr,
                /*.no_alloc   =*/ false,
            };

            model.ctx = ggml_init(params);
            if (!model.ctx) {
                return false;
            }
        }

        // prepare memory for the weights
        {
            const auto & hparams = model.hparams;

            const int n_embd  = hparams.n_embd;
            const int n_layer = hparams.n_layer;
            const int n_vocab = hparams.n_vocab;

            model.layers.resize(n_layer);

            model.wte    = ggml_new_tensor_2d(ctx, wtype,         n_embd, n_vocab);

            model.ln_f_g = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);
            model.ln_f_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

            model.lmh_g  = ggml_new_tensor_2d(ctx, wtype,         n_embd, n_vocab);
            model.lmh_b  = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_vocab);

            // map by name
            model.tensors["transformer.wte.weight"] = model.wte;

            model.tensors["transformer.ln_f.weight"] = model.ln_f_g;
            model.tensors["transformer.ln_f.bias"]   = model.ln_f_b;

            model.tensors["lm_head.weight"] = model.lmh_g;
            model.tensors["lm_head.bias"]   = model.lmh_b;

            for (int i = 0; i < n_layer; ++i) {
                auto & layer = model.layers[i];

                layer.ln_1_g          = ggml_new_tensor_1d(ctx, GGML_TYPE_F32,   n_embd);
                layer.ln_1_b          = ggml_new_tensor_1d(ctx, GGML_TYPE_F32,   n_embd);

                layer.c_attn_q_proj_w = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);
                layer.c_attn_k_proj_w = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);
                layer.c_attn_v_proj_w = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);

                layer.c_attn_proj_w   = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);

                layer.c_mlp_fc_w      = ggml_new_tensor_2d(ctx, wtype,           n_embd, 4*n_embd);
                layer.c_mlp_fc_b      = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, 4*n_embd);

                layer.c_mlp_proj_w    = ggml_new_tensor_2d(ctx, wtype,         4*n_embd,   n_embd);
                layer.c_mlp_proj_b    = ggml_new_tensor_1d(ctx, GGML_TYPE_F32,   n_embd);

                // map by name
                model.tensors["transformer.h." + std::to_string(i) + ".ln_1.weight"]          = layer.ln_1_g;
                model.tensors["transformer.h." + std::to_string(i) + ".ln_1.bias"]            = layer.ln_1_b;

                model.tensors["transformer.h." + std::to_string(i) + ".attn.q_proj.weight"]   = layer.c_attn_q_proj_w;
                model.tensors["transformer.h." + std::to_string(i) + ".attn.k_proj.weight"]   = layer.c_attn_k_proj_w;
                model.tensors["transformer.h." + std::to_string(i) + ".attn.v_proj.weight"]   = layer.c_attn_v_proj_w;

                model.tensors["transformer.h." + std::to_string(i) + ".attn.out_proj.weight"] = layer.c_attn_proj_w;

                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_in.weight"]     = layer.c_mlp_fc_w;
                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_in.bias"]       = layer.c_mlp_fc_b;

                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_out.weight"]    = layer.c_mlp_proj_w;
                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_out.bias"]      = layer.c_mlp_proj_b;
            }
        }

        // key + value memory
        {
            const auto & hparams = model.hparams;

            const int n_embd  = hparams.n_embd;
            const int n_layer = hparams.n_layer;
            const int n_ctx   = hparams.n_ctx;

            const int n_mem      = n_layer*n_ctx;
            const int n_elements = n_embd*n_mem;

            model.memory_k = ggml_new_tensor_1d(ctx, GGML_TYPE_F16, n_elements);
            model.memory_v = ggml_new_tensor_1d(ctx, GGML_TYPE_F16, n_elements);
        }

        // load weights
        {
            int n_tensors = 0;

            while (true) {
                int32_t n_dims;
                int32_t length;
                int32_t ttype;

                fin.read(reinterpret_cast<char *>(&n_dims), sizeof(n_dims));
                fin.read(reinterpret_cast<char *>(&length), sizeof(length));
                fin.read(reinterpret_cast<char *>(&ttype),  sizeof(ttype));

                if (fin.eof()) {
                    break;
                }

                int32_t nelements = 1;
                int32_t ne[2] = { 1, 1 };
                for (int i = 0; i < n_dims; ++i) {
                    fin.read(reinterpret_cast<char *>(&ne[i]), sizeof(ne[i]));
                    nelements *= ne[i];
                }

                std::string nname(length, 0);
                fin.read(nname.data(), length);

                if (model.tensors.find(nname) == model.tensors.end()) {
                    return false;
                }

                auto* tensor = model.tensors[nname];
                if (ggml_nelements(tensor) != nelements) {
                    return false;
                }

                if (tensor->ne[0] != ne[0] || tensor->ne[1] != ne[1]) {
                    return false;
                }

                const size_t bpe = ggml_type_size(ggml_type(ttype));

                if ((nelements*bpe)/ggml_blck_size(tensor->type) != ggml_nbytes(tensor)) {
                    return false;
                }

                fin.read(reinterpret_cast<char *>(tensor->data), ggml_nbytes(tensor));

                if (++n_tensors % 8 == 0) {
                    std::cout << ".";
                    std::cout.flush();
                }
            }

            std::cout << "done" << std::endl;
        }

        fin.close();

        return true;
    }

    void gpt_split_words(std::string_view str, std::vector<std::string> & words) const
    {
        // Originally "   a" would be split into "  " and " a" words
        // Now it's "   " and "a" for simplicity because RE2 has no negative lookaheads like (?!\S)
        static const RE2 pattern{R"(('s|'t|'re|'ve|'m|'ll|'d| ?[[:alpha:]]+| ?[[:digit:]]+| ?[^\s[:alpha:][:digit:]]+|\s+))"};
        std::string_view match;
        while (RE2::FindAndConsume(&str, pattern, &match))
            words.emplace_back(match);
    }

    std::string get_tokens_match_group(const std::vector<std::string> & tokens) const
    {
        assert(!tokens.empty());
        static const std::string to_escape{R"([\^$.|?*+(){})"};
        std::string res = "(";
        for (const std::string & token : tokens)
        {
            res.reserve(res.size() + token.size() + 1);
            for (char c : token)
            {
                if (to_escape.contains(c))
                    res += '\\';
                res += c;
            }
            res += '|';
        }
        res.pop_back();  // remove last '|'
        res += ")";
        return res;
    }

    std::vector<gpt_vocab::id> gpt_tokenize(const gpt_vocab & vocab, const std::string & text) const {
        std::vector<std::string> words;

        // first split the text into words
        {
            std::string_view text_view{text};
            if (!vocab.special_tokens.empty())
            {
                // I am not sure that this branch is possible
                const RE2 re{get_tokens_match_group(vocab.special_tokens)};
                std::string_view input{text_view};
                std::string_view match;
                while (RE2::FindAndConsume(&input, re, &match))
                {
                    std::string_view prefix = text_view.substr(0, text_view.size() - input.size() - match.size());
                    gpt_split_words(prefix, words);
                    words.emplace_back(match);
                    text_view = input;
                }
            }
            gpt_split_words(text_view, words);
        }

        // find the longest token that forms each word in words:
        std::vector<gpt_vocab::id> tokens;
        for (const auto & word : words) {
            for (int i = 0; i < static_cast<int>(word.size()); ){
                for (int j = static_cast<int>(word.size()) - 1; j >= i; j--){
                    auto cand = word.substr(i, j-i+1);
                    auto it = vocab.token_to_id.find(cand);
                    if (it != vocab.token_to_id.end()){ // word.substr(i, j-i+1) in vocab
                        tokens.push_back(it->second);
                        i = j + 1;
                        break;
                    }
                    else if (j == i){ // word.substr(i, 1) has no matching
                        i++;
                    }
                }
            }
        }

        return tokens;
    }
    // evaluate the transformer
    //
    //   - model:     the model
    //   - n_threads: number of threads to use
    //   - n_past:    the context size so far
    //   - embd_inp:  the embeddings of the tokens in the context
    //   - embd_w:    the predicted logits for the next token
    //
    // The GPT-J model requires about 16MB of memory per input token.
    //
    bool gptj_eval(
            const gptj_model & model,
            const int n_threads,
            const int n_past,
            const std::vector<gpt_vocab::id> & embd_inp,
                std::vector<float>         & embd_w,
                size_t                     & mem_per_token) const {
        const int N = static_cast<int>(embd_inp.size());

        const auto & hparams = model.hparams;

        const int n_embd  = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_ctx   = hparams.n_ctx;
        const int n_head  = hparams.n_head;
        const int n_vocab = hparams.n_vocab;
        const int n_rot   = hparams.n_rot;

        static size_t buf_size = 256u*1024*1024;
        static void * buf = malloc(buf_size);

        if (mem_per_token > 0 && mem_per_token*N > buf_size) {
            const size_t buf_size_new = static_cast<size_t>(1.1*(mem_per_token*N)); // add 10% to account for ggml object overhead
            //printf("\n%s: reallocating buffer from %zu to %zu bytes\n", __func__, buf_size, buf_size_new);

            // reallocate
            buf_size = buf_size_new;
            buf = realloc(buf, buf_size);
            if (buf == nullptr) {
                return false;
            }
        }

        struct ggml_init_params params = {
            /*.mem_size   =*/ buf_size,
            /*.mem_buffer =*/ buf,
            /*.no_alloc   =*/ false,
        };

        struct ggml_context * ctx0 = ggml_init(params);
        struct ggml_cgraph * gf = ggml_new_graph(ctx0);

        // KQ_pos - contains the positions
        struct ggml_tensor * KQ_pos = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, N);
        int * data = reinterpret_cast<int*>(KQ_pos->data);
        for (int i = 0; i < N; ++i) {
            data[i] = n_past + i;
        }

        struct ggml_tensor * embd = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, N);
        memcpy(embd->data, embd_inp.data(), N*ggml_element_size(embd));

        // wte
        struct ggml_tensor * inpL = ggml_get_rows(ctx0, model.wte, embd);

        for (int il = 0; il < n_layer; ++il) {
            struct ggml_tensor * cur;

            // norm
            {
                cur = ggml_norm(ctx0, inpL, hparams.eps);

                // cur = ln_1_g*cur + ln_1_b
                cur = ggml_add(ctx0,
                        ggml_mul(ctx0,
                            ggml_repeat(ctx0, model.layers[il].ln_1_g, cur),
                            cur),
                        ggml_repeat(ctx0, model.layers[il].ln_1_b, cur));
            }

            struct ggml_tensor * inpSA = cur;

            // self-attention
            {
                struct ggml_tensor * Qcur = ggml_rope_inplace(ctx0, ggml_reshape_3d(ctx0, ggml_mul_mat(ctx0, model.layers[il].c_attn_q_proj_w, cur), n_embd/n_head, n_head, N), KQ_pos, n_rot, 0, 0);
                struct ggml_tensor * Kcur = ggml_rope_inplace(ctx0, ggml_reshape_3d(ctx0, ggml_mul_mat(ctx0, model.layers[il].c_attn_k_proj_w, cur), n_embd/n_head, n_head, N), KQ_pos, n_rot, 0, 0);

                // store key and value to memory
                {
                    struct ggml_tensor * Vcur = ggml_transpose(ctx0, ggml_mul_mat(ctx0, model.layers[il].c_attn_v_proj_w, cur));

                    struct ggml_tensor * k = ggml_view_1d(ctx0, model.memory_k, N*n_embd, (ggml_element_size(model.memory_k)*n_embd)*(il*n_ctx + n_past));
                    struct ggml_tensor * v = ggml_view_2d(ctx0, model.memory_v, N, n_embd,
                            (   n_ctx)*ggml_element_size(model.memory_v),
                            (il*n_ctx)*ggml_element_size(model.memory_v)*n_embd + n_past*ggml_element_size(model.memory_v));

                    ggml_build_forward_expand(gf, ggml_cpy(ctx0, Kcur, k));
                    ggml_build_forward_expand(gf, ggml_cpy(ctx0, Vcur, v));
                }

                // Q = Qcur.contiguous().view(n_embd/n_head, n_head, N).permute(0, 2, 1, 3)
                struct ggml_tensor * Q =
                    ggml_permute(ctx0,
                            Qcur,
                            0, 2, 1, 3);

                // K = Kmem.view(n_embd/n_head, n_head, n_past + N).permute(0, 2, 1, 3)
                struct ggml_tensor * K =
                    ggml_permute(ctx0,
                            ggml_reshape_3d(ctx0,
                                ggml_view_1d(ctx0, model.memory_k, (n_past + N)*n_embd, il*n_ctx*ggml_element_size(model.memory_k)*n_embd),
                                n_embd/n_head, n_head, n_past + N),
                            0, 2, 1, 3);

                // K * Q
                struct ggml_tensor * KQ = ggml_mul_mat(ctx0, K, Q);

                // KQ_scaled = KQ / sqrt(n_embd/n_head)
                struct ggml_tensor * KQ_scaled =
                    ggml_scale_inplace(ctx0,
                            KQ,
                            1.0f/sqrt(float(n_embd)/n_head));

                // KQ_masked = mask_past(KQ_scaled)
                struct ggml_tensor * KQ_masked = ggml_diag_mask_inf_inplace(ctx0, KQ_scaled, n_past);

                // KQ = soft_max(KQ_masked)
                struct ggml_tensor * KQ_soft_max = ggml_soft_max_inplace(ctx0, KQ_masked);

                // V_trans = Vmem.view(n_embd/n_head, n_head, n_past + N).permute(1, 2, 0, 3).contiguous()
                struct ggml_tensor * V =
                    ggml_view_3d(ctx0, model.memory_v,
                            n_past + N, n_embd/n_head, n_head,
                            n_ctx*ggml_element_size(model.memory_v),
                            n_ctx*ggml_element_size(model.memory_v)*n_embd/n_head,
                            il*n_ctx*ggml_element_size(model.memory_v)*n_embd);

                // KQV = transpose(V) * KQ_soft_max
                struct ggml_tensor * KQV = ggml_mul_mat(ctx0, V, KQ_soft_max);

                // KQV_merged = KQV.permute(0, 2, 1, 3)
                struct ggml_tensor * KQV_merged = ggml_permute(ctx0, KQV, 0, 2, 1, 3);

                // cur = KQV_merged.contiguous().view(n_embd, N)
                cur = ggml_cpy(ctx0,
                        KQV_merged,
                        ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, n_embd, N));

                // projection (no bias)
                cur = ggml_mul_mat(ctx0,
                        model.layers[il].c_attn_proj_w,
                        cur);
            }

            struct ggml_tensor * inpFF = cur;

            // feed-forward network
            // this is independent of the self-attention result, so it could be done in parallel to the self-attention
            {
                // note here we pass inpSA instead of cur
                cur = ggml_mul_mat(ctx0,
                        model.layers[il].c_mlp_fc_w,
                        inpSA);

                cur = ggml_add(ctx0,
                        ggml_repeat(ctx0, model.layers[il].c_mlp_fc_b, cur),
                        cur);

                // GELU activation
                cur = ggml_gelu(ctx0, cur);

                // projection
                // cur = proj_w*cur + proj_b
                cur = ggml_mul_mat(ctx0,
                        model.layers[il].c_mlp_proj_w,
                        cur);

                cur = ggml_add(ctx0,
                        ggml_repeat(ctx0, model.layers[il].c_mlp_proj_b, cur),
                        cur);
            }

            // self-attention + FF
            cur  = ggml_add(ctx0, cur, inpFF);

            // input for next layer
            inpL = ggml_add(ctx0, cur, inpL);
        }

        // norm
        {
            inpL = ggml_norm(ctx0, inpL, hparams.eps);

            // inpL = ln_f_g*inpL + ln_f_b
            inpL = ggml_add(ctx0,
                    ggml_mul(ctx0,
                        ggml_repeat(ctx0, model.ln_f_g, inpL),
                        inpL),
                    ggml_repeat(ctx0, model.ln_f_b, inpL));
        }

        // lm_head
        {
            inpL = ggml_mul_mat(ctx0, model.lmh_g, inpL);

            inpL = ggml_add(ctx0,
                    ggml_repeat(ctx0, model.lmh_b, inpL),
                    inpL);
        }

        // logits -> probs
        //inpL = ggml_soft_max_inplace(ctx0, inpL);

        // run the computation
        ggml_build_forward_expand(gf, inpL);
        ggml_graph_compute_with_ctx(ctx0, gf, n_threads);

        //if (n_past%100 == 0) {
        //    ggml_graph_print   (&gf);
        //    ggml_graph_dump_dot(&gf, NULL, "gpt-j.dot");
        //}

        //embd_w.resize(n_vocab*N);
        //memcpy(embd_w.data(), ggml_get_data(inpL), sizeof(float)*n_vocab*N);

        // return result for just the last token
        embd_w.resize(n_vocab);
        memcpy(embd_w.data(), reinterpret_cast<float*>(ggml_get_data(inpL)) + (n_vocab*(N-1)), sizeof(float)*n_vocab);

        if (mem_per_token == 0) {
            mem_per_token = ggml_used_mem(ctx0)/N;
        }
        //printf("used_mem = %zu\n", ggml_used_mem(ctx0));

        ggml_free(ctx0);

        return true;
    }

    gpt_vocab::id gpt_sample_top_k_top_p(
            const gpt_vocab & vocab,
            const float * logits,
            int    top_k,
            double top_p,
            double temp,
            std::mt19937 & rng) const {
        int n_logits = static_cast<int>(vocab.id_to_token.size());

        std::vector<std::pair<double, gpt_vocab::id>> logits_id;
        logits_id.reserve(n_logits);

        {
            const double scale = 1.0/temp;
            for (int i = 0; i < n_logits; ++i) {
                logits_id.push_back(std::make_pair(logits[i]*scale, i));
            }
        }

        // find the top K tokens
        std::partial_sort(
                logits_id.begin(),
                logits_id.begin() + top_k, logits_id.end(),
                [](const std::pair<double, gpt_vocab::id> & a, const std::pair<double, gpt_vocab::id> & b) {
            return a.first > b.first;
        });

        logits_id.resize(top_k);

        double maxl = -INFINITY;
        for (const auto & kv : logits_id) {
            maxl = std::max(maxl, kv.first);
        }

        // compute probs for the top K tokens
        std::vector<double> probs;
        probs.reserve(logits_id.size());

        double sum = 0.0;
        for (const auto & kv : logits_id) {
            double p = exp(kv.first - maxl);
            probs.push_back(p);
            sum += p;
        }

        // normalize the probs
        for (auto & p : probs) {
            p /= sum;
        }

        if (top_p < 1.0f) {
            double cumsum = 0.0f;
            for (int i = 0; i < top_k; i++) {
                cumsum += probs[i];
                if (cumsum >= top_p) {
                    top_k = i + 1;
                    probs.resize(top_k);
                    logits_id.resize(top_k);
                    break;
                }
            }

            cumsum = 1.0/cumsum;
            for (double & prob : probs) {
                prob *= cumsum;
            }
        }

        //printf("\n");
        //for (int i = 0; i < (int) probs.size(); i++) {
        //    printf("%d: '%s' %f\n", i, vocab.id_to_token.at(logits_id[i].second).c_str(), probs[i]);
        //}
        //exit(0);

        std::discrete_distribution<> dist(probs.begin(), probs.end());
        int idx = dist(rng);

        return logits_id[idx].second;
    }
    #undef return


public:
    static constexpr auto name = "ggmlEvaluate";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGGMLEvaluate>(context_); }

    explicit FunctionGGMLEvaluate(ContextPtr context_) : WithContext(context_) {}
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 1 argument", getName());
        if (arguments.size() > 1)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 1 argument", getName());
        // const auto * name_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        // if (!name_col)
        //     throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of function {} must be a string", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        std::cout << "GGML!!!" << std::endl;
        std::cout << "input_rows_count is : " << input_rows_count << std::endl;
        std::cout << "result_type is : " << result_type->getName() << std::endl;
        if (input_rows_count == 0) {
            ColumnPtr res = arguments[0].column;
            return res;
        }

        gpt_params params;
        gpt_vocab vocab;
        gptj_model model;

        params.model = "/home/ArtNext/ggml-model.bin";

        if (!gptj_model_load(params.model, model, vocab)) {
            throw Exception(ErrorCodes::SYNTAX_ERROR, "No");
        }

        const auto& vals = *arguments[0].column.get();
        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);
        UInt64 totalsize = 0;
        std::vector<String> result_raw(input_rows_count);
        int initial_n_predict = params.n_predict;
        std::mt19937 rng(params.seed);

        for (size_t j = 0; j < input_rows_count; ++j) {
            Field field;
            field = vals[j]; // get(i, field);
            std::string val;
            if (!field.tryGet(val)) {
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Nasrali");
            }
            else {
                std::vector<gpt_vocab::id> embd_inp = gpt_tokenize(vocab, val);
                std::vector<gpt_vocab::id> embd;

                // std::string result;
                // for (auto &x : embd_inp) {
                //     result += std::to_string(x) + " ";
                // }
                // if (!result.empty()) {
                //     result.pop_back();
                // }
                // result = "Tokenized '" + val + "' is '" + result + "'";
                // determine the required inference memory per token:

                int n_past = 0;
                std::vector<float> logits;
                params.n_predict = std::min(initial_n_predict, model.hparams.n_ctx - static_cast<int>(embd_inp.size()));

                size_t mem_per_token = 0;
                gptj_eval(model, params.n_threads, 0, { 0, 1, 2, 3 }, logits, mem_per_token);

                std::string result;

                for (size_t i = embd.size(); i < embd_inp.size() + params.n_predict; i++) {
                    // predict
                    if (!embd.empty()) {
                        if (!gptj_eval(model, params.n_threads, n_past, embd, logits, mem_per_token)) {

                            throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to predict");
                        }
                    }

                    n_past += embd.size();
                    embd.clear();

                    if (i >= embd_inp.size()) {
                        // sample next token
                        const int   top_k = params.top_k;
                        const float top_p = params.top_p;
                        const float temp  = params.temp;

                        const int n_vocab = model.hparams.n_vocab;

                        gpt_vocab::id id = 0;

                        id = gpt_sample_top_k_top_p(vocab, logits.data() + (logits.size() - n_vocab), top_k, top_p, temp, rng);

                        // add it to the context
                        embd.push_back(id);
                    } else {
                        // if here, it means we are still processing the input prompt
                        for (size_t k = i; k < embd_inp.size(); k++) {
                            embd.push_back(embd_inp[k]);
                            if (int32_t(embd.size()) > params.n_batch) {
                                break;
                            }
                        }
                        i += embd.size() - 1;
                    }

                    // display text
                    for (auto id : embd) {
                        result += vocab.id_to_token[id];
                    }

                    // end of text token
                    if (embd.back() == 50256) {
                        break;
                    }
                }

                ggml_free(model.ctx);

                result_raw[j] = std::move(result);
                totalsize += result_raw[j].size() + 1;
            }
        }
        col_res->getChars().resize(totalsize);
        col_res->getOffsets().resize(input_rows_count);
        auto* data_ptr = col_res->getChars().data();
        UInt64 offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            memcpy(data_ptr + offset, result_raw[i].data(), result_raw[i].size());
            data_ptr[offset + result_raw[i].size()] = '\0';
            offset += result_raw[i].size() + 1;
            col_res->getOffsets()[i] = offset;
        }

        // params.n_predict = std::min(params.n_predict, model.hparams.n_ctx - static_cast<int>(embd_inp.size()));
        // std::vector<gpt_vocab::id> embd;

        // size_t mem_per_token = 0;
        // gptj_eval(model, params.n_threads, 0, { 0, 1, 2, 3 }, logits, mem_per_token);


        std::cout << "Success!!!" << std::endl;
        return col_res;
    }
};


REGISTER_FUNCTION(GGMLEvaluate)
{
    factory.registerFunction<FunctionGGMLEvaluate>();
}

}
