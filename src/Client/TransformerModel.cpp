#include "TransformerModel.h"
#include "ggml.h"


#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <fstream>
#include <cmath>
#include <numeric>
#include <Parsers/Lexer.h>
#include <fmt/core.h>

#if defined(_MSC_VER)
#pragma warning(disable: 4244 4267) // possible loss of data
#endif

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

std::string replace(const std::string & s, const std::string & from, const std::string & to) {
    std::string result = s;
    size_t pos = 0;
    while ((pos = result.find(from, pos)) != std::string::npos) {
        result.replace(pos, from.length(), to);
        pos += to.length();
    }
    return result;
}

void GptVocab::addSpecialToken(const std::string & token) {
    special_tokens.push_back(token);
}


GPTJModel::GPTJModel(const std::string & file_name) {
    fname = file_name;
    model_loaded = loadModel(fname);
    reset();
}

const GptVocab::token GPTJModel::unk = "<UNK>";
const GptVocab::token GPTJModel::bos = "<BOS>";
const GptVocab::token GPTJModel::eos = "<EOS>";
const GptVocab::token GPTJModel::pad = "<PAD>";

const GptVocab::token GPTJModel::literal = "Literal";
const GptVocab::token GPTJModel::identifier = "Identifier";
const GptVocab::token GPTJModel::operator_token = "Operator";

bool GPTJModel::loadModel(const std::string & file_name) {
    out_err << fmt::format("Loading model from '{}' - please wait ...\n", file_name);
    out_err.next();

    auto fin = std::ifstream(file_name, std::ios::binary);
    if (!fin) {
        out_err << fmt::format("{}: failed to open '{}'\n", __func__, file_name);
        out_err.next();
        return false;
    }

    // verify magic
    {
        uint32_t magic;
        fin.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (magic != GGML_FILE_MAGIC) {
            out_err << fmt::format("{}: invalid model file '{}' (bad magic)\n", __func__, file_name);
            out_err.next();
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

        //const int32_t qntvr = hparams.ftype / GGML_QNT_VERSION_FACTOR;

        // printf("%s: n_vocab = %d\n", __func__, hparams.n_vocab);
        // printf("%s: n_ctx   = %d\n", __func__, hparams.n_ctx);
        // printf("%s: n_embd  = %d\n", __func__, hparams.n_embd);
        // printf("%s: n_head  = %d\n", __func__, hparams.n_head);
        // printf("%s: n_layer = %d\n", __func__, hparams.n_layer);
        // printf("%s: n_rot   = %d\n", __func__, hparams.n_rot);
        // printf("%s: ftype   = %d\n", __func__, hparams.ftype);
        // printf("%s: qntvr   = %d\n", __func__, qntvr);

        hparams.ftype %= GGML_QNT_VERSION_FACTOR;
    }

    // load vocab
    {
        int32_t n_vocab = 0;
        fin.read(reinterpret_cast<char*>(&n_vocab), sizeof(n_vocab));

        if (n_vocab != model.hparams.n_vocab) {
            out_err << fmt::format("{}: invalid model file '{}' (bad vocab size {} != {})\n", __func__, file_name, n_vocab, model.hparams.n_vocab);
            out_err.next();
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
    ggml_type wtype = ggml_ftype_to_ggml_type(static_cast<ggml_ftype>(model.hparams.ftype));
    if (wtype == GGML_TYPE_COUNT) {
        out_err << fmt::format("{}: invalid model file '{}' (bad ftype value {})\n", __func__, file_name, model.hparams.ftype);
        out_err.next();
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
            out_err << fmt::format("{}: ggml_init() failed\n", __func__);
            out_err.next();
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
        size_t total_size = 0;

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

            std::string name(length, 0);
            fin.read(name.data(), length);

            if (model.tensors.find(name) == model.tensors.end()) {
                out_err << fmt::format("{}: unknown tensor '{}' in model file\n", __func__, name);
                out_err.next();
                return false;
            }

            auto *tensor = model.tensors[name];
            if (ggml_nelements(tensor) != nelements) {
                out_err << fmt::format("{}: tensor '{}' has wrong size in model file\n", __func__, name);
                out_err.next();
                return false;
            }

            if (tensor->ne[0] != ne[0] || tensor->ne[1] != ne[1]) {
                out_err << fmt::format("{}: tensor '{}' has wrong shape in model file: got [{}, {}], expected [{}, {}]\n",
                    __func__, name, static_cast<int>(tensor->ne[0]), static_cast<int>(tensor->ne[1]), ne[0], ne[1]);
                out_err.next();
                return false;
            }

            const size_t bpe = ggml_type_size(ggml_type(ttype));

            if ((nelements*bpe)/ggml_blck_size(tensor->type) != ggml_nbytes(tensor)) {
                out_err << fmt::format("{}: tensor '{}' has wrong size in model file: got {}, expected {}\n",
                    __func__, name, ggml_nbytes(tensor), nelements*bpe);
                out_err.next();
                return false;
            }

            fin.read(reinterpret_cast<char *>(tensor->data), ggml_nbytes(tensor));

            total_size += ggml_nbytes(tensor);
        }

        out_err << fmt::format("Done loading model '{}'\n", file_name);
        out_err.next();
        
        out_err << fmt::format("{}: model size = {} MB / num tensors = {}\n",  __func__, total_size/1024.0/1024.0, n_tensors);
        out_err.next();
    }

    fin.close();

    return true;
}

std::vector<GptVocab::id> GPTJModel::tokens2Ids(const std::vector<std::string> & tokens) {
    if (tokens.empty()) {
        return std::vector<GptVocab::id>{vocab.token_to_id.at(bos)};
    }
    std::vector<GptVocab::id> ids{};
    
    for (const auto & token : tokens) {
        if (vocab.token_to_id.find(token) == vocab.token_to_id.end()) {
            ids.push_back(vocab.token_to_id.at(unk));
            continue;
        }
        ids.push_back(vocab.token_to_id.at(token));
    }

    return ids;
}

bool GPTJModel::gptjEval(const std::vector<GptVocab::id> & ids_input,
              std::vector<float>         & logits) {
    const size_t n = ids_input.size();

    const auto & hparams = model.hparams;

    const int n_embd  = hparams.n_embd;
    const int n_layer = hparams.n_layer;
    const int n_ctx   = hparams.n_ctx;
    const int n_head  = hparams.n_head;
    const int n_vocab = hparams.n_vocab;
    const int n_rot   = hparams.n_rot;

    static size_t buf_size = 256u*1024*1024;
    static void * buf = malloc(buf_size);

    if (mem_per_token > 0 && mem_per_token*n > buf_size) {
        const size_t buf_size_new = static_cast<size_t>(1.1*(mem_per_token*n)); // add 10% to account for ggml object overhead
        //printf("\n%s: reallocating buffer from %zu to %zu bytes\n", __func__, buf_size, buf_size_new);

        // reallocate
        buf_size = buf_size_new;
        void* temp = realloc(buf, buf_size);
        if (temp == nullptr) {
            out_err << fmt::format("{}: failed to allocate {} bytes\n", __func__, buf_size);
            out_err.next();
            return false;
        }
        buf = temp;
    }

    struct ggml_init_params params = {
        /*.mem_size   =*/ buf_size,
        /*.mem_buffer =*/ buf,
        /*.no_alloc   =*/ false,
    };

    struct ggml_context * ctx0 = ggml_init(params);
    struct ggml_cgraph * gf = ggml_new_graph(ctx0);

    // KQ_pos - contains the positions
    struct ggml_tensor * kq_pos = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n);
    int * data = static_cast<int *>(kq_pos->data);
    for (size_t i = 0; i < n; ++i) {
        data[i] = static_cast<int>(n_past + i);
    }

    struct ggml_tensor * embd = ggml_new_tensor_1d(ctx0, GGML_TYPE_I32, n);
    memcpy(embd->data, ids_input.data(), n*ggml_element_size(embd));

    // wte
    struct ggml_tensor * inp_l = ggml_get_rows(ctx0, model.wte, embd);

    for (int il = 0; il < n_layer; ++il) {
        struct ggml_tensor * cur;

        // norm
        {
            cur = ggml_norm(ctx0, inp_l, hparams.eps);

            // cur = ln_1_g*cur + ln_1_b
            cur = ggml_add(ctx0,
                    ggml_mul(ctx0,
                        ggml_repeat(ctx0, model.layers[il].ln_1_g, cur),
                        cur),
                    ggml_repeat(ctx0, model.layers[il].ln_1_b, cur));
        }

        struct ggml_tensor * inp_sa = cur;

        // self-attention
        {
            struct ggml_tensor * qcur = ggml_rope_inplace(ctx0, ggml_reshape_3d(ctx0, ggml_mul_mat(ctx0, model.layers[il].c_attn_q_proj_w, cur), n_embd/n_head, n_head, n), kq_pos, n_rot, 0);
            struct ggml_tensor * kcur = ggml_rope_inplace(ctx0, ggml_reshape_3d(ctx0, ggml_mul_mat(ctx0, model.layers[il].c_attn_k_proj_w, cur), n_embd/n_head, n_head, n), kq_pos, n_rot, 0);

            // store key and value to memory
            {
                struct ggml_tensor * vcur = ggml_transpose(ctx0, ggml_mul_mat(ctx0, model.layers[il].c_attn_v_proj_w, cur));

                struct ggml_tensor * k = ggml_view_1d(ctx0, model.memory_k, n*n_embd, (ggml_element_size(model.memory_k)*n_embd)*(il*n_ctx + n_past));
                struct ggml_tensor * v = ggml_view_2d(ctx0, model.memory_v, n, n_embd,
                        (   n_ctx)*ggml_element_size(model.memory_v),
                        (il*n_ctx)*ggml_element_size(model.memory_v)*n_embd + n_past*ggml_element_size(model.memory_v));

                ggml_build_forward_expand(gf, ggml_cpy(ctx0, kcur, k));
                ggml_build_forward_expand(gf, ggml_cpy(ctx0, vcur, v));
            }

            // Q = Qcur.contiguous().view(n_embd/n_head, n_head, N).permute(0, 2, 1, 3)
            struct ggml_tensor * q =
                ggml_permute(ctx0,
                        qcur,
                        0, 2, 1, 3);

            // K = Kmem.view(n_embd/n_head, n_head, n_past + N).permute(0, 2, 1, 3)
            struct ggml_tensor * k =
                ggml_permute(ctx0,
                        ggml_reshape_3d(ctx0,
                            ggml_view_1d(ctx0, model.memory_k, (n_past + n)*n_embd, il*n_ctx*ggml_element_size(model.memory_k)*n_embd),
                            n_embd/n_head, n_head, n_past + n),
                        0, 2, 1, 3);

            // K * Q
            struct ggml_tensor * kq = ggml_mul_mat(ctx0, k, q);

            // KQ_scaled = KQ / sqrt(n_embd/n_head)
            struct ggml_tensor * kq_scaled =
                ggml_scale_inplace(ctx0,
                        kq,
                        1.0f/sqrt(float(n_embd)/n_head));

            // KQ_masked = mask_past(KQ_scaled)
            struct ggml_tensor * kq_masked = ggml_diag_mask_inf_inplace(ctx0, kq_scaled, n_past);

            // KQ = soft_max(KQ_masked)
            struct ggml_tensor * kq_soft_max = ggml_soft_max_inplace(ctx0, kq_masked);

            // V_trans = Vmem.view(n_embd/n_head, n_head, n_past + N).permute(1, 2, 0, 3).contiguous()
            struct ggml_tensor * v =
                ggml_view_3d(ctx0, model.memory_v,
                        n_past + n, n_embd/n_head, n_head,
                        n_ctx*ggml_element_size(model.memory_v),
                        n_ctx*ggml_element_size(model.memory_v)*n_embd/n_head,
                        il*n_ctx*ggml_element_size(model.memory_v)*n_embd);

            // KQV = transpose(V) * KQ_soft_max
            struct ggml_tensor * kqv = ggml_mul_mat(ctx0, v, kq_soft_max);

            // KQV_merged = KQV.permute(0, 2, 1, 3)
            struct ggml_tensor * kqv_merged = ggml_permute(ctx0, kqv, 0, 2, 1, 3);

            // cur = KQV_merged.contiguous().view(n_embd, N)
            cur = ggml_cpy(ctx0,
                    kqv_merged,
                    ggml_new_tensor_2d(ctx0, GGML_TYPE_F32, n_embd, n));

            // projection (no bias)
            cur = ggml_mul_mat(ctx0,
                    model.layers[il].c_attn_proj_w,
                    cur);
        }

        struct ggml_tensor * inp_ff = cur;

        // feed-forward network
        // this is independent of the self-attention result, so it could be done in parallel to the self-attention
        {
            // note here we pass inpSA instead of cur
            cur = ggml_mul_mat(ctx0,
                    model.layers[il].c_mlp_fc_w,
                    inp_sa);

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
        cur  = ggml_add(ctx0, cur, inp_ff);

        // input for next layer
        inp_l = ggml_add(ctx0, cur, inp_l);
    }

    // norm
    {
        inp_l = ggml_norm(ctx0, inp_l, hparams.eps);

        // inpL = ln_f_g*inpL + ln_f_b
        inp_l = ggml_add(ctx0,
                ggml_mul(ctx0,
                    ggml_repeat(ctx0, model.ln_f_g, inp_l),
                    inp_l),
                ggml_repeat(ctx0, model.ln_f_b, inp_l));
    }

    // lm_head
    {
        inp_l = ggml_mul_mat(ctx0, model.lmh_g, inp_l);

        inp_l = ggml_add(ctx0,
                ggml_repeat(ctx0, model.lmh_b, inp_l),
                inp_l);
    }

    // logits -> probs
    //inpL = ggml_soft_max_inplace(ctx0, inpL);

    // run the computation
    ggml_build_forward_expand(gf, inp_l);
    ggml_graph_compute_with_ctx(ctx0, gf, n_threads);

    //if (n_past%100 == 0) {
    //    ggml_graph_print   (&gf);
    //    ggml_graph_dump_dot(&gf, NULL, "gpt-j.dot");
    //}

    //embd_w.resize(n_vocab*N);
    //memcpy(embd_w.data(), ggml_get_data(inpL), sizeof(float)*n_vocab*N);

    // return result for just the last token
    logits.resize(n_vocab);
    memcpy(logits.data(), reinterpret_cast<float *>(ggml_get_data(inp_l)) + (n_vocab*(n-1)), sizeof(float)*n_vocab);

    if (mem_per_token == 0) {
        mem_per_token = ggml_used_mem(ctx0)/n;
    }
    //printf("used_mem = %zu\n", ggml_used_mem(ctx0));

    ggml_free(ctx0);

    return true;
    }

void GPTJModel::reset() {
        if (!model_loaded) {
            return;
        }
        n_past = 0;
        current_query_ids.clear();
        current_query_ids.push_back(vocab.token_to_id.at(bos));

        std::vector<float> _;

        eval_success = gptjEval(current_query_ids, _);
        n_past += current_query_ids.size();
        last_recs.clear();
    }

std::vector<GptVocab::id> GPTJModel::getTopNIdsFromLogits(const std::vector<float>& logits, size_t top_n) {
    std::vector<GptVocab::id> sorted_idx(logits.size());
    std::iota(sorted_idx.begin(), sorted_idx.end(), 0);

    std::stable_sort(sorted_idx.begin(), sorted_idx.end(),
        [&logits](size_t i1, size_t i2) {return logits[i1] > logits[i2];});

    return std::vector<GptVocab::id>(sorted_idx.begin(), sorted_idx.begin() + top_n);
}

std::vector<std::string> GPTJModel::ids2Tokens(const std::vector<GptVocab::id> & ids) {
    std::vector<std::string> result;
    result.reserve(ids.size());

    for (auto id: ids) {
        result.push_back(vocab.id_to_token.at(id));
    }
    
    return result;
}

bool hasCommonPrefix(const std::vector<GptVocab::id>& cached_query, const std::vector<GptVocab::id>& new_ids) {
    return new_ids.size() >= cached_query.size() - 1 &&
           std::equal(cached_query.begin() + 1, cached_query.end(), new_ids.begin());
}

std::vector<GptVocab::id> getNewElements(const std::vector<GptVocab::id>& cached_query, const std::vector<GptVocab::id>& new_ids) {
    if (hasCommonPrefix(cached_query, new_ids)) {
        return std::vector<GptVocab::id>(new_ids.begin() + (cached_query.size() - 1), new_ids.end());
    }
    return {};
}

/// TODO: replace const vector reference on span here??
std::vector<std::string> GPTJModel::getRecsTopN(const std::vector<std::string>& tokens, size_t top_n) {
    if (!model_loaded || !eval_success) {
        return {};
    }
    auto ids = tokens2Ids(tokens);

    if (!hasCommonPrefix(current_query_ids, ids)) {
        this->reset();
    } else {
        ids = getNewElements(current_query_ids, ids);
        if (ids.empty()) {
            return last_recs; /// TODO: return last recs
        }
    }

    if (static_cast<int>(ids.size()) > model.hparams.n_ctx) {
        ids = std::vector<GptVocab::id>(ids.end() - model.hparams.n_ctx, ids.end());
    }


    std::vector<GptVocab::id> batch_ids{};
    batch_ids.reserve(n_batch);

    std::vector<float> logits;
    for (int batch_num = 0; batch_num != std::ceil(float(ids.size()) / float(n_batch)); ++batch_num) {
        for (int i = batch_num * n_batch; (i != (batch_num + 1) * n_batch) && (i < int(ids.size())); ++i) {
            batch_ids.push_back(ids[i]);
        }
        eval_success = gptjEval(batch_ids, logits);
        if (!eval_success) {
            return {};
        }
        n_past += batch_ids.size();
    }

    current_query_ids.insert(current_query_ids.end(), ids.begin(), ids.end());

    auto top_n_ids = getTopNIdsFromLogits(logits, top_n);

    last_recs = ids2Tokens(top_n_ids);
    return last_recs;
}


