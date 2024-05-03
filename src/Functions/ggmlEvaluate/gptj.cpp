#include "gptj.h"

#include <Functions/ggmlEvaluate/gpt_common.h>

#include <fstream>
#include <iostream>

namespace DB
{

// load the model's weights from a file
bool GptJModel::doLoad(const std::string & fname)
{
    std::cout << "GptJModel::doLoad\n";
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

    std::cout << "Verified magic\n";

    // load hparams
    {
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

        if (n_vocab != hparams.n_vocab) {
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
    ggml_type wtype = ggml_ftype_to_ggml_type(static_cast<enum ggml_ftype>(hparams.ftype));
    if (wtype == GGML_TYPE_COUNT) {
        return false;
    }

    size_t ctx_size = 0;

    {
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

        ctx = ggml_init(params);
        if (!ctx) {
            return false;
        }
    }

    // prepare memory for the weights
    {
        const int n_embd  = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_vocab = hparams.n_vocab;

        layers.resize(n_layer);

        wte    = ggml_new_tensor_2d(ctx, wtype,         n_embd, n_vocab);

        ln_f_g = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);
        ln_f_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

        lmh_g  = ggml_new_tensor_2d(ctx, wtype,         n_embd, n_vocab);
        lmh_b  = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_vocab);

        // map by name
        tensors["transformer.wte.weight"] = wte;

        tensors["transformer.ln_f.weight"] = ln_f_g;
        tensors["transformer.ln_f.bias"]   = ln_f_b;

        tensors["lm_head.weight"] = lmh_g;
        tensors["lm_head.bias"]   = lmh_b;

        for (int i = 0; i < n_layer; ++i) {
            auto & layer = layers[i];

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
            tensors["transformer.h." + std::to_string(i) + ".ln_1.weight"]          = layer.ln_1_g;
            tensors["transformer.h." + std::to_string(i) + ".ln_1.bias"]            = layer.ln_1_b;

            tensors["transformer.h." + std::to_string(i) + ".attn.q_proj.weight"]   = layer.c_attn_q_proj_w;
            tensors["transformer.h." + std::to_string(i) + ".attn.k_proj.weight"]   = layer.c_attn_k_proj_w;
            tensors["transformer.h." + std::to_string(i) + ".attn.v_proj.weight"]   = layer.c_attn_v_proj_w;

            tensors["transformer.h." + std::to_string(i) + ".attn.out_proj.weight"] = layer.c_attn_proj_w;

            tensors["transformer.h." + std::to_string(i) + ".mlp.fc_in.weight"]     = layer.c_mlp_fc_w;
            tensors["transformer.h." + std::to_string(i) + ".mlp.fc_in.bias"]       = layer.c_mlp_fc_b;

            tensors["transformer.h." + std::to_string(i) + ".mlp.fc_out.weight"]    = layer.c_mlp_proj_w;
            tensors["transformer.h." + std::to_string(i) + ".mlp.fc_out.bias"]      = layer.c_mlp_proj_b;
        }
    }

    // key + value memory
    {
        const int n_embd  = hparams.n_embd;
        const int n_layer = hparams.n_layer;
        const int n_ctx   = hparams.n_ctx;

        const int n_mem      = n_layer*n_ctx;
        const int n_elements = n_embd*n_mem;

        memory_k = ggml_new_tensor_1d(ctx, GGML_TYPE_F16, n_elements);
        memory_v = ggml_new_tensor_1d(ctx, GGML_TYPE_F16, n_elements);
    }

    // load weights
    {
        // int n_tensors = 0;

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

            if (tensors.find(nname) == tensors.end()) {
                return false;
            }

            auto* tensor = tensors[nname];
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

            // if (++n_tensors % 8 == 0) {
            //     std::cout << ".";
            //     std::cout.flush();
            // }
        }

        // std::cout << "done" << std::endl;
    }

    fin.close();

    return true;
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
// NOLINTBEGIN
bool GptJModel::doEval(int n_threads, int n_past, const std::vector<GptVocab::id> & embd_inp, std::vector<float> & embd_w, size_t & mem_per_token)
{
    const int N = static_cast<int>(embd_inp.size());

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
    struct ggml_tensor * inpL = ggml_get_rows(ctx0, wte, embd);

    for (int il = 0; il < n_layer; ++il) {
        struct ggml_tensor * cur;

        // norm
        {
            cur = ggml_norm(ctx0, inpL, hparams.eps);

            // cur = ln_1_g*cur + ln_1_b
            cur = ggml_add(ctx0,
                    ggml_mul(ctx0,
                        ggml_repeat(ctx0, layers[il].ln_1_g, cur),
                        cur),
                    ggml_repeat(ctx0, layers[il].ln_1_b, cur));
        }

        struct ggml_tensor * inpSA = cur;

        // self-attention
        {
            struct ggml_tensor * Qcur = ggml_rope_inplace(ctx0, ggml_reshape_3d(ctx0, ggml_mul_mat(ctx0, layers[il].c_attn_q_proj_w, cur), n_embd/n_head, n_head, N), KQ_pos, n_rot, 0, 0);
            struct ggml_tensor * Kcur = ggml_rope_inplace(ctx0, ggml_reshape_3d(ctx0, ggml_mul_mat(ctx0, layers[il].c_attn_k_proj_w, cur), n_embd/n_head, n_head, N), KQ_pos, n_rot, 0, 0);

            // store key and value to memory
            {
                struct ggml_tensor * Vcur = ggml_transpose(ctx0, ggml_mul_mat(ctx0, layers[il].c_attn_v_proj_w, cur));

                struct ggml_tensor * k = ggml_view_1d(ctx0, memory_k, N*n_embd, (ggml_element_size(memory_k)*n_embd)*(il*n_ctx + n_past));
                struct ggml_tensor * v = ggml_view_2d(ctx0, memory_v, N, n_embd,
                        (   n_ctx)*ggml_element_size(memory_v),
                        (il*n_ctx)*ggml_element_size(memory_v)*n_embd + n_past*ggml_element_size(memory_v));

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
                            ggml_view_1d(ctx0, memory_k, (n_past + N)*n_embd, il*n_ctx*ggml_element_size(memory_k)*n_embd),
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
                ggml_view_3d(ctx0, memory_v,
                        n_past + N, n_embd/n_head, n_head,
                        n_ctx*ggml_element_size(memory_v),
                        n_ctx*ggml_element_size(memory_v)*n_embd/n_head,
                        il*n_ctx*ggml_element_size(memory_v)*n_embd);

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
                    layers[il].c_attn_proj_w,
                    cur);
        }

        struct ggml_tensor * inpFF = cur;

        // feed-forward network
        // this is independent of the self-attention result, so it could be done in parallel to the self-attention
        {
            // note here we pass inpSA instead of cur
            cur = ggml_mul_mat(ctx0,
                    layers[il].c_mlp_fc_w,
                    inpSA);

            cur = ggml_add(ctx0,
                    ggml_repeat(ctx0, layers[il].c_mlp_fc_b, cur),
                    cur);

            // GELU activation
            cur = ggml_gelu(ctx0, cur);

            // projection
            // cur = proj_w*cur + proj_b
            cur = ggml_mul_mat(ctx0,
                    layers[il].c_mlp_proj_w,
                    cur);

            cur = ggml_add(ctx0,
                    ggml_repeat(ctx0, layers[il].c_mlp_proj_b, cur),
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
                    ggml_repeat(ctx0, ln_f_g, inpL),
                    inpL),
                ggml_repeat(ctx0, ln_f_b, inpL));
    }

    // lm_head
    {
        inpL = ggml_mul_mat(ctx0, lmh_g, inpL);

        inpL = ggml_add(ctx0,
                ggml_repeat(ctx0, lmh_b, inpL),
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
// NOLINTEND

}
