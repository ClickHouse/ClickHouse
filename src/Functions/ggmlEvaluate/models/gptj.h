#pragma once

#include "ggml/ggml.h"

#include "gpt_common.h"

#include <Functions/ggmlEvaluate/IGgmlModel.h>

namespace DB
{

// default hparams (GPT-J 6B)
struct GptJHparams
{
    int32_t n_vocab = 50400;
    int32_t n_ctx = 2048;
    int32_t n_embd = 4096;
    int32_t n_head = 16;
    int32_t n_layer = 28;
    int32_t n_rot = 64;
    int32_t ftype = 1;
    float eps = 1e-5f;
};

struct GptJLayer
{
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

struct GptJModelState
{
    GptJHparams hparams;

    // normalization
    struct ggml_tensor * ln_f_g;
    struct ggml_tensor * ln_f_b;

    struct ggml_tensor * wte; // position embedding

    struct ggml_tensor * lmh_g; // language model head
    struct ggml_tensor * lmh_b; // language model bias

    std::vector<GptJLayer> layers;

    // key + value memory
    struct ggml_tensor * memory_k;
    struct ggml_tensor * memory_v;

    //
    struct ggml_context * ctx;
    std::map<std::string, struct ggml_tensor *> tensors;
};

class GptJModel : public IGgmlModel, protected GptJModelState
{
public:
    ~GptJModel() override
    {
        ggml_free(ctx); // GGMLTODO : bullshit
    }

private:
    void loadImpl(ConfigPtr config) override;
    std::string evalImpl(GgmlModelParams params, const std::string & input) override;

    bool evalInternal(
        int n_threads, int n_past, const std::vector<GptVocab::id> & embd_inp, std::vector<float> & embd_w, size_t & mem_per_token);
    std::vector<GptVocab::id> predict(GgmlModelParams params, const std::vector<GptVocab::id> & embd_inp);

    GptVocab gpt_vocab;
    GptParams gpt_params;
};

}
