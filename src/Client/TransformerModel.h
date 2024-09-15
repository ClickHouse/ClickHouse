#pragma once

#include "ggml.h"
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <Parsers/Lexer.h>

#include <cstddef>
#include <string>
#include <vector>
#include <map>
#include <thread>

/// TODO: make const where possible

//
// Vocab utils
//
std::string replace(
        const std::string & s,
        const std::string & from,
        const std::string & to);

struct GptVocab {
    using id    = int32_t;
    using token = std::string;

    std::map<token, id> token_to_id;
    std::map<id, token> id_to_token;
    std::vector<std::string> special_tokens;

    void addSpecialToken(const std::string & token);
};


class GPTJModel {

private:
    struct GptjHparams {
        int32_t n_vocab;
        int32_t n_ctx;
        int32_t n_embd;
        int32_t n_head;
        int32_t n_layer;
        int32_t n_rot;
        int32_t ftype   = 1;
        float   eps     = 1e-5f;
    };

    struct GptjLayer {
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

    struct GptjModel {
        GptjHparams hparams;

        // normalization
        struct ggml_tensor * ln_f_g;
        struct ggml_tensor * ln_f_b;

        struct ggml_tensor * wte; // position embedding

        struct ggml_tensor * lmh_g; // language model head
        struct ggml_tensor * lmh_b; // language model bias

        std::vector<GptjLayer> layers;

        // key + value memory
        struct ggml_tensor * memory_k;
        struct ggml_tensor * memory_v;

        //
        struct ggml_context * ctx;
        std::map<std::string, struct ggml_tensor *> tensors;
    };

    std::string fname;
    GptjModel model;
    GptVocab vocab;
    int n_threads = std::min(4u, std::thread::hardware_concurrency());
    int n_batch = 32;
    int n_past;
    bool model_loaded = false;
    bool eval_success = true;
    size_t mem_per_token = 0;
    std::vector<GptVocab::id> current_query_ids;

    

    DB::WriteBufferFromFileDescriptor out_err = DB::WriteBufferFromFileDescriptor(STDERR_FILENO, 4096);

    bool loadModel(const std::string & file_name);

    std::vector<GptVocab::id> tokens2Ids(const std::vector<std::string> & tokens);

    std::vector<std::string> ids2Tokens(const std::vector<GptVocab::id> & ids);

    bool gptjEval(const std::vector<GptVocab::id> & ids_input,
              std::vector<float>         & logits);
    

    std::vector<GptVocab::id> getTopNIdsFromLogits(const std::vector<float>& logits, size_t top_n);


public:
    explicit GPTJModel(const std::string & file_name);

    std::vector<std::string> getRecsTopN(const std::vector<std::string>& tokens, size_t top_n);

    void reset();

    ~GPTJModel() {
        ggml_free(model.ctx);
    }

    static const GptVocab::token unk;
    static const GptVocab::token bos;
    static const GptVocab::token eos;
    static const GptVocab::token pad;

    static const GptVocab::token literal;
    static const GptVocab::token identifier;
    static const GptVocab::token operator_token;

};
