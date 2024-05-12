#pragma once

#include <Functions/ggmlEvaluate/IGgmlModel.h>

#include <cstdint>
#include <map>
#include <string>
#include "Functions/ggmlEvaluate/IGgmlModel.h"
#include <absl/random/random.h>

namespace DB
{

struct GptVocab
{
    using id = int32_t;
    using token = std::string;

    std::map<token, id> token_to_id;
    std::map<id, token> id_to_token;
    std::vector<std::string> special_tokens;

    void addSpecialToken(const std::string & token) { special_tokens.push_back(token); }
};

struct GptParams
{
    int32_t seed = -1; // random_number_generator seed
    int32_t n_threads = 1; // std::min(4, static_cast<int32_t>(std::thread::hardware_concurrency()));
    int32_t n_predict = 200; // new tokens to predict
    int32_t n_parallel = 1; // number of parallel streams
    int32_t n_batch = 8; // batch size for prompt processing
    int32_t n_ctx = 2048; // context size (this is the KV cache max size)
    int32_t n_gpu_layers = 0; // number of layers to offlload to the GPU

    bool ignore_eos = false; // ignore EOS token when generating text

    // sampling parameters
    int32_t top_k = 40;
    float top_p = 0.9f;
    float temp = 0.9f;
    int32_t repeat_last_n = 64;
    float repeat_penalty = 1.00f;

    // std::string model      = "~/ggml-model.bin"; // model path
    std::string prompt;
    std::string token_test;

    bool interactive = false;
    int32_t interactive_port = -1;
};

std::vector<GptVocab::id> gpt_tokenize(const GptVocab & vocab, const std::string & text);

GptVocab::id gpt_sample_top_k_top_p(
    const GptVocab & vocab, const float * logits, int top_k, double top_p, double temp, absl::BitGen & random_number_generator);

std::string getPathFromConfig(const DB::ConfigPtr & config, const std::string & model_name);

}
