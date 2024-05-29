// NOLINTBEGIN

#pragma once

#include "llama.h"

#include "grammar-parser.h"

#include <random>
#include <string>
#include <unordered_map>
#include <vector>

// sampler types
enum class llama_sampler_type : char {
    TOP_K       = 'k',
    TOP_P       = 'p',
    MIN_P       = 'm',
    TFS_Z       = 'f',
    TYPICAL_P   = 'y',
    TEMPERATURE = 't'
};

// sampling parameters
typedef struct llama_sampling_params {
    int32_t     n_prev                = 64;                 // number of previous tokens to remember
    int32_t     n_probs               = 0;                  // if greater than 0, output the probabilities of top n_probs tokens.
    int32_t     min_keep              = 0;                  // 0 = disabled, otherwise samplers should return at least min_keep tokens
    int32_t     top_k                 = 40;                 // <= 0 to use vocab size
    float       top_p                 = 0.95f;              // 1.0 = disabled
    float       min_p                 = 0.05f;              // 0.0 = disabled
    float       tfs_z                 = 1.00f;              // 1.0 = disabled
    float       typical_p             = 1.00f;              // 1.0 = disabled
    float       temp                  = 0.80f;              // <= 0.0 to sample greedily, 0.0 to not output probabilities
    float       dynatemp_range        = 0.00f;              // 0.0 = disabled
    float       dynatemp_exponent     = 1.00f;              // controls how entropy maps to temperature in dynamic temperature sampler
    int32_t     penalty_last_n        = 64;                 // last n tokens to penalize (0 = disable penalty, -1 = context size)
    float       penalty_repeat        = 1.00f;              // 1.0 = disabled
    float       penalty_freq          = 0.00f;              // 0.0 = disabled
    float       penalty_present       = 0.00f;              // 0.0 = disabled
    int32_t     mirostat              = 0;                  // 0 = disabled, 1 = mirostat, 2 = mirostat 2.0
    float       mirostat_tau          = 5.00f;              // target entropy
    float       mirostat_eta          = 0.10f;              // learning rate
    bool        penalize_nl           = false;              // consider newlines as a repeatable token
    uint32_t    seed                  = LLAMA_DEFAULT_SEED; // the seed used to initialize llama_sampling_context

    std::vector<llama_sampler_type> samplers_sequence = {
        llama_sampler_type::TOP_K,
        llama_sampler_type::TFS_Z,
        llama_sampler_type::TYPICAL_P,
        llama_sampler_type::TOP_P,
        llama_sampler_type::MIN_P,
        llama_sampler_type::TEMPERATURE
    };

    std::string grammar;  // optional BNF-like grammar to constrain sampling

    // Classifier-Free Guidance
    // https://arxiv.org/abs/2306.17806
    std::string cfg_negative_prompt; // string to help guidance
    float       cfg_scale     = 1.f; // how strong is guidance

    std::unordered_map<llama_token, float> logit_bias; // logit bias for specific tokens

    std::vector<llama_token> penalty_prompt_tokens;
    bool                     use_penalty_prompt_tokens = false;
} llama_sampling_params;

// general sampler context
// TODO: move to llama.h
struct llama_sampling_context {
    // parameters that will be used for sampling
    llama_sampling_params params;

    // mirostat sampler state
    float mirostat_mu;

    llama_grammar * grammar;

    // internal
    grammar_parser::parse_state parsed_grammar;

    // TODO: replace with ring-buffer
    std::vector<llama_token>      prev;
    std::vector<llama_token_data> cur;
    size_t n_valid; // Number of correct top tokens with correct probabilities.

    std::mt19937 rng;
};

// Create a new sampling context instance.
struct llama_sampling_context * llama_sampling_init(const struct llama_sampling_params & params);

void llama_sampling_free(struct llama_sampling_context * ctx);

// Reset the sampler context
// - clear prev tokens
// - reset grammar
void llama_sampling_reset(llama_sampling_context * ctx);

// Set the sampler seed
void llama_sampling_set_rng_seed(struct llama_sampling_context * ctx, uint32_t seed);

// Copy the sampler context
void llama_sampling_cp(llama_sampling_context * src, llama_sampling_context * dst);

// Get the last sampled token
llama_token llama_sampling_last(llama_sampling_context * ctx);

// Get a string representation of the last sampled tokens
std::string llama_sampling_prev_str(llama_sampling_context * ctx_sampling, llama_context * ctx_main, int n);

// Print sampling parameters into a string
std::string llama_sampling_print(const llama_sampling_params & params);

// Print sampling order into a string
std::string llama_sampling_order_print(const llama_sampling_params & params);

std::string llama_sampling_type_to_str(llama_sampler_type sampler_type);

std::vector<llama_sampler_type> llama_sampling_types_from_names(const std::vector<std::string> & names, bool allow_alt_names);
std::vector<llama_sampler_type> llama_sampling_types_from_chars(const std::string & names_string);

// this is a common sampling function used across the examples for convenience
// it can serve as a starting point for implementing your own sampling function
// Note: When using multiple sequences, it is the caller's responsibility to call
//       llama_sampling_reset when a sequence ends
//
// required:
//  - ctx_main:     context to use for sampling
//  - ctx_sampling: sampling-specific context
//
// optional:
//  - ctx_cfg:      context to use for classifier-free guidance
//  - idx:          sample from llama_get_logits_ith(ctx, idx)
//
// returns:
//  - token:      sampled token
//  - candidates: vector of candidate tokens
//
llama_token llama_sampling_sample(
        struct llama_sampling_context * ctx_sampling,
        struct llama_context * ctx_main,
        struct llama_context * ctx_cfg,
        int idx = -1);

// Prepares and adjusts the set of token candidates for sampling based on penalties, biases, and sampling parameters.
llama_token_data_array llama_sampling_prepare(
        struct llama_sampling_context * ctx_sampling,
        struct llama_context * ctx_main,
        struct llama_context * ctx_cfg,
        int idx = 0,
        bool apply_grammar = true,
        std::vector<float> * original_logits = nullptr);

void llama_sampling_accept(
        struct llama_sampling_context * ctx_sampling,
        struct llama_context * ctx_main,
        llama_token id,
        bool apply_grammar);

// NOLINTEND
