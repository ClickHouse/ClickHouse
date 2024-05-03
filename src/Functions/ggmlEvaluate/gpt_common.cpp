#include "gpt_common.h"

#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void gpt_split_words(std::string_view str, std::vector<std::string> & words)
{
    // Originally "   a" would be split into "  " and " a" words
    // Now it's "   " and "a" for simplicity because RE2 has no negative lookaheads like (?!\S)
    static const RE2 pattern{R"(('s|'t|'re|'ve|'m|'ll|'d| ?[[:alpha:]]+| ?[[:digit:]]+| ?[^\s[:alpha:][:digit:]]+|\s+))"};
    std::string_view match;
    while (RE2::FindAndConsume(&str, pattern, &match))
        words.emplace_back(match);
}

std::string get_tokens_match_group(const std::vector<std::string> & tokens)
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

}

std::vector<GptVocab::id> gpt_tokenize(const GptVocab & vocab, const std::string & text)
{
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
    std::vector<GptVocab::id> tokens;
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

GptVocab::id gpt_sample_top_k_top_p(
    const GptVocab & vocab,
    const float * logits,
    int top_k,
    double top_p,
    double temp,
    std::mt19937 & rng)
{
    int n_logits = static_cast<int>(vocab.id_to_token.size());

    std::vector<std::pair<double, GptVocab::id>> logits_id;
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
            [](const std::pair<double, GptVocab::id> & a, const std::pair<double, GptVocab::id> & b) {
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

std::vector<GptVocab::id> IGptModel::predict(const std::vector<GptVocab::id> & embd_inp)
{
    std::vector<GptVocab::id> total_embd;
    std::vector<GptVocab::id> embd;

    std::mt19937 rng(params.seed);
    int n_past = 0;
    std::vector<float> logits;

    // TODO: I REPLACED hparams.n_ctx with params.n_ctx. IDK WHAT IS THE DIFFERENCE
    int n_predict = std::min(params.n_predict, params.n_ctx - static_cast<int>(embd_inp.size()));

    size_t mem_per_token = 0;
    eval(params.n_threads, 0, { 0, 1, 2, 3 }, logits, mem_per_token);

    std::string result;

    for (size_t i = embd.size(); i < embd_inp.size() + n_predict; i++) {
        // predict
        if (!embd.empty()) {
            if (!eval(params.n_threads, n_past, embd, logits, mem_per_token)) {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to predict");
            }
        }

        n_past += embd.size();
        embd.clear();

        if (i >= embd_inp.size()) {
            // sample next token
            const int   top_k = params.top_k;
            const float top_p = params.top_p;
            const float temp  = params.temp;

            // const int n_vocab = hparams.n_vocab;
            // TODO: I GUESS IT IS THE SAME?:
            const int n_vocab = static_cast<int>(vocab.token_to_id.size());

            GptVocab::id id = 0;

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

        // store result; TODO: less copies maybe
        for (auto id : embd) {
            total_embd.push_back(id);
        }

        // end of text token
        if (embd.back() == 50256) {
            break;
        }
    }

    return total_embd;
}

}
