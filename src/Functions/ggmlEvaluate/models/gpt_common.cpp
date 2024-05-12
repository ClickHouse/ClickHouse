#include "gpt_common.h"
#include <absl/random/discrete_distribution.h>

#include <Common/Exception.h>
#include <Common/re2.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
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
    res.pop_back(); // remove last '|'
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
    for (const auto & word : words)
    {
        for (int i = 0; i < static_cast<int>(word.size());)
        {
            for (int j = static_cast<int>(word.size()) - 1; j >= i; j--)
            {
                auto cand = word.substr(i, j - i + 1);
                auto it = vocab.token_to_id.find(cand);
                if (it != vocab.token_to_id.end())
                { // word.substr(i, j-i+1) in vocab
                    tokens.push_back(it->second);
                    i = j + 1;
                    break;
                }
                else if (j == i)
                { // word.substr(i, 1) has no matching
                    i++;
                }
            }
        }
    }

    return tokens;
}

GptVocab::id gpt_sample_top_k_top_p(
    const GptVocab & vocab, const float * logits, int top_k, double top_p, double temp, absl::BitGen & random_number_generator)
{
    int n_logits = static_cast<int>(vocab.id_to_token.size());

    std::vector<std::pair<double, GptVocab::id>> logits_id;
    logits_id.reserve(n_logits);

    {
        const double scale = 1.0 / temp;
        for (int i = 0; i < n_logits; ++i)
            logits_id.push_back(std::make_pair(logits[i] * scale, i));
    }

    // find the top K tokens
    std::partial_sort(
        logits_id.begin(),
        logits_id.begin() + top_k,
        logits_id.end(),
        [](const std::pair<double, GptVocab::id> & a, const std::pair<double, GptVocab::id> & b) { return a.first > b.first; });

    logits_id.resize(top_k);

    double maxl = -INFINITY;
    for (const auto & kv : logits_id)
        maxl = std::max(maxl, kv.first);

    // compute probs for the top K tokens
    std::vector<double> probs;
    probs.reserve(logits_id.size());

    double sum = 0.0;
    for (const auto & kv : logits_id)
    {
        double p = exp(kv.first - maxl);
        probs.push_back(p);
        sum += p;
    }

    // normalize the probs
    for (auto & p : probs)
        p /= sum;

    if (top_p < 1.0f)
    {
        double cumsum = 0.0f;
        for (int i = 0; i < top_k; i++)
        {
            cumsum += probs[i];
            if (cumsum >= top_p)
            {
                top_k = i + 1;
                probs.resize(top_k);
                logits_id.resize(top_k);
                break;
            }
        }

        cumsum = 1.0 / cumsum;
        for (double & prob : probs)
            prob *= cumsum;
    }

    //printf("\n");
    //for (int i = 0; i < (int) probs.size(); i++) {
    //    printf("%d: '%s' %f\n", i, vocab.id_to_token.at(logits_id[i].second).c_str(), probs[i]);
    //}
    //exit(0);

    absl::discrete_distribution<> dist(probs.begin(), probs.end());
    int idx = dist(random_number_generator);

    return logits_id[idx].second;
}

std::string getPathFromConfig(const DB::ConfigPtr & config, const std::string & model_name)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys(keys);

    if (!config->has(model_name))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "no key 'gptj' set in ggml config");
    ConfigPtr gptj_config{config->createView(model_name)};

    if (!gptj_config->has("path"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "no key 'path' set in ggml.gptj config");

    return gptj_config->getString("path");
}

}
