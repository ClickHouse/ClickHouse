#pragma once

#include "config.h"

#if USE_MECAB

#include <Interpreters/ITokenizer.h>
#include <Interpreters/MecabDictionaryManager.h>

#include <mecab.h>

namespace DB
{

/// Splits Japanese text into words using the MeCab morphological analyzer (each token is one word as
/// segmented by MeCab). The dictionary is loaded at runtime from <tokenizer><japanese> in the server
/// config (see `MecabDictionaryManager`). Lives in its own header so `<mecab.h>` stays out of the
/// widely-included `ITokenizer.h`.
struct JapaneseTokenizer final : public ITokenizerHelper<JapaneseTokenizer>
{
    JapaneseTokenizer();
    JapaneseTokenizer(const JapaneseTokenizer & other);

    static const char * getName() { return "japanese"; }
    static const char * getExternalName() { return getName(); }
    String getDescription() const override { return getName(); }
    bool isStateful() const override { return true; }

    bool nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, VectorWithMemoryTracking<String> & tokens, bool is_prefix, bool is_suffix) const override;

private:
    void ensureLoaded() const;
    void reset() const
    {
        previous_data = nullptr;
        previous_len = 0;
        current_node = nullptr;
    }

    /// Mutable parsing state; not concurrency-safe — clone per thread.
    mutable MecabDictionaryPtr dictionary;
    mutable std::unique_ptr<MeCab::Lattice> lattice;
    mutable const char * previous_data = nullptr;
    mutable size_t previous_len = 0;
    mutable const MeCab::Node * current_node = nullptr;
};

}

#endif
