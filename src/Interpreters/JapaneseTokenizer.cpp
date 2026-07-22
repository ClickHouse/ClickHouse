#include <Interpreters/ITokenizer.h>

#if USE_MECAB

#include <Interpreters/MecabDictionaryManager.h>

#include <mecab.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_LOAD_CONFIG;
}

struct JapaneseTokenizer::Impl
{
    MecabDictionaryPtr dictionary;
    std::unique_ptr<MeCab::Lattice> lattice;

    /// Parsing state for the current string (same "remember the last buffer" pattern as `SparseGramsTokenizer`).
    const char * previous_data = nullptr;
    size_t previous_len = 0;
    const MeCab::Node * current = nullptr;

    void ensureLoaded()
    {
        if (dictionary)
            return;
        dictionary = MecabDictionaryManager::instance().getJapaneseDictionary();
        lattice.reset(dictionary->getModel().createLattice());
        if (!lattice)
            throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Failed to create a MeCab lattice for the Japanese tokenizer");
    }
};

JapaneseTokenizer::JapaneseTokenizer()
    : ITokenizerHelper(Type::Japanese), impl(std::make_shared<Impl>())
{
}

/// Clones (per-thread copies) start with fresh parsing state; the dictionary is re-fetched cheaply
/// from the process-wide cache on first use.
JapaneseTokenizer::JapaneseTokenizer(const JapaneseTokenizer &)
    : ITokenizerHelper(Type::Japanese), impl(std::make_shared<Impl>())
{
}

JapaneseTokenizer::~JapaneseTokenizer() = default;

bool JapaneseTokenizer::nextInString(
    const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
{
    impl->ensureLoaded();

    /// Re-parse whenever a new buffer is presented.
    if (data != impl->previous_data || length != impl->previous_len)
    {
        impl->previous_data = data;
        impl->previous_len = length;

        impl->lattice->clear();
        impl->lattice->set_sentence(data, length);
        if (!MeCab::Tagger::parse(impl->dictionary->getModel(), impl->lattice.get()))
        {
            impl->previous_data = nullptr;
            impl->previous_len = 0;
            impl->current = nullptr;
            throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "MeCab failed to parse input: {}", impl->lattice->what());
        }

        impl->current = impl->lattice->bos_node();
        if (impl->current)
            impl->current = impl->current->next; /// skip BOS
    }

    /// Skip end-of-sentence and empty nodes; emit the surface form of the next real node.
    while (impl->current && (impl->current->stat == MECAB_EOS_NODE || impl->current->length == 0))
        impl->current = impl->current->next;

    if (!impl->current)
    {
        impl->previous_data = nullptr;
        impl->previous_len = 0;
        return false;
    }

    token_start = impl->current->surface - data;
    token_length = impl->current->length;
    pos = token_start + token_length;
    impl->current = impl->current->next;
    return true;
}

bool JapaneseTokenizer::nextInStringLike(const char *, size_t, size_t &, String &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JapaneseTokenizer::nextInStringLike is not implemented");
}

void JapaneseTokenizer::substringToBloomFilter(
    const char * data, size_t length, BloomFilter & bloom_filter, bool /*is_prefix*/, bool /*is_suffix*/) const
{
    stringToBloomFilter(data, length, bloom_filter);
}

void JapaneseTokenizer::substringToTokens(
    const char * data, size_t length, VectorWithMemoryTracking<String> & tokens, bool /*is_prefix*/, bool /*is_suffix*/) const
{
    stringToTokens(data, length, tokens);
}

}

#endif
