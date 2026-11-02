#include <Interpreters/JapaneseTokenizer.h>

#if USE_MECAB

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_LOAD_CONFIG;
}

JapaneseTokenizer::JapaneseTokenizer()
    : ITokenizerHelper(Type::Japanese)
{
}

/// Clones (per-thread copies) start with fresh parsing state; the dictionary is re-fetched cheaply
/// from the process-wide cache on first use.
JapaneseTokenizer::JapaneseTokenizer(const JapaneseTokenizer &)
    : ITokenizerHelper(Type::Japanese)
{
}

void JapaneseTokenizer::ensureLoaded() const
{
    if (lattice)
        return;

    auto loaded_dictionary = MecabDictionaryManager::instance().getJapaneseDictionary();
    std::unique_ptr<MeCab::Lattice> loaded_lattice(loaded_dictionary->getModel().createLattice());
    if (!loaded_lattice)
        throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Failed to initialize the MeCab Japanese tokenizer");

    /// Publish only after both are ready, so a transient failure leaves the tokenizer un-loaded (the
    /// next call retries) instead of half-initialized (dictionary set but lattice null).
    dictionary = std::move(loaded_dictionary);
    lattice = std::move(loaded_lattice);
}

bool JapaneseTokenizer::nextInString(
    const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
{
    ensureLoaded();

    /// Re-parse whenever a new buffer is presented.
    if (data != previous_data || length != previous_len)
    {
        previous_data = data;
        previous_len = length;

        lattice->clear();
        lattice->set_sentence(data, length);
        if (!MeCab::Tagger::parse(dictionary->getModel(), lattice.get()))
        {
            reset();
            throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "MeCab failed to parse input: {}", lattice->what());
        }

        current_node = lattice->bos_node();
        if (current_node)
            current_node = current_node->next; /// skip BOS
    }

    /// Skip end-of-sentence and empty nodes; emit the surface form of the next real node.
    while (current_node && (current_node->stat == MECAB_EOS_NODE || current_node->length == 0))
        current_node = current_node->next;

    if (current_node == nullptr)
    {
        reset();
        return false;
    }

    token_start = current_node->surface - data;
    token_length = current_node->length;
    pos = token_start + token_length;
    current_node = current_node->next;
    return true;
}

bool JapaneseTokenizer::nextInStringLike(const char *, size_t, size_t &, String &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JapaneseTokenizer::nextInStringLike is not implemented");
}

void JapaneseTokenizer::substringToBloomFilter(const char *, size_t, BloomFilter &, bool, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The Japanese tokenizer does not support substring (LIKE) matching");
}

void JapaneseTokenizer::substringToTokens(const char *, size_t, VectorWithMemoryTracking<String> &, bool, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The Japanese tokenizer does not support substring (LIKE) matching");
}

}

#endif
