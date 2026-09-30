#include <Functions/PhraseSearch.h>

namespace DB
{

VectorWithMemoryTracking<size_t> buildPhraseFailureFunction(const VectorWithMemoryTracking<String> & phrase_tokens)
{
    const size_t size = phrase_tokens.size();
    VectorWithMemoryTracking<size_t> failure(size, 0);

    size_t k = 0;
    for (size_t i = 1; i < size; ++i)
    {
        while (k > 0 && phrase_tokens[k] != phrase_tokens[i])
            k = failure[k - 1];

        if (phrase_tokens[k] == phrase_tokens[i])
            ++k;

        failure[i] = k;
    }

    return failure;
}

}
