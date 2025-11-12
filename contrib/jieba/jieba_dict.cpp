#include <jieba_dict.h>

#include <incbin.h>

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
INCBIN(resource_jieba_dict, "dict_be.dat");
#else
INCBIN(resource_jieba_dict, "dict_le.dat");
#endif

namespace Jieba
{

double DartsDict::find(std::span<const Rune> key) const
{
    int res = da.exactMatchSearch<int>(reinterpret_cast<const char *>(key.data()), key.size_bytes());
    if (res < 0 || res >= num_elems)
        return 0;
    return elems[res];
}

DAG DartsDict::buildDAG(std::span<const Rune> runes) const
{
    size_t size = runes.size();
    DAG dag(size);
    for (size_t i = 0; i < size; ++i)
    {
        static constexpr size_t MAX_RESULTS = 128;
        static constexpr size_t MAX_WORD_LENGTH = 32;

        ::Darts::DoubleArray::result_pair_type results[MAX_RESULTS] = {};
        size_t num = da.commonPrefixSearch(
            reinterpret_cast<const char *>(&runes[i]), results, MAX_RESULTS, std::min(MAX_WORD_LENGTH, size - i) * 2);

        dag[i].nexts.emplace_back(i + 1, min_weight); /// Single rune is always a word
        for (size_t j = 0; j < num; ++j)
        {
            auto & match = results[j];
            if (match.value < 0 || match.value >= num_elems)
                continue;

            size_t char_num = match.length / 2;
            if (char_num == 1)
                dag[i].nexts[0].second = elems[match.value];
            else
                dag[i].nexts.emplace_back(i + char_num, elems[match.value]);
        }
    }
    return dag;
}

DartsDict::DartsDict()
{
    const char * jieba_dict = reinterpret_cast<const char *>(gresource_jieba_dictData);
    const DartsHeader * header = reinterpret_cast<const DartsHeader *>(jieba_dict);
    min_weight = header->min_weight;
    num_elems = header->num_elems;
    elems = reinterpret_cast<const double *>(jieba_dict + sizeof(DartsHeader));
    const char * da_ptr = jieba_dict + sizeof(DartsHeader) + sizeof(double) * num_elems;
    da.set_array(da_ptr, header->da_size);
}

}
