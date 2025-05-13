#include "ChineseTokenizer.h"

#include <incbin.h>

#include "config.h"

INCBIN(resource_jieba_dict_utf8, SOURCE_DIR "/contrib/cppjieba/dict/jieba.dict.utf8");
INCBIN(resource_hmm_model_utf8, SOURCE_DIR "/contrib/cppjieba/dict/hmm_model.utf8");

#if USE_CPPJIEBA

namespace DB
{

ChineseTokenizer & ChineseTokenizer::instance()
{
    // {reinterpret_cast<const char *>(gresource_jieba_dict_utf8Data), gresource_play_htmlSize};
    static ChineseTokenizer tokenizer;
    return tokenizer;
}

std::vector<cppjieba::Word> ChineseTokenizer::tokenize(std::string_view str, ChineseTokenizationGranularity granularity)
{
    std::vector<cppjieba::Word> tokens;
    switch (granularity)
    {
        case ChineseTokenizationGranularity::Fine:
            jieba_instance->CutForSearch(str, tokens);
            break;
        case ChineseTokenizationGranularity::Coarse:
            jieba_instance->CutAll(str, tokens);
            break;
    }
    return tokens;
}

ChineseTokenizer::ChineseTokenizer()
    : jieba_instance(std::make_unique<cppjieba::Jieba>(
          reinterpret_cast<const char *>(gresource_jieba_dict_utf8Data),
          gresource_jieba_dict_utf8Size,
          reinterpret_cast<const char *>(gresource_hmm_model_utf8Data),
          gresource_hmm_model_utf8Size))
{
}

}

#endif
