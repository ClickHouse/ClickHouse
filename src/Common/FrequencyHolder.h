#pragma once

#include "config.h"

#if USE_NLP

#include <Common/logger_useful.h>

#include <string_view>
#include <unordered_map>

#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/readFloatText.h>
#include <IO/ZstdInflatingReadBuffer.h>


namespace DB
{

/// FrequencyHolder class is responsible for storing and loading dictionaries
/// needed for text classification functions:
///
/// 1. detectLanguageUnknown
/// 2. detectCharset
/// 3. detectTonality

class FrequencyHolder
{
public:
    struct Encoding
    {
        String name;
        String lang;
        HashMap<UInt16, Float64> map;
    };

    using Map = HashMap<std::string_view, Float64>;

    using EncodingMap = HashMap<UInt16, Float64>;
    using EncodingContainer = std::vector<Encoding>;

    static FrequencyHolder & getInstance();

    const Map & getEmotionalDict() const
    {
        return emotional_dict;
    }

    const EncodingContainer & getEncodingsFrequency() const
    {
        return encodings_freq;
    }

private:
    FrequencyHolder();

    void loadEncodingsFrequency();
    void loadEmotionalDict();

    Arena string_pool;

    Map emotional_dict;
    EncodingContainer encodings_freq;
};
}

#endif
