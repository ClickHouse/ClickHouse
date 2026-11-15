#pragma once

#include "config.h"

#if USE_NLP

#include <Common/logger_useful.h>

#include <string_view>
#include <unordered_map>

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

class FrequencyHolder
{
public:
    struct Encoding
    {
        String name;
        String lang;
        HashMap<UInt16, Float64> map;
    };

    using EncodingMap = HashMap<UInt16, Float64>;
    using EncodingContainer = std::vector<Encoding>;

    static FrequencyHolder & getInstance();

    const EncodingContainer & getEncodingsFrequency() const
    {
        return encodings_freq;
    }

private:
    FrequencyHolder();

    void loadEncodingsFrequency();

    EncodingContainer encodings_freq;
};
}

#endif
