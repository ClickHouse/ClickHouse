#pragma once

#include "config.h"

#if USE_NLP

#include <base/StringRef.h>
#include <Common/logger_useful.h>

#include <string_view>
#include <unordered_map>

#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
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
/// 4. detectProgrammingLanguage

class FrequencyHolder
{
public:
    struct Language
    {
        String name;
        HashMap<StringRef, Float64> map;
    };

    struct Encoding
    {
        String name;
        String lang;
        HashMap<UInt16, Float64> map;
    };

    using Map = HashMap<StringRef, Float64>;
    using Container = std::vector<Language>;

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

    const Container & getProgrammingFrequency() const
    {
        return programming_freq;
    }

private:
    FrequencyHolder();

    void loadEncodingsFrequency();
    void loadEmotionalDict();
    void loadProgrammingFrequency();

    Arena string_pool;

    Map emotional_dict;
    Container programming_freq;
    EncodingContainer encodings_freq;
};
}

#endif
