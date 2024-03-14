#pragma once

#include <base/StringRef.h>
#include <Common/logger_useful.h>

#include <string_view>
#include <unordered_map>

#include <Common/Arena.h>
#include <Common/getResource.h>
#include <Common/HashTable/HashMap.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readFloatText.h>
#include <IO/ZstdInflatingReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

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

public:
    using Map = HashMap<StringRef, Float64>;
    using Container = std::vector<Language>;

    using EncodingMap = HashMap<UInt16, Float64>;
    using EncodingContainer = std::vector<Encoding>;

    static FrequencyHolder & getInstance()
    {
        static FrequencyHolder instance;
        return instance;
    }

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

    FrequencyHolder()
    {
        loadEmotionalDict();
        loadEncodingsFrequency();
        loadProgrammingFrequency();
    }

    void loadEncodingsFrequency()
    {
        Poco::Logger * log = &Poco::Logger::get("EncodingsFrequency");

        LOG_TRACE(log, "Loading embedded charset frequencies");

        auto resource = getResource("charset.zst");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded charset frequencies");

        String line;
        UInt16 bigram;
        Float64 frequency;
        String charset_name;

        auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
        ZstdInflatingReadBuffer in(std::move(buf));

        while (!in.eof())
        {
            readString(line, in);
            in.ignore();

            if (line.empty())
                continue;

            ReadBufferFromString buf_line(line);

            // Start loading a new charset
            if (line.starts_with("// "))
            {
                // Skip "// "
                buf_line.ignore(3);
                readString(charset_name, buf_line);

                /* In our dictionary we have lines with form: <Language>_<Charset>
                * If we need to find language of data, we return <Language>
                * If we need to find charset of data, we return <Charset>.
                */
                size_t sep = charset_name.find('_');

                Encoding enc;
                enc.lang = charset_name.substr(0, sep);
                enc.name = charset_name.substr(sep + 1);
                encodings_freq.push_back(std::move(enc));
            }
            else
            {
                readIntText(bigram, buf_line);
                buf_line.ignore();
                readFloatText(frequency, buf_line);

                encodings_freq.back().map[bigram] = frequency;
            }
        }
        LOG_TRACE(log, "Charset frequencies was added, charsets count: {}", encodings_freq.size());
    }

    void loadEmotionalDict()
    {
        Poco::Logger * log = &Poco::Logger::get("EmotionalDict");
        LOG_TRACE(log, "Loading embedded emotional dictionary");

        auto resource = getResource("tonality_ru.zst");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded emotional dictionary");

        String line;
        String word;
        Float64 tonality;
        size_t count = 0;

        auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
        ZstdInflatingReadBuffer in(std::move(buf));

        while (!in.eof())
        {
            readString(line, in);
            in.ignore();

            if (line.empty())
                continue;

            ReadBufferFromString buf_line(line);

            readStringUntilWhitespace(word, buf_line);
            buf_line.ignore();
            readFloatText(tonality, buf_line);

            StringRef ref{string_pool.insert(word.data(), word.size()), word.size()};
            emotional_dict[ref] = tonality;
            ++count;
        }
        LOG_TRACE(log, "Emotional dictionary was added. Word count: {}", std::to_string(count));
    }

    void loadProgrammingFrequency()
    {
        Poco::Logger * log = &Poco::Logger::get("ProgrammingFrequency");

        LOG_TRACE(log, "Loading embedded programming languages frequencies loading");

        auto resource = getResource("programming.zst");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded programming languages frequencies");

        String line;
        String bigram;
        Float64 frequency;
        String programming_language;

        auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
        ZstdInflatingReadBuffer in(std::move(buf));

        while (!in.eof())
        {
            readString(line, in);
            in.ignore();

            if (line.empty())
                continue;

            ReadBufferFromString buf_line(line);

            // Start loading a new language
            if (line.starts_with("// "))
            {
                // Skip "// "
                buf_line.ignore(3);
                readString(programming_language, buf_line);

                Language lang;
                lang.name = programming_language;
                programming_freq.push_back(std::move(lang));
            }
            else
            {
                readStringUntilWhitespace(bigram, buf_line);
                buf_line.ignore();
                readFloatText(frequency, buf_line);

                StringRef ref{string_pool.insert(bigram.data(), bigram.size()), bigram.size()};
                programming_freq.back().map[ref] = frequency;
            }
        }
        LOG_TRACE(log, "Programming languages frequencies was added");
    }

    Arena string_pool;

    Map emotional_dict;
    Container programming_freq;
    EncodingContainer encodings_freq;
};
}
