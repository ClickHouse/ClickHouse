#pragma once
#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readFloatText.h>
#include <IO/Operators.h>
#include <IO/ZstdInflatingReadBuffer.h>

#include <string_view>
#include <string>
#include <cstring>
#include <unordered_map>
#include <base/logger_useful.h>
#include <base/getResource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

class FrequencyHolder
{

public:
    using Map = std::unordered_map<UInt16, Float64>;
    using Container = std::unordered_map<String, Map>;

    static FrequencyHolder & getInstance()
    {
        static FrequencyHolder instance;
        return instance;
    }


    void loadEncodingsFrequency()
    {
        UInt16 bigram;
        Float64 frequency;
        String charset_name;

        Poco::Logger * log = &Poco::Logger::get("EncodingsFrequency");

        LOG_TRACE(log, "Loading embedded charset frequencies");

        auto resource = getResource("charset_freq.txt.zst");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded charset frequencies");

        auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
        std::unique_ptr<ReadBuffer> in = std::make_unique<ZstdInflatingReadBuffer>(std::move(buf));

        while (!in->eof())
        {
            String line;
            readString(line, *in);
            ++in->position();

            if (line.empty())
                continue;
            
            ReadBufferFromString buf_line(line);

            // Start loading a new charset
            if (line.starts_with("//"))
            {
                buf_line.ignore(3);
                readString(charset_name, buf_line);
            }
            else
            {
                readIntText(bigram, buf_line);
                buf_line.ignore();
                readFloatText(frequency, buf_line);
                encodings_freq[charset_name][bigram] = frequency;
            }
            
        }
        LOG_TRACE(log, "Charset frequencies was added, charsets count: {}", encodings_freq.size());
    }


    void loadEmotionalDict()
    {

        String word;
        Float64 tonality;

        Poco::Logger * log = &Poco::Logger::get("EmotionalDict");
        LOG_TRACE(log, "Loading embedded emotional dictionary (RU)");

        auto resource = getResource("emotional_dictionary_rus.txt.zst");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded emotional dictionary");

        auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
        std::unique_ptr<ReadBuffer> in = std::make_unique<ZstdInflatingReadBuffer>(std::move(buf));

        size_t count = 0;
        while (!in->eof())
        {
            String line;
            readString(line, *in);
            ++in->position();

            if (line.empty())
                continue;
            
            ReadBufferFromString buf_line(line);

            readStringUntilWhitespace(word, buf_line);
            buf_line.ignore();
            readFloatText(tonality, buf_line);

            emotional_dict[word] = tonality;
            ++count;
        }
        LOG_TRACE(log, "Emotional dictionary was added. Word count: {}", std::to_string(count));
    }


    void loadProgrammingFrequency()
    {
        String bigram;
        Float64 frequency;
        String programming_language;

        Poco::Logger * log = &Poco::Logger::get("ProgrammingFrequency");

        LOG_TRACE(log, "Loading embedded programming languages frequencies loading");

        auto resource = getResource("prog_freq.txt.zst");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no embedded programming languages frequencies");

        auto buf = std::make_unique<ReadBufferFromMemory>(resource.data(), resource.size());
        std::unique_ptr<ReadBuffer> in = std::make_unique<ZstdInflatingReadBuffer>(std::move(buf));

        while (!in->eof())
        {
            String line;
            readString(line, *in);
            ++in->position();

            if (line.empty())
                continue;
            
            ReadBufferFromString buf_line(line);

            // Start loading a new language
            if (line.starts_with("//"))
            {
                buf_line.ignore(3);
                readString(programming_language, buf_line);
            }
            else
            {
                readStringUntilWhitespace(bigram, buf_line);
                buf_line.ignore();
                readFloatText(frequency, buf_line);
                programming_freq[programming_language][bigram] = frequency;
            }
        }
        LOG_TRACE(log, "Programming languages frequencies was added");
    }

    const std::unordered_map<String, Float64> & getEmotionalDict()
    {
        std::lock_guard lock(mutex);
        if (emotional_dict.empty())
            loadEmotionalDict();

        return emotional_dict;
    }


    const Container & getEncodingsFrequency()
    {
        std::lock_guard lock(mutex);
        if (encodings_freq.empty())
            loadEncodingsFrequency();

        return encodings_freq;
    }

    const std::unordered_map<String, std::unordered_map<String, Float64>> & getProgrammingFrequency()
    {
        std::lock_guard lock(mutex);
        if (encodings_freq.empty())
            loadProgrammingFrequency();

        return programming_freq;
    }


private:

    std::unordered_map<String, Float64> emotional_dict;
    Container encodings_freq;
    std::unordered_map<String, std::unordered_map<String, Float64>> programming_freq;

    std::mutex mutex;
};
}
