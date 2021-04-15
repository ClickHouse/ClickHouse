#pragma once
#include <Common/TLDListsHolder.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readFloatText.h>
#include <IO/Operators.h>
#include <string_view>
#include <string>
#include <common/find_symbols.h>
#include <fstream>
#include <algorithm>
#include <cstring>
#include <limits>
#include <unordered_map>
#include <common/logger_useful.h>


namespace DB
{

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


    void parseDictionaries(const String & pt)
    {
        is_true = pt;
        loadEmotionalDict("/home/sergey/ClickHouse/src/Common/ClassificationDictionaries/emotional_dictionary_rus.txt");
        loadEncodingsFrequency("/home/sergey/ClickHouse/src/Common/ClassificationDictionaries/charset_freq.txt");
        loadProgrammingFrequency("/home/sergey/ClickHouse/src/Common/ClassificationDictionaries/programming_freq.txt");
    }


    void loadEncodingsFrequency(const String & path_to_charset_freq)
    {
        UInt16 bigram;
        Float64 frequency;
        String charset_name;

        Poco::Logger * log = &Poco::Logger::get("EncodingsFrequency");

        LOG_TRACE(log, "Charset frequencies loading from {}", path_to_charset_freq);

        ReadBufferFromFile in(path_to_charset_freq);
        while (!in.eof())
        {
            char * newline = find_first_symbols<'\n'>(in.position(), in.buffer().end());

            if (newline >= in.buffer().end())
                break;

            std::string_view line(in.position(), newline - in.position());

            if (line.empty())
                continue;
            // Start load new charset
            if (line.size() > 2 && line[0] == '/' && line[1] == '/')
            {
                ReadBufferFromMemory bufline(in.position() + 3, newline - in.position());
                readString(charset_name, bufline);
            } else
            {
                ReadBufferFromMemory buf_line(in.position(), newline - in.position());
                readIntText(bigram, buf_line);
                buf_line.ignore();
                readFloatText(frequency, buf_line);
                encodings_freq[charset_name][bigram] = frequency;
            }
            in.position() = newline + 1;
        }
        LOG_TRACE(log, "Charset frequencies was added");
    }


    void loadEmotionalDict(const String & path_to_emotional_dict)
    {

        String word;
        Float64 tonality;

        Poco::Logger * log = &Poco::Logger::get("EmotionalDict");
        LOG_TRACE(log, "Emotional dictionary loading from {}", path_to_emotional_dict);

        ReadBufferFromFile in(path_to_emotional_dict);
        while (!in.eof())
        {
            char * newline = find_first_symbols<'\n'>(in.position(), in.buffer().end());

            if (newline >= in.buffer().end()) { break; }

            ReadBufferFromMemory buf_line(in.position(), newline - in.position());
            in.position() = newline + 1;

            readStringUntilWhitespace(word, buf_line);
            buf_line.ignore();
            readFloatText(tonality, buf_line);

            emotional_dict[word] = tonality;

        }
        LOG_TRACE(log, "Emotional dictionary was added");
    }


    void loadProgrammingFrequency(const String & path_to_programming_freq)
    {
        String bigram;
        Float64 frequency;
        String programming_language;

        Poco::Logger * log = &Poco::Logger::get("ProgrammingFrequency");

        LOG_TRACE(log, "Programming langugages frequencies loading from {}", path_to_programming_freq);

        ReadBufferFromFile in(path_to_programming_freq);
        while (!in.eof())
        {
            char * newline = find_first_symbols<'\n'>(in.position(), in.buffer().end());

            if (newline >= in.buffer().end())
                break;

            std::string_view line(in.position(), newline - in.position());

            if (line.empty())
                continue;
            // Start load new charset
            if (line.size() > 2 && line[0] == '/' && line[1] == '/')
            {
                ReadBufferFromMemory bufline(in.position() + 3, newline - in.position());
                readString(programming_language, bufline);
            } else
            {
                ReadBufferFromMemory buf_line(in.position(), newline - in.position());
                readStringUntilWhitespace(bigram, buf_line);
                buf_line.ignore();
                readFloatText(frequency, buf_line);
                programming_freq[programming_language][bigram] = frequency;
            }
            in.position() = newline + 1;
        }
        LOG_TRACE(log, "Programming languages frequencies was added");
    }


    const String & get_path()
    {
        return is_true;
    }


    const std::unordered_map<String, Float64> & getEmotionalDict()
    {
        return emotional_dict;
    }


    const Container & getEncodingsFrequency()
    {
        return encodings_freq;
    }

    const std::unordered_map<String, std::unordered_map<String, Float64>> & getProgrammingFrequency()
    {
        return programming_freq;
    }


protected:

    String is_true;
    std::unordered_map<String, Float64> emotional_dict;
    Container encodings_freq;
    std::unordered_map<String, std::unordered_map<String, Float64>> programming_freq;
};
}

