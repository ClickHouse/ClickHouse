#pragma once

#include <Common/TLDListsHolder.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <string_view>
#include <string>
#include <fstream>
#include <algorithm>
#include <cstring>
#include <limits>
#include <unordered_map>


namespace DB
{

class FrequencyHolder
{
public:
    using Map = std::unordered_map<UInt16, double>;
    using Container = std::unordered_map<std::string, Map>;


    static FrequencyHolder & getInstance()
    {
        static FrequencyHolder instance;
        return instance;
    }


    void parseDictionaries(const std::string& pt)
    {
        is_true = pt;
        loadEmotionalDict("/home/sergey/ClickHouse/src/Functions/ClassificationDictionaries/emotional_dictionary_rus.txt");
        loadEncodingsFrequency("/home/sergey/ClickHouse/src/Functions/ClassificationDictionaries/charset_freq.txt");
    }


    void loadEncodingsFrequency(const std::string path_to_charset_freq)
    {
        char charset_name_buf [40];
        UInt16 bigram;
        double frequency;
        std::string charset_name;
        
        ReadBufferFromFile in(path_to_charset_freq);
        while (!in.eof())
        {
            char * newline = find_first_symbols<'\n'>(in.position(), in.buffer().end());
        
            if (newline >= in.buffer().end())
                break;

            std::string_view line(in.position(), newline - in.position());
            in.position() = newline + 1;

            if (line.empty())
                continue;
            // Start load new charset
            if (line.size() > 2 && line[0] == '/' && line[1] == '/')
            {
                const char * st = line.data();
                sscanf(st + 2, "%39s", charset_name_buf);
                std::string s(charset_name_buf);
                charset_name = s;
            } else
            {
                const char * st = line.data();
                sscanf(st, "%hd %lg", &bigram, &frequency);
                encodings_freq[charset_name][bigram] = frequency;

            }
        }
    }


    void loadEmotionalDict(const std::string path_to_emotional_dict)
    {

        char word_buf [40];
        double tonality;
        ReadBufferFromFile in(path_to_emotional_dict);
        while (!in.eof())
        {
            char * newline = find_first_symbols<'\n'>(in.position(), in.buffer().end());
        
            if (newline >= in.buffer().end())
                break;

            std::string_view line(in.position(), newline - in.position());
            in.position() = newline + 1;

            if (line.empty())
                continue;
            const char * st = line.data();
            sscanf(st, "%39s %lg", word_buf, &tonality);
            std::string word(word_buf);

            emotional_dict[word] = tonality;

        }
    }


    const std::string & get_path()
    {
        return is_true;
    }

    const std::unordered_map<std::string, std::vector<double>> getEmotionalDict()
    {
        return emotional_dict;
    }

    const Container getEncodingsFrequency()
    {
        return encodings_freq;
    }


protected:

    std::string is_true;
    std::unordered_map<std::string, double> emotional_dict;
    Container encodings_freq;
};
}

