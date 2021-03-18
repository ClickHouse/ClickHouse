#pragma once

#include <Functions/FunctionsTextClassification.h>

#include <string>
#include <Functions/FunctionFactory.h>
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
        loadEmotionalDict("/home/sergey/datadump/myemo2.txt");
        loadEncodingsFrequency("/home/sergey/data/dumps/encodings/russian/freq_enc/");
    }


    void loadEncodingsFrequency(const std::string path_to_encodings_freq)
    {
        std::vector<std::string> languages = {"freq_CP866", "freq_ISO", "freq_WINDOWS-1251", "freq_UTF-8"};
        for (std::string & lang : languages) {
            std::ifstream file(path_to_encodings_freq + lang + ".txt");
            Map new_lang;
            UInt16 bigram;
            double count;
            double total = 0;
            while (file >> bigram >> count) {
                new_lang[bigram] = count;
                total += count;
            }
            for (auto & el : new_lang) {
                el.second /= total;
            }
            encodings_freq[lang] = new_lang;
            file.close();
        }
    }


    void loadEmotionalDict(const std::string path_to_emotional_dict)
    {
        std::ifstream file(path_to_emotional_dict);
        std::string term, tag;
        double val;
        while (file >> term >> tag >> val) {
            std::vector<double> cur = {val};
            emotional_dict[term] = cur;
        }
        file.close();
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
    std::unordered_map<std::string, std::vector<double>> emotional_dict;
    Container encodings_freq;
};
}

