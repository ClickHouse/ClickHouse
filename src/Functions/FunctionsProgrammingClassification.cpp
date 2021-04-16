#include <Functions/FunctionsTextClassification.h>
#include <Common/FrequencyHolder.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadHelpers.h>

#include <unordered_map>

namespace DB
{


struct ProgrammingClassificationImpl
{

    using ResultType = String;

    static ALWAYS_INLINE inline Float64 state_machine(std::unordered_map<String, Float64> standart, std::unordered_map<String, Float64> model)
    {
        Float64 res = 0;
        for (auto & el : model)
        {
            res += el.second * standart[el.first];
        }
        return res;
    }


    
    static void constant(String data, String & res)
    {
        static std::unordered_map<String, std::unordered_map<String, Float64>> programming_freq = FrequencyHolder::getInstance().getProgrammingFrequency();
        std::unordered_map<String, Float64> data_freq;

        String answer;

        ReadBufferFromMemory in(data.data(), data.size() + 1);
        skipWhitespaceIfAny(in);

        String prev = "";
        String new_word;
        
        while (!in.eof())
        {
            if (data.size() - (in.position() - data.data()) <= 3)
            {
                break;
            }
            readStringUntilWhitespace(new_word, in);
            skipWhitespaceIfAny(in);

            if (prev == "")
            {
                prev = new_word;
            }
            else
            {
                data_freq[prev + new_word] += 1;
                prev = new_word;
            }
        }

        String most_liked;
        Float64 max_result = 0;

        for (const auto& item : programming_freq)
        {
            Float64 result = state_machine(item.second, data_freq);
            if (result > max_result)
            {
                max_result = result;
                most_liked = item.first; 
            }
        }

        res = most_liked;
    }


    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        static std::unordered_map<String, std::unordered_map<String, Float64>> programming_freq = FrequencyHolder::getInstance().getProgrammingFrequency();
        std::unordered_map<String, Float64> data_freq;

        res_data.reserve(1024);
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * haystack = reinterpret_cast<const char *>(&data[prev_offset]);
            String str = haystack;

            String buf;

            ReadBufferFromMemory in(str.data(), str.size() + 1);

            skipWhitespaceIfAny(in);
            String new_word;
            String prev;
            while (!in.eof())
            {
                if (str.size() - (in.position() - str.data()) <= 3)
                {
                    break;
                }
                readStringUntilWhitespace(new_word, in);
                skipWhitespaceIfAny(in);

                if (prev == "")
                {
                    prev = new_word;
                } 
                else
                {
                    data_freq[prev + new_word] += 1;
                    prev = new_word;
                }
            }

            String most_liked;
            Float64 max_result = 0;

            for (const auto& item : programming_freq)
            {
                Float64 result = state_machine(item.second, data_freq);
                if (result > max_result)
                {
                    max_result = result;
                    most_liked = item.first; 
                }
            }

            const auto ans = most_liked.c_str();
            size_t cur_offset = offsets[i];

            res_data.resize(res_offset + strlen(ans) + 1);
            memcpy(&res_data[res_offset], ans, strlen(ans));
            res_offset += strlen(ans);

            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }


};

struct NameGetProgramming
{
    static constexpr auto name = "getProgrammingLanguage";
};


using FunctionGetProgramming = FunctionsTextClassification<ProgrammingClassificationImpl, NameGetProgramming>;

void registerFunctionsProgrammingClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetProgramming>();
}

}
