#include <Functions/FunctionsTextClassification.h>
#include <Common/FrequencyHolder.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadHelpers.h>

#include <unordered_map>

namespace DB
{
/**
  * Determine the programming language from the source code.
  * We calculate all the unigrams and bigrams of commands in the source code.
  * Then using a marked-up dictionary with weights of unigrams and bigrams of commands for various programming languages
  * Find the biggest weight of the programming language and return it
  */
struct ProgrammingClassificationImpl
{

    using ResultType = String;
    /// Calculate total weight
    static ALWAYS_INLINE inline Float64 state_machine(std::unordered_map<String, Float64>& standard, std::unordered_map<String, Float64>& model)
    {
        Float64 res = 0;
        for (auto & el : model)
        {
            /// Try to find each n-gram in dictionary
            if (standard.find(el.first) != standard.end())
            {
                res += el.second * standard[el.first];
            }
        }
        return res;
    }



    static void constant(String data, String & res)
    {
        static std::unordered_map<String, std::unordered_map<String, Float64>> programming_freq = FrequencyHolder::getInstance().getProgrammingFrequency();
        std::unordered_map<String, Float64> data_freq;

        String prev_command;
        String command;
        /// Select all commands from the string
        for (size_t i = 0; i < data.size();)
        {
            /// Assume that all commands are splitted by spaces
            if (!isspace(data[i]))
            {
                command.push_back(data[i]);
                ++i;

                while ((i < data.size()) && (!isspace(data[i])))
                {
                    command.push_back(data[i]);
                    ++i;
                }
                if (prev_command == "")
                {
                    prev_command = command;
                }
                else
                {
                    data_freq[prev_command + command] += 1;
                    data_freq[prev_command] += 1;
                    prev_command = command;
                }
                command = "";
            }
            else
            {
                ++i;
            }
        }

        String most_liked;
        Float64 max_result = 0;
        /// Iterate over all programming languages ​​and find the language with the highest weight
        for (auto& item : programming_freq)
        {
            Float64 result = state_machine(item.second, data_freq);
            if (result > max_result)
            {
                max_result = result;
                most_liked = item.first;
            }
        }
        /// If all weights are zero, then we assume that the language is undefined
        if (most_liked == "")
        {
            most_liked = "Undefined";
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

        res_data.reserve(1024);
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const char * haystack = reinterpret_cast<const char *>(&data[prev_offset]);
            std::unordered_map<String, Float64> data_freq;
            String str_data = haystack;

            String prev_command;
            String command;
            /// Select all commands from the string
            for (size_t ind = 0; ind < str_data.size();)
            {
                /// Assume that all commands are splitted by spaces
                if (!isspace(str_data[ind]))
                {
                    command.push_back(str_data[ind]);
                    ++ind;

                    while ((ind < str_data.size()) && (!isspace(str_data[ind])))
                    {
                        command.push_back(str_data[ind]);
                        ++ind;
                    }
                    if (prev_command == "")
                    {
                        prev_command = command;
                    }
                    else
                    {
                        data_freq[prev_command + command] += 1;
                        data_freq[prev_command] += 1;
                        prev_command = command;
                    }
                    command = "";
                }
                else
                {
                    ++ind;
                }
            }

            String most_liked;
            Float64 max_result = 0;
            /// Iterate over all programming languages ​​and find the language with the highest weight
            for (auto& item : programming_freq)
            {
                Float64 result = state_machine(item.second, data_freq);
                if (result > max_result)
                {
                    max_result = result;
                    most_liked = item.first;
                }
            }
            /// If all weights are zero, then we assume that the language is undefined
            if (most_liked == "")
            {
                most_liked = "Undefined";
            }

            const auto ans = most_liked.c_str();
            size_t cur_offset = offsets[i];
            size_t ans_size = strlen(ans);
            res_data.resize(res_offset + ans_size + 1);
            memcpy(&res_data[res_offset], ans, ans_size);
            res_offset += ans_size;

            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
            prev_offset = cur_offset;
        }
    }


};

struct NameGetProgramming
{
    static constexpr auto name = "detectProgrammingLanguage";
};


using FunctionGetProgramming = FunctionsTextClassification<ProgrammingClassificationImpl, NameGetProgramming>;

void registerFunctionsProgrammingClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetProgramming>();
}

}
