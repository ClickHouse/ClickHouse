#include <Common/FrequencyHolder.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

#include <unordered_map>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/**
  * Determine the programming language from the source code.
  * We calculate all the unigrams and bigrams of commands in the source code.
  * Then using a marked-up dictionary with weights of unigrams and bigrams of commands for various programming languages
  * Find the biggest weight of the programming language and return it
  */
struct ProgrammingClassificationImpl
{
    /// Calculate total weight
    static ALWAYS_INLINE inline Float64 stateMachine(
        const FrequencyHolder::Map & standard,
        const std::unordered_map<String, Float64> & model)
    {
        Float64 res = 0;
        for (const auto & el : model)
        {
            /// Try to find each n-gram in dictionary
            const auto * it = standard.find(el.first);
            if (it != standard.end())
            {
                res += el.second * it->getMapped();
            }
        }
        return res;
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const auto & programming_freq = FrequencyHolder::getInstance().getProgrammingFrequency();

        /// Constant 5 is arbitrary
        res_data.reserve(offsets.size() * 5);
        res_offsets.resize(offsets.size());

        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * str = data.data() + offsets[i - 1];
            const size_t str_len = offsets[i] - offsets[i - 1] - 1;

            std::unordered_map<String, Float64> data_freq;

            String prev_command;
            String command;
            /// Select all commands from the string
            for (size_t ind = 0; ind < str_len;)
            {
                /// Assume that all commands are split by spaces
                if (!isspace(str[ind]))
                {
                    command.push_back(str[ind]);
                    ++ind;

                    while ((ind < str_len) && (!isspace(str[ind])))
                    {
                        command.push_back(str[ind]);
                        ++ind;
                    }
                    if (prev_command.empty())
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

            String res;
            Float64 max_result = 0;
            /// Iterate over all programming languages ​​and find the language with the highest weight
            for (const auto & item : programming_freq)
            {
                Float64 result = stateMachine(item.map, data_freq);
                if (result > max_result)
                {
                    max_result = result;
                    res = item.name;
                }
            }
            /// If all weights are zero, then we assume that the language is undefined
            if (res.empty())
                res = "Undefined";

            res_data.resize(res_offset + res.size() + 1);
            memcpy(&res_data[res_offset], res.data(), res.size());

            res_data[res_offset + res.size()] = 0;
            res_offset += res.size() + 1;

            res_offsets[i] = res_offset;
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Cannot apply function detectProgrammingLanguage to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct NameGetProgramming
{
    static constexpr auto name = "detectProgrammingLanguage";
};


using FunctionGetProgramming = FunctionStringToString<ProgrammingClassificationImpl, NameGetProgramming, false>;

void registerFunctionsProgrammingClassification(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetProgramming>();
}

}
