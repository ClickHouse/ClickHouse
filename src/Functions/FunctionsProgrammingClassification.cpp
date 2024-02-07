#include <Common/FrequencyHolder.h>
#include <Common/StringUtils/StringUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTextClassification.h>

#include <unordered_map>
#include <string_view>

namespace DB
{

/**
  * Determine the programming language from the source code.
  * We calculate all the unigrams and bigrams of commands in the source code.
  * Then using a marked-up dictionary with weights of unigrams and bigrams of commands for various programming languages
  * Find the biggest weight of the programming language and return it
  */
struct FunctionDetectProgrammingLanguageImpl
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
                res += el.second * it->getMapped();
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
            StringRef prev_command;
            StringRef command;

            /// Select all commands from the string
            for (size_t ind = 0; ind < str_len; ++ind)
            {
                /// Assume that all commands are split by spaces
                if (isWhitespaceASCII(str[ind]))
                    continue;

                size_t prev_ind = ind;
                while (ind < str_len && !isWhitespaceASCII(str[ind]))
                    ++ind;

                command = {str + prev_ind, ind - prev_ind};

                /// We add both unigrams and bigrams to later search for them in the dictionary
                if (prev_command.data)
                    data_freq[prev_command.toString() + command.toString()] += 1;

                data_freq[command.toString()] += 1;
                prev_command = command;
            }

            std::string_view res;
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
};

struct NameDetectProgrammingLanguage
{
    static constexpr auto name = "detectProgrammingLanguage";
};


using FunctionDetectProgrammingLanguage = FunctionTextClassificationString<FunctionDetectProgrammingLanguageImpl, NameDetectProgrammingLanguage>;

REGISTER_FUNCTION(DetectProgrammingLanguage)
{
    factory.registerFunction<FunctionDetectProgrammingLanguage>();
}

}
