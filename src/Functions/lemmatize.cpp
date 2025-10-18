#include "config.h"

#if USE_NLP

#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Lemmatizers.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_nlp_functions;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

struct LemmatizeImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        Lemmatizers::LemmPtr & lemmatizer)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);

        UInt64 data_size = 0;
        String buffer;
        for (UInt64 i = 0; i < offsets.size(); ++i)
        {
            /// `lemmatize` requires terminating zero
            buffer.assign(reinterpret_cast<const char *>(data.data() + offsets[i - 1]), offsets[i] - offsets[i - 1]);
            auto result = lemmatizer->lemmatize(buffer.c_str());
            size_t new_size = strlen(result.get());

            if (data_size + new_size > res_data.size())
                res_data.resize(data_size + new_size);

            memcpy(res_data.data() + data_size, reinterpret_cast<const unsigned char *>(result.get()), new_size);

            data_size += new_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
    }
};


class FunctionLemmatize : public IFunction
{
public:
    static constexpr auto name = "lemmatize";
    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_nlp_functions])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Natural language processing function '{}' is experimental. "
                            "Set `allow_experimental_nlp_functions` setting to enable it", name);

        return std::make_shared<FunctionLemmatize>(context->getLemmatizers());
    }

private:
    Lemmatizers & lemmatizers;

public:
    explicit FunctionLemmatize(Lemmatizers & lemmatizers_)
        : lemmatizers(lemmatizers_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());
        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[1]->getName(), getName());
        return arguments[1];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & langcolumn = arguments[0].column;
        const auto & strcolumn = arguments[1].column;

        const ColumnConst * lang_col = checkAndGetColumn<ColumnConst>(langcolumn.get());
        const ColumnString * words_col = checkAndGetColumn<ColumnString>(strcolumn.get());

        if (!lang_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());
        if (!words_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[1].column->getName(), getName());

        String language = lang_col->getValue<String>();
        auto lemmatizer = lemmatizers.getLemmatizer(language);

        auto col_res = ColumnString::create();
        LemmatizeImpl::vector(words_col->getChars(), words_col->getOffsets(), col_res->getChars(), col_res->getOffsets(), lemmatizer);
        return col_res;
    }
};

}

REGISTER_FUNCTION(Lemmatize)
{
    FunctionDocumentation::Description description = R"(
Performs lemmatization on a given word.
This function needs dictionaries to operate, which can be obtained from [github](https://github.com/vpodpecan/lemmagen3/tree/master/src/lemmagen3/models). For more details on loading a dictionary from a local file see page ["Defining Dictionaries"](/sql-reference/dictionaries#local-file).
)";
    FunctionDocumentation::Syntax syntax = "lemmatize(lang, word)";
    FunctionDocumentation::Arguments arguments = {
        {"lang", "Language which rules will be applied.", {"String"}},
        {"word", "Lowercase word that needs to be lemmatized.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the lemmatized form of the word", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"English lemmatization", "SELECT lemmatize('en', 'wolves')", "wolf"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::NLP;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLemmatize>(documentation);
}

}

#endif
