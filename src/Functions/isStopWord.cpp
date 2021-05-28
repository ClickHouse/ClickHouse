#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Interpreters/Context.h>

#include <fstream>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DICTIONARIES_WAS_NOT_LOADED;
}

namespace
{

struct IsStopWordImpl
{   
    static void load(const String & path, std::unordered_set<String> & stop_words) {
        std::ifstream file(path);
        if (!file.is_open()) {
            throw Exception(
            "Cannot find profile " + path + " for function isStopWord",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        String line, word;
        while (getline(file, line)) {
        for (auto i : line) {
            if (i == '#' || i == '|') {
            break;
            } else if (!isspace(i)) {
            word += i;
            }
        }
        if (!word.empty()) {
            stop_words.insert(word);
        }
        word.erase();
        }
    }

    static bool isStopWord (const char * data, UInt64 length, std::unordered_set<String> & stop_words) 
    { 
        return stop_words.find({data, length}) != stop_words.end();
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt8> & res,
        const String & language)
    {
        std::unordered_set<String> stop_words;
        load(language, stop_words);

        size_t size = offsets.size();
        size_t prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = isStopWord(reinterpret_cast<const char *>(data.data() + prev_offset), offsets[i] - 1 - prev_offset, stop_words);
            prev_offset = offsets[i];
        }
    }
};


class FunctionIsStopWord : public IFunction
{
public:
    static constexpr auto name = "isStopWord";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsStopWord>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & langcolumn = arguments[0].column;
        const auto & strcolumn = arguments[1].column;
        // ColumnPtr res;

        const ColumnConst * lang_col = checkAndGetColumn<ColumnConst>(langcolumn.get());
        const ColumnString * words_col = checkAndGetColumn<ColumnString>(strcolumn.get());

        if (!lang_col) 
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        if (!words_col)
            throw Exception(
                "Illegal column " + arguments[1].column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        String language = lang_col->getValue<String>();

        auto col_res = ColumnUInt8::create();
        typename ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(words_col->size());

        IsStopWordImpl::vector(words_col->getChars(), words_col->getOffsets(), vec_res, language);
        return col_res;
    }
};

}

void registerFunctionIsStopWord(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsStopWord>(FunctionFactory::CaseInsensitive);
}

}
