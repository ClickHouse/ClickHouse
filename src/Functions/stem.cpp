#include "config.h"

#if USE_NLP

#include <algorithm>
#include <cstring>
#include <string_view>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/StringUtils.h>
#include <Interpreters/Context.h>

#include <libstemmer.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_nlp_functions;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

bool isValidStemWordType(const IDataType & type)
{
    if (isStringOrFixedString(type))
        return true;
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(&type))
    {
        const IDataType & nested = *array_type->getNestedType();
        if (isStringOrFixedString(nested))
            return true;
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&nested))
            return isStringOrFixedString(*nullable_type->getNestedType());
    }
    return false;
}


/// RAII wrapper around sb_stemmer. Constructed from a language code; throws
/// ILLEGAL_TYPE_OF_ARGUMENT in the constructor if the language is unsupported.
class Stemmer
{
public:
    explicit Stemmer(const String & language)
        : handle(sb_stemmer_new(language.c_str(), "UTF_8"))
    {
        if (!handle)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Language '{}' is not supported for function stem",
                language);
    }

    ~Stemmer() { sb_stemmer_delete(handle); }

    Stemmer(const Stemmer &) = delete;
    Stemmer & operator=(const Stemmer &) = delete;

    /// Stem a single word given as a byte range. Returns a string_view over the
    /// stemmer's internal buffer (valid until the next call to stem).
    /// Throws BAD_ARGUMENTS if the input contains whitespace, or
    /// CANNOT_ALLOCATE_MEMORY if the stemmer runs out of memory.
    /// Note: the input must be lowercase; passing uppercase characters produces undefined results.
    std::string_view stem(std::string_view word)
    {
        if (std::any_of(word.begin(), word.end(), isWhitespaceASCII))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function stem requires each input to be a single word without whitespace, "
                "but got: '{}'",
                String(word));

        const sb_symbol * result = sb_stemmer_stem(
            handle,
            reinterpret_cast<const sb_symbol *>(word.data()),
            static_cast<int>(word.size()));

        if (unlikely(!result))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Function stem failed to allocate memory");

        return {reinterpret_cast<const char *>(result), static_cast<size_t>(sb_stemmer_length(handle))};
    }

    /// Stem all rows of a String or FixedString column into a new ColumnString.
    /// Rows where null_map[i] != 0 are skipped and emitted as empty strings.
    /// For FixedString, getDataAt returns the value with null-byte padding; trimRight removes it.
    /// For String, trailing zero bytes are valid data and must not be trimmed.
    /// Snowball stemming never lengthens a word, so upper_bound bytes is a safe pre-allocation.
    MutableColumnPtr stemColumn(const IColumn & col, size_t input_rows_count, const NullMap * null_map = nullptr)
    {
        size_t upper_bound;
        const bool is_fixed_string = checkAndGetColumn<ColumnFixedString>(&col) != nullptr;
        if (const auto * col_str = checkAndGetColumn<ColumnString>(&col))
            upper_bound = col_str->getChars().size();
        else if (const auto * col_fixed_str = checkAndGetColumn<ColumnFixedString>(&col))
            upper_bound = col_fixed_str->getChars().size();
        else
            UNREACHABLE();

        auto col_res = ColumnString::create();
        ColumnString::Chars & res_data = col_res->getChars();
        ColumnString::Offsets & res_offsets = col_res->getOffsets();

        res_data.resize(upper_bound);
        res_offsets.resize(input_rows_count);

        size_t data_size = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                res_offsets[i] = data_size;
                continue;
            }

            std::string_view word = col.getDataAt(i);
            if (is_fixed_string)
                trimRight(word, '\0');
            std::string_view stemmed = stem(word);
            chassert(data_size + stemmed.size() <= res_data.size());

            memcpy(res_data.data() + data_size, stemmed.data(), stemmed.size());
            data_size += stemmed.size();
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
        return col_res;
    }

private:
    sb_stemmer * handle;
};


class FunctionStem : public IFunction
{
public:
    static constexpr auto name = "stem";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_nlp_functions])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Natural language processing function '{}' is experimental. "
                "Set `allow_experimental_nlp_functions` setting to enable it",
                name);

        return std::make_shared<FunctionStem>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"word", &isValidStemWordType, nullptr, "String, FixedString, Array(String), Array(FixedString), Array(Nullable(String)), or Array(Nullable(FixedString))"},
            {"language", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };
        validateFunctionArguments(*this, arguments, args);

        const IDataType & arg0 = *arguments[0].type;

        if (isStringOrFixedString(arg0))
            return std::make_shared<DataTypeString>();

        const auto & array_type = assert_cast<const DataTypeArray &>(arg0);
        const IDataType & nested = *array_type.getNestedType();

        if (isStringOrFixedString(nested))
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());

        /// Array(Nullable(String/FixedString))
        const auto & nullable_type = assert_cast<const DataTypeNullable &>(nested);
        chassert(isStringOrFixedString(*nullable_type.getNestedType()));
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnConst * language_col = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
        chassert(language_col);

        Stemmer stemmer(language_col->getValue<String>());

        const ColumnPtr & input_col = arguments[0].column;

        /// Case 1: String or FixedString → String
        if (checkAndGetColumn<ColumnString>(input_col.get()) || checkAndGetColumn<ColumnFixedString>(input_col.get()))
            return stemmer.stemColumn(*input_col, input_rows_count);

        /// Case 2: Array(String/FixedString/Nullable(String/FixedString)) → Array(...)
        const auto & array_col = assert_cast<const ColumnArray &>(*input_col);
        const IColumn & array_data = array_col.getData();
        const size_t nested_rows = array_col.getOffsets().empty() ? 0 : array_col.getOffsets().back();

        MutableColumnPtr stemmed;
        if (const auto * nullable_nested = checkAndGetColumn<ColumnNullable>(&array_data))
        {
            auto stemmed_inner = stemmer.stemColumn(
                nullable_nested->getNestedColumn(), nested_rows, &nullable_nested->getNullMapData());
            stemmed = ColumnNullable::create(std::move(stemmed_inner), nullable_nested->getNullMapColumn().clone());
        }
        else
        {
            stemmed = stemmer.stemColumn(array_data, nested_rows);
        }

        return ColumnArray::create(std::move(stemmed), array_col.getOffsetsPtr());
    }
};

}

REGISTER_FUNCTION(Stem)
{
    FunctionDocumentation::Description description = R"(
Performs stemming on a word or an array of words using the Snowball algorithms.
Each input string must be a single, lowercase word — strings containing whitespace cause an exception.
Passing uppercase characters produces undefined results.
Returns String for scalar inputs (including FixedString) and Array(String) for array inputs.
Nullable and LowCardinality variants of String and FixedString are supported.
)";
    FunctionDocumentation::Syntax syntax = "stem(word, language)";
    FunctionDocumentation::Arguments arguments = {
        {"word",
         "A single lowercase word (or array of words) to stem. "
         "Must be lowercase — uppercase characters produce undefined results. "
         "Accepts String, FixedString, Array(String), Array(FixedString), "
         "Array(Nullable(String)), or Array(Nullable(FixedString)).",
         {"String", "FixedString", "Array(String)", "Array(FixedString)"}},
        {"language",
         "Language whose stemming rules will be applied. Use the two-letter ISO 639-1 code (e.g. 'en', 'de', 'fr'), see https://en.wikipedia.org/wiki/List_of_ISO_639_language_codes.",
         {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "The stemmed form of the word (String), or an array of stemmed words (Array(String)).",
        {"String", "Array(String)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Stemming a single word",
            "SELECT stem('blessing', 'en') AS res",
            "bless",
        },
        {
            "Stemming an array of words",
            "SELECT stem(['blessing', 'disguise'], 'en') AS res",
            "['bless','disguis']",
        },
        {
            "Stemming a FixedString",
            "SELECT stem(toFixedString('blessing', 10), 'en') AS res",
            "bless",
        },
        {
            "Stemming a Nullable word",
            "SELECT stem(toNullable('blessing'), 'en') AS res",
            "bless",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::NLP;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStem>(documentation);
}

}

#endif
