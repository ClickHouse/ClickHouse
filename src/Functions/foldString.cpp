#include "config.h"

#if USE_ICU

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <unicode/uchar.h>
#include <unicode/unorm2.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_NORMALIZE_STRING;
}

namespace
{

enum class FoldFunction
{
    CaseFold,
    AccentFold,
    FullFold
};

/// Shared processing pipeline for all fold functions.
/// Template parameters control which steps are compiled in.
template <FoldFunction fold_function>
struct FoldUTF8Impl
{
    static void process(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        bool aggressive,
        bool handle_special_i)
    {
        UErrorCode err = U_ZERO_ERROR;

        const UNormalizer2 * nfc_normalizer = unorm2_getNFCInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFC normalizer: {}", u_errorName(err));

        const UNormalizer2 * nfkc_cf_normalizer = nullptr;
        const UNormalizer2 * nfd_normalizer = nullptr;

        if constexpr (fold_function == FoldFunction::CaseFold || fold_function == FoldFunction::FullFold)
        {
            if (aggressive)
            {
                err = U_ZERO_ERROR;
                nfkc_cf_normalizer = unorm2_getNFKCCasefoldInstance(&err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFKC_Casefold normalizer: {}", u_errorName(err));
            }
        }

        if constexpr (fold_function == FoldFunction::AccentFold || fold_function == FoldFunction::FullFold)
        {
            err = U_ZERO_ERROR;
            nfd_normalizer = unorm2_getNFDInstance(&err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFD normalizer: {}", u_errorName(err));
        }

        uint32_t fold_options = U_FOLD_CASE_DEFAULT;
        if (handle_special_i)
            fold_options = U_FOLD_CASE_EXCLUDE_SPECIAL_I;

        res_offsets.resize(input_rows_count);
        res_data.reserve(data.size());

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        PODArray<UChar> buf1;
        PODArray<UChar> buf2;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from_size = offsets[i] - current_from_offset;

            if (from_size > 0)
            {
                /// Step 1: UTF-8 → UTF-16
                buf1.resize(from_size);
                int32_t u16_len = 0;
                err = U_ZERO_ERROR;
                u_strFromUTF8(
                    buf1.data(), static_cast<int32_t>(buf1.size()), &u16_len,
                    reinterpret_cast<const char *>(&data[current_from_offset]), static_cast<int32_t>(from_size),
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (strFromUTF8): {}", u_errorName(err));

                int32_t len = u16_len;

                if constexpr (fold_function == FoldFunction::CaseFold || fold_function == FoldFunction::FullFold)
                {
                    if (aggressive)
                    {
                        /// Aggressive: NFKC_Casefold (does NFKC + case fold in one shot)
                        buf2.resize(len * 18);
                        err = U_ZERO_ERROR;
                        len = unorm2_normalize(nfkc_cf_normalizer, buf1.data(), len,
                            buf2.data(), static_cast<int32_t>(buf2.size()), &err);
                        if (U_FAILURE(err))
                            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (NFKC_Casefold): {}", u_errorName(err));
                        std::swap(buf1, buf2);
                    }
                    else
                    {
                        /// Conservative step 1: NFC normalize
                        buf2.resize(len * 3);
                        err = U_ZERO_ERROR;
                        len = unorm2_normalize(nfc_normalizer, buf1.data(), len,
                            buf2.data(), static_cast<int32_t>(buf2.size()), &err);
                        if (U_FAILURE(err))
                            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (NFC pre-fold): {}", u_errorName(err));
                        std::swap(buf1, buf2);

                        /// Conservative step 2: Case fold
                        buf2.resize(len * 2);
                        err = U_ZERO_ERROR;
                        len = u_strFoldCase(buf2.data(), static_cast<int32_t>(buf2.size()),
                            buf1.data(), len, fold_options, &err);
                        if (U_FAILURE(err))
                            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (u_strFoldCase): {}", u_errorName(err));
                        std::swap(buf1, buf2);
                    }
                }

                if constexpr (fold_function == FoldFunction::AccentFold || fold_function == FoldFunction::FullFold)
                {
                    /// NFD decompose (to separate base characters from combining marks)
                    buf2.resize(len * 4);
                    err = U_ZERO_ERROR;
                    len = unorm2_normalize(nfd_normalizer, buf1.data(), len,
                        buf2.data(), static_cast<int32_t>(buf2.size()), &err);
                    if (U_FAILURE(err))
                        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (NFD): {}", u_errorName(err));

                    /// Strip combining marks (category Mn = U_NON_SPACING_MARK)
                    int32_t write_pos = 0;
                    for (int32_t j = 0; j < len; ++j)
                    {
                        UChar32 cp;
                        int32_t prev = j;
                        U16_NEXT(buf2.data(), j, len, cp);
                        if (u_charType(cp) != U_NON_SPACING_MARK)
                        {
                            /// Copy the code units for this code point
                            for (int32_t k = prev; k < j; ++k)
                                buf2[write_pos++] = buf2[k];
                        }
                        j--; /// U16_NEXT already advanced j; the loop will advance again
                    }
                    len = write_pos;
                    std::swap(buf1, buf2);
                }

                if constexpr (fold_function == FoldFunction::FullFold)
                {
                    /// Final NFC recompose (both paths)
                    buf2.resize(len * 3);
                    err = U_ZERO_ERROR;
                    len = unorm2_normalize(nfc_normalizer, buf1.data(), len,
                        buf2.data(), static_cast<int32_t>(buf2.size()), &err);
                    if (U_FAILURE(err))
                        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (NFC final): {}", u_errorName(err));
                    std::swap(buf1, buf2);
                }
                else if constexpr (fold_function == FoldFunction::CaseFold)
                {
                    /// caseFoldUTF8: NFC recompose at the end
                    buf2.resize(len * 3);
                    err = U_ZERO_ERROR;
                    len = unorm2_normalize(nfc_normalizer, buf1.data(), len,
                        buf2.data(), static_cast<int32_t>(buf2.size()), &err);
                    if (U_FAILURE(err))
                        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (NFC recompose): {}", u_errorName(err));
                    std::swap(buf1, buf2);
                }
                else if constexpr (fold_function == FoldFunction::AccentFold)
                {
                    /// accentFoldUTF8: NFC recompose at the end
                    buf2.resize(len * 3);
                    err = U_ZERO_ERROR;
                    len = unorm2_normalize(nfc_normalizer, buf1.data(), len,
                        buf2.data(), static_cast<int32_t>(buf2.size()), &err);
                    if (U_FAILURE(err))
                        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (NFC recompose): {}", u_errorName(err));
                    std::swap(buf1, buf2);
                }

                /// UTF-16 → UTF-8
                size_t max_to_size = current_to_offset + 4 * static_cast<size_t>(len);
                if (res_data.size() < max_to_size)
                    res_data.resize(max_to_size);

                int32_t to_size = 0;
                err = U_ZERO_ERROR;
                u_strToUTF8(
                    reinterpret_cast<char *>(&res_data[current_to_offset]),
                    static_cast<int32_t>(res_data.size() - current_to_offset),
                    &to_size,
                    buf1.data(), len, &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (strToUTF8): {}", u_errorName(err));

                current_to_offset += to_size;
            }

            res_offsets[i] = current_to_offset;
            current_from_offset = offsets[i];
        }

        res_data.resize(current_to_offset);
    }
};


template <FoldFunction fold_function>
class FunctionFoldUTF8 : public IFunction
{
public:
    static constexpr auto name =
        fold_function == FoldFunction::CaseFold ? "caseFoldUTF8" :
        fold_function == FoldFunction::AccentFold ? "accentFoldUTF8" :
        "foldUTF8";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFoldUTF8>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (fold_function == FoldFunction::CaseFold)
            return {1};
        else if constexpr (fold_function == FoldFunction::AccentFold)
            return {1};
        else
            return {1, 2};
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if constexpr (fold_function == FoldFunction::CaseFold)
        {
            if (arguments.empty() || arguments.size() > 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires 1 or 2 arguments, got {}", getName(), arguments.size());
        }
        else if constexpr (fold_function == FoldFunction::AccentFold)
        {
            if (arguments.empty() || arguments.size() > 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires 1 or 2 arguments, got {}", getName(), arguments.size());
        }
        else
        {
            if (arguments.empty() || arguments.size() > 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires 1 to 3 arguments, got {}", getName(), arguments.size());
        }

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String",
                arguments[0]->getName(), getName());

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (!isString(arguments[i]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}, expected String",
                    arguments[i]->getName(), i + 1, getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & col = arguments[0].column;
        const ColumnString * col_str = checkAndGetColumn<ColumnString>(col.get());
        if (!col_str)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", col->getName(), getName());

        bool aggressive = true;
        bool handle_special_i = false;

        if constexpr (fold_function == FoldFunction::CaseFold)
        {
            if (arguments.size() >= 2)
                aggressive = parseMethodArgument(arguments[1], "second");
        }
        else if constexpr (fold_function == FoldFunction::AccentFold)
        {
            if (arguments.size() >= 2)
                handle_special_i = parseBoolArgument(arguments[1], "second");
        }
        else /// FullFold
        {
            if (arguments.size() >= 2)
                aggressive = parseMethodArgument(arguments[1], "second");
            if (arguments.size() >= 3)
                handle_special_i = parseBoolArgument(arguments[2], "third");
        }

        auto col_res = ColumnString::create();
        FoldUTF8Impl<fold_function>::process(
            col_str->getChars(), col_str->getOffsets(),
            col_res->getChars(), col_res->getOffsets(),
            input_rows_count, aggressive, handle_special_i);
        return col_res;
    }

private:
    static bool parseMethodArgument(const ColumnsWithTypeAndName::value_type & arg, const char * ordinal)
    {
        const ColumnConst * col_const = checkAndGetColumnConstStringOrFixedString(arg.column.get());
        if (!col_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The {} argument of function {} must be a constant string ('aggressive' or 'conservative')", ordinal,
                fold_function == FoldFunction::CaseFold ? "caseFoldUTF8" : "foldUTF8");

        String method = col_const->getValue<String>();
        if (method == "aggressive")
            return true;
        if (method == "conservative")
            return false;
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid method '{}' for function {}, expected 'aggressive' or 'conservative'", method,
            fold_function == FoldFunction::CaseFold ? "caseFoldUTF8" : "foldUTF8");
    }

    static bool parseBoolArgument(const ColumnsWithTypeAndName::value_type & arg, const char * ordinal)
    {
        const ColumnConst * col_const = checkAndGetColumnConstStringOrFixedString(arg.column.get());
        if (!col_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The {} argument of function {} must be a constant string ('true' or 'false')", ordinal,
                fold_function == FoldFunction::AccentFold ? "accentFoldUTF8" : "foldUTF8");

        String val = col_const->getValue<String>();
        if (val == "true" || val == "1")
            return true;
        if (val == "false" || val == "0")
            return false;
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid value '{}' for handle_special_I parameter, expected 'true' or 'false'", val);
    }
};


using FunctionCaseFoldUTF8 = FunctionFoldUTF8<FoldFunction::CaseFold>;
using FunctionAccentFoldUTF8 = FunctionFoldUTF8<FoldFunction::AccentFold>;
using FunctionFullFoldUTF8 = FunctionFoldUTF8<FoldFunction::FullFold>;

}

REGISTER_FUNCTION(FoldUTF8)
{
    /// caseFoldUTF8
    FunctionDocumentation::Description case_desc = R"(
Applies Unicode case folding to a UTF-8 string, converting it to a lowercase-like normalized form suitable for case-insensitive comparisons.
Two methods are available: 'aggressive' (default) applies NFKC_Casefold normalization which also resolves compatibility equivalences;
'conservative' applies NFC normalization followed by standard Unicode case folding.
)";
    FunctionDocumentation::Syntax case_syntax = "caseFoldUTF8(str[, method])";
    FunctionDocumentation::Arguments case_args = {
        {"str", "UTF-8 encoded input string.", {"String"}},
        {"method", "Optional. 'aggressive' (default) or 'conservative'.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue case_ret = {"Case-folded UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples case_examples = {{
        "Usage example",
        "SELECT caseFoldUTF8('Straße'), caseFoldUTF8('Straße', 'conservative')",
        R"(
┌─caseFoldUTF8('Straße')─┬─caseFoldUTF8('Straße', 'conservative')─┐
│ strasse                 │ strasse                                 │
└─────────────────────────┴─────────────────────────────────────────┘
)"
    }};
    FunctionDocumentation::IntroducedIn case_intro = {25, 6};
    FunctionDocumentation::Category case_cat = FunctionDocumentation::Category::String;
    factory.registerFunction<FunctionCaseFoldUTF8>({case_desc, case_syntax, case_args, {}, case_ret, case_examples, case_intro, case_cat});

    /// accentFoldUTF8
    FunctionDocumentation::Description accent_desc = R"(
Removes diacritical marks (accents) from a UTF-8 string by decomposing characters via NFD and stripping combining marks (Unicode category Mn), then recomposing via NFC.
)";
    FunctionDocumentation::Syntax accent_syntax = "accentFoldUTF8(str[, handle_special_I])";
    FunctionDocumentation::Arguments accent_args = {
        {"str", "UTF-8 encoded input string.", {"String"}},
        {"handle_special_I", "Optional. 'true' to enable Turkish/Azerbaijani special I handling. Default 'false'.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue accent_ret = {"UTF-8 string with diacritics removed.", {"String"}};
    FunctionDocumentation::Examples accent_examples = {{
        "Usage example",
        "SELECT accentFoldUTF8('café résumé naïve')",
        R"(
┌─accentFoldUTF8('café résumé naïve')─┐
│ cafe resume naive                    │
└──────────────────────────────────────┘
)"
    }};
    factory.registerFunction<FunctionAccentFoldUTF8>({accent_desc, accent_syntax, accent_args, {}, accent_ret, accent_examples, case_intro, case_cat});

    /// foldUTF8
    FunctionDocumentation::Description fold_desc = R"(
Applies both case folding and accent (diacritical mark) removal to a UTF-8 string.
'aggressive' mode (default) applies NFKC_Casefold, then NFD + strip combining marks + NFC.
'conservative' mode applies NFC, case fold, NFD, strip combining marks, then NFC.
)";
    FunctionDocumentation::Syntax fold_syntax = "foldUTF8(str[, case_fold_method][, handle_special_I])";
    FunctionDocumentation::Arguments fold_args = {
        {"str", "UTF-8 encoded input string.", {"String"}},
        {"case_fold_method", "Optional. 'aggressive' (default) or 'conservative'.", {"String"}},
        {"handle_special_I", "Optional. 'true' to enable Turkish/Azerbaijani special I handling. Default 'false'.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue fold_ret = {"Case-folded and accent-stripped UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples fold_examples = {{
        "Usage example",
        "SELECT foldUTF8('Café Résumé')",
        R"(
┌─foldUTF8('Café Résumé')─┐
│ cafe resume               │
└───────────────────────────┘
)"
    }};
    factory.registerFunction<FunctionFullFoldUTF8>({fold_desc, fold_syntax, fold_args, {}, fold_ret, fold_examples, case_intro, case_cat});
}

}

#endif
