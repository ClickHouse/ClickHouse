#include "config.h"

#if USE_ICU

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
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

/// Maximum expansion factors for UTF-16 normalization/folding operations.
/// See https://unicode.org/faq/normalization.html#12
constexpr int MAX_NFC_EXPANSION = 3;
constexpr int MAX_NFD_EXPANSION = 4;
constexpr int MAX_NFKC_CASEFOLD_EXPANSION = 18;

/// Case folding can also expand (e.g. `ﬃ` → `ffi`). See https://unicode.org/Public/UCD/latest/ucd/CaseFolding.txt
constexpr int MAX_CASEFOLD_EXPANSION = 3;

/// Each UTF-16 code unit produces at most 3 UTF-8 bytes.
/// Chars which require 4 UTF-8 bytes also require 2 UTF-16 code units, so the max expansion factor is 3.
constexpr int MAX_UTF16_TO_UTF8_EXPANSION = 3;


/// Normalizer context: holds ICU normalizer instances needed by a pipeline.
struct FoldContext
{
    const UNormalizer2 * nfc = nullptr;
    const UNormalizer2 * nfd = nullptr;
    const UNormalizer2 * nfkc_cf = nullptr;
    uint32_t fold_options = U_FOLD_CASE_DEFAULT;
    bool aggressive = true;
};

/// Helper: normalize in[0..len) into out, return new length.
/// in and out must be distinct PODArrays.
inline int32_t normalizeBuffer(
    const UNormalizer2 * normalizer,
    PODArray<UChar> & in, int32_t len,
    PODArray<UChar> & out, int expansion,
    const char * step_name)
{
    out.resize(len * expansion);
    UErrorCode err = U_ZERO_ERROR;
    int32_t result = unorm2_normalize(normalizer, in.data(), len, out.data(), static_cast<int32_t>(out.size()), &err);
    if (U_FAILURE(err))
        throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed ({}): {}", step_name, u_errorName(err));
    return result;
}

/// Helper: strip combining marks (Mn category) in-place, return new length.
inline int32_t stripCombiningMarks(UChar * data, int32_t len)
{
    int32_t write_pos = 0;
    for (int32_t j = 0; j < len;)
    {
        UChar32 cp;
        int32_t prev = j;
        U16_NEXT(data, j, len, cp); /// advances j to next code point boundary
        if (u_charType(cp) != U_NON_SPACING_MARK)
        {
            for (int32_t k = prev; k < j; ++k)
                data[write_pos++] = data[k];
        }
    }
    return write_pos;
}

struct CaseFoldImpl
{
    static constexpr auto name = "caseFoldUTF8";

    static void init(FoldContext & ctx, bool aggressive, bool handle_special_i)
    {
        UErrorCode err = U_ZERO_ERROR;
        ctx.aggressive = aggressive;

        ctx.nfc = unorm2_getNFCInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFC normalizer: {}", u_errorName(err));

        if (aggressive)
        {
            err = U_ZERO_ERROR;
            ctx.nfkc_cf = unorm2_getNFKCCasefoldInstance(&err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFKC_Casefold normalizer: {}", u_errorName(err));
        }

        ctx.fold_options = handle_special_i ? U_FOLD_CASE_EXCLUDE_SPECIAL_I : U_FOLD_CASE_DEFAULT;
    }

    /// Result is left in buf1.
    static int32_t transform(const FoldContext & ctx, PODArray<UChar> & buf1, PODArray<UChar> & buf2, int32_t len)
    {
        if (ctx.aggressive)
        {
            /// NFKC_Casefold → NFC
            len = normalizeBuffer(ctx.nfkc_cf, buf1, len, buf2, MAX_NFKC_CASEFOLD_EXPANSION, "NFKC_Casefold");
            std::swap(buf1, buf2);
            len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC");
            std::swap(buf1, buf2);
        }
        else
        {
            /// NFC → case fold → NFC
            len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC pre-fold");
            std::swap(buf1, buf2);

            buf2.resize(len * MAX_CASEFOLD_EXPANSION);
            UErrorCode err = U_ZERO_ERROR;
            len = u_strFoldCase(buf2.data(), static_cast<int32_t>(buf2.size()),
                buf1.data(), len, ctx.fold_options, &err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (u_strFoldCase): {}", u_errorName(err));
            std::swap(buf1, buf2);

            len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC recompose");
            std::swap(buf1, buf2);
        }
        return len;
    }
};

struct AccentFoldImpl
{
    static constexpr auto name = "accentFoldUTF8";

    static void init(FoldContext & ctx, bool /* aggressive */, bool /* handle_special_i */)
    {
        UErrorCode err = U_ZERO_ERROR;

        ctx.nfc = unorm2_getNFCInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFC normalizer: {}", u_errorName(err));

        err = U_ZERO_ERROR;
        ctx.nfd = unorm2_getNFDInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFD normalizer: {}", u_errorName(err));
    }

    /// Result is left in buf1.
    static int32_t transform(const FoldContext & ctx, PODArray<UChar> & buf1, PODArray<UChar> & buf2, int32_t len)
    {
        /// NFD → strip Mn → NFC
        len = normalizeBuffer(ctx.nfd, buf1, len, buf2, MAX_NFD_EXPANSION, "NFD");
        std::swap(buf1, buf2);
        len = stripCombiningMarks(buf1.data(), len);
        len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC recompose");
        std::swap(buf1, buf2);
        return len;
    }
};

struct FullFoldImpl
{
    static constexpr auto name = "foldUTF8";

    static void init(FoldContext & ctx, bool aggressive, bool /* handle_special_i */)
    {
        UErrorCode err = U_ZERO_ERROR;
        ctx.aggressive = aggressive;

        ctx.nfc = unorm2_getNFCInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFC normalizer: {}", u_errorName(err));

        err = U_ZERO_ERROR;
        ctx.nfd = unorm2_getNFDInstance(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFD normalizer: {}", u_errorName(err));

        if (aggressive)
        {
            err = U_ZERO_ERROR;
            ctx.nfkc_cf = unorm2_getNFKCCasefoldInstance(&err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Failed to get NFKC_Casefold normalizer: {}", u_errorName(err));
        }
    }

    /// Result is left in buf1.
    static int32_t transform(const FoldContext & ctx, PODArray<UChar> & buf1, PODArray<UChar> & buf2, int32_t len)
    {
        if (ctx.aggressive)
        {
            /// NFKC_Casefold → NFD → strip Mn → NFC
            len = normalizeBuffer(ctx.nfkc_cf, buf1, len, buf2, MAX_NFKC_CASEFOLD_EXPANSION, "NFKC_Casefold");
            std::swap(buf1, buf2);
            len = normalizeBuffer(ctx.nfd, buf1, len, buf2, MAX_NFD_EXPANSION, "NFD");
            std::swap(buf1, buf2);
            len = stripCombiningMarks(buf1.data(), len);
            len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC final");
            std::swap(buf1, buf2);
        }
        else
        {
            /// NFC → case fold → NFD → strip Mn → NFC
            len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC pre-fold");
            std::swap(buf1, buf2);

            buf2.resize(len * MAX_CASEFOLD_EXPANSION);
            UErrorCode err = U_ZERO_ERROR;
            len = u_strFoldCase(buf2.data(), static_cast<int32_t>(buf2.size()),
                buf1.data(), len, ctx.fold_options, &err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (u_strFoldCase): {}", u_errorName(err));
            std::swap(buf1, buf2);

            len = normalizeBuffer(ctx.nfd, buf1, len, buf2, MAX_NFD_EXPANSION, "NFD");
            std::swap(buf1, buf2);
            len = stripCombiningMarks(buf1.data(), len);
            len = normalizeBuffer(ctx.nfc, buf1, len, buf2, MAX_NFC_EXPANSION, "NFC final");
            std::swap(buf1, buf2);
        }
        return len;
    }
};

/// Common row-loop template: handles UTF-8 ↔ UTF-16 conversion and iteration.
template <typename Impl>
struct FoldUTF8Common
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
        FoldContext ctx;
        Impl::init(ctx, aggressive, handle_special_i);

        res_data.reserve(data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        PODArray<UChar> buf_in;
        PODArray<UChar> buf_out;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from_size = offsets[i] - current_from_offset;

            if (from_size > 0)
            {
                /// UTF-8 → UTF-16
                buf_in.resize(from_size);
                int32_t u16_len = 0;
                UErrorCode err = U_ZERO_ERROR;
                u_strFromUTF8(
                    buf_in.data(),
                    static_cast<int32_t>(buf_in.size()),
                    &u16_len,
                    reinterpret_cast<const char *>(&data[current_from_offset]),
                    static_cast<int32_t>(from_size),
                    &err);
                if (U_FAILURE(err))
                    throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Fold failed (strFromUTF8): {}", u_errorName(err));

                /// Run the impl-specific transform pipeline (result left in buf_in)
                int32_t len = Impl::transform(ctx, buf_in, buf_out, u16_len);

                /// UTF-16 → UTF-8
                size_t max_to_size = current_to_offset + MAX_UTF16_TO_UTF8_EXPANSION * static_cast<size_t>(len);
                if (res_data.size() < max_to_size)
                    res_data.resize(max_to_size);

                int32_t to_size = 0;
                err = U_ZERO_ERROR;
                u_strToUTF8(
                    reinterpret_cast<char *>(&res_data[current_to_offset]),
                    static_cast<int32_t>(res_data.size() - current_to_offset),
                    &to_size,
                    buf_in.data(),
                    len,
                    &err);
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

/// IFunction wrapper — handles argument parsing and dispatches to FoldUTF8Common.
template <typename Impl>
class FunctionFoldUTF8 : public IFunction
{
    static constexpr bool has_method_arg = std::is_same_v<Impl, CaseFoldImpl> || std::is_same_v<Impl, FullFoldImpl>;
    static constexpr bool has_special_i_arg = std::is_same_v<Impl, CaseFoldImpl>;
    static constexpr size_t max_args = 1 + has_method_arg + has_special_i_arg;

public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFoldUTF8>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (max_args == 3)
            return {1, 2};
        else
            return {1};
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > max_args)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 to {} arguments, got {}", getName(), max_args, arguments.size());

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String",
                arguments[0]->getName(), getName());

        size_t arg_idx = 1;
        if constexpr (has_method_arg)
        {
            if (arg_idx < arguments.size())
            {
                if (!isString(arguments[arg_idx]))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}, expected String",
                        arguments[arg_idx]->getName(), arg_idx + 1, getName());
                ++arg_idx;
            }
        }
        if constexpr (has_special_i_arg)
        {
            if (arg_idx < arguments.size())
            {
                if (!isUInt8(arguments[arg_idx]))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}, expected UInt8",
                        arguments[arg_idx]->getName(), arg_idx + 1, getName());
            }
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
        size_t arg_idx = 1;

        if constexpr (has_method_arg)
        {
            if (arg_idx < arguments.size())
            {
                aggressive = parseMethodArgument(arguments[arg_idx]);
                ++arg_idx;
            }
        }
        if constexpr (has_special_i_arg)
        {
            if (arg_idx < arguments.size())
                handle_special_i = parseUInt8Argument(arguments[arg_idx]);
        }

        auto col_res = ColumnString::create();
        FoldUTF8Common<Impl>::process(
            col_str->getChars(), col_str->getOffsets(),
            col_res->getChars(), col_res->getOffsets(),
            input_rows_count, aggressive, handle_special_i);
        return col_res;
    }

private:
    static bool parseMethodArgument(const ColumnsWithTypeAndName::value_type & arg)
    {
        const ColumnConst * col_const = checkAndGetColumnConstStringOrFixedString(arg.column.get());
        if (!col_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The 'method' argument of function {} must be a constant string ('aggressive' or 'conservative')", Impl::name);

        String method = col_const->getValue<String>();
        if (method == "aggressive")
            return true;
        if (method == "conservative")
            return false;
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid method '{}' for function {}, expected 'aggressive' or 'conservative'", method, Impl::name);
    }

    static bool parseUInt8Argument(const ColumnsWithTypeAndName::value_type & arg)
    {
        const auto * col_const = checkAndGetColumnConst<ColumnUInt8>(arg.column.get());
        if (!col_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The 'handle_special_I' argument of function {} must be a constant UInt8", Impl::name);
        return col_const->getValue<UInt8>() != 0;
    }
};


using FunctionCaseFoldUTF8 = FunctionFoldUTF8<CaseFoldImpl>;
using FunctionAccentFoldUTF8 = FunctionFoldUTF8<AccentFoldImpl>;
using FunctionFullFoldUTF8 = FunctionFoldUTF8<FullFoldImpl>;

}

REGISTER_FUNCTION(FoldUTF8)
{
    /// caseFoldUTF8
    FunctionDocumentation::Description case_desc = R"(
Applies Unicode case folding to a UTF-8 string, converting it to a lowercase-like normalized form suitable for case-insensitive comparisons.

Two methods are available:
- 'aggressive' (default): applies NFKC_Casefold normalization, which performs case folding and also resolves
  Unicode compatibility equivalences — for example, ligatures like `ﬃ` are decomposed to `ffi`, and
  circled numbers like `①` become `1`. This is faster as it uses a single ICU normalization pass.
- 'conservative': applies NFC normalization followed by standard Unicode case folding, preserving the
  visual form of compatibility characters (e.g. ligatures remain intact). This requires multiple ICU passes.
)";
    FunctionDocumentation::Syntax case_syntax = "caseFoldUTF8(str[, method][, handle_special_I])";
    FunctionDocumentation::Arguments case_args = {
        {"str", "UTF-8 encoded input string.", {"String"}},
        {"method", "Optional. 'aggressive' (default) or 'conservative'.", {"String"}},
        {"handle_special_I", "Optional. 1 to exclude Turkish/Azerbaijani special I mapping (U_FOLD_CASE_EXCLUDE_SPECIAL_I). Default 0.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue case_ret = {"Case-folded UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples case_examples = {
    {
        "Basic case folding",
        "SELECT caseFoldUTF8('Straße')",
        R"(
┌─caseFoldUTF8('Straße')─┐
│ strasse                 │
└─────────────────────────┘
)"
    },
    {
        "Aggressive vs conservative — ligature handling",
        "SELECT caseFoldUTF8('ﬃ') AS aggressive, caseFoldUTF8('ﬃ', 'conservative') AS conservative",
        R"(
┌─aggressive─┬─conservative─┐
│ ffi        │ ﬃ            │
└────────────┴──────────────┘
)"
    }};
    FunctionDocumentation::IntroducedIn intro = {26, 3};
    FunctionDocumentation::Category cat = FunctionDocumentation::Category::String;
    factory.registerFunction<FunctionCaseFoldUTF8>({case_desc, case_syntax, case_args, {}, case_ret, case_examples, intro, cat});

    /// accentFoldUTF8
    FunctionDocumentation::Description accent_desc = R"(
Removes diacritical marks (accents) from a UTF-8 string by decomposing characters via NFD,
stripping combining marks (Unicode category Mn), then recomposing via NFC.
)";
    FunctionDocumentation::Syntax accent_syntax = "accentFoldUTF8(str)";
    FunctionDocumentation::Arguments accent_args = {
        {"str", "UTF-8 encoded input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue accent_ret = {"UTF-8 string with diacritics removed.", {"String"}};
    FunctionDocumentation::Examples accent_examples = {{
        "Basic accent removal",
        "SELECT accentFoldUTF8('café résumé naïve')",
        R"(
┌─accentFoldUTF8('café résumé naïve')─┐
│ cafe resume naive                    │
└──────────────────────────────────────┘
)"
    }};
    factory.registerFunction<FunctionAccentFoldUTF8>({accent_desc, accent_syntax, accent_args, {}, accent_ret, accent_examples, intro, cat});

    /// foldUTF8
    FunctionDocumentation::Description fold_desc = R"(
Applies both case folding and accent (diacritical mark) removal to a UTF-8 string.
This combines the behavior of `caseFoldUTF8` and `accentFoldUTF8` into a single function.

Two methods are available:
- 'aggressive' (default): applies NFKC_Casefold (case fold + compatibility decomposition in one pass),
  then NFD + strip combining marks + NFC. Faster, but decomposes compatibility characters like ligatures.
- 'conservative': applies NFC, case fold, NFD, strip combining marks, then NFC.
  Preserves compatibility characters like ligatures while still folding case and removing accents.

See the caseFoldUTF8 and accentFoldUTF8 functions for more info on each step. The result of this function will be equivalent
to accentFoldUTF8(caseFoldUTF8(input)) however foldUTF8(input) will be slightly faster due to avoiding a redundant normalization step.
Note that there is no handle_special_I parameter since the special I would be stripped of any accents anyway.
)";
    FunctionDocumentation::Syntax fold_syntax = "foldUTF8(str[, case_fold_method])";
    FunctionDocumentation::Arguments fold_args = {
        {"str", "UTF-8 encoded input string.", {"String"}},
        {"case_fold_method", "Optional. 'aggressive' (default) or 'conservative'.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue fold_ret = {"Case-folded and accent-stripped UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples fold_examples = {
    {
        "Basic usage",
        "SELECT foldUTF8('Café Résumé')",
        R"(
┌─foldUTF8('Café Résumé')─┐
│ cafe resume               │
└───────────────────────────┘
)"
    },
    {
        "Aggressive vs conservative — ligature with accents",
        "SELECT foldUTF8('Ǆeﬃcient') AS aggressive, foldUTF8('Ǆeﬃcient', 'conservative') AS conservative",
        R"(
┌─aggressive─┬─conservative─┐
│ dzefficient │ dzeﬃcient   │
└─────────────┴──────────────┘
)"
    }};
    factory.registerFunction<FunctionFullFoldUTF8>({fold_desc, fold_syntax, fold_args, {}, fold_ret, fold_examples, intro, cat});
}

}

#endif
