#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemUnicode.h>
#include <Storages/VirtualColumnUtils.h>
#include <base/types.h>
#include <fmt/format.h>
#include <unicode/errorcode.h>
#include <unicode/uchar.h>
#include <unicode/umachine.h>
#include <unicode/uniset.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/uscript.h>
#include <unicode/usetiter.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>
#include <unicode/uversion.h>
#include <Poco/String.h>
#include <Poco/UTF8Encoding.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int UNICODE_ERROR;
extern const int LOGICAL_ERROR;
}

// Binary properties
constexpr UProperty binary_properties[] = {
    UCHAR_ALPHABETIC,
    UCHAR_ASCII_HEX_DIGIT,
    UCHAR_BIDI_CONTROL,
    UCHAR_BIDI_MIRRORED,
    UCHAR_DASH,
    UCHAR_DEFAULT_IGNORABLE_CODE_POINT,
    UCHAR_DEPRECATED,
    UCHAR_DIACRITIC,
    UCHAR_EXTENDER,
    UCHAR_FULL_COMPOSITION_EXCLUSION,
    UCHAR_GRAPHEME_BASE,
    UCHAR_GRAPHEME_EXTEND,
    UCHAR_GRAPHEME_LINK,
    UCHAR_HEX_DIGIT,
    UCHAR_HYPHEN,
    UCHAR_ID_CONTINUE,
    UCHAR_ID_START,
    UCHAR_IDEOGRAPHIC,
    UCHAR_IDS_BINARY_OPERATOR,
    UCHAR_IDS_TRINARY_OPERATOR,
    UCHAR_JOIN_CONTROL,
    UCHAR_LOGICAL_ORDER_EXCEPTION,
    UCHAR_LOWERCASE,
    UCHAR_MATH,
    UCHAR_NONCHARACTER_CODE_POINT,
    UCHAR_QUOTATION_MARK,
    UCHAR_RADICAL,
    UCHAR_SOFT_DOTTED,
    UCHAR_TERMINAL_PUNCTUATION,
    UCHAR_UNIFIED_IDEOGRAPH,
    UCHAR_UPPERCASE,
    UCHAR_WHITE_SPACE,
    UCHAR_XID_CONTINUE,
    UCHAR_XID_START,
    UCHAR_CASE_SENSITIVE,
    UCHAR_S_TERM,
    UCHAR_VARIATION_SELECTOR,
    UCHAR_NFD_INERT,
    UCHAR_NFKD_INERT,
    UCHAR_NFC_INERT,
    UCHAR_NFKC_INERT,
    UCHAR_SEGMENT_STARTER,
    UCHAR_PATTERN_SYNTAX,
    UCHAR_PATTERN_WHITE_SPACE,
    UCHAR_POSIX_ALNUM,
    UCHAR_POSIX_BLANK,
    UCHAR_POSIX_GRAPH,
    UCHAR_POSIX_PRINT,
    UCHAR_POSIX_XDIGIT,
    UCHAR_CASED,
    UCHAR_CASE_IGNORABLE,
    UCHAR_CHANGES_WHEN_LOWERCASED,
    UCHAR_CHANGES_WHEN_UPPERCASED,
    UCHAR_CHANGES_WHEN_TITLECASED,
    UCHAR_CHANGES_WHEN_CASEFOLDED,
    UCHAR_CHANGES_WHEN_CASEMAPPED,
    UCHAR_CHANGES_WHEN_NFKC_CASEFOLDED,
    UCHAR_EMOJI,
    UCHAR_EMOJI_PRESENTATION,
    UCHAR_EMOJI_MODIFIER,
    UCHAR_EMOJI_MODIFIER_BASE,
    UCHAR_EMOJI_COMPONENT,
    UCHAR_REGIONAL_INDICATOR,
    UCHAR_PREPENDED_CONCATENATION_MARK,
    UCHAR_EXTENDED_PICTOGRAPHIC,
    UCHAR_BASIC_EMOJI,
    UCHAR_EMOJI_KEYCAP_SEQUENCE,
    UCHAR_RGI_EMOJI_MODIFIER_SEQUENCE,
    UCHAR_RGI_EMOJI_FLAG_SEQUENCE,
    UCHAR_RGI_EMOJI_TAG_SEQUENCE,
    UCHAR_RGI_EMOJI_ZWJ_SEQUENCE,
    UCHAR_RGI_EMOJI,
    UCHAR_IDS_UNARY_OPERATOR,
    UCHAR_ID_COMPAT_MATH_START,
    UCHAR_ID_COMPAT_MATH_CONTINUE,
};

// Integer/Enumerated properties
constexpr UProperty int_properties[]
    = {UCHAR_BIDI_CLASS,
       UCHAR_BLOCK,
       UCHAR_CANONICAL_COMBINING_CLASS,
       UCHAR_DECOMPOSITION_TYPE,
       UCHAR_EAST_ASIAN_WIDTH,
       UCHAR_GENERAL_CATEGORY,
       UCHAR_JOINING_GROUP,
       UCHAR_JOINING_TYPE,
       UCHAR_LINE_BREAK,
       UCHAR_NUMERIC_TYPE,
       UCHAR_SCRIPT,
       UCHAR_HANGUL_SYLLABLE_TYPE,
       UCHAR_NFD_QUICK_CHECK,
       UCHAR_NFKD_QUICK_CHECK,
       UCHAR_NFC_QUICK_CHECK,
       UCHAR_NFKC_QUICK_CHECK,
       UCHAR_LEAD_CANONICAL_COMBINING_CLASS,
       UCHAR_TRAIL_CANONICAL_COMBINING_CLASS,
       UCHAR_GRAPHEME_CLUSTER_BREAK,
       UCHAR_SENTENCE_BREAK,
       UCHAR_WORD_BREAK,
       UCHAR_BIDI_PAIRED_BRACKET_TYPE,
       UCHAR_INDIC_POSITIONAL_CATEGORY,
       UCHAR_INDIC_SYLLABIC_CATEGORY,
       UCHAR_VERTICAL_ORIENTATION,
       UCHAR_IDENTIFIER_STATUS};

// Mask properties
constexpr UProperty mask_properties[] = {UCHAR_GENERAL_CATEGORY_MASK};

// Double properties
constexpr UProperty double_properties[] = {UCHAR_NUMERIC_VALUE};

// String properties
constexpr UProperty string_properties[]
    = {UCHAR_AGE,
       UCHAR_BIDI_MIRRORING_GLYPH,
       UCHAR_CASE_FOLDING,
       UCHAR_LOWERCASE_MAPPING,
       UCHAR_NAME,
       UCHAR_SIMPLE_CASE_FOLDING,
       UCHAR_SIMPLE_LOWERCASE_MAPPING,
       UCHAR_SIMPLE_TITLECASE_MAPPING,
       UCHAR_SIMPLE_UPPERCASE_MAPPING,
       UCHAR_TITLECASE_MAPPING,
       UCHAR_UPPERCASE_MAPPING,
       UCHAR_BIDI_PAIRED_BRACKET};

// Other properties
constexpr UProperty other_properties[] = {UCHAR_SCRIPT_EXTENSIONS, UCHAR_IDENTIFIER_TYPE};

std::vector<std::pair<String, UProperty>> getPropNames()
{
    std::vector<std::pair<String, UProperty>> properties;

    auto add_properties = [&properties](const UProperty * props, size_t count)
    {
        for (size_t i = 0; i < count; ++i)
        {
            UProperty prop = props[i];
            const char * prop_name = u_getPropertyName(prop, U_LONG_PROPERTY_NAME);
            if (!prop_name)
            {
                throw Exception(ErrorCodes::UNICODE_ERROR, "Failed to get property name for property {}", static_cast<int>(prop));
            }
            properties.emplace_back(String(prop_name), prop);
        }
    };

    // Add all property categories
    add_properties(binary_properties, sizeof(binary_properties) / sizeof(binary_properties[0]));
    add_properties(int_properties, sizeof(int_properties) / sizeof(int_properties[0]));
    add_properties(mask_properties, sizeof(mask_properties) / sizeof(mask_properties[0]));
    add_properties(double_properties, sizeof(double_properties) / sizeof(double_properties[0]));
    add_properties(string_properties, sizeof(string_properties) / sizeof(string_properties[0]));
    add_properties(other_properties, sizeof(other_properties) / sizeof(other_properties[0]));

    return properties;
}

ColumnsDescription StorageSystemUnicode::getColumnsDescription()
{
    NamesAndTypes names_and_types;
    auto prop_names = getPropNames();
    names_and_types.emplace_back("code_point", std::make_shared<DataTypeString>());
    names_and_types.emplace_back("code_point_value", std::make_shared<DataTypeInt32>());
    names_and_types.emplace_back("notation", std::make_shared<DataTypeString>());
    size_t prop_index = 0;
    // Process binary properties
    for (size_t i = 0; i < sizeof(binary_properties) / sizeof(binary_properties[0]); ++i)
    {
        const auto & [prop_name, prop] = prop_names[prop_index++];
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeUInt8>());
    }

    // Process integer properties
    for (size_t i = 0; i < sizeof(int_properties) / sizeof(int_properties[0]); ++i)
    {
        const auto & [prop_name, prop] = prop_names[prop_index++];
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeInt32>());
    }

    // Process mask properties
    for (size_t i = 0; i < sizeof(mask_properties) / sizeof(mask_properties[0]); ++i)
    {
        const auto & [prop_name, prop] = prop_names[prop_index++];
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeInt32>());
    }

    // Process double properties
    for (size_t i = 0; i < sizeof(double_properties) / sizeof(double_properties[0]); ++i)
    {
        const auto & [prop_name, prop] = prop_names[prop_index++];
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeFloat64>());
    }

    // Process string properties
    for (size_t i = 0; i < sizeof(string_properties) / sizeof(string_properties[0]); ++i)
    {
        const auto & [prop_name, prop] = prop_names[prop_index++];
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeString>());
    }

    // Process other properties
    for (size_t i = 0; i < sizeof(other_properties) / sizeof(other_properties[0]); ++i)
    {
        const auto & [prop_name, prop] = prop_names[prop_index++];
        // UCHAR_SCRIPT_EXTENSIONS and UCHAR_IDENTIFIER_TYPE are arrays of LowCardinality(String)
        names_and_types.emplace_back(
            Poco::toLower(prop_name),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())));
    }

    return ColumnsDescription::fromNamesAndTypes(names_and_types);
}

Block StorageSystemUnicode::getFilterSampleBlock() const
{
    return {
        {{}, std::make_shared<DataTypeString>(), "code_point"},
        {{}, std::make_shared<DataTypeInt32>(), "code_point_value"},
    };
}

static void toUTF8(UChar32 code, IColumn & column, Poco::UTF8Encoding & encoding)
{
    uint8_t utf8[4]{};
    int res = encoding.convert(code, utf8, 4);
    if (!res || res > 4)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert code point {} to UTF-8", code);
    assert_cast<ColumnString &>(column).insertData(reinterpret_cast<const char *>(utf8), res);
}

static Block getFilteredCodePoints(const ActionsDAG::Node * predicate, ContextPtr context, Poco::UTF8Encoding & encoding)
{
    icu::UnicodeSet all_assigned;
    UErrorCode status = U_ZERO_ERROR;
    all_assigned.applyPattern("[[:Assigned:]]", status);
    if (U_FAILURE(status))
        throw Exception(ErrorCodes::UNICODE_ERROR, "Cannot obtain the list of assigned code points");

    auto code_point_column = ColumnString::create();
    auto code_point_value_column = ColumnInt32::create();

    icu::UnicodeSetIterator iter(all_assigned);
    while (iter.next())
    {
        UChar32 code = iter.getCodepoint();
        toUTF8(code, *code_point_column, encoding);
        code_point_value_column->getData().push_back(code);
    }

    Block filter_block{
        ColumnWithTypeAndName(std::move(code_point_column), std::make_shared<DataTypeString>(), "code_point"),
        ColumnWithTypeAndName(std::move(code_point_value_column), std::make_shared<DataTypeInt32>(), "code_point_value")};

    VirtualColumnUtils::filterBlockWithPredicate(predicate, filter_block, context);
    return filter_block;
}

void StorageSystemUnicode::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    Poco::UTF8Encoding encoding;

    /// Common buffers/err_code used for ICU API calls
    UChar buffer[32];
    char char_name_buffer[100];
    UErrorCode err_code;

    auto prop_names = getPropNames();

    /// Get filtered code points based on the predicate.
    Block filtered_block = getFilteredCodePoints(predicate, context, encoding);

    res_columns[0] = IColumn::mutate(std::move(filtered_block.getByPosition(0).column));
    res_columns[1] = IColumn::mutate(std::move(filtered_block.getByPosition(1).column));

    const ColumnInt32::Container & filtered_code_points = assert_cast<const ColumnInt32 &>(*res_columns[1]).getData();

    for (UChar32 code : filtered_code_points)
    {
        size_t column_index = 2;

        /// U+XXXX notation
        ColumnString & col_notation = assert_cast<ColumnString &>(*res_columns[column_index++]);
        ColumnString::Offsets & col_notation_offsets = col_notation.getOffsets();
        ColumnString::Chars & col_notation_chars = col_notation.getChars();
        ColumnString::Offset offset = col_notation_offsets.back();
        if (code <= 0xFFFF)
        {
            col_notation_chars.resize(offset + 7);
            col_notation_chars[offset] = 'U';
            ++offset;
            col_notation_chars[offset] = '+';
            ++offset;
            writeHexByteUppercase(code >> 8, &col_notation_chars[offset]);
            offset += 2;
            writeHexByteUppercase(code & 0xFF, &col_notation_chars[offset]);
            offset += 2;
            col_notation_chars[offset] = 0;
            ++offset;
            col_notation_offsets.push_back(offset);
        }
        else if (code <= 0xFFFFF)
        {
            col_notation_chars.resize(offset + 8);
            col_notation_chars[offset] = 'U';
            ++offset;
            col_notation_chars[offset] = '+';
            ++offset;
            col_notation_chars[offset] = hexDigitUppercase(code >> 16);
            ++offset;
            writeHexByteUppercase((code >> 8) & 0xFF, &col_notation_chars[offset]);
            offset += 2;
            writeHexByteUppercase(code & 0xFF, &col_notation_chars[offset]);
            offset += 2;
            col_notation_chars[offset] = 0;
            ++offset;
            col_notation_offsets.push_back(offset);
        }
        else if (code <= 0x10FFFF)
        {
            col_notation_chars.resize(offset + 9);
            col_notation_chars[offset] = 'U';
            ++offset;
            col_notation_chars[offset] = '+';
            ++offset;
            writeHexByteUppercase(code >> 16, &col_notation_chars[offset]);
            offset += 2;
            writeHexByteUppercase((code >> 8) & 0xFF, &col_notation_chars[offset]);
            offset += 2;
            writeHexByteUppercase(code & 0xFF, &col_notation_chars[offset]);
            offset += 2;
            col_notation_chars[offset] = 0;
            ++offset;
            col_notation_offsets.push_back(offset);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Code point {} is outside of the Unicode range", code);

        // Process binary properties
        for (const auto & binary_prop : binary_properties)
        {
            assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).insert(u_hasBinaryProperty(code, binary_prop));
        }

        // Process integer properties
        for (const auto & int_prop : int_properties)
        {
            assert_cast<ColumnInt32 &>(*res_columns[column_index++]).insert(u_getIntPropertyValue(code, int_prop));
        }

        // Process mask properties
        for (const auto & mask_prop : mask_properties)
        {
            assert_cast<ColumnInt32 &>(*res_columns[column_index++]).insert(u_getIntPropertyValue(code, mask_prop));
        }

        // Process double properties
        for (const auto & double_prop : double_properties)
        {
            if (double_prop == UCHAR_NUMERIC_VALUE)
            {
                auto type = u_getIntPropertyValue(code, UCHAR_NUMERIC_TYPE);
                if (type == U_NT_NUMERIC)
                {
                    assert_cast<ColumnFloat64 &>(*res_columns[column_index++]).insert(u_getNumericValue(code));
                }
                else
                {
                    assert_cast<ColumnFloat64 &>(*res_columns[column_index++]).insert(0.0);
                }
            }
        }

        // Process string properties
        for (const auto & string_prop : string_properties)
        {
            if (string_prop == UCHAR_AGE)
            {
                UVersionInfo version_info;
                u_charAge(code, version_info);
                char uvbuf[U_MAX_VERSION_STRING_LENGTH];
                u_versionToString(version_info, uvbuf);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insertData(uvbuf, strlen(uvbuf));
            }
            else if (string_prop == UCHAR_BIDI_MIRRORING_GLYPH)
            {
                auto cm = u_charMirror(code);
                toUTF8(cm, *res_columns[column_index++], encoding);
            }
            else if (string_prop == UCHAR_CASE_FOLDING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strFoldCase(buffer, 32, s, length, U_FOLD_CASE_DEFAULT, &err_code);
                if (U_FAILURE(err_code))
                    throw Exception(ErrorCodes::UNICODE_ERROR, "Failed to fold case for code point {}: {}", code, u_errorName(err_code));
                icu::UnicodeString str(buffer);
                String ret;
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else if (string_prop == UCHAR_LOWERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strToLower(buffer, 32, s, length, "", &err_code);
                if (U_FAILURE(err_code))
                    throw Exception(
                        ErrorCodes::UNICODE_ERROR, "Failed to convert to lowercase for code point {}: {}", code, u_errorName(err_code));
                icu::UnicodeString str(buffer);
                String ret;
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else if (string_prop == UCHAR_NAME)
            {
                err_code = U_ZERO_ERROR;
                auto len = u_charName(code, U_UNICODE_CHAR_NAME, char_name_buffer, sizeof(char_name_buffer), &err_code);
                if (U_FAILURE(err_code))
                    throw Exception(
                        ErrorCodes::UNICODE_ERROR, "Failed to get character name for code point {}: {}", code, u_errorName(err_code));
                assert_cast<ColumnString &>(*res_columns[column_index++]).insertData(char_name_buffer, len);
            }
            else if (string_prop == UCHAR_SIMPLE_CASE_FOLDING)
            {
                auto cp = u_foldCase(code, U_FOLD_CASE_DEFAULT);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (string_prop == UCHAR_SIMPLE_LOWERCASE_MAPPING)
            {
                auto cp = u_tolower(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (string_prop == UCHAR_SIMPLE_TITLECASE_MAPPING)
            {
                auto cp = u_totitle(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (string_prop == UCHAR_SIMPLE_UPPERCASE_MAPPING)
            {
                auto cp = u_toupper(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (string_prop == UCHAR_TITLECASE_MAPPING)
            {
                auto cp = u_totitle(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (string_prop == UCHAR_UPPERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strToUpper(buffer, 32, s, length, "", &err_code);
                if (U_FAILURE(err_code))
                    throw Exception(
                        ErrorCodes::UNICODE_ERROR, "Failed to convert to uppercase for code point {}: {}", code, u_errorName(err_code));
                String ret;
                icu::UnicodeString str(buffer);
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else if (string_prop == UCHAR_BIDI_PAIRED_BRACKET)
            {
                auto cp = u_getBidiPairedBracket(code);
                String ret;
                icu::UnicodeString str(cp);
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else
            {
                assert_cast<ColumnString &>(*res_columns[column_index++]).insertDefault();
            }
        }

        // Process other properties
        for (const auto & other_prop : other_properties)
        {
            if (other_prop == UCHAR_SCRIPT_EXTENSIONS)
            {
                const static int32_t SCX_ARRAY_CAPACITY = 32;
                UScriptCode scx_val_array[SCX_ARRAY_CAPACITY];
                err_code = U_ZERO_ERROR;
                int32_t num_scripts = uscript_getScriptExtensions(code, scx_val_array, SCX_ARRAY_CAPACITY, &err_code);
                if (U_FAILURE(err_code))
                    throw Exception(
                        ErrorCodes::UNICODE_ERROR, "Failed to get script extensions for code point {}: {}", code, u_errorName(err_code));
                if (num_scripts < 0)
                    throw Exception(
                        ErrorCodes::UNICODE_ERROR, "Invalid number of scripts returned for code point {}: {}", code, num_scripts);

                Array arr;
                for (int32_t j = 0; j < num_scripts; ++j)
                {
                    const char * script_name = uscript_getName(scx_val_array[j]);
                    if (script_name == nullptr)
                    {
                        throw Exception(
                            ErrorCodes::UNICODE_ERROR, "Failed to get script name for code point {}: {}", code, scx_val_array[j]);
                    }
                    arr.emplace_back(std::string_view(script_name));
                }
                assert_cast<ColumnArray &>(*res_columns[column_index++]).insert(arr);
            }
            else if (other_prop == UCHAR_IDENTIFIER_TYPE)
            {
                UIdentifierType types[12];
                err_code = U_ZERO_ERROR;
                int32_t count = u_getIDTypes(code, types, 12, &err_code);
                if (U_FAILURE(err_code))
                {
                    throw Exception(
                        ErrorCodes::UNICODE_ERROR, "Failed to get identifier types for code point {}: {}", code, u_errorName(err_code));
                }
                Array arr;
                for (int32_t i = 0; i < count; i++)
                {
                    const char * type_name = u_getPropertyValueName(UCHAR_IDENTIFIER_TYPE, types[i], U_LONG_PROPERTY_NAME);
                    if (type_name == nullptr)
                    {
                        throw Exception(
                            ErrorCodes::UNICODE_ERROR, "Failed to get identifier type name for code point {}: {}", code, types[i]);
                    }
                    arr.emplace_back(std::string_view(type_name));
                }
                assert_cast<ColumnArray &>(*res_columns[column_index++]).insert(arr);
            }
        }
    }
}

}
