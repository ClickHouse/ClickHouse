#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn_fwd.h>
#include <Common/assert_cast.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <fmt/format.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemUnicode.h>
#include <Storages/VirtualColumnUtils.h>
#include <base/types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/String.h>
#include <Poco/UTF8Encoding.h>
#include <unicode/errorcode.h>
#include <unicode/umachine.h>
#include <unicode/uniset.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/uscript.h>
#include <unicode/usetiter.h>
#include <unicode/uchar.h>
#include <unicode/utypes.h>
#include <unicode/uversion.h>
#include <unicode/ustring.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNICODE_ERROR;
    extern const int LOGICAL_ERROR;
}


std::vector<std::pair<String, UProperty>> getPropNames()
{
    std::vector<std::pair<String, UProperty>> properties;
    int i = UCHAR_BINARY_START;
    while (true)
    {
        if (i == UCHAR_BINARY_LIMIT)
        {
            i = UCHAR_INT_START;
        }
        else if (i == UCHAR_INT_LIMIT)
        {
            i = UCHAR_MASK_START;
        }
        else if (i == UCHAR_MASK_LIMIT)
        {
            i = UCHAR_DOUBLE_START;
        }
        else if (i == UCHAR_DOUBLE_LIMIT)
        {
            i = UCHAR_STRING_START;
        }
        else if (i == UCHAR_STRING_LIMIT)
        {
            i = UCHAR_OTHER_PROPERTY_START;
        }
        else if (i == UCHAR_OTHER_PROPERTY_LIMIT)
        {
            break;
        }
        UProperty prop = static_cast<UProperty>(i);
        const char * prop_name = u_getPropertyName(prop, U_LONG_PROPERTY_NAME);

        // TODO: maybe we can use short name as alias name?

        if (prop_name)
        {
            properties.emplace_back(String(prop_name), prop);
        }
        i ++;
    }
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
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_BINARY_LIMIT)
            break;
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeUInt8>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_INT_LIMIT)
            break;

        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeInt32>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_MASK_LIMIT)
            break;
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeInt32>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_DOUBLE_LIMIT)
            break;

        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeFloat64>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_STRING_LIMIT)
            break;
        names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeString>());
        ++prop_index;
    }

    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_OTHER_PROPERTY_LIMIT)
            break;
        if (prop == UCHAR_SCRIPT_EXTENSIONS)
        {
            names_and_types.emplace_back(Poco::toLower(prop_name), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()));
        }

        // NOT handle UCHAR_IDENTIFIER_TYPE

        ++prop_index;
    }

    return ColumnsDescription::fromNamesAndTypes(names_and_types);
}

Block StorageSystemUnicode::getFilterSampleBlock() const
{
    return
    {
        { {}, std::make_shared<DataTypeString>(), "code_point" },
        { {}, std::make_shared<DataTypeInt32>(), "code_point_value" },
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

    Block filter_block
    {
        ColumnWithTypeAndName(std::move(code_point_column), std::make_shared<DataTypeString>(), "code_point"),
        ColumnWithTypeAndName(std::move(code_point_value_column), std::make_shared<DataTypeInt32>(), "code_point_value")
    };

    VirtualColumnUtils::filterBlockWithPredicate(predicate, filter_block, context);
    return filter_block;
}

void StorageSystemUnicode::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    Poco::UTF8Encoding encoding;

    /// Common buffers/err_code used for ICU API calls
    UChar buffer[32];
    char char_name_buffer[100];
    UErrorCode err_code;

    auto prop_names = getPropNames();

    /// Get filtered code points based on the predicate.
    Block filtered_block = getFilteredCodePoints(predicate, context, encoding);
    size_t num_filtered_code_points = filtered_block.rows();

    res_columns[0] = IColumn::mutate(std::move(filtered_block.getByPosition(0).column));
    res_columns[1] = IColumn::mutate(std::move(filtered_block.getByPosition(1).column));

    const ColumnInt32::Container & filtered_code_points = assert_cast<const ColumnInt32 &>(*res_columns[1]).getData();

    for (size_t i = 0; i < num_filtered_code_points; ++i)
    {
        size_t column_index = 2;
        UChar32 code = filtered_code_points[i];

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

        size_t prop_index = 0;
        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_BINARY_LIMIT)
                break;
            assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).insert(u_hasBinaryProperty(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_INT_LIMIT)
                break;

            // TODO: Using Int32 for now, not sure if Int16 would be sufficient
            assert_cast<ColumnInt32 &>(*res_columns[column_index++]).insert(u_getIntPropertyValue(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_MASK_LIMIT)
                break;
            // Only handle UCHAR_GENERAL_CATEGORY_MASK
            // Result is a mask,, U_GC_L_MASK, U_GC_M_MASK, U_GC_N_MASK, U_GC_P_MASK, U_GC_S_MASK ...
            // Now we just use Int32 to store it
            assert_cast<ColumnInt32 &>(*res_columns[column_index++]).insert(u_getIntPropertyValue(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_DOUBLE_LIMIT)
                break;
            if (prop == UCHAR_NUMERIC_VALUE)
            {
                auto type = u_getIntPropertyValue(code, UCHAR_NUMERIC_TYPE);
                if (type == U_NT_NUMERIC)
                {
                    assert_cast<ColumnFloat64 &>(*res_columns[column_index++]).insert(u_getNumericValue(code));
                }
                else
                {
                    // Not a numeric value, set to 0.0
                    assert_cast<ColumnFloat64 &>(*res_columns[column_index++]).insert(0.0);
                }
            }
            ++prop_index;
        }
        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_STRING_LIMIT)
                break;

            if (prop == UCHAR_AGE)
            {
                UVersionInfo version_info;
                u_charAge(code, version_info);
                // format version info to string
                char uvbuf[U_MAX_VERSION_STRING_LENGTH];
                u_versionToString(version_info, uvbuf);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insertData(uvbuf, strlen(uvbuf));
            }
            else if (prop == UCHAR_BIDI_MIRRORING_GLYPH)
            {
                auto cm = u_charMirror(code);
                toUTF8(cm, *res_columns[column_index++], encoding);
            }
            else if (prop == UCHAR_CASE_FOLDING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strFoldCase(buffer, 32, s, length, U_FOLD_CASE_DEFAULT, &err_code);
                icu::UnicodeString str(buffer);
                String ret;
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else if (prop == UCHAR_LOWERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strToLower(buffer, 32, s, length, "", &err_code);
                icu::UnicodeString str(buffer);
                String ret;
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else if (prop == UCHAR_NAME)
            {
                auto len = u_charName(code, U_UNICODE_CHAR_NAME, char_name_buffer, sizeof(char_name_buffer), &err_code);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insertData(char_name_buffer, len);
            }
            else if (prop == UCHAR_SIMPLE_CASE_FOLDING)
            {
                auto cp = u_foldCase(code, U_FOLD_CASE_DEFAULT);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (prop == UCHAR_SIMPLE_LOWERCASE_MAPPING)
            {
                auto cp = u_tolower(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            // Now there is no code point where the two properties are different
            else if (prop == UCHAR_SIMPLE_TITLECASE_MAPPING || prop == UCHAR_TITLECASE_MAPPING)
            {
                auto cp = u_totitle(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (prop == UCHAR_SIMPLE_UPPERCASE_MAPPING)
            {
                auto cp = u_toupper(code);
                toUTF8(cp, *res_columns[column_index++], encoding);
            }
            else if (prop == UCHAR_UPPERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strToUpper(buffer, 32, s, length, "", &err_code);
                String ret;
                icu::UnicodeString str(buffer);
                str.toUTF8String(ret);
                assert_cast<ColumnString &>(*res_columns[column_index++]).insert(ret);
            }
            else if (prop == UCHAR_BIDI_PAIRED_BRACKET)
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

            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_OTHER_PROPERTY_LIMIT)
                break;

            if (prop == UCHAR_SCRIPT_EXTENSIONS)
            {
                const static int32_t SCX_ARRAY_CAPACITY = 32;
                UScriptCode scx_val_array[SCX_ARRAY_CAPACITY];
                int32_t num_scripts = uscript_getScriptExtensions(code, scx_val_array, SCX_ARRAY_CAPACITY, &err_code);
                Array arr;
                if (err_code == U_ZERO_ERROR)
                {
                    for (int32_t j = 0; j < num_scripts; ++j)
                    {
                        arr.push_back(scx_val_array[j]);
                    }
                }
                assert_cast<ColumnArray &>(*res_columns[column_index++]).insert(arr);
            }

            // NOT handle UCHAR_IDENTIFIER_TYPE

            ++prop_index;
        }
    }
}

}
