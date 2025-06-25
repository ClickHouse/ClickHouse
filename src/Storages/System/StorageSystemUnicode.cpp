#include "Columns/ColumnArray.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn_fwd.h"
#include "Common/assert_cast.h"
#include "Core/NamesAndTypes.h"
#include "DataTypes/DataTypeArray.h"
#include <fmt/format.h>
#include "Interpreters/Context_fwd.h"
#include "Storages/ColumnsDescription.h"
#include <Storages/System/StorageSystemUnicode.h>
#include <Storages/VirtualColumnUtils.h>
#include <base/types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/String.h>
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
    return {
        { {}, std::make_shared<DataTypeString>(), "code_point" },
        { {}, std::make_shared<DataTypeInt32>(), "code_point_value" },
    };
}

static ColumnPtr getFilteredCodePoints(const ActionsDAG::Node * predicate, ContextPtr context)
{
    icu::UnicodeSet all_unicode(0x0000, 0x10FFFF);
    // Remove surrogate pairs
    all_unicode.remove(0xD800, 0xDFFF);
    // Remove non character code points
    all_unicode.remove(0xFDD0, 0xFDEF);
    // Remove non-character code points at the end of each Unicode plane
    for (UChar32 base = 0xFFFE; base <= 0x10FFFF; base += 0x10000)
    {
        all_unicode.remove(base, base + 1);
    }
    icu::UnicodeSetIterator iter(all_unicode);

    MutableColumnPtr code_point_column = ColumnString::create();
    MutableColumnPtr code_point_value_column = ColumnInt32::create();

    while (iter.next())
    {
        UChar32 code = iter.getCodepoint();
        icu::UnicodeString u_value(code);
        String value;
        u_value.toUTF8String(value);
        code_point_column->insert(value);
        code_point_value_column->insert(code);
    }

    Block filter_block
    {
        ColumnWithTypeAndName(std::move(code_point_column), std::make_shared<DataTypeString>(), "code_point"),
        ColumnWithTypeAndName(std::move(code_point_value_column), std::make_shared<DataTypeInt32>(), "code_point_value")
    };

    VirtualColumnUtils::filterBlockWithPredicate(predicate, filter_block, context);
    return filter_block.getByPosition(1).column; // Return code_point_value column
}

void StorageSystemUnicode::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    // Common buffers/err_code used for ICU API calls
    UChar buffer[32];
    char char_name_buffer[100];
    UErrorCode err_code;

    auto prop_names = getPropNames();

    // Get filtered code points based on predicate
    ColumnPtr filtered_code_points = getFilteredCodePoints(predicate, context);

    for (size_t i = 0; i < filtered_code_points->size(); ++i)
    {
        UChar32 code = filtered_code_points->getInt(i);
        int index = 0;

        icu::UnicodeString u_value(code);
        String value;
        u_value.toUTF8String(value);
        assert_cast<ColumnString &>(*res_columns[index++]).insert(value);
        assert_cast<ColumnInt32 &>(*res_columns[index++]).insert(code);
        // Unicode string notation
        assert_cast<ColumnString &>(*res_columns[index++]).insert(fmt::format("U+{:04X}", code));
        size_t prop_index = 0;
        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_BINARY_LIMIT)
                break;
            assert_cast<ColumnUInt8 &>(*res_columns[index++]).insert(u_hasBinaryProperty(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_INT_LIMIT)
                break;

            // TODO: Using Int32 for now, not sure if Int16 would be sufficient
            assert_cast<ColumnInt32 &>(*res_columns[index++]).insert(u_getIntPropertyValue(code, prop));
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
            assert_cast<ColumnInt32 &>(*res_columns[index++]).insert(u_getIntPropertyValue(code, prop));
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
                    assert_cast<ColumnFloat64 &>(*res_columns[index++]).insert(u_getNumericValue(code));
                }
                else
                {
                    // Not a numeric value, set to 0.0
                    assert_cast<ColumnFloat64 &>(*res_columns[index++]).insert(0.0);
                }
            }
            ++prop_index;
        }
        while (prop_index < prop_names.size())
        {
            const auto & [_, prop] = prop_names[prop_index];
            if (prop >= UCHAR_STRING_LIMIT)
                break;

            String ret;
            if (prop == UCHAR_AGE)
            {
                UVersionInfo version_info;
                u_charAge(code, version_info);
                // format version info to string
                char uvbuf[U_MAX_VERSION_STRING_LENGTH];
                u_versionToString(version_info, uvbuf);
                ret = String(uvbuf);
            }
            else if (prop == UCHAR_BIDI_MIRRORING_GLYPH)
            {
                auto cm = u_charMirror(code);
                icu::UnicodeString str(cm);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_CASE_FOLDING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strFoldCase(buffer, 32, s, length, U_FOLD_CASE_DEFAULT, &err_code);
                icu::UnicodeString str(buffer);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_LOWERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strToLower(buffer, 32, s, length, "", &err_code);
                icu::UnicodeString str(buffer);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_NAME)
            {
                auto len = u_charName(code, U_UNICODE_CHAR_NAME, char_name_buffer, sizeof(char_name_buffer), &err_code);
                ret.assign(char_name_buffer, len);
            }
            else if (prop == UCHAR_SIMPLE_CASE_FOLDING)
            {
                auto cp = u_foldCase(code, U_FOLD_CASE_DEFAULT);
                icu::UnicodeString str(cp);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_SIMPLE_LOWERCASE_MAPPING)
            {
                auto cp = u_tolower(code);
                icu::UnicodeString str(cp);
                str.toUTF8String(ret);
            }
            // Now there is no code point where the two properties are different
            else if (prop == UCHAR_SIMPLE_TITLECASE_MAPPING || prop == UCHAR_TITLECASE_MAPPING)
            {
                auto cp = u_totitle(code);
                icu::UnicodeString str(cp);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_SIMPLE_UPPERCASE_MAPPING)
            {
                auto cp = u_toupper(code);
                icu::UnicodeString str(cp);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_UPPERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                u_strToUpper(buffer, 32, s, length, "", &err_code);
                icu::UnicodeString str(buffer);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_BIDI_PAIRED_BRACKET)
            {
                auto cp = u_getBidiPairedBracket(code);
                icu::UnicodeString str(cp);
                str.toUTF8String(ret);
            }

            assert_cast<ColumnString &>(*res_columns[index++]).insert(ret);
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
                assert_cast<ColumnArray &>(*res_columns[index++]).insert(arr);
            }

            // NOT handle UCHAR_IDENTIFIER_TYPE

            prop_index++;
        }
    }
}

}
