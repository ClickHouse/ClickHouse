#include <cassert>
#include <string>
#include <Storages/System/StorageSystemUnicode.h>
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn_fwd.h"
#include "Core/NamesAndTypes.h"
#include "Interpreters/Context_fwd.h"
#include "QueryPipeline/Pipe.h"
#include "Storages/ColumnsDescription.h"
#include "base/types.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <unicode/errorcode.h>
#include <unicode/umachine.h>
#include <unicode/uniset.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/usetiter.h>
#include <unicode/uchar.h>
#include <unicode/utypes.h>
#include <unicode/uversion.h>
#include <unicode/ustring.h>
#include "Common/Exception.h"
#include "Common/assert_cast.h"

namespace DB
{

std::vector<std::pair<const char *, UProperty>> getPropNames()
{
    std::vector<std::pair<const char *, UProperty>> properties;
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
        const char* prop_name = u_getPropertyName(prop, U_LONG_PROPERTY_NAME);
        
        // TODO: maybe we can use short name as alias name
        if (prop_name)
        {
            properties.emplace_back(prop_name, prop);
        }
        i ++;
    }
    return properties;
}

ColumnsDescription StorageSystemUnicode::getColumnsDescription()
{
    NamesAndTypes names_and_types;
    auto prop_names = getPropNames();
    names_and_types.emplace_back("code_point", std::make_shared<DataTypeInt32>());
    names_and_types.emplace_back("value", std::make_shared<DataTypeString>());
    size_t prop_index = 0;
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_BINARY_LIMIT)
            break;
        names_and_types.emplace_back(String(prop_name), std::make_shared<DataTypeUInt8>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_GENERAL_CATEGORY_MASK)
            break;

        names_and_types.emplace_back(String(prop_name), std::make_shared<DataTypeInt32>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_MASK_LIMIT)
            break;
        names_and_types.emplace_back(String(prop_name), std::make_shared<DataTypeInt32>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_DOUBLE_LIMIT)
            break;
        
        names_and_types.emplace_back(String(prop_name), std::make_shared<DataTypeFloat64>());
        ++prop_index;
    }
    while (prop_index < prop_names.size())
    {
        const auto & [prop_name, prop] = prop_names[prop_index];
        if (prop >= UCHAR_STRING_LIMIT)
            break;
        names_and_types.emplace_back(String(prop_name), std::make_shared<DataTypeString>());
        ++prop_index;
    }
    // TODO: Excludes properties available through the UnicodeSet API and patterns
    return ColumnsDescription::fromNamesAndTypes(names_and_types);
}

void StorageSystemUnicode::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    icu::UnicodeSet all_unicode(0x0000, 0x10FFFF);
    // Remove surrogate pairs
    all_unicode.remove(0xD800, 0xDFFF);
    // Remove non character code points
    all_unicode.remove(0xFDD0, 0xFDEF); // 直接移除整个区间
    // Remove non-character code points at the end of each Unicode plane
    for (UChar32 base = 0xFFFE; base <= 0x10FFFF; base += 0x10000)
    {
        all_unicode.remove(base, base + 1);
    }
    std::cout << "unicode size: " << all_unicode.size() << std::endl;
    icu::UnicodeSetIterator iter(all_unicode);


    // common
    UChar buffer[32];
    char char_buffer[80];
    UErrorCode err_code;

    /*
    UErrorCode error_code = U_ZERO_ERROR;
    const UNormalizer2 * nfkc = unorm2_getNFKCInstance(&error_code);
    if (error_code != U_ZERO_ERROR)
    {
        // throw Exception
        return;
    }
    */

    auto prop_names = getPropNames();
    while (iter.next())
    {
        UChar32 code = iter.getCodepoint();
        int index = 0;
        assert_cast<ColumnInt32 &>(*res_columns[index++]).insert(code);
        icu::UnicodeString u_value(code);
        String value;
        u_value.toUTF8String(value);
        assert_cast<ColumnString &>(*res_columns[index++]).insert(value);

        size_t prop_index = 0;
        while (prop_index < prop_names.size())
        {
            const auto & [prop_name, prop] = prop_names[prop_index];
            if (prop >= UCHAR_BINARY_LIMIT)
                break;
            assert_cast<ColumnUInt8 &>(*res_columns[index++]).insert(u_hasBinaryProperty(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [prop_name, prop] = prop_names[prop_index];
            if (prop >= UCHAR_GENERAL_CATEGORY_MASK)
                break;

            // TODO: May not need int32_t, we can get max value to decide the column type
            assert_cast<ColumnInt32 &>(*res_columns[index++]).insert(u_getIntPropertyValue(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [prop_name, prop] = prop_names[prop_index];
            if (prop >= UCHAR_MASK_LIMIT)
                break;
            // TODO: Result is a mask, now we just use int32_t to store it
            // U_GC_L_MASK, U_GC_M_MASK, U_GC_N_MASK, U_GC_P_MASK, U_GC_S_MASK ...
            assert_cast<ColumnInt32 &>(*res_columns[index++]).insert(u_getIntPropertyValue(code, prop));
            ++prop_index;
        }

        while (prop_index < prop_names.size())
        {
            const auto & [prop_name, prop] = prop_names[prop_index];
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
                    // TODO: zero value or null or other value?
                    assert_cast<ColumnFloat64 &>(*res_columns[index++]).insert(0.0);
                }
            }
            ++prop_index;
        }
        while (prop_index < prop_names.size())
        {
            const auto & [prop_name, prop] = prop_names[prop_index];
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
                auto len = u_strFoldCase(buffer, 32, s, 2, U_FOLD_CASE_DEFAULT, &err_code);
                icu::UnicodeString str(buffer, len);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_LOWERCASE_MAPPING)
            {
                err_code = U_ZERO_ERROR;
                UChar s[2];
                int32_t length = 0;
                U16_APPEND_UNSAFE(s, length, code);
                auto len = u_strToLower(buffer, 32, s, 1, "", &err_code);
                icu::UnicodeString str(buffer, len);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_NAME)
            {
                auto len = u_charName(code, U_UNICODE_CHAR_NAME, char_buffer, 32, &err_code);
                ret.assign(char_buffer, len);
            }
            else if (prop == UCHAR_SIMPLE_CASE_FOLDING)
            {
                auto fc = u_foldCase(code, U_FOLD_CASE_DEFAULT);
                icu::UnicodeString str(fc);
                str.toUTF8String(ret);
            }
            else if (prop == UCHAR_SIMPLE_LOWERCASE_MAPPING)
            {
                auto tl = u_tolower(code);
                icu::UnicodeString str(tl);
                str.toUTF8String(ret);
            }
            
            assert_cast<ColumnString &>(*res_columns[index++]).insert(ret);
            ++prop_index;
        }
        // TODO: add OTHER_PROPERTIES: UCHAR_SCRIPT_EXTENSIONS UCHAR_IDENTIFIER_TYPE

        // TODO: add more excludes properties available through the UnicodeSet API and patterns?

    }
}

}
