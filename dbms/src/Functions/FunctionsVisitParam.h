#pragma once

#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Common/Volnitsky.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


/** Функции для извлечения параметров визитов.
 *  Реализованы через шаблоны из FunctionsStringSearch.h.
 *
 * Проверить есть ли параметр
 *         visitParamHas
 *
 * Извлечь числовое значение параметра
 *         visitParamExtractUInt
 *         visitParamExtractInt
 *         visitParamExtractFloat
 *         visitParamExtractBool
 *
 * Извлечь строкое значение параметра
 *         visitParamExtractString - значение разэскейпливается
 *         visitParamExtractRaw
 */

namespace DB
{

struct HasParam
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 * begin, const UInt8 * end)
    {
        return true;
    }
};

template<typename NumericType>
struct ExtractNumericType
{
    using ResultType = NumericType;

    static ResultType extract(const UInt8 * begin, const UInt8 * end)
    {
        ReadBufferFromMemory in(begin, end - begin);

        /// Учимся читать числа в двойных кавычках
        if (!in.eof() && *in.position() == '"')
            ++in.position();

        ResultType x = 0;
        if (!in.eof())
            readText(x, in);
        return x;
    }
};

struct ExtractBool
{
    using ResultType = UInt8;

    static UInt8 extract(const UInt8 * begin, const UInt8 * end)
    {
        return begin + 4 <= end && 0 == strncmp(reinterpret_cast<const char *>(begin), "true", 4);
    }
};


struct ExtractRaw
{
    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars_t & res_data)
    {
        if (pos == end)
            return;

        UInt8 open_char = *pos;
        UInt8 close_char = 0;
        switch (open_char)
        {
            case '[':
                close_char = ']';
                break;
            case '{':
                close_char = '}';
                break;
            case '"':
                close_char = '"';
                break;
        }

        if (close_char != 0)
        {
            size_t balance = 1;
            char last_char = 0;

            res_data.push_back(*pos);

            ++pos;
            for (; pos != end && balance > 0; ++pos)
            {
                res_data.push_back(*pos);

                if (open_char == '"' && *pos == '"')
                {
                    if (last_char != '\\')
                        break;
                }
                else
                {
                    if (*pos == open_char)
                        ++balance;
                    if (*pos == close_char)
                        --balance;
                }

                if (last_char == '\\')
                    last_char = 0;
                else
                    last_char = *pos;
            }
        }
        else
        {
            for (; pos != end && *pos != ',' && *pos != '}'; ++pos)
                res_data.push_back(*pos);
        }
    }
};

struct ExtractString
{
    static bool tryParseDigit(UInt8 c, UInt8 & res)
    {
        if ('0' <= c && c <= '9')
        {
            res = c - '0';
            return true;
        }
        if ('A' <= c && c <= 'Z')
        {
            res = c - ('A' - 10);
            return true;
        }
        if ('a' <= c && c <= 'z')
        {
            res = c - ('a' - 10);
            return true;
        }
        return false;
    }

    static bool tryUnhex(const UInt8 * pos, const UInt8 * end, int & res)
    {
        if (pos + 3 >= end)
            return false;

        res = 0;
        {
            UInt8 major, minor;
            if (!tryParseDigit(*(pos++), major))
                return false;
            if (!tryParseDigit(*(pos++), minor))
                return false;
            res |= (major << 4) | minor;
        }
        res <<= 8;
        {
            UInt8 major, minor;
            if (!tryParseDigit(*(pos++), major))
                return false;
            if (!tryParseDigit(*(pos++), minor))
                return false;
            res |= (major << 4) | minor;
        }
        return true;
    }

    static bool tryExtract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars_t & res_data)
    {
        if (pos == end || *pos != '"')
            return false;

        ++pos;
        while (pos != end)
        {
            switch (*pos)
            {
                case '\\':
                    ++pos;
                    if (pos >= end)
                        return false;

                    switch(*pos)
                    {
                        case '"':
                            res_data.push_back('"');
                            break;
                        case '\\':
                            res_data.push_back('\\');
                            break;
                        case '/':
                            res_data.push_back('/');
                            break;
                        case 'b':
                            res_data.push_back('\b');
                            break;
                        case 'f':
                            res_data.push_back('\f');
                            break;
                        case 'n':
                            res_data.push_back('\n');
                            break;
                        case 'r':
                            res_data.push_back('\r');
                            break;
                        case 't':
                            res_data.push_back('\t');
                            break;
                        case 'u':
                        {
                            ++pos;

                            int unicode;
                            if (!tryUnhex(pos, end, unicode))
                                return false;
                            pos += 3;

                            res_data.resize(res_data.size() + 6);    /// максимальный размер UTF8 многобайтовой последовательности

                            Poco::UTF8Encoding utf8;
                            int length = utf8.convert(unicode, const_cast<UInt8 *>(&res_data[0]) + res_data.size() - 6, 6);

                            if (!length)
                                return false;

                            res_data.resize(res_data.size() - 6 + length);
                            break;
                        }
                        default:
                            res_data.push_back(*pos);
                            break;
                    }
                    ++pos;
                    break;
                case '"':
                    return true;
                default:
                    res_data.push_back(*pos);
                    ++pos;
                    break;
            }
        }
        return false;
    }

    static void extract(const UInt8 * pos, const UInt8 * end, ColumnString::Chars_t & res_data)
    {
        size_t old_size = res_data.size();

        if (!tryExtract(pos, end, res_data))
            res_data.resize(old_size);
    }
};


/** Ищет вхождения поля в параметре визитов и вызывает ParamExtractor
 * на каждое вхождение поля, передавая ему указатель на часть строки,
 * где начинается вхождение значения поля.
 * ParamExtractor должен распарсить и вернуть значение нужного типа.
 *
 * Если поле не было найдено или полю соответствует некорректное значение,
 * то используется значение по умолчанию - 0.
 */
template <typename ParamExtractor>
struct ExtractParamImpl
{
    using ResultType = typename ParamExtractor::ResultType;

    /// Предполагается, что res нужного размера и инициализирован нулями.
    static void vector_constant(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
        std::string needle,
        PaddedPODArray<ResultType> & res)
    {
        /// Ищем параметр просто как подстроку вида "name":
        needle = "\"" + needle + "\":";

        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// Текущий индекс в массиве строк.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// Искать будем следующее вхождение сразу во всех строках.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Определим, к какому индексу оно относится.
            while (begin + offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }

            /// Проверяем, что вхождение не переходит через границы строк.
            if (pos + needle.size() < begin + offsets[i])
                res[i] = ParamExtractor::extract(pos + needle.size(), begin + offsets[i]);
            else
                res[i] = 0;

            pos = begin + offsets[i];
            ++i;
        }

        memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    static void constant_constant(const std::string & data, std::string needle, ResultType & res)
    {
        needle = "\"" + needle + "\":";
        size_t pos = data.find(needle);
        if (pos == std::string::npos)
            res = 0;
        else
            res = ParamExtractor::extract(
                reinterpret_cast<const UInt8 *>(data.data() + pos + needle.size()),
                reinterpret_cast<const UInt8 *>(data.data() + data.size())
            );
    }

    static void vector_vector(
        const ColumnString::Chars_t & haystack_data, const ColumnString::Offsets_t & haystack_offsets,
        const ColumnString::Chars_t & needle_data, const ColumnString::Offsets_t & needle_offsets,
        PaddedPODArray<ResultType> & res)
    {
        throw Exception("Functions 'visitParamHas' and 'visitParamExtract*' doesn't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }

    static void constant_vector(
        const String & haystack,
        const ColumnString::Chars_t & needle_data, const ColumnString::Offsets_t & needle_offsets,
        PaddedPODArray<ResultType> & res)
    {
        throw Exception("Functions 'visitParamHas' and 'visitParamExtract*' doesn't support non-constant needle argument", ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** Для случая когда тип поля, которое нужно извлечь - строка.
 */
template<typename ParamExtractor>
struct ExtractParamToStringImpl
{
    static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
                       std::string needle,
                       ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
    {
        /// Константа 5 взята из функции, выполняющей похожую задачу FunctionsStringSearch.h::ExtractImpl
        res_data.reserve(data.size()  / 5);
        res_offsets.resize(offsets.size());

        /// Ищем параметр просто как подстроку вида "name":
        needle = "\"" + needle + "\":";

        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// Текущий индекс в массиве строк.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// Искать будем следующее вхождение сразу во всех строках.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Определим, к какому индексу оно относится.
            while (begin + offsets[i] <= pos)
            {
                res_data.push_back(0);
                res_offsets[i] = res_data.size();
                ++i;
            }

            /// Проверяем, что вхождение не переходит через границы строк.
            if (pos + needle.size() < begin + offsets[i])
                ParamExtractor::extract(pos + needle.size(), begin + offsets[i], res_data);

            pos = begin + offsets[i];

            res_data.push_back(0);
            res_offsets[i] = res_data.size();
            ++i;
        }

        while (i < res_offsets.size())
        {
            res_data.push_back(0);
            res_offsets[i] = res_data.size();
            ++i;
        }
    }
};



}
