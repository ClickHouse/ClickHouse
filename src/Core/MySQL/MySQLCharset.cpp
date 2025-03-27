#include "MySQLCharset.h"
#include "config.h"
#include <Common/Exception.h>
#include <unordered_set>

#if USE_ICU
#include <unicode/ucnv.h>
#endif

namespace DB
{

#if USE_ICU
static constexpr auto CHUNK_SIZE = 1024;
static constexpr auto TARGET_CHARSET = "utf8";
#endif

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

const std::unordered_map<Int32, String> MySQLCharset::charsets
    = {
          {1, "big5"},
          {2, "latin2"},
          {3, "dec8"},
          {4, "cp850"},
          {5, "latin1"},
          {6, "hp8"},
          {7, "koi8r"},
          {8, "latin1"},
          {9, "latin2"},
          {10, "swe7"},
          {11, "ascii"},
          {12, "ujis"},
          {13, "sjis"},
          {14, "cp1251"},
          {15, "latin1"},
          {16, "hebrew"},
          {18, "tis620"},
          {19, "euckr"},
          {20, "latin7"},
          {21, "latin2"},
          {22, "koi8u"},
          {23, "cp1251"},
          {24, "gb2312"},
          {25, "greek"},
          {26, "cp1250"},
          {27, "latin2"},
          {28, "gbk"},
          {29, "cp1257"},
          {30, "latin5"},
          {31, "latin1"},
          {32, "armscii8"},
          {34, "cp1250"},
          {35, "ucs2"},
          {36, "cp866"},
          {37, "keybcs2"},
          {38, "macce"},
          {39, "macroman"},
          {40, "cp852"},
          {41, "latin7"},
          {42, "latin7"},
          {43, "macce"},
          {44, "cp1250"},
          {47, "latin1"},
          {48, "latin1"},
          {49, "latin1"},
          {50, "cp1251"},
          {51, "cp1251"},
          {52, "cp1251"},
          {53, "macroman"},
          {54, "utf16"},
          {55, "utf16"},
          {56, "utf16le"},
          {57, "cp1256"},
          {58, "cp1257"},
          {59, "cp1257"},
          {60, "utf32"},
          {61, "utf32"},
          {62, "utf16le"},
          {64, "armscii8"},
          {65, "ascii"},
          {66, "cp1250"},
          {67, "cp1256"},
          {68, "cp866"},
          {69, "dec8"},
          {70, "greek"},
          {71, "hebrew"},
          {72, "hp8"},
          {73, "keybcs2"},
          {74, "koi8r"},
          {75, "koi8u"},
          {77, "latin2"},
          {78, "latin5"},
          {79, "latin7"},
          {80, "cp850"},
          {81, "cp852"},
          {82, "swe7"},
          {84, "big5"},
          {85, "euckr"},
          {86, "gb2312"},
          {87, "gbk"},
          {88, "sjis"},
          {89, "tis620"},
          {90, "ucs2"},
          {91, "ujis"},
          {92, "geostd8"},
          {93, "geostd8"},
          {94, "latin1"},
          {95, "cp932"},
          {96, "cp932"},
          {97, "eucjpms"},
          {98, "eucjpms"},
          {99, "cp1250"},
          {101, "utf16"},
          {102, "utf16"},
          {103, "utf16"},
          {104, "utf16"},
          {105, "utf16"},
          {106, "utf16"},
          {107, "utf16"},
          {108, "utf16"},
          {109, "utf16"},
          {110, "utf16"},
          {111, "utf16"},
          {112, "utf16"},
          {113, "utf16"},
          {114, "utf16"},
          {115, "utf16"},
          {116, "utf16"},
          {117, "utf16"},
          {118, "utf16"},
          {119, "utf16"},
          {120, "utf16"},
          {121, "utf16"},
          {122, "utf16"},
          {123, "utf16"},
          {124, "utf16"},
          {128, "ucs2"},
          {129, "ucs2"},
          {130, "ucs2"},
          {131, "ucs2"},
          {132, "ucs2"},
          {133, "ucs2"},
          {134, "ucs2"},
          {135, "ucs2"},
          {136, "ucs2"},
          {137, "ucs2"},
          {138, "ucs2"},
          {139, "ucs2"},
          {140, "ucs2"},
          {141, "ucs2"},
          {142, "ucs2"},
          {143, "ucs2"},
          {144, "ucs2"},
          {145, "ucs2"},
          {146, "ucs2"},
          {147, "ucs2"},
          {148, "ucs2"},
          {149, "ucs2"},
          {150, "ucs2"},
          {151, "ucs2"},
          {159, "ucs2"},
          {160, "utf32"},
          {161, "utf32"},
          {162, "utf32"},
          {163, "utf32"},
          {164, "utf32"},
          {165, "utf32"},
          {166, "utf32"},
          {167, "utf32"},
          {168, "utf32"},
          {169, "utf32"},
          {170, "utf32"},
          {171, "utf32"},
          {172, "utf32"},
          {173, "utf32"},
          {174, "utf32"},
          {175, "utf32"},
          {176, "utf32"},
          {177, "utf32"},
          {178, "utf32"},
          {179, "utf32"},
          {180, "utf32"},
          {181, "utf32"},
          {182, "utf32"},
          {183, "utf32"},
          {248, "gb18030"},
          {249, "gb18030"},
          {250, "gb18030"}
      };

/// https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
/// SHOW CHARACTER SET
static const std::unordered_set<String> available_charsets = {
    "armscii8",
    "ascii",
    "big5",
    "binary",
    "cp1250",
    "cp1251",
    "cp1256",
    "cp1257",
    "cp850",
    "cp852",
    "cp866",
    "cp932",
    "dec8",
    "eucjpms",
    "euckr",
    "gb18030",
    "gb2312",
    "gbk",
    "geostd8",
    "greek",
    "hebrew",
    "hp8",
    "keybcs2",
    "koi8r",
    "koi8u",
    "latin1",
    "latin2",
    "latin5",
    "latin7",
    "macce",
    "macroman",
    "sjis",
    "swe7",
    "tis620",
    "ucs2",
    "ujis",
    "utf16",
    "utf16le",
    "utf32",
    "utf8mb3",
    "utf8mb4",
};

MySQLCharset::~MySQLCharset()
{
#if USE_ICU
    std::lock_guard lock(mutex);
    for (auto & conv : conv_cache)
    {
        ucnv_close(conv.second);
    }
    conv_cache.clear();
#endif
}

bool MySQLCharset::needConvert(UInt32 id)
{
    return charsets.contains(id);
}

bool MySQLCharset::needConvert(const String & charset)
{
    if (charset == "utf8mb4" || /// the conversion is not needed for utf8*
        charset == "utf8mb3" ||
        charset == "binary")    /// not implemented
        return false;
    return true;
}

bool MySQLCharset::isCharsetAvailable(const String & name)
{
    return available_charsets.contains(name);
}

String MySQLCharset::getCharsetFromId(UInt32 id)
{
    return charsets.at(id);
}

UConverter * MySQLCharset::getCachedConverter(const String & charset [[maybe_unused]])
{
    UConverter * conv = nullptr;
#if USE_ICU
    UErrorCode error = U_ZERO_ERROR;
    /// Get conv from cache
    auto result = conv_cache.find(charset);
    if (result != conv_cache.end())
    {
        conv = result->second;
        //reset to init state
        ucnv_reset(conv);
    }
    else
    {
        conv = ucnv_open(charset.c_str(), &error);
        if (error != U_ZERO_ERROR)
        {
            throw Exception(
                ErrorCodes::UNKNOWN_EXCEPTION, "MySQLCharset::getCachedConveter: ucnv_open failed, error={}", std::to_string(error));
        }
        conv_cache[charset.c_str()] = conv;
    }
#endif
    return conv;
}

Int32 MySQLCharset::convertFromId(UInt32 id, String & to, const String & from)
{
    return convert(getCharsetFromId(id), to, from);
}

Int32 MySQLCharset::convert(const String & source_charset [[maybe_unused]], String & to, const String & from)
{
#if USE_ICU
    std::lock_guard lock(mutex);
    UErrorCode error = U_ZERO_ERROR;
    to.clear();
    if (source_charset.empty())
    {
        return U_ILLEGAL_ARGUMENT_ERROR;
    }

    UChar pivot_buf[CHUNK_SIZE]; // stream mode must use this buf
    char target_buf[CHUNK_SIZE];
    UChar * pivot;
    UChar * pivot2;
    UConverter * in_conv;
    UConverter * out_conv;
    char * cur_target;
    const char * source_end;
    const char * target_end;

    size_t source_len = from.size();
    const char * source = from.data();
    source_end = source + source_len;

    out_conv = getCachedConverter(TARGET_CHARSET);
    in_conv = getCachedConverter(source_charset);
    pivot = pivot_buf;
    pivot2 = pivot_buf;

    target_end = target_buf + CHUNK_SIZE;
    do
    {
        error = U_ZERO_ERROR;
        cur_target = target_buf;
        ucnv_convertEx(
            out_conv,
            in_conv,
            &cur_target,
            target_end,
            &source,
            source_end,
            pivot_buf,
            &pivot,
            &pivot2,
            pivot_buf + CHUNK_SIZE,
            false,
            true,
            &error);
        to.append(target_buf, cur_target - target_buf);
    } while (error == U_BUFFER_OVERFLOW_ERROR);

    return error;
#else
    to = from;
    return 0;
#endif
}

}
