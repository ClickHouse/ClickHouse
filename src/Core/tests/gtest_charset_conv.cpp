#include <Core/MySQL/MySQLCharset.h>
#include <gtest/gtest.h>
#include <cstdio>

namespace DB
{

struct CheckResult
{
    Int32 id;
    String name;
    bool need_convert;
};

TEST(CharsetTest, CharsetTest)
{
    MySQLCharset charset;
    UInt32 big5_id = 1;
    UInt32 gbk_id = 28;
    UInt32 gb2312_id = 24;
    UInt32 utf8mb4_ai_ci_id = 255;
    EXPECT_TRUE(charset.needConvert(big5_id));
    EXPECT_TRUE(charset.needConvert(gbk_id));
    EXPECT_TRUE(charset.needConvert(gb2312_id));
    EXPECT_FALSE(charset.needConvert(utf8mb4_ai_ci_id));
    EXPECT_FALSE(charset.needConvert(0));
    EXPECT_FALSE(charset.needConvert(1000));

    EXPECT_EQ(charset.getCharsetFromId(big5_id), String("big5"));
    EXPECT_EQ(charset.getCharsetFromId(gbk_id), String("gbk"));
    EXPECT_EQ(charset.getCharsetFromId(gb2312_id), String("gb2312"));
}

TEST(CharsetTest, ConvTest)
{
    MySQLCharset charset;
    UInt32 big5_id = 1;
    UInt32 gbk_id = 28;
    UInt32 gb2312_id = 24;
    Int32 error = 0;
    String source("\xc4\xe3\xba\xc3"); // gbk "你好"
    String target;
    String expect("\xe4\xbd\xa0\xe5\xa5\xbd");

    error = charset.convertFromId(gbk_id, target, source);
    EXPECT_EQ(error, 0);
    EXPECT_TRUE(target == expect);

    error = charset.convertFromId(gb2312_id, target, source);
    EXPECT_EQ(error, 0);
    EXPECT_TRUE(target == expect);

    source.assign("\xa7\x41\xa6\x6e"); // big5 "你好"
    error = charset.convertFromId(big5_id, target, source);
    EXPECT_EQ(error, 0);
    EXPECT_TRUE(target == expect);
}

TEST(CharsetTest, FullCharsetCheck)
{
    CheckResult result[] =
    {
        {1, "big5", true}, // "big5_chinese_ci",
        {2, "latin2", true}, // "latin2_czech_cs",
        {3, "dec8", true}, // "dec8_swedish_ci",
        {4, "cp850", true}, // "cp850_general_ci",
        {5, "latin1", true}, // "latin1_german1_ci",
        {6, "hp8", true}, // "hp8_english_ci",
        {7, "koi8r", true}, // "koi8r_general_ci",
        {8, "latin1", true}, // "latin1_swedish_ci",
        {9, "latin2", true}, // "latin2_general_ci",
        {10, "swe7", true}, // "swe7_swedish_ci",
        {11, "ascii", true}, // "ascii_general_ci",
        {12, "ujis", true}, // "ujis_japanese_ci",
        {13, "sjis", true}, // "sjis_japanese_ci",
        {14, "cp1251", true}, // "cp1251_bulgarian_ci",
        {15, "latin1", true}, // "latin1_danish_ci",
        {16, "hebrew", true}, // "hebrew_general_ci",
        {18, "tis620", true}, // "tis620_thai_ci",
        {19, "euckr", true}, // "euckr_korean_ci",
        {20, "latin7", true}, // "latin7_estonian_cs",
        {21, "latin2", true}, // "latin2_hungarian_ci",
        {22, "koi8u", true}, // "koi8u_general_ci",
        {23, "cp1251", true}, // "cp1251_ukrainian_ci",
        {24, "gb2312", true}, // "gb2312_chinese_ci",
        {25, "greek", true}, // "greek_general_ci",
        {26, "cp1250", true}, // "cp1250_general_ci",
        {27, "latin2", true}, // "latin2_croatian_ci",
        {28, "gbk", true}, // "gbk_chinese_ci",
        {29, "cp1257", true}, // "cp1257_lithuanian_ci",
        {30, "latin5", true}, // "latin5_turkish_ci",
        {31, "latin1", true}, // "latin1_german2_ci",
        {32, "armscii8", true}, // "armscii8_general_ci",
        {33, "utf8", false}, // "utf8_general_ci",
        {34, "cp1250", true}, // "cp1250_czech_cs",
        {35, "ucs2", true}, // "ucs2_general_ci",
        {36, "cp866", true}, // "cp866_general_ci",
        {37, "keybcs2", true}, // "keybcs2_general_ci",
        {38, "macce", true}, // "macce_general_ci",
        {39, "macroman", true}, // "macroman_general_ci",
        {40, "cp852", true}, // "cp852_general_ci",
        {41, "latin7", true}, // "latin7_general_ci",
        {42, "latin7", true}, // "latin7_general_cs",
        {43, "macce", true}, // "macce_bin",
        {44, "cp1250", true}, // "cp1250_croatian_ci",
        {45, "utf8mb4", false}, // "utf8mb4_general_ci",
        {46, "utf8mb4", false}, // "utf8mb4_bin",
        {47, "latin1", true}, // "latin1_bin",
        {48, "latin1", true}, // "latin1_general_ci",
        {49, "latin1", true}, // "latin1_general_cs",
        {50, "cp1251", true}, // "cp1251_bin",
        {51, "cp1251", true}, // "cp1251_general_ci",
        {52, "cp1251", true}, // "cp1251_general_cs",
        {53, "macroman", true}, // "macroman_bin",
        {54, "utf16", true}, // "utf16_general_ci",
        {55, "utf16", true}, // "utf16_bin",
        {56, "utf16le", true}, // "utf16le_general_ci",
        {57, "cp1256", true}, // "cp1256_general_ci",
        {58, "cp1257", true}, // "cp1257_bin",
        {59, "cp1257", true}, // "cp1257_general_ci",
        {60, "utf32", true}, // "utf32_general_ci",
        {61, "utf32", true}, // "utf32_bin",
        {62, "utf16le", true}, // "utf16le_bin",
        {64, "armscii8", true}, // "armscii8_bin",
        {65, "ascii", true}, // "ascii_bin",
        {66, "cp1250", true}, // "cp1250_bin",
        {67, "cp1256", true}, // "cp1256_bin",
        {68, "cp866", true}, // "cp866_bin",
        {69, "dec8", true}, // "dec8_bin",
        {70, "greek", true}, // "greek_bin",
        {71, "hebrew", true}, // "hebrew_bin",
        {72, "hp8", true}, // "hp8_bin",
        {73, "keybcs2", true}, // "keybcs2_bin",
        {74, "koi8r", true}, // "koi8r_bin",
        {75, "koi8u", true}, // "koi8u_bin",
        {77, "latin2", true}, // "latin2_bin",
        {78, "latin5", true}, // "latin5_bin",
        {79, "latin7", true}, // "latin7_bin",
        {80, "cp850", true}, // "cp850_bin",
        {81, "cp852", true}, // "cp852_bin",
        {82, "swe7", true}, // "swe7_bin",
        {83, "utf8", false}, // "utf8_bin",
        {84, "big5", true}, // "big5_bin",
        {85, "euckr", true}, // "euckr_bin",
        {86, "gb2312", true}, // "gb2312_bin",
        {87, "gbk", true}, // "gbk_bin",
        {88, "sjis", true}, // "sjis_bin",
        {89, "tis620", true}, // "tis620_bin",
        {90, "ucs2", true}, // "ucs2_bin",
        {91, "ujis", true}, // "ujis_bin",
        {92, "geostd8", true}, // "geostd8_general_ci",
        {93, "geostd8", true}, // "geostd8_bin",
        {94, "latin1", true}, // "latin1_spanish_ci",
        {95, "cp932", true}, // "cp932_japanese_ci",
        {96, "cp932", true}, // "cp932_bin",
        {97, "eucjpms", true}, // "eucjpms_japanese_ci",
        {98, "eucjpms", true}, // "eucjpms_bin",
        {99, "cp1250", true}, // "cp1250_polish_ci",
        {101, "utf16", true}, // "utf16_unicode_ci",
        {102, "utf16", true}, // "utf16_icelandic_ci",
        {103, "utf16", true}, // "utf16_latvian_ci",
        {104, "utf16", true}, // "utf16_romanian_ci",
        {105, "utf16", true}, // "utf16_slovenian_ci",
        {106, "utf16", true}, // "utf16_polish_ci",
        {107, "utf16", true}, // "utf16_estonian_ci",
        {108, "utf16", true}, // "utf16_spanish_ci",
        {109, "utf16", true}, // "utf16_swedish_ci",
        {110, "utf16", true}, // "utf16_turkish_ci",
        {111, "utf16", true}, // "utf16_czech_ci",
        {112, "utf16", true}, // "utf16_danish_ci",
        {113, "utf16", true}, // "utf16_lithuanian_ci",
        {114, "utf16", true}, // "utf16_slovak_ci",
        {115, "utf16", true}, // "utf16_spanish2_ci",
        {116, "utf16", true}, // "utf16_roman_ci",
        {117, "utf16", true}, // "utf16_persian_ci",
        {118, "utf16", true}, // "utf16_esperanto_ci",
        {119, "utf16", true}, // "utf16_hungarian_ci",
        {120, "utf16", true}, // "utf16_sinhala_ci",
        {121, "utf16", true}, // "utf16_german2_ci",
        {122, "utf16", true}, // "utf16_croatian_ci",
        {123, "utf16", true}, // "utf16_unicode_520_ci",
        {124, "utf16", true}, // "utf16_vietnamese_ci",
        {128, "ucs2", true}, // "ucs2_unicode_ci",
        {129, "ucs2", true}, // "ucs2_icelandic_ci",
        {130, "ucs2", true}, // "ucs2_latvian_ci",
        {131, "ucs2", true}, // "ucs2_romanian_ci",
        {132, "ucs2", true}, // "ucs2_slovenian_ci",
        {133, "ucs2", true}, // "ucs2_polish_ci",
        {134, "ucs2", true}, // "ucs2_estonian_ci",
        {135, "ucs2", true}, // "ucs2_spanish_ci",
        {136, "ucs2", true}, // "ucs2_swedish_ci",
        {137, "ucs2", true}, // "ucs2_turkish_ci",
        {138, "ucs2", true}, // "ucs2_czech_ci",
        {139, "ucs2", true}, // "ucs2_danish_ci",
        {140, "ucs2", true}, // "ucs2_lithuanian_ci",
        {141, "ucs2", true}, // "ucs2_slovak_ci",
        {142, "ucs2", true}, // "ucs2_spanish2_ci",
        {143, "ucs2", true}, // "ucs2_roman_ci",
        {144, "ucs2", true}, // "ucs2_persian_ci",
        {145, "ucs2", true}, // "ucs2_esperanto_ci",
        {146, "ucs2", true}, // "ucs2_hungarian_ci",
        {147, "ucs2", true}, // "ucs2_sinhala_ci",
        {148, "ucs2", true}, // "ucs2_german2_ci",
        {149, "ucs2", true}, // "ucs2_croatian_ci",
        {150, "ucs2", true}, // "ucs2_unicode_520_ci",
        {151, "ucs2", true}, // "ucs2_vietnamese_ci",
        {159, "ucs2", true}, // "ucs2_general_mysql500_ci",
        {160, "utf32", true}, // "utf32_unicode_ci",
        {161, "utf32", true}, // "utf32_icelandic_ci",
        {162, "utf32", true}, // "utf32_latvian_ci",
        {163, "utf32", true}, // "utf32_romanian_ci",
        {164, "utf32", true}, // "utf32_slovenian_ci",
        {165, "utf32", true}, // "utf32_polish_ci",
        {166, "utf32", true}, // "utf32_estonian_ci",
        {167, "utf32", true}, // "utf32_spanish_ci",
        {168, "utf32", true}, // "utf32_swedish_ci",
        {169, "utf32", true}, // "utf32_turkish_ci",
        {170, "utf32", true}, // "utf32_czech_ci",
        {171, "utf32", true}, // "utf32_danish_ci",
        {172, "utf32", true}, // "utf32_lithuanian_ci",
        {173, "utf32", true}, // "utf32_slovak_ci",
        {174, "utf32", true}, // "utf32_spanish2_ci",
        {175, "utf32", true}, // "utf32_roman_ci",
        {176, "utf32", true}, // "utf32_persian_ci",
        {177, "utf32", true}, // "utf32_esperanto_ci",
        {178, "utf32", true}, // "utf32_hungarian_ci",
        {179, "utf32", true}, // "utf32_sinhala_ci",
        {180, "utf32", true}, // "utf32_german2_ci",
        {181, "utf32", true}, // "utf32_croatian_ci",
        {182, "utf32", true}, // "utf32_unicode_520_ci",
        {183, "utf32", true}, // "utf32_vietnamese_ci",
        {192, "utf8", false}, // "utf8_unicode_ci",
        {193, "utf8", false}, // "utf8_icelandic_ci",
        {194, "utf8", false}, // "utf8_latvian_ci",
        {195, "utf8", false}, // "utf8_romanian_ci",
        {196, "utf8", false}, // "utf8_slovenian_ci",
        {197, "utf8", false}, // "utf8_polish_ci",
        {198, "utf8", false}, // "utf8_estonian_ci",
        {199, "utf8", false}, // "utf8_spanish_ci",
        {200, "utf8", false}, // "utf8_swedish_ci",
        {201, "utf8", false}, // "utf8_turkish_ci",
        {202, "utf8", false}, // "utf8_czech_ci",
        {203, "utf8", false}, // "utf8_danish_ci",
        {204, "utf8", false}, // "utf8_lithuanian_ci",
        {205, "utf8", false}, // "utf8_slovak_ci",
        {206, "utf8", false}, // "utf8_spanish2_ci",
        {207, "utf8", false}, // "utf8_roman_ci",
        {208, "utf8", false}, // "utf8_persian_ci",
        {209, "utf8", false}, // "utf8_esperanto_ci",
        {210, "utf8", false}, // "utf8_hungarian_ci",
        {211, "utf8", false}, // "utf8_sinhala_ci",
        {212, "utf8", false}, // "utf8_german2_ci",
        {213, "utf8", false}, // "utf8_croatian_ci",
        {214, "utf8", false}, // "utf8_unicode_520_ci",
        {215, "utf8", false}, // "utf8_vietnamese_ci",
        {223, "utf8", false}, // "utf8_general_mysql500_ci",
        {224, "utf8mb4", false}, // "utf8mb4_unicode_ci",
        {225, "utf8mb4", false}, // "utf8mb4_icelandic_ci",
        {226, "utf8mb4", false}, // "utf8mb4_latvian_ci",
        {227, "utf8mb4", false}, // "utf8mb4_romanian_ci",
        {228, "utf8mb4", false}, // "utf8mb4_slovenian_ci",
        {229, "utf8mb4", false}, // "utf8mb4_polish_ci",
        {230, "utf8mb4", false}, // "utf8mb4_estonian_ci",
        {231, "utf8mb4", false}, // "utf8mb4_spanish_ci",
        {232, "utf8mb4", false}, // "utf8mb4_swedish_ci",
        {233, "utf8mb4", false}, // "utf8mb4_turkish_ci",
        {234, "utf8mb4", false}, // "utf8mb4_czech_ci",
        {235, "utf8mb4", false}, // "utf8mb4_danish_ci",
        {236, "utf8mb4", false}, // "utf8mb4_lithuanian_ci",
        {237, "utf8mb4", false}, // "utf8mb4_slovak_ci",
        {238, "utf8mb4", false}, // "utf8mb4_spanish2_ci",
        {239, "utf8mb4", false}, // "utf8mb4_roman_ci",
        {240, "utf8mb4", false}, // "utf8mb4_persian_ci",
        {241, "utf8mb4", false}, // "utf8mb4_esperanto_ci",
        {242, "utf8mb4", false}, // "utf8mb4_hungarian_ci",
        {243, "utf8mb4", false}, // "utf8mb4_sinhala_ci",
        {244, "utf8mb4", false}, // "utf8mb4_german2_ci",
        {245, "utf8mb4", false}, // "utf8mb4_croatian_ci",
        {246, "utf8mb4", false}, // "utf8mb4_unicode_520_ci",
        {247, "utf8mb4", false}, // "utf8mb4_vietnamese_ci",
        {248, "gb18030", true}, // "gb18030_chinese_ci",
        {249, "gb18030", true}, // "gb18030_bin",
        {250, "gb18030", true}, // "gb18030_unicode_520_ci",
        {255, "utf8mb4", false}, // "utf8mb4_0900_ai_ci",
        {256, "utf8mb4", false}, // "utf8mb4_de_pb_0900_ai_ci",
        {257, "utf8mb4", false}, // "utf8mb4_is_0900_ai_ci",
        {258, "utf8mb4", false}, // "utf8mb4_lv_0900_ai_ci",
        {259, "utf8mb4", false}, // "utf8mb4_ro_0900_ai_ci",
        {260, "utf8mb4", false}, // "utf8mb4_sl_0900_ai_ci",
        {261, "utf8mb4", false}, // "utf8mb4_pl_0900_ai_ci",
        {262, "utf8mb4", false}, // "utf8mb4_et_0900_ai_ci",
        {263, "utf8mb4", false}, // "utf8mb4_es_0900_ai_ci",
        {264, "utf8mb4", false}, // "utf8mb4_is_0900_ai_ci",
        {265, "utf8mb4", false}, // "utf8mb4_tr_0900_ai_ci",
        {266, "utf8mb4", false}, // "utf8mb4_cs_0900_ai_ci",
        {267, "utf8mb4", false}, // "utf8mb4_da_0900_ai_ci",
        {268, "utf8mb4", false}, // "utf8mb4_lt_0900_ai_ci",
        {269, "utf8mb4", false}, // "utf8mb4_sk_0900_ai_ci",
        {270, "utf8mb4", false}, // "utf8mb4_es_trad_0900_ai_ci",
        {271, "utf8mb4", false}, // "utf8mb4_la_0900_ai_ci",
        {272, "utf8mb4", false}, // "utf8mb4_fa_0900_ai_ci",
        {273, "utf8mb4", false}, // "utf8mb4_eo_0900_ai_ci",
        {274, "utf8mb4", false}, // "utf8mb4_hu_0900_ai_ci",
        {275, "utf8mb4", false}, // "utf8mb4_hr_0900_ai_ci",
        {276, "utf8mb4", false}, // "utf8mb4_si_0900_ai_ci",
        {277, "utf8mb4", false}, // "utf8mb4_vi_0900_ai_ci",
        {278, "utf8mb4", false}, // "utf8mb4_0900_as_cs",
        {279, "utf8mb4", false}, // "utf8mb4_de_pb_0900_as_cs",
        {280, "utf8mb4", false}, // "utf8mb4_is_0900_as_cs",
        {281, "utf8mb4", false}, // "utf8mb4_lv_0900_as_cs",
        {282, "utf8mb4", false}, // "utf8mb4_ro_0900_as_cs",
        {283, "utf8mb4", false}, // "utf8mb4_sl_0900_as_cs",
        {284, "utf8mb4", false}, // "utf8mb4_pl_0900_as_cs",
        {285, "utf8mb4", false}, // "utf8mb4_et_0900_as_cs",
        {286, "utf8mb4", false}, // "utf8mb4_es_0900_as_cs",
        {287, "utf8mb4", false}, // "utf8mb4_sv_0900_as_cs",
        {288, "utf8mb4", false}, // "utf8mb4_tr_0900_as_cs",
        {289, "utf8mb4", false}, // "utf8mb4_cs_0900_as_cs",
        {290, "utf8mb4", false}, // "utf8mb4_da_0900_as_cs"
        {291, "utf8mb4", false}, // "utf8mb4_lt_0900_as_cs"
        {292, "utf8mb4", false}, // "utf8mb4_sk_0900_as_cs"
        {293, "utf8mb4", false}, // "utf8mb4_es_trad_0900_as_cs"
        {294, "utf8mb4", false}, // "utf8mb4_la_0900_as_cs"
        {295, "utf8mb4", false}, // "utf8mb4_fa_0900_as_cs"
        {296, "utf8mb4", false}, // "utf8mb4_eo_0900_as_cs"
        {297, "utf8mb4", false}, // "utf8mb4_hu_0900_as_cs"
        {298, "utf8mb4", false}, // "utf8mb4_hr_0900_as_cs"
        {299, "utf8mb4", false}, // "utf8mb4_si_0900_as_cs"
        {300, "utf8mb4", false}, // "utf8mb4_vi_0900_as_cs"
        {303, "utf8mb4", false}, // "utf8mb4_ja_0900_as_cs_ks"
        {304, "utf8mb4", false}, // "utf8mb4_la_0900_as_cs"
        {305, "utf8mb4", false}, // "utf8mb4_0900_as_ci"
        {306, "utf8mb4", false}, // "utf8mb4_ru_0900_ai_ci"
        {307, "utf8mb4", false}, // "utf8mb4_ru_0900_as_cs"
        {308, "utf8mb4", false}, // "utf8mb4_zh_0900_as_cs"
        {309, "utf8mb4", false} // "utf8mb4_0900_bin"
    };

    MySQLCharset charset;

    for (auto & item : result)
    {
        EXPECT_TRUE(charset.needConvert(item.id) == item.need_convert);
        if (charset.needConvert(item.id))
        {
            EXPECT_TRUE(charset.getCharsetFromId(item.id) == item.name);
        }
    }
}

}
