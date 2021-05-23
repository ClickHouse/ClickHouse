#if USE_SSL
#include <gtest/gtest.h>
#include <IO/WriteBufferFromString.h>
#include <Functions/FileEncryption.h>


using namespace FileEncryption;
using namespace DB;

struct InitVectorTestParam
{
    const std::string_view comment;
    const String init;
    UInt128 adder;
    UInt128 setter;
    const String after_inc;
    const String after_add;
    const String after_set;
};


class InitVectorTest : public ::testing::TestWithParam<InitVectorTestParam> {};


String string_ends_with(size_t size, String str)
{
    String res(size, 0);
    res.replace(size - str.size(), str.size(), str);
    return res;
}


static std::ostream & operator << (std::ostream & ostr, const InitVectorTestParam & param)
{
    return ostr << param.comment;
}


TEST_P(InitVectorTest, InitVector)
{
    const auto & param = GetParam();

    auto iv = InitVector(param.init);
    ASSERT_EQ(param.init, iv.GetRef());

    iv.Inc();
    ASSERT_EQ(param.after_inc, iv.GetRef());

    iv.Inc(param.adder);
    ASSERT_EQ(param.after_add, iv.GetRef());

    iv.Set(param.setter);
    ASSERT_EQ(param.after_set, iv.GetRef());

    iv.Set(0);
    ASSERT_EQ(param.init, iv.GetRef());
}


INSTANTIATE_TEST_SUITE_P(InitVectorInputs,
                         InitVectorTest,
                         ::testing::ValuesIn(std::initializer_list<InitVectorTestParam>{
        {
            "Basic init vector test. Get zero-string, add 0, set 0",
            String(16, 0),
            0,
            0,
            string_ends_with(16, "\x1"),
            string_ends_with(16, "\x1"),
            String(16, 0),
        },
        {
            "Init vector test. Get zero-string, add 85, set 1024",
            String(16, 0),
            85,
            1024,
            string_ends_with(16, "\x1"),
            string_ends_with(16, "\x56"),
            string_ends_with(16, "\x4\0"),
        },
        {
            "Long init vector test",
            "\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x5c\xa6\x8c\x19\xf4\x77\x80\xe1",
            6081234986671206102341,
            1698923461902341,
            "\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x5c\xa6\x8c\x19\xf4\x77\x80\xe2",
            "\xa8\x65\x9c\x73\xf8\x5d\x84\xfe\x06\xbd\x44\xb7\x0b\x0c\x76\x27",
            "\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x5c\xac\x95\x43\x65\xea\x00\xe6",
        },
    })
);


TEST(FileEncryption, Encryption)
{
    String iv(16, 0);
    EncryptionKey key("1234567812345678");
    String input = "abcd1234efgh5678ijkl";
    String expected = "\xfb\x8a\x9e\x66\x82\x72\x1b\xbe\x6b\x1d\xd8\x98\xc5\x8c\x63\xee\xcd\x36\x4a\x50";

    String result(expected.size(), 0);
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        auto buf = WriteBufferFromString(result);
        auto encryptor = Encryptor(iv, key, 0);
        encryptor.Encrypt(input.data(), buf, i);
        ASSERT_EQ(expected.substr(0, i), result.substr(0, i));
    }

    size_t offset = 25;
    String offset_expected = "\x6c\x67\xe4\xf5\x8f\x86\xb0\x19\xe5\xcd\x53\x59\xe0\xc6\x01\x5e\xc1\xfd\x60\x9d";
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        auto buf = WriteBufferFromString(result);
        auto encryptor = Encryptor(iv, key, offset);
        encryptor.Encrypt(input.data(), buf, i);
        ASSERT_EQ(offset_expected.substr(0, i), result.substr(0, i));
    }
}


TEST(FileEncryption, Decryption)
{
    String iv(16, 0);
    EncryptionKey key("1234567812345678");
    String expected = "abcd1234efgh5678ijkl";
    String input = "\xfb\x8a\x9e\x66\x82\x72\x1b\xbe\x6b\x1d\xd8\x98\xc5\x8c\x63\xee\xcd\x36\x4a\x50";
    auto decryptor = Decryptor(iv, key);
    String result(expected.size(), 0);

    for (size_t i = 0; i <= expected.size(); ++i)
    {
        decryptor.Decrypt(input.data(), result.data(), i, 0);
        ASSERT_EQ(expected.substr(0, i), result.substr(0, i));
    }

    size_t offset = 25;
    String offset_input = "\x6c\x67\xe4\xf5\x8f\x86\xb0\x19\xe5\xcd\x53\x59\xe0\xc6\x01\x5e\xc1\xfd\x60\x9d";
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        decryptor.Decrypt(offset_input.data(), result.data(), i, offset);
        ASSERT_EQ(expected.substr(0, i), result.substr(0, i));
    }
}

#endif
