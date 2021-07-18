#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_SSL
#include <gtest/gtest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/FileEncryptionCommon.h>


using namespace DB;
using namespace DB::FileEncryption;

struct InitVectorTestParam
{
    const std::string_view comment;
    const String init;
    const String after_inc;
    UInt64 adder;
    const String after_add;
};


class InitVectorTest : public ::testing::TestWithParam<InitVectorTestParam> {};


static std::ostream & operator << (std::ostream & ostr, const InitVectorTestParam & param)
{
    return ostr << param.comment;
}


TEST_P(InitVectorTest, InitVector)
{
    const auto & param = GetParam();

    auto iv = InitVector::fromString(param.init);
    ASSERT_EQ(param.init, iv.toString());

    ++iv;
    ASSERT_EQ(param.after_inc, iv.toString());

    iv += param.adder;
    ASSERT_EQ(param.after_add, iv.toString());
}


INSTANTIATE_TEST_SUITE_P(InitVectorInputs,
                         InitVectorTest,
                         ::testing::ValuesIn(std::initializer_list<InitVectorTestParam>{
        {
            "Basic init vector test. Get zero-string, add 1, add 0",
            String(16, 0),
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
            0,
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
        },
        {
            "Init vector test. Get zero-string, add 1, add 85, add 1024",
            String(16, 0),
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
            85,
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x56", 16),
        },
        {
            "Init vector test #2. Get zero-string, add 1, add 1024",
            String(16, 0),
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
            1024,
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x01", 16)
        },
        {
            "Long init vector test",
            String("\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x9c\xa6\x8c\x19\xf4\x77\x80\xe1", 16),
            String("\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x9c\xa6\x8c\x19\xf4\x77\x80\xe2", 16),
            9349249176525638641ULL,
            String("\xa8\x65\x9c\x73\xf8\x5d\x83\xb5\x1e\x65\xc0\xb1\x67\xe4\x0c\xd3", 16)
        },
    })
);


TEST(FileEncryption, Encryption)
{
    String key = "1234567812345678";
    InitVector iv;
    Encryptor encryptor{key, iv};

    std::string_view input = "abcd1234efgh5678ijkl";
    std::string_view expected = "\xfb\x8a\x9e\x66\x82\x72\x1b\xbe\x6b\x1d\xd8\x98\xc5\x8c\x63\xee\xcd\x36\x4a\x50";

    for (size_t i = 0; i < expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.encrypt(&input[i], 1, buf);
        ASSERT_EQ(expected.substr(i, 1), buf.str());
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.setOffset(i);
        encryptor.encrypt(&input[i], 1, buf);
        ASSERT_EQ(expected.substr(i, 1), buf.str());
    }

    for (size_t i = 0; i <= expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.setOffset(0);
        encryptor.encrypt(input.data(), i, buf);
        ASSERT_EQ(expected.substr(0, i), buf.str());
    }

    size_t offset = 25;
    std::string_view offset_expected = "\x6c\x67\xe4\xf5\x8f\x86\xb0\x19\xe5\xcd\x53\x59\xe0\xc6\x01\x5e\xc1\xfd\x60\x9d";
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.setOffset(offset);
        encryptor.encrypt(input.data(), i, buf);
        ASSERT_EQ(offset_expected.substr(0, i), buf.str());
    }
}


TEST(FileEncryption, Decryption)
{
    String key("1234567812345678");
    InitVector iv;
    Encryptor encryptor{key, iv};

    std::string_view input = "\xfb\x8a\x9e\x66\x82\x72\x1b\xbe\x6b\x1d\xd8\x98\xc5\x8c\x63\xee\xcd\x36\x4a\x50";
    std::string_view expected = "abcd1234efgh5678ijkl";

    for (size_t i = 0; i < expected.size(); ++i)
    {
        char c;
        encryptor.decrypt(&input[i], 1, &c);
        ASSERT_EQ(expected[i], c);
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        char c;
        encryptor.setOffset(i);
        encryptor.decrypt(&input[i], 1, &c);
        ASSERT_EQ(expected[i], c);
    }

    String buf(expected.size(), 0);
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        encryptor.setOffset(0);
        encryptor.decrypt(input.data(), i, buf.data());
        ASSERT_EQ(expected.substr(0, i), buf.substr(0, i));
    }

    size_t offset = 25;
    String offset_input = "\x6c\x67\xe4\xf5\x8f\x86\xb0\x19\xe5\xcd\x53\x59\xe0\xc6\x01\x5e\xc1\xfd\x60\x9d";
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        encryptor.setOffset(offset);
        encryptor.decrypt(offset_input.data(), i, buf.data());
        ASSERT_EQ(expected.substr(0, i), buf.substr(0, i));
    }
}

#endif
