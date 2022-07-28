#include <Common/config.h>

#if USE_SSL
#include <gtest/gtest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/getRandomASCIIString.h>
#include <filesystem>


using namespace DB;
using namespace DB::FileEncryption;


struct InitVectorTestParam
{
    const String init;
    const String after_inc;
    const UInt64 adder;
    const String after_add;
};

class FileEncryptionInitVectorTest : public ::testing::TestWithParam<InitVectorTestParam> {};

TEST_P(FileEncryptionInitVectorTest, InitVector)
{
    const auto & param = GetParam();

    auto iv = InitVector::fromString(param.init);
    ASSERT_EQ(param.init, iv.toString());

    ++iv;
    ASSERT_EQ(param.after_inc, iv.toString());

    iv += param.adder;
    ASSERT_EQ(param.after_add, iv.toString());
}

INSTANTIATE_TEST_SUITE_P(All,
                         FileEncryptionInitVectorTest,
                         ::testing::ValuesIn(std::initializer_list<InitVectorTestParam>
    {
        {   // #0. Basic init vector test. Get zero-string, add 1, add 0.
            String(16, 0),
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
            0,
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
        },
        {
            // #1. Init vector test. Get zero-string, add 1, add 85, add 1024.
            String(16, 0),
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
            85,
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x56", 16),
        },
        {
            // #2. Init vector test #2. Get zero-string, add 1, add 1024.
            String(16, 0),
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01", 16),
            1024,
            String("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x01", 16)
        },
        {
            // #3. Long init vector test.
            String("\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x9c\xa6\x8c\x19\xf4\x77\x80\xe1", 16),
            String("\xa8\x65\x9c\x73\xf8\x5d\x83\xb4\x9c\xa6\x8c\x19\xf4\x77\x80\xe2", 16),
            9349249176525638641ULL,
            String("\xa8\x65\x9c\x73\xf8\x5d\x83\xb5\x1e\x65\xc0\xb1\x67\xe4\x0c\xd3", 16)
        },
    })
);


struct CipherTestParam
{
    const Algorithm algorithm;
    const String key;
    const InitVector iv;
    const size_t offset;
    const String plaintext;
    const String ciphertext;
};

class FileEncryptionCipherTest : public ::testing::TestWithParam<CipherTestParam> {};

TEST_P(FileEncryptionCipherTest, Encryption)
{
    const auto & param = GetParam();

    Encryptor encryptor{param.algorithm, param.key, param.iv};
    std::string_view input = param.plaintext;
    std::string_view expected = param.ciphertext;
    size_t base_offset = param.offset;

    encryptor.setOffset(base_offset);
    for (size_t i = 0; i < expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.encrypt(&input[i], 1, buf);
        ASSERT_EQ(expected.substr(i, 1), buf.str());
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.setOffset(base_offset + i);
        encryptor.encrypt(&input[i], 1, buf);
        ASSERT_EQ(expected.substr(i, 1), buf.str());
    }

    for (size_t i = 0; i <= expected.size(); ++i)
    {
        WriteBufferFromOwnString buf;
        encryptor.setOffset(base_offset);
        encryptor.encrypt(input.data(), i, buf);
        ASSERT_EQ(expected.substr(0, i), buf.str());
    }
}

TEST_P(FileEncryptionCipherTest, Decryption)
{
    const auto & param = GetParam();

    Encryptor encryptor{param.algorithm, param.key, param.iv};
    std::string_view input = param.ciphertext;
    std::string_view expected = param.plaintext;
    size_t base_offset = param.offset;

    encryptor.setOffset(base_offset);
    for (size_t i = 0; i < expected.size(); ++i)
    {
        char c;
        encryptor.decrypt(&input[i], 1, &c);
        ASSERT_EQ(expected[i], c);
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        char c;
        encryptor.setOffset(base_offset + i);
        encryptor.decrypt(&input[i], 1, &c);
        ASSERT_EQ(expected[i], c);
    }

    String buf(expected.size(), 0);
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        encryptor.setOffset(base_offset);
        encryptor.decrypt(input.data(), i, buf.data());
        ASSERT_EQ(expected.substr(0, i), buf.substr(0, i));
    }
}

INSTANTIATE_TEST_SUITE_P(All,
                         FileEncryptionCipherTest,
                         ::testing::ValuesIn(std::initializer_list<CipherTestParam>
    {
        {
            // #0
            Algorithm::AES_128_CTR,
            "1234567812345678",
            InitVector{},
            0,
            "abcd1234efgh5678ijkl",
            "\xfb\x8a\x9e\x66\x82\x72\x1b\xbe\x6b\x1d\xd8\x98\xc5\x8c\x63\xee\xcd\x36\x4a\x50"
        },
        {
            // #1
            Algorithm::AES_128_CTR,
            "1234567812345678",
            InitVector{},
            25,
            "abcd1234efgh5678ijkl",
            "\x6c\x67\xe4\xf5\x8f\x86\xb0\x19\xe5\xcd\x53\x59\xe0\xc6\x01\x5e\xc1\xfd\x60\x9d"
        },
        {
            // #2
            Algorithm::AES_128_CTR,
            String{"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f", 16},
            InitVector{},
            0,
            "abcd1234efgh5678ijkl",
            "\xa7\xc3\x58\x53\xb6\xbd\x68\xb6\x0a\x29\xe6\x0a\x94\xfe\xef\x41\x1a\x2c\x78\xf9"
        },
        {
            // #3
            Algorithm::AES_128_CTR,
            "1234567812345678",
            InitVector::fromString(String{"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f", 16}),
            0,
            "abcd1234efgh5678ijkl",
            "\xcf\xab\x7c\xad\xa9\xdc\x67\x60\x90\x85\x7b\xb8\x72\xa9\x6f\x9c\x29\xb2\x4f\xf6"
        },
        {
            // #4
            Algorithm::AES_192_CTR,
            "123456781234567812345678",
            InitVector{},
            0,
            "abcd1234efgh5678ijkl",
            "\xcc\x25\x2b\xad\xe8\xa2\xdc\x64\x3e\xf9\x60\xe0\x6e\xde\x70\xb6\x63\xa8\xfa\x02"
         },
         {
             // #5
             Algorithm::AES_256_CTR,
             "12345678123456781234567812345678",
             InitVector{},
             0,
             "abcd1234efgh5678ijkl",
             "\xc7\x41\xa6\x63\x04\x60\x1b\x1a\xcb\x84\x19\xce\x3a\x36\xa3\xbd\x21\x71\x93\xfb"
          },
    })
);

TEST(FileEncryptionPositionUpdateTest, Decryption)
{
    String tmp_path = std::filesystem::current_path() / "test_offset_update";
    if (std::filesystem::exists(tmp_path))
        std::filesystem::remove(tmp_path);

    String key = "1234567812345678";
    FileEncryption::Header header;
    header.algorithm = Algorithm::AES_128_CTR;
    header.key_id = 1;
    header.key_hash = calculateKeyHash(key);
    header.init_vector = InitVector::random();

    auto lwb = std::make_unique<WriteBufferFromFile>(tmp_path);
    WriteBufferFromEncryptedFile wb(10, std::move(lwb), key, header);
    auto data = getRandomASCIIString(20);
    wb.write(data.data(), data.size());
    wb.finalize();

    auto lrb = std::make_unique<ReadBufferFromFile>(tmp_path);
    ReadBufferFromEncryptedFile rb(10, std::move(lrb), key, header);
    rb.ignore(5);
    rb.ignore(5);
    rb.ignore(5);
    ASSERT_EQ(rb.getPosition(), 15);

    String res;
    readStringUntilEOF(res, rb);
    ASSERT_EQ(res, data.substr(15));
    res.clear();

    rb.seek(0, SEEK_SET);
    ASSERT_EQ(rb.getPosition(), 0);
    res.resize(5);
    rb.read(res.data(), res.size());
    ASSERT_EQ(res, data.substr(0, 5));
    res.clear();

    rb.seek(1, SEEK_CUR);
    ASSERT_EQ(rb.getPosition(), 6);
    readStringUntilEOF(res, rb);
    ASSERT_EQ(res, data.substr(6));
}

#endif
