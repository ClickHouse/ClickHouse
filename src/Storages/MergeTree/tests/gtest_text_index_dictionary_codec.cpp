#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// The two plain codecs differ, and so do the two encrypting ones, so an assertion on pointer
/// identity cannot pass by the two arguments happening to be the same object.
CompressionCodecPtr plainDefault() { return CompressionCodecFactory::instance().get("LZ4"); }
CompressionCodecPtr plainDictionary() { return CompressionCodecFactory::instance().get("ZSTD"); }
CompressionCodecPtr encryptingDefault() { return CompressionCodecFactory::instance().get("LZ4, AES_128_GCM_SIV"); }
CompressionCodecPtr encryptingDictionary() { return CompressionCodecFactory::instance().get("ZSTD, AES_128_GCM_SIV"); }

}

/// The four quadrants of (does the part default encrypt?) x (does the dictionary codec encrypt?).

TEST(TextIndexDictionaryCodec, TemporarySegmentKeepsDefaultWhenNeitherEncrypts)
{
    auto default_codec = plainDefault();
    auto dictionary_codec = plainDictionary();
    ASSERT_FALSE(default_codec->isEncryption());
    ASSERT_FALSE(dictionary_codec->isEncryption());

    EXPECT_EQ(getTextIndexTemporarySegmentDictionaryCodec(dictionary_codec, default_codec), default_codec);
}

TEST(TextIndexDictionaryCodec, TemporarySegmentTakesEncryptingDictionaryOverPlainDefault)
{
    auto default_codec = plainDefault();
    auto dictionary_codec = encryptingDictionary();
    ASSERT_FALSE(default_codec->isEncryption());
    ASSERT_TRUE(dictionary_codec->isEncryption());

    /// The security-relevant case: the temporary copy of the dictionary must not be written in
    /// plaintext just because the part default is plain.
    EXPECT_EQ(getTextIndexTemporarySegmentDictionaryCodec(dictionary_codec, default_codec), dictionary_codec);
}

TEST(TextIndexDictionaryCodec, TemporarySegmentKeepsEncryptingDefaultOverPlainDictionary)
{
    auto default_codec = encryptingDefault();
    auto dictionary_codec = plainDictionary();
    ASSERT_TRUE(default_codec->isEncryption());
    ASSERT_FALSE(dictionary_codec->isEncryption());

    EXPECT_EQ(getTextIndexTemporarySegmentDictionaryCodec(dictionary_codec, default_codec), default_codec);
}

TEST(TextIndexDictionaryCodec, TemporarySegmentKeepsDefaultWhenBothEncrypt)
{
    auto default_codec = encryptingDefault();
    auto dictionary_codec = encryptingDictionary();
    ASSERT_TRUE(default_codec->isEncryption());
    ASSERT_TRUE(dictionary_codec->isEncryption());

    EXPECT_EQ(getTextIndexTemporarySegmentDictionaryCodec(dictionary_codec, default_codec), default_codec);
}
