#include "config.h"

#if USE_SSL
#include <gtest/gtest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadHelpers.h>
#include <Common/getRandomASCIIString.h>
#include <filesystem>
#include <thread>
#include <utility>
#include <vector>


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
        encryptor.encrypt(input.data(), i, buf); /// NOLINT(bugprone-suspicious-stringview-data-usage)
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
        char c = {};
        encryptor.decrypt(&input[i], 1, &c);
        ASSERT_EQ(expected[i], c);
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        char c = {};
        encryptor.setOffset(base_offset + i);
        encryptor.decrypt(&input[i], 1, &c);
        ASSERT_EQ(expected[i], c);
    }

    String buf(expected.size(), 0);
    for (size_t i = 0; i <= expected.size(); ++i)
    {
        encryptor.setOffset(base_offset);
        encryptor.decrypt(input.data(), i, buf.data()); /// NOLINT(bugprone-suspicious-stringview-data-usage)
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
    header.key_fingerprint = calculateKeyFingerprint(key);
    header.init_vector = InitVector::random();

    auto lwb = std::make_unique<WriteBufferFromFile>(tmp_path);
    WriteBufferFromEncryptedFile wb(10, std::move(lwb), key, header, /*old_file_size=*/0, /*use_adaptive_buffer_size_=*/ false, /*adaptive_buffer_initial_size=*/ 0);
    auto data = getRandomASCIIString(20);
    wb.write(data.data(), data.size());
    wb.finalize();

    auto lrb = std::make_unique<ReadBufferFromFile>(tmp_path);
    ReadBufferFromEncryptedFile rb(tmp_path, 10, std::move(lrb), key, header);
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
    ASSERT_EQ(rb.read(res.data(), res.size()), 5);
    ASSERT_EQ(res, data.substr(0, 5));
    res.clear();

    rb.seek(1, SEEK_CUR);
    ASSERT_EQ(rb.getPosition(), 6);
    readStringUntilEOF(res, rb);
    ASSERT_EQ(res, data.substr(6));
}

namespace
{
    /// An encrypted file plus the plaintext it was built from, for the `readBigAt` tests below.
    struct EncryptedFileFixture
    {
        String path;
        String key = "1234567812345678";
        FileEncryption::Header header;
        String data;

        EncryptedFileFixture(const String & name, size_t size)
            : path(std::filesystem::current_path() / name)
            , data(getRandomASCIIString(size))
        {
            if (std::filesystem::exists(path))
                std::filesystem::remove(path);

            header.algorithm = Algorithm::AES_128_CTR;
            header.key_fingerprint = calculateKeyFingerprint(key);
            header.init_vector = InitVector::random();

            auto out = std::make_unique<WriteBufferFromFile>(path);
            WriteBufferFromEncryptedFile wb(
                DBMS_DEFAULT_BUFFER_SIZE, std::move(out), key, header, /*old_file_size=*/0,
                /*use_adaptive_buffer_size_=*/false, /*adaptive_buffer_initial_size=*/0);
            wb.write(data.data(), data.size());
            wb.finalize();
        }

        ~EncryptedFileFixture()
        {
            std::filesystem::remove(path);
        }

        /// `ReadBufferFromFilePRead` is the inner buffer because plain `ReadBufferFromFile`
        /// uses `read` and so reports no `readBigAt` support.
        std::unique_ptr<ReadBufferFromEncryptedFile> open() const
        {
            return std::make_unique<ReadBufferFromEncryptedFile>(
                path, DBMS_DEFAULT_BUFFER_SIZE, std::make_unique<ReadBufferFromFilePRead>(path), key, header);
        }
    };

    /// Splits a positional read into small chunks, reporting progress and honoring cancellation
    /// after each one. Plain `pread` does neither, so it cannot exercise those paths.
    /// `chunk_size` is deliberately not a multiple of the 16-byte cipher block, so incremental
    /// decryption is exercised at unaligned offsets.
    class ChunkedProgressReadBuffer : public ReadBufferFromFileDecorator
    {
    public:
        using ReadBufferFromFileDecorator::ReadBufferFromFileDecorator;

        static constexpr size_t chunk_size = 25;
        mutable bool cancelled = false;

        size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) const override
        {
            size_t copied = 0;
            while (copied < n)
            {
                size_t chunk = std::min(chunk_size, n - copied);
                size_t got = impl->readBigAt(to + copied, chunk, offset + copied, {});
                copied += got;
                if (got < chunk)
                    break;
                if (progress_callback && progress_callback(copied))
                {
                    cancelled = true;
                    break;
                }
            }
            return copied;
        }
    };
}

/// `readBigAt` must return the same plaintext as a sequential read for any range, including
/// ranges that do not start or end on a 16-byte cipher block boundary, and must deliver the
/// whole requested range rather than stopping at the first short read.
TEST(FileEncryptionReadBigAtTest, ArbitraryRanges)
{
    constexpr size_t file_size = 1000;
    EncryptedFileFixture file{"test_read_big_at_ranges", file_size};

    auto rb = file.open();
    ASSERT_TRUE(rb->supportsReadAt());

    /// Block-aligned, unaligned, block-crossing, whole-file, and past-the-end ranges.
    const std::vector<std::pair<size_t, size_t>> ranges = {
        {0, 16}, {0, 1}, {1, 5}, {15, 2}, {16, 32}, {7, 100}, {63, 130},
        {0, file_size}, {file_size - 3, 3}, {file_size - 3, 10}, {file_size, 10},
    };

    for (auto [offset, count] : ranges)
    {
        String got(count, 0);
        size_t bytes_read = rb->readBigAt(got.data(), count, offset, {});

        size_t expected_size = offset >= file_size ? 0 : std::min(count, file_size - offset);
        ASSERT_EQ(bytes_read, expected_size) << "offset=" << offset << " count=" << count;
        ASSERT_EQ(got.substr(0, bytes_read), file.data.substr(offset, bytes_read))
            << "offset=" << offset << " count=" << count;
    }
}

/// The contract allows concurrent `readBigAt` calls. Decryption state is per-call, so a shared
/// mutable encryptor would make these results interfere.
TEST(FileEncryptionReadBigAtTest, ConcurrentReads)
{
    constexpr size_t file_size = 4096;
    EncryptedFileFixture file{"test_read_big_at_concurrent", file_size};

    auto rb = file.open();
    ASSERT_TRUE(rb->supportsReadAt());

    constexpr size_t num_threads = 8;
    constexpr size_t chunk = file_size / num_threads;

    std::vector<String> results(num_threads);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]
        {
            /// Deliberately unaligned to the cipher block size where possible.
            size_t offset = i * chunk + (i % 2);
            size_t count = chunk - (i % 2);
            String got(count, 0);
            size_t bytes_read = rb->readBigAt(got.data(), count, offset, {});
            got.resize(bytes_read);
            results[i] = std::move(got);
        });
    }
    for (auto & thread : threads)
        thread.join();

    for (size_t i = 0; i < num_threads; ++i)
    {
        size_t offset = i * chunk + (i % 2);
        size_t count = chunk - (i % 2);
        ASSERT_EQ(results[i], file.data.substr(offset, count)) << "thread " << i;
    }
}

/// `progress_callback` must observe plaintext for every reported prefix, and returning true must
/// stop the inner read and produce a short result. Reporting progress repeatedly also covers the
/// incremental decryption: a byte decrypted twice would be turned back into ciphertext.
TEST(FileEncryptionReadBigAtTest, ProgressCallbackAndCancellation)
{
    constexpr size_t file_size = 1000;
    EncryptedFileFixture file{"test_read_big_at_cancel", file_size};

    auto inner = std::make_unique<ChunkedProgressReadBuffer>(std::make_unique<ReadBufferFromFilePRead>(file.path), file.path);
    auto * inner_ptr = inner.get();
    ReadBufferFromEncryptedFile rb(file.path, DBMS_DEFAULT_BUFFER_SIZE, std::move(inner), file.key, file.header);
    ASSERT_TRUE(rb.supportsReadAt());

    /// Cancel after the 4th chunk, i.e. at 100 bytes out of the 256 requested.
    constexpr size_t cancel_at = 4 * ChunkedProgressReadBuffer::chunk_size;
    String got(256, 0);
    size_t bytes_read = rb.readBigAt(got.data(), got.size(), 0, [&](size_t m) -> bool
    {
        EXPECT_EQ(got.substr(0, m), file.data.substr(0, m)) << "m=" << m;
        return m >= cancel_at;
    });

    ASSERT_TRUE(inner_ptr->cancelled);
    ASSERT_EQ(bytes_read, cancel_at);
    ASSERT_EQ(got.substr(0, bytes_read), file.data.substr(0, bytes_read));
}

#endif
