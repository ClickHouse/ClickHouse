#include <gtest/gtest.h>
#include <IO/ZstdDeflatingWriteBuffer.h>
#include <IO/ZstdDeflatingAppendableWriteBuffer.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>

using namespace DB;

class WriteBufferFromVectorPretendingToBeFile : public WriteBuffer
{
public:
    std::vector<char> & v;

    explicit WriteBufferFromVectorPretendingToBeFile(std::vector<char> & v_)
        : WriteBuffer(nullptr, 0), v(v_)
    {
        nextImpl();
    }

    void nextImpl() override
    {
        ASSERT_EQ(pos, working_buffer.end());
        v.push_back(0);
        set(v.data() + v.size() - 1, 1);
    }

    void finalizeImpl() override
    {
        v.resize(pos - v.data());
    }

    std::vector<char> get()
    {
        return std::vector<char>(v.data(), pos);
    }
};

class ReadBufferFromStringPretendingToBeFile : public ReadBufferFromFileBase
{
public:
    std::vector<char> v;

    explicit ReadBufferFromStringPretendingToBeFile(std::vector<char> v_)
        : ReadBufferFromFileBase(0, nullptr, 1, v_.size()), v(std::move(v_))
    {
        BufferBase::set(v.data(), v.size(), 0);
    }

    std::string getFileName() const override { return ""; }
    off_t getPosition() override { return pos - working_buffer.begin(); }
    size_t getFileOffsetOfBufferEnd() const override { return working_buffer.size(); }
    off_t seek(off_t off, int whence) override {
        EXPECT_EQ(whence, SEEK_SET);
        pos = working_buffer.begin() + off;
        return off;
    }
    size_t getFileSize() override { return working_buffer.size(); }
};

static void decompressAndCheck(const std::vector<char> & data, const std::string expected)
{
    std::cout << "decompressing " << expected << std::endl;
    ZstdInflatingReadBuffer zstd(std::make_unique<ReadBufferFromStringPretendingToBeFile>(data));
    std::string found(expected.size() + 1, '-');
    size_t n = zstd.read(found.data(), expected.size() + 1);
    EXPECT_EQ(n, expected.size());
    found.resize(n);
    EXPECT_EQ(expected, found);
}

TEST(Zstd, Simple)
{
    std::vector<char> data;
    {
        ZstdDeflatingWriteBuffer zstd(std::make_unique<WriteBufferFromVectorPretendingToBeFile>(data), 3);
        zstd.write("henlo", 5);
        zstd.finalize();
    }
    decompressAndCheck(data, "henlo");
}

TEST(Zstd, Append)
{
    std::vector<char> data;

    auto append = [&](std::string s, bool finalize)
    {
        std::cout << "appending " << s << std::endl;
        auto temp = data;
        auto v = std::make_unique<WriteBufferFromVectorPretendingToBeFile>(temp);
        auto buf = v.get();
        ZstdDeflatingAppendableWriteBuffer zstd(std::move(v), 3, true,
            [&]() -> std::unique_ptr<ReadBufferFromFileBase> {
                return std::make_unique<ReadBufferFromStringPretendingToBeFile>(buf->get());
            });
        zstd.write(s.data(), s.size());
        if (finalize)
        {
            zstd.finalize();
            data = temp;
        }
        else
        {
            zstd.next(); // flush
            data = buf->get();
            zstd.finalize();
        }
    };

    append("hell", true);
    decompressAndCheck(data, "hell");
    append("owo", false);
    decompressAndCheck(data, "hellowo");
    append("", true);
    decompressAndCheck(data, "hellowo");
    append("rld", false);
    decompressAndCheck(data, "helloworld");
}
