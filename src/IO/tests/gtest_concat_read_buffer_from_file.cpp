#include <gtest/gtest.h>

#include <IO/ConcatReadBufferFromFile.h>
#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

using namespace DB;

namespace
{

/// Three separately allocated parts [0, 100), [100, 150), [150, 250); byte `i` is `char(i)`.
std::vector<std::string> makeParts()
{
    std::vector<std::string> parts;
    size_t offset = 0;
    for (size_t size : {100uz, 50uz, 100uz})
    {
        auto & part = parts.emplace_back(size, '\0');
        for (auto & c : part)
            c = static_cast<char>(offset++);
    }
    return parts;
}

std::unique_ptr<ReadBufferFromMemory> child(const std::string & part)
{
    return std::make_unique<ReadBufferFromMemory>(part.data(), part.size());
}

/// A view of `part` inside a copy with `0xff` padding (never a byte of `makeParts`) on both sides; the view
/// is positioned inside its working buffer, whose bytes before the position are padding.
std::unique_ptr<ReadBufferFromFileView> viewChild(const std::string & part)
{
    constexpr size_t padding = 100;
    const std::string data = std::string(padding, '\xff') + part + std::string(padding, '\xff');
    return std::make_unique<ReadBufferFromFileView>(
        std::make_unique<ReadBufferFromOwnMemoryFile>("archive", data),
        "file",
        static_cast<off_t>(padding),
        static_cast<off_t>(padding + part.size()));
}

void expectByteAt(ConcatReadBufferFromFile & buf, off_t offset)
{
    EXPECT_EQ(buf.getPosition(), offset);
    char c = 0;
    readChar(c, buf);
    EXPECT_EQ(c, static_cast<char>(offset));
    EXPECT_EQ(buf.getPosition(), offset + 1);
}

void checkSeeksAndReads(ConcatReadBufferFromFile & buf)
{
    for (off_t i = 0; i < 250; ++i)
        expectByteAt(buf, i);
    EXPECT_TRUE(buf.eof());

    buf.seek(120, SEEK_SET); /// back from the EOF reached by reading
    expectByteAt(buf, 120);
    buf.seek(30, SEEK_CUR); /// into the next part
    expectByteAt(buf, 151);
    buf.seek(160, SEEK_SET); /// inside the same part
    expectByteAt(buf, 160);
    buf.seek(-100, SEEK_CUR); /// back across a part boundary
    expectByteAt(buf, 61);

    buf.seek(250, SEEK_SET);
    EXPECT_TRUE(buf.eof());
    EXPECT_EQ(buf.getPosition(), 250);
    buf.seek(-1, SEEK_CUR); /// back from the EOF reached by seeking
    expectByteAt(buf, 249);
    EXPECT_TRUE(buf.eof());
}

}

TEST(ConcatReadBufferFromFile, SeekAndRead)
{
    auto parts = makeParts();
    ConcatReadBufferFromFile buf("");
    for (const auto & part : parts)
        buf.appendBuffer(child(part), part.size());
    checkSeeksAndReads(buf);
}

TEST(ConcatReadBufferFromFile, NestedChild)
{
    auto parts = makeParts();
    auto inner = std::make_unique<ConcatReadBufferFromFile>("", child(parts[0]), 100, child(parts[1]), 50);
    ConcatReadBufferFromFile buf("", std::move(inner), 150, child(parts[2]), 100);
    checkSeeksAndReads(buf);
}

TEST(ConcatReadBufferFromFile, ChildrenPositionedInsideTheirBuffers)
{
    auto parts = makeParts();
    ConcatReadBufferFromFile buf("");
    for (const auto & part : parts)
        buf.appendBuffer(viewChild(part), part.size());
    checkSeeksAndReads(buf);
}

TEST(ConcatReadBufferFromFile, AppendAfterEOF)
{
    auto parts = makeParts();
    ConcatReadBufferFromFile buf("");
    buf.appendBuffer(viewChild(parts[0]), parts[0].size());
    buf.ignore(parts[0].size());
    EXPECT_TRUE(buf.eof());
    buf.appendBuffer(viewChild(parts[1]), parts[1].size());
    buf.appendBuffer(viewChild(parts[2]), parts[2].size());
    buf.seek(0, SEEK_SET);
    checkSeeksAndReads(buf);
}
