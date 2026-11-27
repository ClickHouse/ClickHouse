#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <vector>


namespace DB
{

/// Reads from the concatenation of multiple SeekableReadBuffer's
class ConcatReadBufferFromFile final : public ReadBufferFromFileBase
{
public:
    explicit ConcatReadBufferFromFile(std::string file_name_);

    ConcatReadBufferFromFile(std::string file_name_,
        std::unique_ptr<SeekableReadBuffer> buf1,
        size_t size1,
        std::unique_ptr<SeekableReadBuffer> buf2,
        size_t size2);

    ConcatReadBufferFromFile(std::string file_name_,
        SeekableReadBuffer & buf1,
        size_t size1,
        SeekableReadBuffer & buf2,
        size_t size2);

    std::string getFileName() const override { return file_name; }

    void appendBuffer(std::unique_ptr<SeekableReadBuffer> buffer, size_t size);
    void appendBuffer(SeekableReadBuffer & buffer, size_t size);

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    std::optional<size_t> tryGetFileSize() override { return total_size; }

private:
    bool nextImpl() override;
    void appendBuffer(SeekableReadBuffer * buffer, bool own, size_t size);

    struct BufferInfo
    {
        BufferInfo() = default;
        BufferInfo(BufferInfo && src) noexcept;
        ~BufferInfo();
        SeekableReadBuffer * in = nullptr;
        bool own_in = false;
        size_t size = 0;
    };

    const std::string file_name;
    std::vector<BufferInfo> buffers;
    size_t total_size = 0;
    size_t current = 0;
    size_t current_start_pos = 0; /// Position of the current buffer's begin.
};

}
