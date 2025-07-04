#pragma once

#include "Common/JemallocNodumpSTLAllocator.h"
#include "config.h"

#if USE_SSL
#include <IO/ReadBufferFromFileBase.h>
#include <IO/FileEncryptionCommon.h>


namespace DB
{

/// Reads data from the underlying read buffer and decrypts it.
class ReadBufferFromEncryptedFile : public ReadBufferFromFileBase
{
public:
    ReadBufferFromEncryptedFile(
        const String & file_name_,
        size_t buffer_size_,
        std::unique_ptr<ReadBufferFromFileBase> in_,
        const NoDumpString & key_,
        const FileEncryption::Header & header_,
        size_t offset_ = 0);

    std::string getFileName() const override { return file_name; }
    std::optional<size_t> tryGetFileSize() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;
    bool supportsRightBoundedReads() const override { return true; }

    void prefetch(Priority priority) override;

private:
    bool nextImpl() override;

    /// Sets the current position and the position until we read in the internal read buffer
    /// if `need_seek` or `need_set_read_until_position` is true.
    void performSeekAndSetReadUntilPosition();

    const String file_name;
    std::unique_ptr<ReadBufferFromFileBase> in;

    off_t offset = 0;
    bool need_seek = false;

    std::optional<off_t> read_until_position;
    bool need_set_read_until_position = false;

    Memory<> encrypted_buffer;
    FileEncryption::Encryptor encryptor;

    LoggerPtr log;
};

}

#endif
