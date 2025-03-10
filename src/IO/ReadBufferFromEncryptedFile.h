#pragma once

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
        const String & key_,
        const FileEncryption::Header & header_,
        size_t offset_ = 0);

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    std::string getFileName() const override { return file_name; }

    void setReadUntilPosition(size_t position) override { in->setReadUntilPosition(position + FileEncryption::Header::kSize); }

    void setReadUntilEnd() override { in->setReadUntilEnd(); }

    std::optional<size_t> tryGetFileSize() override { return in->tryGetFileSize(); }

    void prefetch(Priority priority) override;
private:
    bool nextImpl() override;

    const String file_name;
    std::unique_ptr<ReadBufferFromFileBase> in;

    off_t offset = 0;
    bool need_seek = false;

    Memory<> encrypted_buffer;
    FileEncryption::Encryptor encryptor;

    LoggerPtr log;
};

}

#endif
