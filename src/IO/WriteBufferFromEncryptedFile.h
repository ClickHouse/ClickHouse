#pragma once

#include <Common/config.h>

#if USE_SSL
#include <IO/WriteBufferFromFileBase.h>
#include <IO/FileEncryptionCommon.h>


namespace DB
{

/// Encrypts data and writes the encrypted data to the underlying write buffer.
class WriteBufferFromEncryptedFile : public WriteBufferFromFileBase
{
public:
    /// `old_file_size` should be set to non-zero if we're going to append an existing file.
    WriteBufferFromEncryptedFile(
        size_t buffer_size_,
        std::unique_ptr<WriteBufferFromFileBase> out_,
        const String & key_,
        const FileEncryption::Header & header_,
        size_t old_file_size = 0);
    ~WriteBufferFromEncryptedFile() override;

    void sync() override;
    void finalize() override { finish(); }

    std::string getFileName() const override { return out->getFileName(); }

private:
    void nextImpl() override;

    void finish();
    void finishImpl();

    bool finished = false;
    std::unique_ptr<WriteBufferFromFileBase> out;

    FileEncryption::Header header;
    bool flush_header = false;

    FileEncryption::Encryptor encryptor;
};

}

#endif
