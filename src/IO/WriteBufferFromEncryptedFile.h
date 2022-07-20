#pragma once

#include <Common/config.h>
#include <Common/assert_cast.h>

#if USE_SSL
#include <IO/WriteBufferFromFileBase.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferDecorator.h>


namespace DB
{

/// Encrypts data and writes the encrypted data to the underlying write buffer.
class WriteBufferFromEncryptedFile : public WriteBufferDecorator<WriteBufferFromFileBase>
{
public:
    /// `old_file_size` should be set to non-zero if we're going to append an existing file.
    WriteBufferFromEncryptedFile(
        size_t buffer_size_,
        std::unique_ptr<WriteBufferFromFileBase> out_,
        const String & key_,
        const FileEncryption::Header & header_,
        size_t old_file_size = 0);

    void sync() override;

    std::string getFileName() const override { return assert_cast<WriteBufferFromFileBase *>(out.get())->getFileName(); }

private:
    void nextImpl() override;

    void finalizeBefore() override;

    FileEncryption::Header header;
    bool flush_header = false;

    FileEncryption::Encryptor encryptor;
};

}

#endif
