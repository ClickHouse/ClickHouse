#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_SSL
#include <IO/ReadBufferFromFileBase.h>
#include <IO/FileEncryptionCommon.h>


namespace DB
{

class ReadBufferFromEncryptedFile : public ReadBufferFromFileBase
{
public:
    ReadBufferFromEncryptedFile(
        size_t buf_size_,
        std::unique_ptr<ReadBufferFromFileBase> in_,
        const String & init_vector_,
        const FileEncryption::EncryptionKey & key_,
        const size_t iv_offset_);

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override { return start_pos + offset(); }

    std::string getFileName() const override { return in->getFileName(); }

private:
    bool nextImpl() override;

    void initialize();

    std::unique_ptr<ReadBufferFromFileBase> in;
    size_t buf_size;

    FileEncryption::Decryptor decryptor;
    bool initialized = false;

    // current working_buffer.begin() offset from decrypted file
    size_t start_pos = 0;
    size_t iv_offset = 0;
};

}


#endif
