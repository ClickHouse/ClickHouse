#pragma once

#include "config.h"

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>
#include <base/types.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>

#include "SSHWrapper.h"


namespace DB
{
    class ReadBufferFromSFTP : public ReadBufferFromFileBase
    {
        struct ReadBufferFromSFTPImpl;

    public:
        ReadBufferFromSFTP(
                const std::shared_ptr<SFTPWrapper> &client_,
                const String &file_path_,
                const ReadSettings &read_settings_,
                size_t read_until_position_ = 0,
                bool use_external_buffer_ = false);

        ~ReadBufferFromSFTP() override;

        bool nextImpl() override;

        off_t seek(off_t offset_, int whence) override;

        off_t getPosition() override;

        size_t getFileSize() override;

        size_t getFileOffsetOfBufferEnd() const override;

        String getFileName() const override;

    private:
        std::unique_ptr<ReadBufferFromSFTPImpl> impl;
        bool use_external_buffer;
    };
}
