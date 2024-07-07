#pragma once

#include "config.h"

#if USE_CEPH

#include <IO/Ceph/RadosIO.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>
#include <rados/librados.hpp>
#include <base/types.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{
/// Accepts a pool and an object id, opens and read the object
class ReadBufferFromCeph : public ReadBufferFromFileBase
{
struct Impl;
public:
    ReadBufferFromCeph(
        std::shared_ptr<librados::Rados> rados_,
        const String & pool,
        const String & nspace,
        const String & object_id_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    ReadBufferFromCeph(
        std::shared_ptr<Ceph::RadosIO> io_,
        const String & object_id_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    ~ReadBufferFromCeph() override;

    size_t getFileSize() override;
    String getFileName() const override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;
    bool supportsRightBoundedReads() const override { return true; }

    off_t seek(off_t off, int whence) override;

    bool nextImpl() override;

    off_t getPosition() override;
    size_t getFileOffsetOfBufferEnd() const override;

    bool supportsReadAt() override { return true; }
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

private:

    std::unique_ptr<Impl> impl;
    bool use_external_buffer;
};

}

#endif
