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
public:
    ReadBufferFromCeph(
        std::shared_ptr<librados::Rados> rados_,
        const String & pool,
        const String & object_id_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        bool restricted_seek_ = false,
        size_t read_until_position_ = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    ReadBufferFromCeph(
        std::unique_ptr<Ceph::RadosIO> impl_,
        const String & object_id_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        bool restricted_seek_ = false,
        size_t read_until_position_ = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    ~ReadBufferFromCeph() override = default;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    bool nextImpl() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    String getFileName() const override { return object_id; }

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    bool supportsRightBoundedReads() const override { return true; }

    size_t getFileSize() override;

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    bool supportsReadAt() override { return true; }

private:

    void initialize();
    size_t readImpl(char * to, size_t len, off_t begin) const;

    std::unique_ptr<Ceph::RadosIO> impl;

    String object_id;

    ReadSettings read_settings;
    std::vector<char> tmp_buffer;
    size_t tmp_buffer_size;
    bool use_external_buffer;
    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;

    off_t read_until_position = 0;

    off_t offset = 0;
    size_t total_size;
    bool initialized = false;
    char * data_ptr;
    size_t data_capacity;

    LoggerPtr log = getLogger("ReadBufferFromCeph");
};

}

#endif
