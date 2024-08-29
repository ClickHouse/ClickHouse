#pragma once

#include <optional>
#include <config.h>

#if USE_CEPH

#include <memory>
#include <IO/BufferWithOwnMemory.h>
#include <IO/Ceph/RadosIOContext.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <base/types.h>


namespace DB
{
/// Accepts a pool and an object id, opens and read the object
class ReadBufferFromRados : public ReadBufferFromFileBase
{
struct Impl;
public:
    ReadBufferFromRados(
        std::shared_ptr<librados::Rados> rados_,
        const String & pool,
        const String & nspace,
        const String & object_id_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false,
        std::optional<size_t> file_size_ = std::nullopt);

    ReadBufferFromRados(
        std::shared_ptr<RadosIOContext> io_ctx_,
        const String & object_id_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false,
        std::optional<size_t> file_size_ = std::nullopt);

    ~ReadBufferFromRados() override;

    std::optional<size_t> tryGetFileSize() override;
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

    size_t readImpl(char * to, size_t len, off_t begin) const;

    LoggerPtr log = getLogger("ReadBufferFromRados");
    // std::unique_ptr<Impl> impl;
    std::shared_ptr<RadosIOContext> io_ctx;
    String object_id;
    ReadSettings read_settings;
    std::atomic<off_t> file_offset = 0;
    std::atomic<off_t> read_until_position = 0;
    bool use_external_buffer;
    bool restricted_seek;
};

}

#endif
