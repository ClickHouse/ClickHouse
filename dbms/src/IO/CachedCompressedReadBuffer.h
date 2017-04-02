#pragma once

#include <memory>
#include <time.h>

#include <IO/createReadBufferFromFileBase.h>
#include <IO/CompressedReadBufferBase.h>
#include <IO/UncompressedCache.h>


namespace DB
{


/** Буфер для чтения из сжатого файла с использованием кэша разжатых блоков.
  * Кэш внешний - передаётся в качестве аргумента в конструктор.
  * Позволяет увеличить производительность в случае, когда часто читаются одни и те же блоки.
  * Недостатки:
  * - в случае, если нужно читать много данных подряд, но из них только часть закэширована, приходится делать seek-и.
  */
class CachedCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
private:
    const std::string path;
    UncompressedCache * cache;
    size_t buf_size;
    size_t estimated_size;
    size_t aio_threshold;

    std::unique_ptr<ReadBufferFromFileBase> file_in;
    size_t file_pos;

    /// Кусок данных из кэша, или кусок считанных данных, который мы положим в кэш.
    UncompressedCache::MappedPtr owned_cell;

    void initInput();
    bool nextImpl() override;

    /// Передаётся в file_in.
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;

public:
    CachedCompressedReadBuffer(
        const std::string & path_, UncompressedCache * cache_, size_t estimated_size_, size_t aio_threshold_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);


    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }
};

}
