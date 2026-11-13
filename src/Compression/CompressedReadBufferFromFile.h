#pragma once

#include <memory>
#include <time.h>
#include <Compression/CompressedReadBufferBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/LoggingHelpers.h>


namespace DB
{

class MMappedFileCache;


/// Unlike CompressedReadBuffer, it can do seek.
class CompressedReadBufferFromFile final : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
      /** At any time, one of three things is true:
      * a) size_compressed = 0 and working_buffer is empty
      * b)
      *  - `working_buffer` contains the entire block.
      *  - `file_in` points to the end of this block.
      *  - `size_compressed` contains the compressed size of this block.
      * c) the same as b), but the block is not decoded: `has_pending_fsst_block` is set, `working_buffer` is empty,
      *    and `fsst_block` holds the encoded block and the current position in it (see `readFSSTBlock`).
      */
    std::unique_ptr<ReadBufferFromFileBase> p_file_in;
    ReadBufferFromFileBase & file_in;
    size_t size_compressed = 0;

    LogSeriesLimiter log;

    /// This field inherited from ReadBuffer. It's used to perform "lazy" seek, so in seek() call we:
    /// 1) actually seek only underlying compressed file_in to offset_in_compressed_file;
    /// 2) reset current working_buffer;
    /// 3) remember the position in decompressed block in nextimpl_working_buffer_offset.
    /// After following ReadBuffer::next() -> nextImpl call we will read new data into working_buffer and
    /// ReadBuffer::next() will move our position in the fresh working_buffer to nextimpl_working_buffer_offset and
    /// reset it to zero.
    ///
    /// NOTE: We have independent readBig implementation, so we have to take
    /// nextimpl_working_buffer_offset into account there as well.
    ///
    /* size_t nextimpl_working_buffer_offset; */

    bool nextImpl() override;

    void prefetch(Priority priority) override;

    /// See `readFSSTBlock`.
    FSSTBlock fsst_block;
    bool has_pending_fsst_block = false;
    /// The codes of `fsst_block`, if they are not in the memory of `compressed_in`, which can be reused by the next read.
    PODArray<char> fsst_codes_buffer;

    /// Decodes the pending block into `working_buffer`, at the position where the reading of the encoded block ended.
    void decodePendingFSSTBlock();

public:
    explicit CompressedReadBufferFromFile(std::unique_ptr<ReadBufferFromFileBase> buf, bool allow_different_codecs_ = false);

    /// Seek is lazy in some sense. We move position in compressed file_in to offset_in_compressed_file, but don't
    /// read data into working_buffer and don't shift our position to offset_in_decompressed_block. Instead
    /// we store this offset inside nextimpl_working_buffer_offset.
    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    off_t getPosition() const override;

    [[nodiscard]] size_t readBig(char * to, size_t n) override;

    /** Reading of the blocks compressed by the `FSST` codec without decoding them, to process the encoded data,
      * e.g. to check the values in it against a filter (see `StringValueFilter`).
      *
      * If there is no unread data in the working buffer and the next block is encoded by `FSST` (alone or as
      * the first codec of a chain), returns the encoded block. The reading starts at `FSSTBlock::start_offset`
      * in its decoded data (`FSSTBlock::start_cursor` in the codes), which is not zero after `seek`. After processing
      * the block from there, the caller must call `finishFSSTBlock` with the position where the reading ended.
      * If the block is not consumed entirely, it is decoded lazily on the next ordinary read; the next call
      * of `readFSSTBlock` returns it again instead.
      *
      * Otherwise returns nullptr: if there is unread data in the working buffer, or at the end of the file,
      * or after it read the next block, which is not encoded by `FSST`, and decompressed it into the working buffer
      * as `next` does. The caller reads the data in the working buffer as usual then.
      */
    const FSSTBlock * readFSSTBlock();
    void finishFSSTBlock(size_t end_offset, const FSST::Cursor & end_cursor);

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        file_in.setProfileCallback(profile_callback_, clock_type_);
    }

    void setReadUntilPosition(size_t position) override { file_in.setReadUntilPosition(position); }

    void setReadUntilEnd() override { file_in.setReadUntilEnd(); }

    void setRequestMap(ByteRangeSet ranges) override { file_in.setRequestMap(std::move(ranges)); }
};

}
