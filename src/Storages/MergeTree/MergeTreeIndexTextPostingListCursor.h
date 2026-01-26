#pragma once
#include <absl/container/flat_hash_map.h>

#include <Storages//MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Common/PODArray.h>

namespace DB
{

struct TokenPostingsInfo;
class WriteBuffer;
class ReadBuffer;
class MergeTreeReaderStream;
class IColumn;

class PostingListCursor
{
public:
    PostingListCursor(MergeTreeReaderStream * stream_, const TokenPostingsInfo & info_, size_t segment)
        : stream(stream_)
        , info(info_)
    {
        current_values.reserve(BLOCK_SIZE);
        segments.push_back(segment);
        prepare(segment);
    }

    MergeTreeReaderStream * stream = nullptr;

    void addSegment(size_t);

    void linearOr(UInt8 * data, size_t row_offset, size_t num_rows);
    void linearAnd(UInt8 * data, size_t row_offset, size_t num_rows);

    void next();
    bool valid() const { return is_valid; }
    uint32_t value() const { return current_values[index]; }
    void seek(uint32_t target);
    double density() const { return density_val; }
private:
    void prepare(size_t segment);
    void linearOrImpl(size_t segment, UInt8 *, size_t row_begin, size_t row_end);
    void linearAndImpl(size_t segment, UInt8 *, size_t row_begin, size_t row_end);

    bool seekImpl(uint32_t target);
    uint32_t min() const { return header.first_row_id; }
    uint32_t max() const { return block_row_ends.back(); }

    const TokenPostingsInfo & info;
    PostingListCodecBitpackingImpl::Header header;

    std::vector<uint32_t> current_values;
    size_t index = 0;

    std::string compressed_data;

    size_t block_count = 0;
    size_t current_block = 0;
    size_t tail_size = 0;

    std::vector<uint32_t> block_row_ends;
    std::vector<size_t> block_offsets;

    std::vector<size_t> segments;
    size_t current_segment = std::numeric_limits<size_t>::max();

    bool is_valid = true;
    double density_val = 0;
};

using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;
using PostingListCursorMap = absl::flat_hash_map<std::string_view, PostingListCursorPtr>;

void streamApplyPostingsAny(IColumn & column, std::vector<PostingListCursorPtr> & postings, size_t column_offset, size_t row_offset, size_t num_rows, UInt64 kind);
void streamApplyPostingsAll(IColumn & column, std::vector<PostingListCursorPtr> & postings, size_t column_offset, size_t row_offset, size_t num_rows, UInt64 kind);
}
