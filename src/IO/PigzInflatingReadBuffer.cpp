#include <IO/PigzInflatingReadBuffer.h>
#include <IO/PigzDeflatingWriteBuffer.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ZLIB_INFLATE_FAILED;
}

PigzInflatingReadBuffer::PigzInflatingReadBuffer(
    std::unique_ptr<ReadBuffer> in_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
    , pool()
{
    curr_result_it = results.end();
}

PigzInflatingReadBuffer::~PigzInflatingReadBuffer()
{
}


bool PigzInflatingReadBuffer::nextImpl() 
{
    internal_pos = 0;
    if (!writeToInternal()) {
        return true;
    }

    if (eof_flag) {
        return false; 
    }

    if (in->eof()) {
        eof_flag = true;
        if (!prev_last_slice.empty()) {
            CompressedBuf result = decompressBlock(
                reinterpret_cast<unsigned char *>(&prev_last_slice.front()),
                prev_last_slice.size()
            );
            results.push_back(std::make_shared<CompressedBuf>(result));
            curr_result_it = results.begin();
            prev_last_slice.clear();

            if (!writeToInternal()) {
                return true;
            }
        }
        return !working_buffer.empty();
    }

    in->nextIfAtEnd();
    auto *in_buf = reinterpret_cast<unsigned char *>(in->position());
    uint32_t in_len = in->buffer().end() - in->position();
    
    if (!skipped_header_flag) {
        in_buf += 10;
        in_len -= 10;
        skipped_header_flag = true;
    }
    
    size_t prev_sep = 0;
    bool prev_sep_flag = false;
    size_t last_sep = 0;
    for (size_t i = 0; i + 9 < in_len; ++i) {
        size_t j = i;
        if (in_buf[j++] == 0 &&
            in_buf[j++] == 0 &&
            in_buf[j++] == 255 &&
            in_buf[j++] == 255 &&
            in_buf[j++] == 0 &&
            in_buf[j++] == 0 &&
            in_buf[j++] == 0 &&
            in_buf[j++] == 255 &&
            in_buf[j++] == 255) {

            size_t curr_sep = j;
            last_sep = curr_sep;
            
            if (!prev_sep_flag) {
                prev_sep_flag = true;
                prev_last_slice.append(reinterpret_cast<char*>(in_buf), curr_sep);

                runDecompressBlockTask(reinterpret_cast<unsigned char *>(&prev_last_slice.front()), prev_last_slice.size());
            } else {
                runDecompressBlockTask(in_buf + prev_sep, curr_sep - prev_sep);
            }
            prev_sep = curr_sep;
        }
    }
    if (!prev_sep_flag) {
        std::cout << "!prev_sep_flag" << std::endl;
        throw "!prev_sep_flag";
    }
    in->position() = in->buffer().end();

    pool.wait();
    writeToInternal();

    prev_last_slice.clear();
    prev_last_slice.append(reinterpret_cast<char*>(in_buf + last_sep), in_len - last_sep);

    return true;
}

PigzInflatingReadBuffer::CompressedBuf PigzInflatingReadBuffer::decompressBlock(unsigned char * in_buf, size_t in_len) {
    size_t mem_size = BLOCK_SIZE * 2;
    
    auto mem = std::make_shared<Memory<>>(mem_size);

    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;

    infstream.next_in = in_buf; 
    infstream.avail_in = in_len;
    infstream.next_out = reinterpret_cast<unsigned char *>(mem->data());
    infstream.avail_out = mem_size;

    int rc = inflateInit2(&infstream, -15);
    if (rc != 0)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateInit2 failed: {}", zError(rc));
    
    int inflate_rc = inflate(&infstream, Z_NO_FLUSH);
    if (inflate_rc != 0 && inflate_rc != 1)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflate failed: {}", zError(inflate_rc));

    rc = inflateReset(&infstream);
    if (rc != 0)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateReset failed: {}", zError(rc));
    
    rc = inflateEnd(&infstream);
    if (rc != 0)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateEnd failed: {}", zError(rc));

    sum_decomp += mem_size - infstream.avail_out;

    return {mem, mem_size - infstream.avail_out, inflate_rc};
}

bool PigzInflatingReadBuffer::writeToInternal() {
    if (results.empty())
        return true;

    for (; curr_result_it != results.end(); curr_result_it++) {
        std::shared_ptr<CompressedBuf> curr_result = *curr_result_it;
        // TODO: записывать через memcpy 
        for (; curr_result_pos < curr_result->len; ++curr_result_pos) {
            if (internal_pos == internal_buffer.size()) {
                working_buffer.resize(internal_pos);
                return false;
            }
            internal_buffer.begin()[internal_pos++] = curr_result->mem->data()[curr_result_pos];
            sum_written++;        
        }
        curr_result_pos = 0;
    }
    results.clear();
    curr_result_it = results.end();
    curr_result_pos = 0;
    
    working_buffer.resize(internal_pos);
    return true;
}

void PigzInflatingReadBuffer::runDecompressBlockTask(unsigned char * in_buf, size_t in_len) {
    results.push_back(std::make_shared<CompressedBuf>());
    if (results.size() == 1) {
        curr_result_it = results.begin();
    }
    pool.scheduleOrThrowOnError(
        [&, res = *results.rbegin(), in_buf = in_buf, in_len = in_len]() mutable
        {
            *res = decompressBlock(in_buf, in_len);
        });
}

}
