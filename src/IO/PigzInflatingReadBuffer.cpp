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
            results.push_back(result);
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
    
    std::vector<size_t> seps;
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
            // TODO: тут можно запускать декомпрессию в треде
            seps.push_back(j);
        }
    }
    in->position() = in->buffer().end();

    // TODO: seps.size() == 0
    if (seps.size() == 0) {
        throw "seps.size() == 0";
    }

    prev_last_slice.append(reinterpret_cast<char*>(in_buf), seps[0]);

    results.clear();
    results.resize(seps.size());

    pool.scheduleOrThrowOnError(
        [&, in_buf = reinterpret_cast<unsigned char *>(&prev_last_slice.front()), in_len = prev_last_slice.size()]() mutable
        {
            results[0] = decompressBlock(in_buf, in_len);
        });
    for (size_t i = 0; i < seps.size() - 1; i++) {
        pool.scheduleOrThrowOnError(
            [&, i = i, in_buf = in_buf + seps[i], in_len = seps[i + 1] - seps[i]]() mutable
            {
                results[i + 1] = decompressBlock(in_buf, in_len);
            });
    }
    pool.wait();

    writeToInternal();

    prev_last_slice.clear();
    prev_last_slice.append(reinterpret_cast<char*>(in_buf + seps[seps.size() - 1]), in_len - seps[seps.size() - 1]);

    return true;
}

PigzInflatingReadBuffer::CompressedBuf PigzInflatingReadBuffer::decompressBlock(unsigned char * in_buf, size_t in_len) {
    size_t mem_size = BLOCK_SIZE * 10;
    
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

    for (; curr_result_i < results.size(); ++curr_result_i) {
        CompressedBuf curr_result = results[curr_result_i];
        // TODO: записывать через memcpy 
        for (; curr_result_pos < curr_result.len; ++curr_result_pos) {
            if (internal_pos == internal_buffer.size()) {
                working_buffer.resize(internal_pos);
                return false;
            }
            internal_buffer.begin()[internal_pos++] = curr_result.mem->data()[curr_result_pos];
            sum_written++;        
        }
        curr_result_pos = 0;
    }
    results.clear();
    curr_result_i = 0;
    curr_result_pos = 0;
    
    working_buffer.resize(internal_pos);
    return true;
}

}
