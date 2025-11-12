#include <IO/UTFConvertingReadBuffer.h>

#include "config.h"
#if USE_ICU
#    include <unicode/ucnv.h>
#endif
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

UTFConvertingReadBuffer::UTFConvertingReadBuffer(std::unique_ptr<ReadBuffer> impl_)
    : BufferWithOwnMemory<ReadBuffer>(DBMS_DEFAULT_BUFFER_SIZE)
    , impl(std::move(impl_))
    , detected_encoding(Encoding::UTF8)
    , bom_detected(false)
{
    // BOM detection will be performed lazily on first nextImpl() call
}

void UTFConvertingReadBuffer::detectAndProcessBOM()
{
    if (!impl->hasPendingData())
    {
        if(!impl->next())
        {
            detected_encoding = Encoding::UTF8;
            return;
        }
    }

    const char * pos = impl->position();
    size_t available = impl->available();

    if (available < 2)
    {
        detected_encoding = Encoding::UTF8;
        return;
    }

    const unsigned char * bytes = reinterpret_cast<const unsigned char *>(pos);

    if (available >= 4 && bytes[0] == 0xFF && bytes[1] == 0xFE && bytes[2] == 0x00 && bytes[3] == 0x00)
    {
        detected_encoding = Encoding::UTF32_LE;
        impl->ignore(4);
    }
    else if (available >= 4 && bytes[0] == 0x00 && bytes[1] == 0x00 && bytes[2] == 0xFE && bytes[3] == 0xFF)
    {
        detected_encoding = Encoding::UTF32_BE;
        impl->ignore(4);
    }
    else if (available >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
    {
        detected_encoding = Encoding::UTF8;
        impl->ignore(3);
    }
    else if (bytes[0] == 0xFF && bytes[1] == 0xFE)
    {
        detected_encoding = Encoding::UTF16_LE;
        impl->ignore(2);
    }
    else if (bytes[0] == 0xFE && bytes[1] == 0xFF)
    {
        detected_encoding = Encoding::UTF16_BE;
        impl->ignore(2);
    }
    else
    {
        detected_encoding = Encoding::UTF8;
    }
}

bool UTFConvertingReadBuffer::nextImpl()
{
    if (!bom_detected)
    {
        detectAndProcessBOM();
        bom_detected = true;
    }

    if (detected_encoding == Encoding::UTF8)
    {
        if (!impl->hasPendingData())
        {
            if (!impl->next())
                return false;
        }

        size_t size = impl->available();
        if (size == 0)
            return false;

        internal_buffer = Buffer(impl->buffer().begin(), impl->buffer().end());
        working_buffer = internal_buffer;
        pos = working_buffer.begin();

        impl->ignore(size);

        return true;
    }

    if (!impl->hasPendingData() && !impl->next())
        return false;

    const char * src = impl->position();
    size_t src_size = impl->available();

    if (src_size == 0)
        return false;

    size_t bytes_written = 0;
    if (detected_encoding == Encoding::UTF16_LE || detected_encoding == Encoding::UTF16_BE)
    {
        bytes_written = convertUTF16ToUTF8(src, src_size, memory.data(), memory.size());
    }
    else if (detected_encoding == Encoding::UTF32_LE || detected_encoding == Encoding::UTF32_BE)
    {
        bytes_written = convertUTF32ToUTF8(src, src_size, memory.data(), memory.size());
    }

    if (bytes_written == 0)
        return false;

    impl->ignore(src_size);

    working_buffer = Buffer(memory.data(), memory.data() + bytes_written);
    pos = working_buffer.begin();
    return true;
}

size_t UTFConvertingReadBuffer::convertUTF16ToUTF8(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
#if USE_ICU
    UErrorCode status = U_ZERO_ERROR;

    const char * from_encoding = (detected_encoding == Encoding::UTF16_LE) ? "UTF-16LE" : "UTF-16BE";

    int32_t bytes_written
        = ucnv_convert("UTF-8", from_encoding, dst, static_cast<int32_t>(dst_capacity), src, static_cast<int32_t>(src_size), &status);

    if (U_FAILURE(status))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to convert UTF-16 to UTF-8: {}", u_errorName(status));
    }

    return static_cast<size_t>(bytes_written);
#else
    (void)src;
    (void)src_size;
    (void)dst;
    (void)dst_capacity;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "ICU support is required for UTF-16 conversion");
#endif
}

size_t UTFConvertingReadBuffer::convertUTF32ToUTF8(const char * src, size_t src_size, char * dst, size_t dst_capacity)
{
#if USE_ICU
    UErrorCode status = U_ZERO_ERROR;

    const char * from_encoding = (detected_encoding == Encoding::UTF32_LE) ? "UTF-32LE" : "UTF-32BE";

    int32_t bytes_written
        = ucnv_convert("UTF-8", from_encoding, dst, static_cast<int32_t>(dst_capacity), src, static_cast<int32_t>(src_size), &status);

    if (U_FAILURE(status))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to convert UTF-32 to UTF-8: {}", u_errorName(status));
    }

    return static_cast<size_t>(bytes_written);
#else
    (void)src;
    (void)src_size;
    (void)dst;
    (void)dst_capacity;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "ICU support is required for UTF-32 conversion");
#endif
}

}
