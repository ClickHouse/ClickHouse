#include <IO/UTFConvertingReadBuffer.h>
#include <Common/Exception.h>

#if USE_ICU
#    include <unicode/ucnv.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

UTFConvertingReadBuffer::UTFConvertingReadBuffer(std::unique_ptr<ReadBuffer> impl_, Encoding encoding_)
    : BufferWithOwnMemory<ReadBuffer>(DBMS_DEFAULT_BUFFER_SIZE)
    , impl(std::move(impl_))
    , encoding(encoding_)
{
#if USE_ICU
    UErrorCode status = U_ZERO_ERROR;
    const char * name = nullptr;
    switch (encoding)
    {
        case Encoding::UTF16_LE: name = "UTF-16LE"; break;
        case Encoding::UTF16_BE: name = "UTF-16BE"; break;
        case Encoding::UTF32_LE: name = "UTF-32LE"; break;
        case Encoding::UTF32_BE: name = "UTF-32BE"; break;
        case Encoding::UTF8: name = "UTF-8"; break;
    }

    if (name)
    {
        source_converter = ucnv_open(name, &status);
        if (U_FAILURE(status))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to open ICU converter for {}: {}", name, u_errorName(status));
            
        target_converter = ucnv_open("UTF-8", &status);
        if (U_FAILURE(status))
        {
            ucnv_close(reinterpret_cast<UConverter *>(source_converter));
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to open ICU converter for UTF-8: {}", u_errorName(status));
        }

        pivot_buffer.resize(DBMS_DEFAULT_BUFFER_SIZE);
        pivot_source = pivot_buffer.data();
        pivot_target = pivot_buffer.data();
    }
#else
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "ICU support is disabled");
#endif
}

UTFConvertingReadBuffer::~UTFConvertingReadBuffer()
{
#if USE_ICU
    if (source_converter)
        ucnv_close(reinterpret_cast<UConverter *>(source_converter));
    if (target_converter)
        ucnv_close(reinterpret_cast<UConverter *>(target_converter));
#endif
}

bool UTFConvertingReadBuffer::nextImpl()
{
#if USE_ICU
    if (eof)
        return false;

    while (true)
    {
        if (!impl->hasPendingData())
        {
            if (!impl->next())
            {
                // No more input data, but we might have data in pivot buffer or internal states.
                // We proceed with empty source to flush.
            }
        }
        
        bool input_finished = !impl->hasPendingData(); 

        // Reset pivot pointers if empty to maximize space
        if (pivot_source == pivot_target)
        {
            pivot_source = pivot_buffer.data();
            pivot_target = pivot_buffer.data();
        }

        const char * source = input_finished ? nullptr : impl->position();
        const char * source_limit = input_finished ? nullptr : impl->buffer().end();
        
        char * target = memory.data();
        const char * target_limit = memory.data() + memory.size();
        
        UErrorCode status = U_ZERO_ERROR;
        
        ucnv_convertEx(reinterpret_cast<UConverter *>(target_converter),
                       reinterpret_cast<UConverter *>(source_converter), 
                       &target, target_limit,
                       &source, source_limit, 
                       reinterpret_cast<UChar **>(&pivot_source), 
                       reinterpret_cast<UChar **>(&pivot_target), 
                       pivot_buffer.data() + pivot_buffer.size(), 
                       false /* reset */, input_finished /* flush */, &status);

        if (source && !input_finished)
        {
            size_t consumed = source - impl->position();
            impl->ignore(consumed);
        }

        size_t written = target - memory.data();
        if (written > 0)
        {
             internal_buffer = Buffer(memory.data(), memory.data() + written);
             working_buffer = internal_buffer;
             pos = working_buffer.begin();
             
             // If we finished input and flushed, mark eof.
             if (input_finished && pivot_source == pivot_target && U_SUCCESS(status)) // Check status too?
             {
                 // We should be careful. input_finished means impl->next() failed.
                 // So we are flushing.
                 // If status is U_BUFFER_OVERFLOW_ERROR, we still have data to output.
                 // If status is U_TRUNCATED_CHAR_FOUND, we are done (error ignored/logged potentially, but treated as EOF).
                 
                 // If written > 0, we return true. Next call we continue flushing.
             }
             return true;
        }
        
        // Nothing written.
        if (input_finished)
        {
            // We tried to flush but nothing came out.
             
            // Check for errors on flush.
            if (status == U_TRUNCATED_CHAR_FOUND)
            {
                // Partial character at end of stream. Ignore.
            }
            else if (U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR) 
            {
                // Should throw?
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Conversion failed: {}", u_errorName(status));
            }
             
            eof = true;
            return false;
        }
        
        // If not input_finished, and nothing written, it means we consumed input but probably buffered it in pivot or internal state.
        // Loop again to get more input.
        if (U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR && status != U_TRUNCATED_CHAR_FOUND)
        {
             throw Exception(ErrorCodes::BAD_ARGUMENTS, "Conversion failed: {}", u_errorName(status));
        }
    }
#else
    return false;
#endif
}

}
