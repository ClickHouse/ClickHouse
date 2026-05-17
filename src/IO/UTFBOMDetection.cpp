#include <cstring>
#include <IO/PeekableReadBuffer.h>
#include <IO/UTFBOMDetection.h>
#include <IO/UTFConvertingReadBuffer.h>

namespace DB
{
namespace
{
/// BOM signatures
constexpr uint8_t UTF8_BOM[] = {0xEF, 0xBB, 0xBF};
constexpr uint8_t UTF16_LE_BOM[] = {0xFF, 0xFE};
constexpr uint8_t UTF16_BE_BOM[] = {0xFE, 0xFF};
constexpr uint8_t UTF32_LE_BOM[] = {0xFF, 0xFE, 0x00, 0x00};
constexpr uint8_t UTF32_BE_BOM[] = {0x00, 0x00, 0xFE, 0xFF};
}

std::unique_ptr<ReadBuffer> wrapWithUTFBOMDetection(std::unique_ptr<ReadBuffer> buffer)
{
    /// Peek at first 4 bytes without consuming
    auto peekable = std::make_unique<PeekableReadBuffer>(*buffer);
    peekable->setCheckpoint();

    uint8_t bom_buffer[4] = {};
    size_t bytes_read = 0;

    while (bytes_read < 4 && !peekable->eof())
    {
        if (!peekable->hasPendingData())
        {
            if (!peekable->next())
                break;
        }

        size_t available = peekable->available();
        size_t to_copy = std::min(available, 4 - bytes_read);
        memcpy(bom_buffer + bytes_read, peekable->position(), to_copy);
        peekable->ignore(to_copy);
        bytes_read += to_copy;
    }

    /// Check for UTF-32 BOMs (must check before UTF-16 due to prefix matching)
    if (bytes_read >= 4)
    {
        if (memcmp(bom_buffer, UTF32_LE_BOM, 4) == 0)
        {
            /// Needs conversion - rollback peek and wrap with UTFConvertingReadBuffer
            peekable->rollbackToCheckpoint();
            return std::make_unique<UTFConvertingReadBuffer>(std::move(buffer));
        }

        if (memcmp(bom_buffer, UTF32_BE_BOM, 4) == 0)
        {
            peekable->rollbackToCheckpoint();
            return std::make_unique<UTFConvertingReadBuffer>(std::move(buffer));
        }
    }

    /// Check for UTF-8 BOM
    if (bytes_read >= 3 && memcmp(bom_buffer, UTF8_BOM, 3) == 0)
    {
        /// UTF-8 BOM detected - skip it and return original buffer
        peekable->rollbackToCheckpoint();
        buffer->ignore(3); /// Skip the 3-byte UTF-8 BOM
        return buffer;
    }

    /// Check for UTF-16 BOMs
    if (bytes_read >= 2)
    {
        if (memcmp(bom_buffer, UTF16_LE_BOM, 2) == 0)
        {
            /// Check it's not actually UTF-32 LE (already handled above)
            if (bytes_read < 4 || memcmp(bom_buffer, UTF32_LE_BOM, 4) != 0)
            {
                peekable->rollbackToCheckpoint();
                return std::make_unique<UTFConvertingReadBuffer>(std::move(buffer));
            }
        }

        if (memcmp(bom_buffer, UTF16_BE_BOM, 2) == 0)
        {
            peekable->rollbackToCheckpoint();
            return std::make_unique<UTFConvertingReadBuffer>(std::move(buffer));
        }
    }

    /// No BOM or UTF-8 content - rollback and return original buffer unchanged
    peekable->rollbackToCheckpoint();
    return buffer;
}

}
