#pragma once

#include <memory>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Detects UTF BOM and returns appropriate buffer:
/// - For UTF-16/32 BOM: wraps with UTFConvertingReadBuffer
/// - For UTF-8 BOM: skips 3 bytes, returns original buffer
/// - For no BOM: returns original buffer unchanged
std::unique_ptr<ReadBuffer> wrapWithUTFBOMDetection(std::unique_ptr<ReadBuffer> buffer);

}
