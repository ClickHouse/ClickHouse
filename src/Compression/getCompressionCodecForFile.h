#pragma once

#include <Compression/ICompressionCodec.h>
#include <Disks/IDisk.h>

namespace DB
{

/// Return compression codec with default parameters for file compressed in
/// clickhouse fashion (with checksums, headers for each block, etc). This
/// method should be used as fallback when we cannot deduce compression codec
/// from metadata.
CompressionCodecPtr getCompressionCodecForFile(const DiskPtr & disk, const String & relative_path);

}
