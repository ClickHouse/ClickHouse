#pragma once

#include <Compression/ICompressionCodec.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IDataPartStorage.h>

namespace DB
{

/// Return compression codec with default parameters for file compressed in
/// clickhouse fashion (with checksums, headers for each block, etc). This
/// method should be used as fallback when we cannot deduce compression codec
/// from metadata.
CompressionCodecPtr getCompressionCodecForFile(const DataPartStoragePtr & data_part_storage, const String & relative_path);

}
