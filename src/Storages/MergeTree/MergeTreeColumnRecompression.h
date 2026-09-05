#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Compression/ICompressionCodec.h>

namespace DB
{

class IMergeTreeDataPart;
struct MergeTreeSettings;
struct ReadSettings;
struct WriteSettings;
struct MergeTreeDataPartChecksums;

/// Names of all on-disk data files (`.bin` and marks) of a column's substreams that exist in `part`.
/// Used to exclude those files from hardlinking so `recompressColumnStreams` can write fresh copies.
NameSet getColumnDataStreamFileNames(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column);

/// Re-compress all data streams of a single wide-part column with the column's current codec
/// WITHOUT deserializing the values.
///
/// The serialized representation of a column does not depend on its compression codec, so it is
/// enough to read every compressed block of each of the column's `.bin` substreams, decompress it
/// (the source codec is self-describing via the block header) and compress it again with the new
/// codec. Compressed blocks are re-emitted one-to-one, which keeps the decompressed content and the
/// granule boundaries byte-identical; only the compressed offsets change, so the corresponding
/// marks file is rewritten with remapped offsets while the decompressed offsets and per-granule row
/// counts are preserved.
///
/// The recompressed `.bin` and marks files are written into `new_data_part`'s storage and their
/// checksums are added to `checksums` (replacing any pre-existing entries). All other files of the
/// part are expected to be hardlinked by the caller.
///
/// A single on-disk stream can be shared by several columns -- most notably the offsets stream of
/// `Nested` siblings when `share_nested_offsets` is enabled (`n.a`/`n.b` share `n.size0`). Such a
/// stream must be rewritten exactly once across the whole mutation, otherwise the shared file would
/// be written several times and the codec of the last writer would silently win. The caller passes a
/// `recompressed_streams` set that is shared across the calls for all recompressed columns; the first
/// call that reaches a stream rewrites it, later calls skip it. The caller may also pre-seed the set
/// with streams that another writer of the same mutation (the column output stream of an updated
/// sibling) will produce, so that recompression leaves those shared streams to that writer.
///
/// Only wide parts with full (local) storage are supported; the caller must guarantee that.
void recompressColumnStreams(
    const IMergeTreeDataPart & source_part,
    IMergeTreeDataPart & new_data_part,
    const NameAndTypePair & column,
    const StorageMetadataPtr & metadata_snapshot,
    const CompressionCodecPtr & default_codec,
    const MergeTreeSettings & storage_settings,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    NameSet & recompressed_streams,
    MergeTreeDataPartChecksums & checksums);

}
