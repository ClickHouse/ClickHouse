#pragma once

#include <base/types.h>

#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace roaring
{
class Roaring;
class Roaring64Map;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** UNIQUE KEY per-part delete bitmap — row positions (within a part, 0-based)
  * that are logically deleted.
  *
  * The bitmap picks its underlying roaring representation dynamically: a
  * narrow `roaring::Roaring` while every set value fits in `UInt32`, then
  * auto-upgrades to `roaring::Roaring64Map` on the first value above. The
  * choice is internal — the public API is uniformly `UInt64`.
  *
  * Persistence: one file per bitmap version, named
  *   `delete_bitmap_{block_number}.rbm`
  * inside the part directory. Format (all little-endian on the wire):
  *   magic(4) "RBM1" | version(4) | body_size(4) | body[body_size] | crc32(4)
  * `version` (`VERSION_R32` / `VERSION_R64`) selects which roaring layout
  * the body uses. CRC covers the LE-encoded magic + version + body_size +
  * body bytes — its bytes-on-disk, so the check is host-independent.
  *
  * Endian portability: this matches the conventional MergeTree sidecar
  * pattern (`MergeTreeDataPartChecksum`, `MarkRange`, `MergeTreeIndexText`
  * posting list, compressed-block checksums) — LE-explicit on the wire.
  *   - Header fields (magic, version, body_size, crc) and `VERSION_R32`
  *     bodies (roaring `portable=true`) are fully cross-endian portable.
  *   - `VERSION_R64` bodies are the one known exception: the croaring C++
  *     `Roaring64Map::write(portable=true)` writes its outer `map_size`
  *     (`uint64_t`) and per-bucket high-32 keys (`uint32_t`) host-native.
  *     Cross-endian reads of an R64 body fail loudly at
  *     `Roaring64Map::readSafe` (the byteswapped `map_size` won't parse)
  *     rather than silently mis-decode.
  *
  * TODO(UNIQUE KEY, endian): switch the R64 path from the croaring C++
  * `Roaring64Map` to the C-API `roaring64_bitmap_t` so we can use
  * `roaring64_bitmap_portable_serialize` /
  * `roaring64_bitmap_portable_deserialize_safe` (RoaringFormatSpec 64-bit
  * extension) and drop the cross-endian limitation above.
  */
class DeleteBitmap
{
public:
    DeleteBitmap();
    ~DeleteBitmap();

    DeleteBitmap(const DeleteBitmap &) = delete;
    DeleteBitmap & operator=(const DeleteBitmap &) = delete;
    DeleteBitmap(DeleteBitmap &&) noexcept;
    DeleteBitmap & operator=(DeleteBitmap &&) noexcept;

    /// True if `row` is set.
    bool contains(UInt64 row) const;

    /// Bulk point-containment; writes 1 to `out_keep[i]` when `rows[i]` is
    /// *not* in the bitmap, 0 otherwise. `n == 0` is a no-op.
    void containsBulk(const UInt64 * rows, size_t n, uint8_t * out_keep) const;

    /// Set `row`.
    void add(UInt64 row);
    /// Set every entry of `rows`. Empty input is a no-op.
    void addMany(const std::vector<UInt64> & rows);
    /// In-place union: `*this |= other`.
    void merge(const DeleteBitmap & other);

    /// Number of set bits.
    size_t cardinality() const;
    /// True if no bits are set.
    bool empty() const;

    /// |bitmap ∩ [begin, end)|, computed as `rank(end-1) - rank(begin-1)`.
    /// O(log N) per `rank` on bitset containers, O(log K) on array containers.
    size_t rangeCardinality(UInt64 begin, UInt64 end) const;

    /// All set row indices in ascending order. O(cardinality).
    std::vector<UInt64> toVector() const;

    /// Portable-serialized size + a small entry overhead. Stable proxy for
    /// the on-disk `.rbm` cost; empty bitmap returns a small non-zero constant
    /// so cache weighting works.
    size_t memoryUsage() const;

    /// Serialize to the on-disk format.
    void serialize(WriteBuffer & out) const;
    /// Deserialize; validates magic / version / declared body size / crc and
    /// throws on mismatch. Returned bitmap is independent of `in`.
    static std::unique_ptr<DeleteBitmap> deserialize(ReadBuffer & in);

    /// File name convention: `delete_bitmap_{block_number}.rbm`.
    static std::string fileNameForBlockNumber(UInt64 block_number);

    /// True if `file_name` matches the canonical `delete_bitmap_{N}.rbm` form.
    static bool isDeleteBitmapFile(std::string_view file_name);

    /// Extract N from `delete_bitmap_{N}.rbm`. Caller must have screened the
    /// name via `isDeleteBitmapFile`; throws if `file_name` does not match.
    static UInt64 parseBlockNumberFromFileName(std::string_view file_name);

    /// File-format constants. Exposed so tests can corrupt bytes deterministically.
    static constexpr UInt32 MAGIC = 0x314D4252; /// "RBM1" little-endian
    static constexpr UInt32 VERSION_R32 = 1;
    static constexpr UInt32 VERSION_R64 = 2;

private:
    using R32Ptr = std::unique_ptr<roaring::Roaring>;
    using R64Ptr = std::unique_ptr<roaring::Roaring64Map>;
    /// `std::variant` makes the "exactly one representation active" invariant
    /// type-system enforced. `unique_ptr` keeps the roaring headers out of
    /// this file.
    std::variant<R32Ptr, R64Ptr> bitmap;

    bool is64Bit() const;
    void upgradeTo64();
};

using DeleteBitmapPtr = std::shared_ptr<DeleteBitmap>;

}
