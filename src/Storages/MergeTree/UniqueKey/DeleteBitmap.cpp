#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/transformEndianness.h>

#include <roaring/roaring.hh>
#include <roaring/roaring64map.hh>
#include <zlib.h>

#include <cstring>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{
    constexpr std::string_view FILE_PREFIX = "delete_bitmap_";
    constexpr std::string_view FILE_SUFFIX = ".rbm";

    /// Keeps `memoryUsage()` non-zero for an empty bitmap so cache weighting works.
    constexpr size_t EMPTY_BITMAP_OVERHEAD = 64;

    /// Reject implausibly large declared payloads before allocating.
    constexpr UInt32 MAX_SERIALIZED_BODY_SIZE = 256U * 1024U * 1024U;

    /// CRC over (magic, version, body_size, body); the trailing crc itself is not covered.
    UInt32 computeCRC32(const char * header, size_t header_size, const char * body, size_t body_size)
    {
        uLong crc = crc32(0L, Z_NULL, 0);
        crc = crc32(crc, reinterpret_cast<const Bytef *>(header), static_cast<uInt>(header_size));
        if (body_size)
            crc = crc32(crc, reinterpret_cast<const Bytef *>(body), static_cast<uInt>(body_size));
        return static_cast<UInt32>(crc);
    }

    /// Pack/unpack a UInt32 to/from a little-endian byte sequence. The
    /// conventional ClickHouse on-disk pattern (see `MarkRange.cpp` /
    /// `MergeTreeDataPartChecksum.cpp` / `CompressedReadBufferBase.cpp`)
    /// is LE-explicit so files survive a host-endian crossing.
    void packUInt32LE(char * dst, UInt32 value)
    {
        transformEndianness<std::endian::little>(value);
        std::memcpy(dst, &value, sizeof(UInt32));
    }
    UInt32 unpackUInt32LE(const char * src)
    {
        UInt32 value = 0;
        std::memcpy(&value, src, sizeof(UInt32));
        transformEndianness<std::endian::native, std::endian::little>(value);
        return value;
    }

    /// `{N}` slice of `delete_bitmap_{N}.rbm`, or empty view if `file_name`
    /// doesn't match the prefix/suffix shape.
    std::string_view extractBlockNumberPart(std::string_view file_name)
    {
        if (file_name.size() <= FILE_PREFIX.size() + FILE_SUFFIX.size())
            return {};
        if (!file_name.starts_with(FILE_PREFIX))
            return {};
        if (!file_name.ends_with(FILE_SUFFIX))
            return {};
        return file_name.substr(
            FILE_PREFIX.size(),
            file_name.size() - FILE_PREFIX.size() - FILE_SUFFIX.size());
    }

    /// Public `DeleteBitmap` methods dispatch into the right overload below via
    /// `std::visit`. The asymmetric work (narrowing cast / `BulkContext` /
    /// out-of-range short-circuit) is folded into the narrow overload so the
    /// public method bodies stay one-liners. Mutating overloads (`addAny`,
    /// `addManyAny`, `mergeAny`) require the caller to have upgraded first
    /// when the input wouldn't fit in the narrow representation.

    bool containsAny(const roaring::Roaring & r, UInt64 row)
    {
        /// Without this guard, the cast below would truncate high bits and
        /// produce a wrong positive for some unrelated low row.
        if (row > std::numeric_limits<UInt32>::max())
            return false;
        return r.contains(static_cast<UInt32>(row));
    }
    bool containsAny(const roaring::Roaring64Map & r, UInt64 row)
    {
        return r.contains(row);
    }

    void containsBulkAny(const roaring::Roaring & r, const UInt64 * rows, size_t n, uint8_t * out_keep)
    {
        if (r.isEmpty())
        {
            std::memset(out_keep, 1, n);
            return;
        }
        constexpr UInt64 max_row = std::numeric_limits<UInt32>::max();
        roaring::BulkContext ctx;
        for (size_t i = 0; i < n; ++i)
        {
            const UInt64 v = rows[i];
            if (v > max_row)
                out_keep[i] = 1;
            else
                out_keep[i] = r.containsBulk(ctx, static_cast<UInt32>(v)) ? 0 : 1;
        }
    }
    void containsBulkAny(const roaring::Roaring64Map & r, const UInt64 * rows, size_t n, uint8_t * out_keep)
    {
        if (r.isEmpty())
        {
            std::memset(out_keep, 1, n);
            return;
        }
        for (size_t i = 0; i < n; ++i)
            out_keep[i] = r.contains(rows[i]) ? 0 : 1;
    }

    void addAny(roaring::Roaring & r, UInt64 row)
    {
        r.add(static_cast<UInt32>(row));
    }
    void addAny(roaring::Roaring64Map & r, UInt64 row)
    {
        r.add(row);
    }

    void addManyAny(roaring::Roaring & r, const std::vector<UInt64> & rows)
    {
        std::vector<UInt32> narrow(rows.size());
        for (size_t i = 0; i < rows.size(); ++i)
            narrow[i] = static_cast<UInt32>(rows[i]);
        r.addMany(narrow.size(), narrow.data());
    }
    void addManyAny(roaring::Roaring64Map & r, const std::vector<UInt64> & rows)
    {
        r.addMany(rows.size(), rows.data());
    }

    void mergeAny(roaring::Roaring & dst, const roaring::Roaring & src)
    {
        dst |= src;
    }
    void mergeAny(roaring::Roaring64Map & dst, const roaring::Roaring64Map & src)
    {
        dst |= src;
    }
    void mergeAny(roaring::Roaring64Map & dst, const roaring::Roaring & src)
    {
        dst |= roaring::Roaring64Map(src);
    }
    /// `(narrow dst, wide src)` overload is intentionally absent — the caller
    /// must upgrade `dst` first; see `DeleteBitmap::merge`.

    size_t rangeCardinalityAny(const roaring::Roaring & r, UInt64 begin, UInt64 end)
    {
        /// Range portion above the addressable ceiling contributes zero.
        constexpr UInt64 max_row = std::numeric_limits<UInt32>::max();
        if (begin > max_row)
            return 0;
        const UInt64 hi_inclusive = std::min(end - 1, max_row);
        if (hi_inclusive < begin)
            return 0;
        const uint64_t upper = r.rank(static_cast<uint32_t>(hi_inclusive));
        const uint64_t lower = (begin == 0) ? 0 : r.rank(static_cast<uint32_t>(begin - 1));
        return static_cast<size_t>(upper - lower);
    }
    size_t rangeCardinalityAny(const roaring::Roaring64Map & r, UInt64 begin, UInt64 end)
    {
        const UInt64 hi_inclusive = end - 1;
        const uint64_t upper = r.rank(hi_inclusive);
        const uint64_t lower = (begin == 0) ? 0 : r.rank(begin - 1);
        return static_cast<size_t>(upper - lower);
    }

    void toVectorAny(const roaring::Roaring & r, std::vector<UInt64> & out)
    {
        std::vector<UInt32> narrow(out.size());
        r.toUint32Array(narrow.data());
        for (size_t i = 0; i < narrow.size(); ++i)
            out[i] = narrow[i];
    }
    void toVectorAny(const roaring::Roaring64Map & r, std::vector<UInt64> & out)
    {
        r.toUint64Array(out.data());
    }
}

DeleteBitmap::DeleteBitmap() : bitmap(std::make_unique<roaring::Roaring>())
{
}

DeleteBitmap::~DeleteBitmap() = default;

DeleteBitmap::DeleteBitmap(DeleteBitmap &&) noexcept = default;
DeleteBitmap & DeleteBitmap::operator=(DeleteBitmap &&) noexcept = default;

bool DeleteBitmap::is64Bit() const
{
    return bitmap.index() == 1;
}

void DeleteBitmap::upgradeTo64()
{
    if (is64Bit())
        return;
    auto & r32 = std::get<R32Ptr>(bitmap);
    bitmap = std::make_unique<roaring::Roaring64Map>(std::move(*r32));
}

bool DeleteBitmap::contains(UInt64 row) const
{
    return std::visit([row](const auto & p) { return containsAny(*p, row); }, bitmap);
}

void DeleteBitmap::containsBulk(const UInt64 * rows, size_t n, uint8_t * out_keep) const
{
    if (n == 0)
        return;
    std::visit([&](const auto & p) { containsBulkAny(*p, rows, n, out_keep); }, bitmap);
}

void DeleteBitmap::add(UInt64 row)
{
    /// Upgrade before dispatch so the visit can pick the right overload.
    if (!is64Bit() && row > std::numeric_limits<UInt32>::max())
        upgradeTo64();
    std::visit([row](auto & p) { addAny(*p, row); }, bitmap);
}

void DeleteBitmap::addMany(const std::vector<UInt64> & rows)
{
    if (rows.empty())
        return;

    if (!is64Bit())
    {
        constexpr UInt64 max_row = std::numeric_limits<UInt32>::max();
        for (UInt64 v : rows)
        {
            if (v > max_row)
            {
                upgradeTo64();
                break;
            }
        }
    }
    std::visit([&](auto & p) { addManyAny(*p, rows); }, bitmap);
}

void DeleteBitmap::merge(const DeleteBitmap & other)
{
    if (other.is64Bit() && !is64Bit())
        upgradeTo64();

    /// Outer dispatch is explicit (not nested `std::visit`) so the impossible
    /// `(narrow dst, wide src)` case never has to compile.
    if (auto * dst = std::get_if<R64Ptr>(&bitmap))
    {
        std::visit([&](const auto & src) { mergeAny(**dst, *src); }, other.bitmap);
        return;
    }
    mergeAny(*std::get<R32Ptr>(bitmap), *std::get<R32Ptr>(other.bitmap));
}

size_t DeleteBitmap::cardinality() const
{
    return std::visit([](const auto & p) -> size_t { return p->cardinality(); }, bitmap);
}

bool DeleteBitmap::empty() const
{
    return std::visit([](const auto & p) { return p->isEmpty(); }, bitmap);
}

size_t DeleteBitmap::rangeCardinality(UInt64 begin, UInt64 end) const
{
    /// |bitmap ∩ [begin, end)| = rank(end-1) - rank(begin-1).
    if (end <= begin || empty())
        return 0;
    return std::visit([&](const auto & p) { return rangeCardinalityAny(*p, begin, end); }, bitmap);
}

std::vector<UInt64> DeleteBitmap::toVector() const
{
    std::vector<UInt64> out;
    const size_t card = cardinality();
    if (card == 0)
        return out;
    out.resize(card);
    std::visit([&](const auto & p) { toVectorAny(*p, out); }, bitmap);
    return out;
}

size_t DeleteBitmap::memoryUsage() const
{
    /// Portable-serialized size is the most stable proxy for the in-memory
    /// footprint — roaring's internal container overhead is not a public API.
    const size_t serialized = std::visit(
        [](const auto & p) -> size_t { return p->getSizeInBytes(/*portable=*/true); }, bitmap);
    if (serialized == 0)
        return EMPTY_BITMAP_OVERHEAD;
    return serialized + EMPTY_BITMAP_OVERHEAD;
}

void DeleteBitmap::serialize(WriteBuffer & out) const
{
    const size_t body_size = std::visit(
        [](const auto & p) -> size_t { return p->getSizeInBytes(/*portable=*/true); }, bitmap);

    /// Enforce the same upper bound `deserialize` will check, so anything we
    /// emit is guaranteed to be readable.
    if (body_size > MAX_SERIALIZED_BODY_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap serialized body too large: {} bytes (max {})", body_size, MAX_SERIALIZED_BODY_SIZE);

    const UInt32 version = is64Bit() ? VERSION_R64 : VERSION_R32;
    const UInt32 body_size_u32 = static_cast<UInt32>(body_size);

    /// `make_unique_for_overwrite` skips the zero-init `std::vector<char>(N)`
    /// would otherwise pay for — roaring overwrites every byte.
    std::unique_ptr<char[]> body;
    if (body_size)
    {
        body = std::make_unique_for_overwrite<char[]>(body_size);
        const size_t written = std::visit(
            [&](const auto & p) -> size_t { return p->write(body.get(), /*portable=*/true); }, bitmap);
        if (written != body_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "DeleteBitmap roaring::write returned {} bytes, expected {}", written, body_size);
    }

    /// Pack the 12-byte header in little-endian byte order so we can both
    /// CRC it and write it as one contiguous blob.
    char header[sizeof(UInt32) * 3];
    packUInt32LE(header + 0,                  MAGIC);
    packUInt32LE(header + sizeof(UInt32),     version);
    packUInt32LE(header + sizeof(UInt32) * 2, body_size_u32);

    const UInt32 crc = computeCRC32(header, sizeof(header), body.get(), body_size);

    out.write(header, sizeof(header));
    if (body_size)
        out.write(body.get(), body_size);

    char crc_buf[sizeof(UInt32)];
    packUInt32LE(crc_buf, crc);
    out.write(crc_buf, sizeof(crc_buf));
}

std::unique_ptr<DeleteBitmap> DeleteBitmap::deserialize(ReadBuffer & in)
{
    char header[sizeof(UInt32) * 3];
    in.readStrict(header, sizeof(header));

    const UInt32 magic         = unpackUInt32LE(header + 0);
    const UInt32 version       = unpackUInt32LE(header + sizeof(UInt32));
    const UInt32 body_size_u32 = unpackUInt32LE(header + sizeof(UInt32) * 2);

    if (magic != MAGIC)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap magic mismatch: expected {:#x}, got {:#x}", MAGIC, magic);

    if (version != VERSION_R32 && version != VERSION_R64)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "DeleteBitmap version {} is not supported (expected {} or {})",
            version, VERSION_R32, VERSION_R64);

    /// Bound the body size before allocating so a corrupt header can't force
    /// a multi-GB allocation.
    if (body_size_u32 > MAX_SERIALIZED_BODY_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap serialized body too large: {} bytes (max {})",
            body_size_u32, MAX_SERIALIZED_BODY_SIZE);

    std::unique_ptr<char[]> body;
    if (body_size_u32)
    {
        body = std::make_unique_for_overwrite<char[]>(body_size_u32);
        in.readStrict(body.get(), body_size_u32);
    }

    char crc_buf[sizeof(UInt32)];
    in.readStrict(crc_buf, sizeof(crc_buf));
    const UInt32 stored_crc = unpackUInt32LE(crc_buf);

    const UInt32 computed_crc = computeCRC32(header, sizeof(header), body.get(), body_size_u32);
    if (stored_crc != computed_crc)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap CRC mismatch: stored {:#x}, computed {:#x}", stored_crc, computed_crc);

    /// Reject trailing bytes after the CRC so torn copies / accidental
    /// appends surface as corruption.
    if (!in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap deserialize: unexpected trailing bytes after CRC");

    auto result = std::make_unique<DeleteBitmap>();
    if (version == VERSION_R64)
    {
        auto r64 = std::make_unique<roaring::Roaring64Map>();
        if (body_size_u32)
        {
            /// `readSafe` validates container counts against `maxbytes`, so a
            /// malformed payload that survived CRC still can't over-read.
            *r64 = roaring::Roaring64Map::readSafe(body.get(), body_size_u32);
        }
        result->bitmap = std::move(r64);
    }
    else if (body_size_u32)
    {
        *std::get<R32Ptr>(result->bitmap) = roaring::Roaring::readSafe(body.get(), body_size_u32);
    }
    return result;
}

std::string DeleteBitmap::fileNameForBlockNumber(UInt64 block_number)
{
    return fmt::format("{}{}{}", FILE_PREFIX, block_number, FILE_SUFFIX);
}

bool DeleteBitmap::isDeleteBitmapFile(std::string_view file_name)
{
    auto number_part = extractBlockNumberPart(file_name);
    if (number_part.empty())
        return false;
    /// `tryParse<UInt64>` accepts leading `+` and ignores leading zeros, so a
    /// noncanonical name would resolve to the same block number as the
    /// canonical one and confuse the later read. Require digit-only, then
    /// round-trip against `fileNameForBlockNumber` to accept only the canonical form.
    for (char c : number_part)
        if (c < '0' || c > '9')
            return false;
    UInt64 parsed = 0;
    if (!tryParse<UInt64>(parsed, number_part))
        return false;
    return fileNameForBlockNumber(parsed) == file_name;
}

UInt64 DeleteBitmap::parseBlockNumberFromFileName(std::string_view file_name)
{
    /// Caller is expected to have screened the name via `isDeleteBitmapFile`.
    /// If they didn't, `parse<UInt64>` throws on a malformed slice rather than
    /// silently returning 0.
    auto number_part = extractBlockNumberPart(file_name);
    return parse<UInt64>(number_part);
}

}
