#pragma once

/// Kafka wire primitives — sans-io, no sockets, no clock.
///
/// Naming follows the Kafka protocol spec:
///   * Fixed integers (INT8..INT64) are big-endian.
///   * VARINT / VARLONG are zig-zag encoded signed integers (record batches).
///   * UNSIGNED_VARINT carries lengths and tags in flexible versions.
///   * STRING / BYTES / ARRAY use INT16/INT32 length prefixes (non-flexible).
///   * COMPACT_* use UNSIGNED_VARINT(len + 1); 0 means null for nullable kinds.

#include <Storages/Kafka/Clients/Error.h>
#include <Storages/Kafka/Clients/Types.h>

#include <fmt/format.h>

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace Kafka::Protocol
{

[[noreturn]] void throwProtocol(const String & message);

/// Guard at the top of every encode/decode: the codec only understands the
/// declared range. Widening a range is the designed extension point for
/// supporting more broker versions.
void checkVersionSupported(Int16 version, Int16 min_version, Int16 max_version, const char * api);

/// Append-only encoder over an owned byte buffer.
class Writer
{
public:
    void writeBool(bool v) { buf.push_back(v ? 1 : 0); }
    void writeInt8(Int8 v) { buf.push_back(static_cast<char>(v)); }
    void writeInt16(Int16 v) { writeBe(static_cast<UInt16>(v)); }
    void writeInt32(Int32 v) { writeBe(static_cast<UInt32>(v)); }
    void writeUInt32(UInt32 v) { writeBe(v); }
    void writeInt64(Int64 v) { writeBe(static_cast<UInt64>(v)); }

    void writeUnsignedVarint(UInt32 value);
    void writeUnsignedVarlong(UInt64 value);
    /// Zig-zag encoded signed varint (used inside record batches).
    void writeVarint(Int32 value);
    void writeVarlong(Int64 value);

    void writeUuid(const Uuid & id);

    /// STRING: i16 length prefix.
    void writeString(std::string_view s);
    /// NULLABLE_STRING: i16 length, -1 = null.
    void writeNullableString(const std::optional<String> & s);
    /// COMPACT_STRING: uvarint(len + 1).
    void writeCompactString(std::string_view s);
    /// COMPACT_NULLABLE_STRING: 0 = null.
    void writeCompactNullableString(const std::optional<String> & s);

    /// BYTES: i32 length prefix.
    void writeBytes(std::string_view data);
    void writeNullableBytes(const std::optional<Bytes> & data);
    /// COMPACT_BYTES: uvarint(len + 1).
    void writeCompactBytes(std::string_view data);
    void writeCompactNullableBytes(const std::optional<Bytes> & data);

    void writeRaw(std::string_view data) { buf.append(data); }

    /// Empty tagged-field buffer (single 0 uvarint). The client never emits tags.
    void writeEmptyTaggedFields() { writeUnsignedVarint(0); }

    /// Mode-aware variants for codecs that span the flexible boundary.
    void writeStr(std::string_view s, bool compact) { compact ? writeCompactString(s) : writeString(s); }
    void writeNullableStr(const std::optional<String> & s, bool compact)
    {
        compact ? writeCompactNullableString(s) : writeNullableString(s);
    }
    void writeNullableBytesField(const std::optional<Bytes> & data, bool compact)
    {
        compact ? writeCompactNullableBytes(data) : writeNullableBytes(data);
    }
    /// View flavour for zero-copy fields (Fetch/Produce record blobs).
    void writeNullableBytesViewField(const std::optional<std::string_view> & data, bool compact)
    {
        if (!data)
        {
            compact ? writeUnsignedVarint(0) : writeInt32(-1);
            return;
        }
        compact ? writeCompactBytes(*data) : writeBytes(*data);
    }
    void writeBytesField(std::string_view data, bool compact) { compact ? writeCompactBytes(data) : writeBytes(data); }
    void writeTags(bool flexible)
    {
        if (flexible)
            writeEmptyTaggedFields();
    }

    template <typename T, typename F>
    void writeArr(const std::vector<T> & items, bool compact, F && encodeOne)
    {
        if (compact)
            writeCompactArray(items, std::forward<F>(encodeOne));
        else
            writeArray(items, std::forward<F>(encodeOne));
    }

    template <typename T, typename F>
    void writeNullableArr(const std::optional<std::vector<T>> & items, bool compact, F && encodeOne)
    {
        if (compact)
        {
            writeCompactNullableArray(items, std::forward<F>(encodeOne));
            return;
        }
        if (!items)
        {
            writeInt32(-1);
            return;
        }
        writeArray(*items, std::forward<F>(encodeOne));
    }

    /// COMPACT_ARRAY: uvarint(len + 1), then elements via `encodeOne(writer, item)`.
    template <typename T, typename F>
    void writeCompactArray(const std::vector<T> & items, F && encodeOne)
    {
        writeUnsignedVarint(static_cast<UInt32>(items.size()) + 1);
        for (const auto & item : items)
            encodeOne(*this, item);
    }

    /// Nullable COMPACT_ARRAY: 0 = null.
    template <typename T, typename F>
    void writeCompactNullableArray(const std::optional<std::vector<T>> & items, F && encodeOne)
    {
        if (!items)
        {
            writeUnsignedVarint(0);
            return;
        }
        writeCompactArray(*items, std::forward<F>(encodeOne));
    }

    /// Legacy ARRAY: i32 count (never null here).
    template <typename T, typename F>
    void writeArray(const std::vector<T> & items, F && encodeOne)
    {
        writeInt32(static_cast<Int32>(items.size()));
        for (const auto & item : items)
            encodeOne(*this, item);
    }

    size_t size() const { return buf.size(); }
    std::string_view view() const { return buf; }
    String take() { return std::move(buf); }

    /// Back-fill helpers for size prefixes / CRC fields written after the fact.
    void overwriteInt32At(size_t pos, Int32 v);
    void overwriteUInt32At(size_t pos, UInt32 v);

private:
    template <typename U>
    void writeBe(U v)
    {
        for (int shift = (sizeof(U) - 1) * 8; shift >= 0; shift -= 8)
            buf.push_back(static_cast<char>((v >> shift) & 0xFF));
    }

    String buf;
};

/// Bounds-checked decoder over a borrowed byte range. Throws
/// Exception(ErrorKind::Protocol) on underflow or malformed data.
class Reader
{
public:
    explicit Reader(std::string_view data_) : data(data_) { }

    bool readBool() { return readByte() != 0; }
    Int8 readInt8() { return static_cast<Int8>(readByte()); }
    Int16 readInt16() { return static_cast<Int16>(readBe<UInt16>()); }
    Int32 readInt32() { return static_cast<Int32>(readBe<UInt32>()); }
    UInt32 readUInt32() { return readBe<UInt32>(); }
    Int64 readInt64() { return static_cast<Int64>(readBe<UInt64>()); }

    UInt32 readUnsignedVarint();
    Int32 readVarint();
    Int64 readVarlong();

    Uuid readUuid();

    String readString();
    std::optional<String> readNullableString();
    String readCompactString();
    std::optional<String> readCompactNullableString();

    String readBytes();
    std::optional<String> readNullableBytes();
    String readCompactBytes();
    std::optional<String> readCompactNullableBytes();

    std::string_view readRaw(size_t n);
    void skip(size_t n);

    /// Read and discard a tagged-field buffer.
    void skipTaggedFields();

    /// Mode-aware variants for codecs that span the flexible boundary.
    String readStr(bool compact) { return compact ? readCompactString() : readString(); }
    std::optional<String> readNullableStr(bool compact)
    {
        return compact ? readCompactNullableString() : readNullableString();
    }
    std::optional<String> readNullableBytesField(bool compact)
    {
        return compact ? readCompactNullableBytes() : readNullableBytes();
    }
    /// View flavour for zero-copy fields: the result references the Reader's
    /// underlying buffer and is valid only while that buffer lives.
    std::optional<std::string_view> readNullableBytesViewField(bool compact)
    {
        if (compact)
        {
            const UInt32 raw = readUnsignedVarint();
            if (raw == 0)
                return std::nullopt;
            return readRaw(raw - 1);
        }
        const Int32 len = readInt32();
        if (len < 0)
            return std::nullopt;
        return readRaw(static_cast<size_t>(len));
    }
    String readBytesField(bool compact) { return compact ? readCompactBytes() : readBytes(); }
    void readTags(bool flexible)
    {
        if (flexible)
            skipTaggedFields();
    }

    template <typename T, typename F>
    std::vector<T> readArr(bool compact, F && decodeOne)
    {
        if (compact)
            return readCompactArray<T>(std::forward<F>(decodeOne));
        return readArray<T>(std::forward<F>(decodeOne));
    }

    template <typename T, typename F>
    std::optional<std::vector<T>> readNullableArr(bool compact, F && decodeOne)
    {
        if (compact)
            return readCompactNullableArray<T>(std::forward<F>(decodeOne));
        const Int32 len = readInt32();
        if (len < 0)
            return std::nullopt;
        return readElements<T>(static_cast<UInt32>(len), std::forward<F>(decodeOne));
    }

    /// COMPACT_ARRAY decoded via `decodeOne(reader) -> T`; null decodes as empty.
    template <typename T, typename F>
    std::vector<T> readCompactArray(F && decodeOne)
    {
        auto items = readCompactNullableArray<T>(std::forward<F>(decodeOne));
        return items ? std::move(*items) : std::vector<T>{};
    }

    template <typename T, typename F>
    std::optional<std::vector<T>> readCompactNullableArray(F && decodeOne)
    {
        const UInt32 raw = readUnsignedVarint();
        if (raw == 0)
            return std::nullopt;
        return readElements<T>(raw - 1, std::forward<F>(decodeOne));
    }

    /// Legacy ARRAY (i32 count, -1 = null -> empty).
    template <typename T, typename F>
    std::vector<T> readArray(F && decodeOne)
    {
        const Int32 len = readInt32();
        if (len < 0)
            return {};
        return readElements<T>(static_cast<UInt32>(len), std::forward<F>(decodeOne));
    }

    size_t remaining() const { return data.size() - pos; }
    bool empty() const { return remaining() == 0; }
    size_t position() const { return pos; }

private:
    template <typename T, typename F>
    std::vector<T> readElements(UInt32 count, F && decodeOne)
    {
        /// Every element consumes at least one byte; a count beyond remaining()
        /// is corrupt and must not drive a huge reserve().
        if (count > remaining())
            throwProtocol(fmt::format("Array count {} exceeds remaining bytes {}", count, remaining()));
        std::vector<T> out;
        out.reserve(count);
        for (UInt32 i = 0; i < count; ++i)
            out.push_back(decodeOne(*this));
        return out;
    }

    void need(size_t n) const
    {
        if (remaining() < n)
            throwProtocol(fmt::format("Buffer underflow: needed {} bytes, had {}", n, remaining()));
    }

    UInt8 readByte()
    {
        need(1);
        return static_cast<UInt8>(data[pos++]);
    }

    template <typename U>
    U readBe()
    {
        need(sizeof(U));
        U v = 0;
        for (size_t i = 0; i < sizeof(U); ++i)
            v = static_cast<U>((v << 8) | static_cast<UInt8>(data[pos + i]));
        pos += sizeof(U);
        return v;
    }

    String readUtf8(size_t n);

    std::string_view data;
    size_t pos = 0;
};

}
