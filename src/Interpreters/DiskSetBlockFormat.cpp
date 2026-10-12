#include <Interpreters/DiskSetBlockFormat.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <limits>

namespace DB
{

template <typename Key>
void DiskSetBlockFormat<Key>::Encoder::add(std::span<const Key> keys, BlockSink & sink)
{
    /// Each step either takes keys into the pending block or writes the pending block. A block is written
    /// only when a key is waiting that it cannot take, so the last block stays pending for `finish`.
    while (!keys.empty())
    {
        if (pending_keys.empty())
        {
            startPendingBlock(keys.front());
            keys = keys.subspan(1);
        }
        else if (pending_layout == Layout::PackedGaps)
        {
            if (tryAddToPackedBlock(keys.front()))
                keys = keys.subspan(1);
            else if (pending_keys.size() >= min_keys_per_packed_block)
                writeBlock(pending_keys.size(), sink); /// Packing pays: the waiting key starts the next block
            else
                pending_layout = Layout::Unpacked; /// Packing does not pay: the keys stay as they are
        }
        else if (pending_keys.size() >= keys_per_unpacked_block)
        {
            writeBlock(keys_per_unpacked_block, sink); /// The unpacked block is full
        }
        else
        {
            keys = keys.subspan(addToUnpackedBlock(keys));
        }
    }
}

template <typename Key>
void DiskSetBlockFormat<Key>::Encoder::finish(BlockSink & sink)
{
    writeBlock(pending_keys.size(), sink); /// The last block, in the layout that it has by now
}

template <typename Key>
void DiskSetBlockFormat<Key>::Encoder::startPendingBlock(const Key & key)
{
    pending_layout = Layout::PackedGaps;
    payload_bound = {};
    pending_keys.push_back(key);
}

template <typename Key>
bool DiskSetBlockFormat<Key>::Encoder::tryAddToPackedBlock(const Key & key)
{
    const auto offset = getOffsetInPendingBlock(key); /// Nothing when `key` is too far from the first key

    /// `payload_bound` takes the offset only when the packed payload still fits, so it is asked last.
    if (!offset || pending_keys.size() >= max_keys_per_packed_block || !payload_bound.tryAddValue(*offset, block_bytes))
        return false;

    pending_keys.push_back(key);
    return true;
}

template <typename Key>
size_t DiskSetBlockFormat<Key>::Encoder::addToUnpackedBlock(std::span<const Key> keys)
{
    const size_t count = std::min(keys.size(), keys_per_unpacked_block - pending_keys.size()); /// Room left
    pending_keys.insert(pending_keys.end(), keys.begin(), keys.begin() + count); /// Copied in bulk
    return count;
}

template <typename Key>
std::optional<typename DiskSetBlockFormat<Key>::Offset> DiskSetBlockFormat<Key>::Encoder::getOffsetInPendingBlock(const Key & key) const
{
    const auto offset = static_cast<Key>(key - pending_keys.front());
    if constexpr (sizeof(Key) > sizeof(Offset)) /// Only keys wider than `Offset` can be too far apart
    {
        if (offset > std::numeric_limits<Offset>::max())
            return {};
    }
    return static_cast<Offset>(offset);
}

template <typename Key>
void DiskSetBlockFormat<Key>::Encoder::writeBlock(size_t count, BlockSink & sink)
{
    chassert(count && count <= pending_keys.size());
    const std::span<const Key> block_keys(pending_keys.data(), count);

    WriteBuffer & out = sink.startBlock(block_keys);
    [[maybe_unused]] const size_t block_start = out.count();
    writeBinaryLittleEndian(static_cast<UInt8>(pending_layout), out); /// Header: packed, or keys as they are
    writeBinaryLittleEndian(static_cast<UInt32>(count), out); /// Header: the number of keys
    if (pending_layout == Layout::Unpacked)
        out.write(reinterpret_cast<const char *>(block_keys.data()), count * sizeof(Key)); /// Raw keys
    else
        writePackedGaps(block_keys, out);

    /// The file of a set sizes its buffer for `max_block_bytes_with_header`, so that each block is one frame
    /// (see `DiskSetImpl::createFileScope`). The block must not exceed it.
    chassert(out.count() - block_start <= max_block_bytes_with_header);

    pending_keys.erase(pending_keys.begin(), pending_keys.begin() + count); /// The rest starts the next block
}

template <typename Key>
void DiskSetBlockFormat<Key>::Encoder::writePackedGaps(std::span<const Key> block_keys, WriteBuffer & out)
{
    offsets.resize(block_keys.size() - 1); /// The first key needs no offset: the directory keeps it
    for (size_t i = 1; i < block_keys.size(); ++i)
        offsets[i - 1] = static_cast<Offset>(block_keys[i] - block_keys.front());

    payload.resize(PFor::maxCompressedBytes<Offset>(offsets.size())); /// Room for the worst case of `PFor`
    const size_t payload_bytes = PFor::encodeBlocks<Offset>(std::span<const Offset>(offsets), offsets_delta, payload.data());

    /// `tryAddToPackedBlock` keeps a block of packed gaps while `payload_bound` stays within `block_bytes`.
    chassert(payload_bytes <= block_bytes);

    writeBinaryLittleEndian(static_cast<UInt32>(payload_bytes), out); /// Header: the size of the payload
    out.write(reinterpret_cast<const char *>(payload.data()), payload_bytes); /// The packed offsets
}

template <typename Key>
DiskSetBlockHeader DiskSetBlockFormat<Key>::Decoder::readHeader(ReadBuffer & in)
{
    UInt8 layout = 0;
    DiskSetBlockHeader header;
    readBinaryLittleEndian(layout, in); /// Whether the keys are packed or stored as they are
    readBinaryLittleEndian(header.key_count, in);
    header.packed = static_cast<Layout>(layout) == Layout::PackedGaps;
    if (header.packed)
    {
        chassert(header.key_count <= max_keys_per_packed_block);
        readBinaryLittleEndian(header.body_bytes, in); /// The size of the packed payload
        chassert(header.body_bytes <= block_bytes);
    }
    else
    {
        chassert(static_cast<Layout>(layout) == Layout::Unpacked && header.key_count <= keys_per_unpacked_block);
        header.body_bytes = static_cast<UInt32>(header.key_count * sizeof(Key)); /// The keys as they are
    }
    return header;
}

template <typename Key>
std::span<const Key> DiskSetBlockFormat<Key>::Decoder::decode(const DiskSetBlockHeader & header, ReadBuffer & in, const Key & first_key)
{
    keys.resize(header.key_count);
    if (header.packed)
        decodePackedGaps(header.body_bytes, in, first_key);
    else
        in.readStrict(reinterpret_cast<char *>(keys.data()), header.body_bytes); /// The keys as they are
    return {keys.data(), keys.size()};
}

template <typename Key>
void DiskSetBlockFormat<Key>::Decoder::decodePackedGaps(size_t payload_bytes, ReadBuffer & in, const Key & first_key)
{
    in.readStrict(reinterpret_cast<char *>(payload.data()), payload_bytes);

    offsets.resize(keys.size() - 1); /// One offset per key but the first, which comes from the directory
    [[maybe_unused]] const size_t decoded_bytes
        = PFor::decodeBlocks<Offset>(payload.data(), offsets.size(), offsets_delta, offsets.data());
    chassert(decoded_bytes == payload_bytes);

    keys[0] = first_key;
    for (size_t i = 0; i < offsets.size(); ++i)
        keys[i + 1] = static_cast<Key>(first_key + offsets[i]); /// Each key is its offset from the first key
}

template class DiskSetBlockFormat<UInt8>::Encoder;
template class DiskSetBlockFormat<UInt8>::Decoder;
template class DiskSetBlockFormat<UInt16>::Encoder;
template class DiskSetBlockFormat<UInt16>::Decoder;
template class DiskSetBlockFormat<UInt32>::Encoder;
template class DiskSetBlockFormat<UInt32>::Decoder;
template class DiskSetBlockFormat<UInt64>::Encoder;
template class DiskSetBlockFormat<UInt64>::Decoder;
template class DiskSetBlockFormat<UInt128>::Encoder;
template class DiskSetBlockFormat<UInt128>::Decoder;
template class DiskSetBlockFormat<UInt256>::Encoder;
template class DiskSetBlockFormat<UInt256>::Decoder;

}
