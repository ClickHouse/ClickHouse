#pragma once

#include <Common/FST.h>
#include <Compression/ICompressionCodec.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <roaring.hh>
#include <vector>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <ic.h>

namespace DB
{

using GinIndexPostingsList = roaring::Roaring;
using GinIndexPostingsListPtr = std::shared_ptr<GinIndexPostingsList>;

class GinIndexPostingsListAsRoaringBuilder
{
public:
    /// Serialize the content of builder to given WriteBuffer, returns the bytes of serialized data
    static UInt64 serialize(const roaring::Roaring& rowid_bitmap, UInt64 header, const CompressionCodecPtr & codec, WriteBuffer & buffer) {
        const bool compress = rowid_bitmap.cardinality() >= ROARING_BITMAP_COMPRESSION_CARDINALITY_THRESHOLD;
        const UInt64 uncompressed_size = rowid_bitmap.getSizeInBytes();

        auto buf = std::make_unique<char[]>(uncompressed_size);
        rowid_bitmap.write(buf.get());

        header |= (uncompressed_size << 1);
        if (compress)
        {
            header |= 0x1; // compressed

            Memory<> memory;
            memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(uncompressed_size)));
            auto compressed_size = codec->compress(buf.get(), static_cast<UInt32>(uncompressed_size), memory.data());

            writeVarUInt(header, buffer);
            writeVarUInt(compressed_size, buffer);
            buffer.write(memory.data(), compressed_size);

            return getLengthOfVarUInt(header) + getLengthOfVarUInt(compressed_size) + compressed_size;
        }

        header |= 0x0; // no compressed

        writeVarUInt(header, buffer);
        buffer.write(buf.get(), uncompressed_size);

        return getLengthOfVarUInt(header) + uncompressed_size;
    }

    /// Deserialize the postings list data from given ReadBuffer, return a pointer to the GinIndexPostingsList created by deserialization
    static GinIndexPostingsListPtr deserialize(UInt64 header, const CompressionCodecPtr & codec, ReadBuffer & buffer) {
        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();

        const bool compressed = header & 0x1;
        const UInt64 uncompressed_size = (header >> 1);

        if (compressed)
        {
            size_t compressed_size = 0;
            readVarUInt(compressed_size, buffer);
            auto buf = std::make_unique<char[]>(compressed_size);
            buffer.readStrict(reinterpret_cast<char *>(buf.get()), compressed_size);

            Memory<> memory;
            memory.resize(uncompressed_size);
            codec->decompress(buf.get(), static_cast<UInt32>(compressed_size), memory.data());

            return std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(memory.data()));
        }

        /// Deserialize uncompressed roaring bitmap
        auto buf = std::make_unique<char[]>(uncompressed_size);
        buffer.readStrict(reinterpret_cast<char *>(buf.get()), uncompressed_size);
        return std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(buf.get()));
    }

private:
    static constexpr size_t ROARING_BITMAP_COMPRESSION_CARDINALITY_THRESHOLD = 5000;
};

class GinIndexPostingsListAsDynamicBitsetBuilder
{
public:
    /// Serialize the content of builder to given WriteBuffer, returns the bytes of serialized data
    static UInt64 serialize(const roaring::Roaring& rowid_bitmap, UInt64 header, const CompressionCodecPtr & codec, WriteBuffer & buffer) {
        const UInt32 base = rowid_bitmap.minimum();
        const UInt32 rowid_max = rowid_bitmap.maximum();
        const UInt32 range_size = (rowid_max - base) + 1;

        using block_type = boost::dynamic_bitset<UInt64>::block_type;

        boost::dynamic_bitset<> bitset(range_size);
        for (const auto rowid : rowid_bitmap)
            bitset.set(rowid - base);

        std::vector<block_type> blocks(bitset.num_blocks());
        boost::to_block_range(bitset, blocks.begin());

        const bool compress = bitset.num_blocks() >= DYNAMIC_BITSET_COMPRESSION_BLOCKS_THRESHOLD;
        const UInt64 uncompressed_size = blocks.size() * sizeof(block_type);

        header |= (blocks.size() << 1);
        if (compress)
        {
            header |= 0x1; // compressed

            Memory<> memory;
            memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(uncompressed_size)));
            UInt64 compressed_size = codec->compress(reinterpret_cast<char *>(blocks.data()), uncompressed_size, memory.data());

            writeVarUInt(header, buffer);
            writeVarUInt(base, buffer);
            writeVarUInt(compressed_size, buffer);
            buffer.write(memory.data(), compressed_size);
            return getLengthOfVarUInt(header) + getLengthOfVarUInt(base) + getLengthOfVarUInt(compressed_size) + compressed_size;
        }

        header |= 0x0; // no compressed

        writeVarUInt(header, buffer);
        writeVarUInt(base, buffer);
        buffer.write(reinterpret_cast<char *>(blocks.data()), uncompressed_size);
        return getLengthOfVarUInt(header) + getLengthOfVarUInt(base) + uncompressed_size;
    }

    /// Deserialize the postings list data from given ReadBuffer, return a pointer to the GinIndexPostingsList created by deserialization
    static GinIndexPostingsListPtr deserialize(UInt64 header, const CompressionCodecPtr & codec, ReadBuffer & buffer) {
        const bool compressed = header & 0x1;

        UInt32 num_blocks = (header >> 1);
        UInt64 base = 0;
        readVarUInt(base, buffer);

        using block_type = boost::dynamic_bitset<>::block_type;
        std::vector<block_type> blocks(num_blocks);
        if (compressed)
        {
            UInt64 compressed_size = 0;
            readVarUInt(compressed_size, buffer);

            Memory<> memory(compressed_size);
            buffer.readStrict(memory.data(), compressed_size);
            codec->decompress(memory.data(), static_cast<UInt32>(compressed_size), reinterpret_cast<char *>(blocks.data()));
        }
        else
        {
            const UInt64 uncompressed_size = blocks.size() * sizeof(block_type);
            buffer.readStrict(reinterpret_cast<char *>(blocks.data()), uncompressed_size);
        }

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
        const size_t bits_per_block = sizeof(block_type) * 8;
        std::vector<UInt32> block_values;
        for (size_t block_idx = 0; const auto & block : blocks)
        {
            if (block == 0)
                continue;
            const size_t block_base = base + (block_idx * bits_per_block);

            block_values.clear();
            for (size_t offset = 0; offset < bits_per_block; ++offset)
                if (block & (block_type(1) << offset))
                    block_values.emplace_back(block_base + offset);
            postings_list->addMany(block_values.size(), block_values.data());
            ++block_idx;
        }

        return postings_list;
    }

private:
    static constexpr size_t DYNAMIC_BITSET_COMPRESSION_BLOCKS_THRESHOLD = 128;
};

class GinIndexPostingsListAsTurboPForArrayBuilder
{
public:
    static UInt64 serialize(const roaring::Roaring & rowid_bitmap, UInt64 header, const CompressionCodecPtr & /* codec */, WriteBuffer & buffer)
    {
        std::vector<UInt32> rows(rowid_bitmap.cardinality());
        rowid_bitmap.toUint32Array(rows.data());

        UInt64 uncompressed_size = rows.size() * sizeof(UInt32);

        auto compressed = std::make_unique<unsigned char[]>(uncompressed_size); /// reserve enough space

#if defined(__AVX512__)
        UInt64 compressed_size = p4nd1enc256v32(rows.data(), rows.size(), compressed.get());
#elif defined(__AVX2__)
        UInt64 compressed_size = p4nd1enc128v32(rows.data(), rows.size(), compressed.get());
#else
        UInt64 compressed_size = p4nd1enc32(rows.data(), rows.size(), compressed.get());
#endif

        header |= rows.size();
        writeVarUInt(header, buffer);
        writeVarUInt(compressed_size, buffer);
        buffer.write(reinterpret_cast<char *>(compressed.get()), compressed_size);

        return getLengthOfVarUInt(header) + getLengthOfVarUInt(compressed_size) + compressed_size;
    }

    static GinIndexPostingsListPtr deserialize(UInt64 header, const CompressionCodecPtr & /* codec */, ReadBuffer & buffer) {
        UInt64 num_rows = header;

        UInt64 compressed_size = 0;
        readVarUInt(compressed_size, buffer);

        auto compressed = std::make_unique<unsigned char[]>(compressed_size); /// reserve enough space
        buffer.readStrict(reinterpret_cast<char *>(compressed.get()), compressed_size);

        std::vector<UInt32> rows(num_rows);

#if defined(__AVX512__)
        p4nd1dec256v32(compressed.get(), rows.size(), rows.data());
#elif defined(__AVX2__)
        p4nd1dec128v32(compressed.get(), rows.size(), rows.data());
#else
        p4nd1dec32(compressed.get(), rows.size(), rows.data());
#endif

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
        postings_list->addMany(rows.size(), rows.data());
        return postings_list;
    }
};

class GinIndexPostingsListAsTurboPForGolombBuilder
{
public:
    static UInt64 serialize(const roaring::Roaring & rowid_bitmap, UInt64 header, const CompressionCodecPtr & /* codec */, WriteBuffer & buffer)
    {
        std::vector<UInt32> rows(rowid_bitmap.cardinality());
        rowid_bitmap.toUint32Array(rows.data());

        std::vector<UInt32> deltas(rows.size());
        deltas[0] = rows[0];
        for (size_t i = 1; i < rows.size(); ++i)
            deltas[i] = rows[i] - rows[i - 1];

        UInt64 uncompressed_size = deltas.size() * sizeof(UInt32);
        auto compressed = std::make_unique<unsigned char[]>(2 * uncompressed_size); /// reserve enough space - worse case
        UInt64 compressed_size = bitrenc32(reinterpret_cast<unsigned char*>(deltas.data()), uncompressed_size, compressed.get());

        header |= deltas.size();
        writeVarUInt(header, buffer);
        writeVarUInt(compressed_size, buffer);
        buffer.write(reinterpret_cast<char *>(compressed.get()), compressed_size);

        return getLengthOfVarUInt(header) + getLengthOfVarUInt(compressed_size) + compressed_size;
    }

    static GinIndexPostingsListPtr deserialize(UInt64 header, const CompressionCodecPtr & /* codec */, ReadBuffer & buffer) {
        UInt64 num_rows = header;
        UInt64 uncompressed_size = num_rows * sizeof(UInt32);

        UInt64 compressed_size = 0;
        readVarUInt(compressed_size, buffer);

        auto compressed = std::make_unique<unsigned char[]>(compressed_size); /// reserve enough space
        buffer.readStrict(reinterpret_cast<char *>(compressed.get()), compressed_size);

        std::vector<UInt32> rows(num_rows);
        bitrdec32(compressed.get(), uncompressed_size, reinterpret_cast<unsigned char*>(rows.data()));

        for (size_t i = 1; i < rows.size(); ++i)
            rows[i] += rows[i - 1];

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
        postings_list->addMany(rows.size(), rows.data());
        return postings_list;
    }
};
}
