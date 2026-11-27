#include <Storages/Pulsar/Client/Protocol.h>

#include <base/unaligned.h>


/// The input is a stream of frames received by a consumer, each with its 4-byte size.
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    using namespace DB::Pulsar;

    Limits limits;
    limits.max_message_size = 1024 * 1024;
    limits.max_uncompressed_size = 4 * 1024 * 1024;

    UInt64 now = 1'000'000;
    ChunkAssembler chunks([&] { return now; });
    Frame frame;
    Entry entry;

    std::string_view input(reinterpret_cast<const char *>(data), size);
    try
    {
        while (input.size() >= FRAME_SIZE_LENGTH)
        {
            size_t frame_size = decodeFrameSize(input.data(), limits);
            if (input.size() - FRAME_SIZE_LENGTH < frame_size)
                break;
            auto body = std::make_shared<StringWithMemoryTracking>(input.substr(FRAME_SIZE_LENGTH, frame_size));
            input.remove_prefix(FRAME_SIZE_LENGTH + frame_size);
            now += 10'000;

            decodeFrame(*body, limits, frame);
            if (!frame.command.has_type() || frame.command.type() != DB::PulsarProto::BaseCommand::MESSAGE)
                continue;

            decodeEntry(frame.tail, entry);
            if (entry.checksum_mismatch || entry.metadata.has_encryption_keys())
                continue;

            const auto & metadata = entry.metadata;
            DecodedPayload decoded;
            if (!metadata.has_num_messages_in_batch() && metadata.num_chunks_from_msg() > 1)
            {
                ChunkAssembler::Result result;
                chunks.add(metadata, {1, now}, entry.payload, limits.max_message_size, limits, result);
                chunks.expire(result);
                if (!result.message)
                    continue;
                auto assembled = std::make_shared<StringWithMemoryTracking>(std::move(*result.message));
                decoded = decodePayload(metadata, *assembled, assembled, 0, false, limits);
            }
            else
            {
                decoded = decodePayload(metadata, entry.payload, body, limits.max_message_size, true, limits);
            }
            if (decoded.error)
                continue;

            BatchCursor cursor(std::move(decoded.batch), metadata);
            size_t total = 0;
            while (cursor.hasNext())
                total += cursor.next().payload.size();
            (void)total;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed frames are rejected with exceptions.
    }

    return 0;
}
