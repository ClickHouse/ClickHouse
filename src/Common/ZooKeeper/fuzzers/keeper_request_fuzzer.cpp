#include <Common/OpenTelemetryTracingContext.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <base/unaligned.h>

#include <bit>


/// The input is what Keeper reads from a client after the handshake (`KeeperTCPHandler::receiveRequest`):
/// a flags byte, then requests, each with its 4-byte length. With flag 4, each request is followed by
/// a response to it, read as the client does in `ZooKeeper::receiveEvent`.

namespace
{

constexpr size_t max_request_size = 1024 * 1024;

/// The length prefix of a serialized request or response must match the bytes that follow it.
void checkLengthPrefix(const std::string & serialized)
{
    if (serialized.size() < sizeof(int32_t))
        __builtin_trap();
    Int32 length = std::byteswap(unalignedLoad<Int32>(serialized.data()));
    if (static_cast<size_t>(length) + sizeof(int32_t) != serialized.size())
        __builtin_trap();
}

Coordination::ZooKeeperRequestPtr readRequest(DB::ReadBuffer & in, bool use_xid_64, bool with_tracing)
{
    DB::LimitReadBuffer limited(in, DB::LimitReadBuffer::Settings{.read_no_more = max_request_size, .expect_eof = false});

    int32_t length = 0;
    Coordination::read(length, limited);
    if (length < 0 || static_cast<size_t>(length) > max_request_size)
        return nullptr;

    Coordination::XID xid = 0;
    if (use_xid_64)
    {
        Coordination::read(xid, limited);
    }
    else
    {
        int32_t xid_32 = 0;
        Coordination::read(xid_32, limited);
        xid = xid_32;
    }

    Coordination::OpNum op_num{};
    Coordination::read(op_num, limited);

    auto request = Coordination::ZooKeeperRequestFactory::instance().get(op_num);
    request->xid = xid;
    if (auto * multi = dynamic_cast<Coordination::ZooKeeperMultiRequest *>(request.get()))
        multi->readImpl(limited, /*request_validator=*/{});
    else
        request->readImpl(limited);

    if (with_tracing)
    {
        uint8_t has_tracing_context = 0;
        Coordination::read(has_tracing_context, limited);
        if (has_tracing_context)
        {
            request->tracing_context = std::make_shared<DB::OpenTelemetry::TracingContext>();
            request->tracing_context->deserialize(limited);
        }
    }

    return request;
}

std::string writeRequest(const Coordination::ZooKeeperRequest & request, bool use_xid_64)
{
    DB::WriteBufferFromOwnString out;
    request.write(out, use_xid_64);
    out.finalize();
    checkLengthPrefix(out.str());
    return out.str();
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    if (size < 1)
        return 0;

    const bool use_xid_64 = data[0] & 1;
    const bool with_tracing = data[0] & 2;
    const bool with_responses = data[0] & 4;

    DB::ReadBufferFromMemory in(data + 1, size - 1);
    try
    {
        while (!in.eof())
        {
            auto request = readRequest(in, use_xid_64, with_tracing);
            if (!request)
                break;

            std::ignore = request->toString(/*short_format=*/true);
            std::ignore = request->toString(/*short_format=*/false);
            std::ignore = request->getPath();
            std::ignore = request->isReadRequest();

            /// Keeper writes the requests it receives to its log and its followers read them back,
            /// so a parsed request must be serialized the same way after another round.
            const std::string serialized = writeRequest(*request, use_xid_64);
            DB::ReadBufferFromString reread_in(serialized);
            auto reread = readRequest(reread_in, use_xid_64, /*with_tracing=*/false);
            if (!reread || !reread_in.eof() || writeRequest(*reread, use_xid_64) != serialized)
                __builtin_trap();

            if (!with_responses)
                continue;

            Coordination::XID xid = 0;
            int64_t zxid = 0;
            Coordination::Error error{};
            int32_t length = 0;
            Coordination::read(length, in);
            if (use_xid_64)
            {
                Coordination::read(xid, in);
            }
            else
            {
                int32_t xid_32 = 0;
                Coordination::read(xid_32, in);
                xid = xid_32;
            }
            Coordination::read(zxid, in);
            Coordination::read(error, in);

            Coordination::ZooKeeperResponsePtr response;
            if (xid == Coordination::WATCH_XID)
                response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
            else
                response = request->makeResponse();

            response->xid = xid;
            response->zxid = zxid;
            if (error != Coordination::Error::ZOK)
                response->error = error;
            else
                response->readImpl(in);

            DB::WriteBufferFromOwnString out;
            response->write(out, use_xid_64);
            out.finalize();
            checkLengthPrefix(out.str());
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed requests and responses are rejected with exceptions.
    }

    return 0;
}
