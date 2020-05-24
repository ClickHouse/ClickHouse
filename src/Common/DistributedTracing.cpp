#include "DistributedTracing.h"

//#include <Interpreters/QueryLog.h>
//#include <Interpreters/Context.h>

#include <Common/CurrentThread.h>
#include <Common/thread_local_rng.h>


#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSession.h>
#include <Poco/Net/HTTPClientSession.h>

#include <Poco/Timestamp.h>


#if !__clang__
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wsuggest-override"
#    pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

//#include <thrift/protocol/TBinaryProtocol.h>
//#include <thrift/transport/TSocket.h>
//#include <thrift/transport/TTransportUtils.h>
//#include <thrift/transport/THttpClient.h>
//#include "./gen-cpp/Collector.h"


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::opentracing
{


//! Span implementation.


Span::Span(
    String operation_name_,
    SpanReferenceList references_,
    time_t start_timestamp_)
    : operation_name(std::move(operation_name_))
    , references(std::move(references_))
    , start_timestamp(start_timestamp_)
{
    if (references.empty())
    {
        //! This span is the root-span of a trace, hence we need to create trace_id.
        //! `enableDistributedTracing` should have been checked earlier.
        span_context.trace_id = generateTracingId();
        span_context.isInitialized = true;
    } else
    {
        TracingId trace_id = references[0].second->trace_id;
        for (const auto& context : references)
        {
            if (trace_id != context.second->trace_id)
            {
                throw Exception("References of a span must all have the same trace id", ErrorCodes::LOGICAL_ERROR);
            }
        }

        span_context.trace_id = trace_id;
        span_context.isInitialized = references[0].second->isInitialized;
    }

    span_context.span_id = generateTracingId();

    setTag("peer.service", "clickhouse");
}

Span::~Span()
{
//    finishSpan();
    if (finish_timestamp == 0)
    {
        setTag("i'm late", "true");
        finishSpan();
    }
}

SpanContext Span::getSpanContext() const
{
    /// We intentionally make a copy here.
//    assert(finish_timestamp == 0);

    // CHECKME
//    if (finish_timestamp != 0)
//        abort();

    return span_context;
}

void Span::finishSpan(std::optional<time_t> finish_timestamp_opt)
{
//    assert(finish_timestamp == 0);
    if (finish_timestamp != 0)
        abort();
    assert(!finish_timestamp_opt || *finish_timestamp_opt != 0);

    finish_timestamp = finish_timestamp_opt
                       ? *finish_timestamp_opt
                       : Poco::Timestamp().epochMicroseconds();
//                       : clock_gettime_ns();

    attachSpanInfoToQueryLog();
}

SpanReferenceList Span::getReferences() const
{
    return references;
}

void Span::setTag(String key, TagValue value)
{
    tags.emplace_back(std::move(key), std::move(value));
}

void Span::addLog(LogEntity log_entity, std::optional<UInt64> timestamp)
{
    UInt64 event_timestamp = timestamp
            ? *timestamp
            : Poco::Timestamp().epochMicroseconds();
//            : clock_gettime_ns();
    logs.emplace_back(std::move(log_entity), event_timestamp);
}

void Span::attachSpanInfoToQueryLog()
{
//    if (!CurrentThread::getGroup())
//        return;
//
//    auto context = CurrentThread::getGroup()->query_context;
//
//    const Settings &settings = context->getSettingsRef();
//    if (!settings.log_queries)
//        return;
//
//    QueryLogElement elem;
//    elem.type = QueryLogElementType::QUERY_FINISH;
//
//    elem.query_start_time = start_timestamp;
//    elem.event_time = finish_timestamp;
//    elem.query = operation_name + " " + context->getDistributedTracer()->SerializeSpanContext(span_context);
//    elem.stack_trace = references.empty()
//       ? String()
//       : context->getDistributedTracer()->SerializeSpanContext(*references[0].second);
//    elem.client_info = context->getClientInfo();
//
//
//    String all_tags_string = "tags: {";
//    for (const auto& tag : tags)
//    {
//        // This is only for purposes of the draft.
//        all_tags_string += tag.first + "; ";
//    }
//    all_tags_string += "};";
//    elem.query += all_tags_string;
//
//    if (auto query_log = context->getQueryLog())
//        query_log->add(elem);
//
//
//    // Simplified way of sending spans to jaeger-collector
//    // (avoiding jaeger-agent) with jaeger-all-in-one in-memory instance.
//
//    using namespace jaegertracing::thrift;
//
//    jaegertracing::thrift::Span span;
//
//    span.traceIdLow = span_context.trace_id;
//    span.traceIdHigh = 1;
//    span.spanId = span_context.span_id;
//
//    span.parentSpanId = references.empty()
//                        ? 0
//                        : references[0].second->span_id;
//
//    span.flags = 2;
//    span.operationName = operation_name;
//    span.startTime = start_timestamp;
//    span.duration = finish_timestamp - start_timestamp;
//
//
//    jaegertracing::thrift::Process proc;
//    proc.serviceName = "ClickHouse";
//
//    jaegertracing::thrift::Batch batch;
//    batch.process = proc;
//    batch.spans.push_back(span);
//
//    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> _buffer (new apache::thrift::transport::TMemoryBuffer(10000 /*maxPacketSize*/));
//    std::shared_ptr<apache::thrift::protocol::TProtocol> _protocol;
//    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocolFactory(
//            new apache::thrift::protocol::TBinaryProtocolFactory());
//    _protocol = protocolFactory->getProtocol(_buffer);
//
//
//    _buffer->resetBuffer();
//
//    // Does the serialisation to Thrift
//    auto oprot = _protocol.get();
//    batch.write(oprot);
//    oprot->writeMessageEnd();
//    oprot->getTransport()->writeEnd();
//    oprot->getTransport()->flush();
//
//    uint8_t* data = nullptr;
//    uint32_t size = 0;
//    _buffer->getBuffer(&data, &size);
//
//    Poco::URI uri("http://localhost:14268/api/traces");
//    std::string path(uri.getPathAndQuery());
//
//    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
//    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, path, Poco::Net::HTTPMessage::HTTP_1_1);
//    Poco::Net::HTTPResponse response;
//
//    request.setContentType("application/vnd.apache.thrift.binary");
//
//
//    request.setContentLength(size);
//
//    std::ostream& os = session.sendRequest(request);
//    os.write(reinterpret_cast<char*>(data), size);
//
//    // request.write(std::cout);
//
//    std::istream &is = session.receiveResponse(response);
//
//    std::string s;
//    std::ostringstream os2;
//    os2 << is.rdbuf();
//    s = os2.str();
//
//    //std::cout << s << std::endl;
}


TracingId Span::generateTracingId()
{
    return std::uniform_int_distribution<TracingId>(0)(thread_local_rng);
}


std::shared_ptr<Span> parkSpan(const std::shared_ptr<Span>& new_span)
{
    auto parked_span = CurrentThread::getSpan();
    CurrentThread::setSpan(new_span);

    return parked_span;
}

ParkingSpanGuard::ParkingSpanGuard(const std::shared_ptr<Span>& new_span)
    : parked_span(parkSpan(new_span))
{
    if (!new_span)
        abort(); // this is very temporary.
}

ParkingSpanGuard::~ParkingSpanGuard()
{
    parkSpan(parked_span);
}

//private:
//    std::shared_ptr<DB::opentracing::Span> parked_span;
//};


//! Tracer implementation.


std::shared_ptr<Span> DistributedTracer::CreateSpan(
    String operation_name,
    SpanReferenceList references,
    std::optional<time_t> start_timestamp_opt) const
{
    time_t start_timestamp = start_timestamp_opt
                             ? *start_timestamp_opt
                             : Poco::Timestamp().epochMicroseconds();
//                             : clock_gettime_ns();

    // get rid of ad hoc.
    if (!references.empty() && !references[0].second->isInitialized)
    {
        return nullptr;
    }

    return std::make_shared<Span>(std::move(operation_name), std::move(references), start_timestamp);
}

String DistributedTracer::SerializeSpan(const std::shared_ptr<Span>& span) const
{
    if (!span)
        return String();

    const auto& span_context = span->getSpanContext();
    if (!span_context.isInitialized)
        return String();

    return SerializeSpanContext(span_context);
}

String DistributedTracer::SerializeSpanContext(const SpanContext& span_context) const
{
    return std::to_string(span_context.trace_id) + ":" + std::to_string(span_context.span_id);
}

SpanContext DistributedTracer::DeserializeSpanContext(const String& span_context_string) const
{
    auto pos = span_context_string.find(":");
    assert(pos != std::string::npos);

    return SpanContext{
        .trace_id = std::stoul(span_context_string.substr(0, pos)),
        .span_id = std::stoul(span_context_string.substr(pos + 1)),
        .isInitialized = true};
}

void DistributedTracer::InjectSpanContext(const std::shared_ptr<Span>& span, WriteBuffer & out) const
{
    if (!!span)
    {
        const auto &spanContext = span->getSpanContext();
        writeBinary(spanContext.trace_id, out);
        writeBinary(spanContext.span_id, out);
    } else
    {
        // TODO: Shorten this with NullTracingId only.
        writeBinary(TracingId(), out);
        writeBinary(TracingId(), out);
    }
}

std::shared_ptr<SpanContext> DistributedTracer::ExtractSpanContext(ReadBuffer & in) const
{
    abort();
    // TODO: OR we need string??
    SpanContext spanContext;
    readVarUInt(spanContext.trace_id, in);
    readVarUInt(spanContext.span_id, in);
    spanContext.isInitialized = true;
    if (!spanContext.trace_id)
        abort(); // change to nullptr
    return std::make_shared<SpanContext>(spanContext);
}

bool DistributedTracer::IsActiveTracer() const
{
    return true;
}

void NoopTracer::InjectSpanContext(const std::shared_ptr<Span>& /*span*/, WriteBuffer & out) const
{
    writeBinary(TracingId(), out);
    writeBinary(TracingId(), out);
}

std::shared_ptr<SpanContext> NoopTracer::ExtractSpanContext(ReadBuffer & in) const
{
    // We perform read from buffer for consistency of the protocol.

    SpanContext spanContext;
    readBinary(spanContext.trace_id, in);
    readBinary(spanContext.span_id, in);
    spanContext.isInitialized = true;

    if (!spanContext.trace_id)
        return nullptr;
    return std::make_shared<SpanContext>(spanContext);
}

}
