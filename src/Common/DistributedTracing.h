#pragma once

#include <Common/Exception.h>

#include <Common/thread_local_rng.h>

#include <Common/Stopwatch.h>

#include <Core/Types.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <optional>
#include <variant>


namespace DB
{


//! This file provides some basic support of OpenTracing API.
namespace opentracing
{

//! Span implementation.


enum class SpanReferenceType
{
    ChildOfRef = 1,
    FollowsFromRef = 2
};


using TracingId = UInt64;

struct SpanContext
{
    // FIXME which underlying type to use?
    TracingId trace_id;
    TracingId span_id;

    bool isInitialized = false;
    // bool PropagateTracing may be useful
};


using SpanReference = std::pair<SpanReferenceType, std::shared_ptr<const SpanContext>>;
using SpanReferenceList = std::vector<SpanReference>;
using TagValue = std::variant<Int64, String, bool>;

using LogEvent = std::pair<String, String>;
using LogEntity = std::vector<LogEvent>;


class Span
{
public:
    Span(
        String operation_name_,
        SpanReferenceList references_,
        time_t start_timestamp_);

    SpanContext getSpanContext() const;

    void setTag(String key, TagValue value);

    void addLog(LogEntity log_entity, std::optional<UInt64> timestamp = std::nullopt);

    ~Span();

    void finishSpan(std::optional<time_t> finish_timestamp_opt = std::nullopt);

    SpanReferenceList getReferences() const;

private:
    const String operation_name;
    const SpanReferenceList references;

    const time_t start_timestamp;
    time_t finish_timestamp = 0;

    SpanContext span_context;

    std::vector<std::pair<String, TagValue>> tags;
    std::vector<std::pair<LogEntity, UInt64>> logs;

    bool is_finished = false;

    static TracingId generateTracingId()
    {
        return std::uniform_int_distribution<TracingId>(0)(thread_local_rng);
    }

    // This is a workaround for logging traces without something like jaeger-client.
    void attachSpanInfoToQueryLog();
};

struct IDistributedTracer;

std::shared_ptr<Span> getCurrentSpan();
std::shared_ptr<IDistributedTracer> getCurrentTracer();

std::shared_ptr<Span> switchToChildSpan(const String& operation_name);

void switchToParentSpan(const std::shared_ptr<Span>& parent_span);

std::shared_ptr<Span> parkSpan(const std::shared_ptr<Span>& new_span);

class SpanGuard
{
public:
    SpanGuard();
    SpanGuard(const String& operation_name);
    ~SpanGuard();

private:
    std::shared_ptr<DB::opentracing::Span> parent_span;
};


class ParkingSpanGuard
{
public:
    ParkingSpanGuard(const std::shared_ptr<Span>& new_span);
    ~ParkingSpanGuard();

private:
    std::shared_ptr<DB::opentracing::Span> parked_span;
};


//! Tracer implementation.


struct IDistributedTracer
{
    virtual ~IDistributedTracer() = default;

    virtual std::shared_ptr<Span> CreateSpan(
        String operation_name,
        SpanReferenceList references,
        std::optional<time_t> start_timestamp_opt) const = 0;

    virtual String SerializeSpan(const std::shared_ptr<Span>& span) const = 0;
    virtual String SerializeSpanContext(const SpanContext& span_context) const = 0;

    virtual SpanContext DeserializeSpanContext(const String& span_context_string) const = 0;

    virtual void InjectSpanContext(const std::shared_ptr<Span>& span, WriteBuffer & out) const = 0;
    virtual std::shared_ptr<SpanContext> ExtractSpanContext(ReadBuffer & in) const = 0;

    virtual bool IsActiveTracer() const = 0;
};


class DistributedTracer
    : public IDistributedTracer
{
public:
    // TODO add tags to span class.
    std::shared_ptr<Span> CreateSpan(
        String operation_name,
        SpanReferenceList references,
        std::optional<time_t> start_timestamp_opt) const override;

    // TODO: OpenTracing API actually tells to support 3 formats (and corresponding carriers),
    // these are: text-map, http-header and binary format.

    String SerializeSpan(const std::shared_ptr<Span>& span) const override;
    String SerializeSpanContext(const SpanContext& span_context) const override;

    SpanContext DeserializeSpanContext(const String& span_context_string) const override;

    void InjectSpanContext(const std::shared_ptr<Span>& span, WriteBuffer & out) const override;
    std::shared_ptr<SpanContext> ExtractSpanContext(ReadBuffer & in) const override;

    bool IsActiveTracer() const override;
};


class NoopTracer
    : public IDistributedTracer
{
public:
    std::shared_ptr<Span> CreateSpan(
        String /*operation_name*/,
        SpanReferenceList /*references*/,
        std::optional<time_t> /*start_timestamp_opt*/) const override
    {
        return nullptr;
    }

    String SerializeSpan(const std::shared_ptr<Span>& /*span*/) const override
    {
        return String();
    }

    String SerializeSpanContext(const SpanContext& /*span_context*/) const override
    {
        return String();
    }

    SpanContext DeserializeSpanContext(const String& /*span_context_string*/) const override
    {
        return SpanContext();
    }

    void InjectSpanContext(const std::shared_ptr<Span>& span, WriteBuffer & out) const override;

    std::shared_ptr<SpanContext> ExtractSpanContext(ReadBuffer & in) const override;

    bool IsActiveTracer() const override
    {
        return false;
    }
};


}

}
