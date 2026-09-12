#pragma once

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <memory>

namespace DB
{

class ISink;
class ISource;
class IProcessor;
class Block;

/// Describes an individual stream of an Exchange, e.g. ShuffleExchange from M buckets to N buckets has M*N streams
struct ExchangeStreamId
{
    String exchange_id;
    String source_bucket;
    String destination_bucket;

    ExchangeStreamId() = default;

    ExchangeStreamId(const String & exchange_id_, const String & source_bucket_, const String & destination_bucket_)
        : exchange_id(exchange_id_)
        , source_bucket(source_bucket_)
        , destination_bucket(destination_bucket_)
    {}

    ExchangeStreamId(const String & exchange_id_, size_t source_bucket_, size_t destination_bucket_)
        : exchange_id(exchange_id_)
        , source_bucket(std::to_string(source_bucket_))
        , destination_bucket(std::to_string(destination_bucket_))
    {}

    String toString() const
    {
        return exchange_id + "__" + source_bucket + "_" + destination_bucket;
    }
};

/// Interface for creating Sink and Source processors by exchange logical id when building query pipeline for
/// distributed query plan fragment. The idea is to store only logical names of exchange streams in the query plan
/// and create actual Sink and Source processors only when the query pipeline is built.
struct IExchangeLookup : boost::noncopyable
{
    virtual ~IExchangeLookup() = default;

    /// `input_is_serialized`: the sink's input chunks are the packets made by the processors of
    /// `createSerializer`, which is only true for an exchange kind that returns such processors.
    virtual std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id, bool input_is_serialized) = 0;
    virtual std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id) = 0;

    /// A processor that turns data chunks into the form the sinks of exchange `exchange_id` send.
    /// The send steps put one on every stream in front of a sink, so serialization runs on all
    /// streams instead of in the single sink of a destination. Returns nullptr when the sinks send
    /// data chunks as they are.
    virtual std::shared_ptr<IProcessor> createSerializer(SharedHeader /*input_header*/, const String & /*exchange_id*/)
    {
        return nullptr;
    }
};

using ExchangeLookupPtr = std::shared_ptr<IExchangeLookup>;

}
