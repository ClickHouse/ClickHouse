#pragma once

namespace DB::Pulsar
{

/// The outcome of a client operation. The values the engine distinguishes are `ResultOk` (success)
/// and everything else (failure, reported with `strResult`). The set is intentionally small: this
/// is a minimal client that speaks only the Pulsar binary protocol.
enum Result
{
    ResultOk = 0,
    ResultError,
    ResultTimeout,
    ResultConnectError,
    ResultAuthenticationError,
    ResultBrokerError,
    ResultTopicNotFound,
    ResultServiceNotReady,
    ResultProducerClosed,
    ResultConsumerClosed,
    ResultAlreadyClosed,
    ResultChecksumError,
    ResultTooManyLookupRedirects,
    ResultNotSupported,
    ResultInvalidConfiguration,
    ResultMessageTooBig,
};

const char * strResult(Result result);

}
