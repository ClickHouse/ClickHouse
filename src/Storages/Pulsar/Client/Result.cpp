#include <Storages/Pulsar/Client/Result.h>

namespace DB::Pulsar
{

const char * strResult(Result result)
{
    switch (result)
    {
        case ResultOk:
            return "Ok";
        case ResultError:
            return "Error";
        case ResultTimeout:
            return "TimedOut";
        case ResultConnectError:
            return "ConnectError";
        case ResultAuthenticationError:
            return "AuthenticationError";
        case ResultBrokerError:
            return "BrokerError";
        case ResultTopicNotFound:
            return "TopicNotFound";
        case ResultServiceNotReady:
            return "ServiceNotReady";
        case ResultProducerClosed:
            return "ProducerClosed";
        case ResultConsumerClosed:
            return "ConsumerClosed";
        case ResultAlreadyClosed:
            return "AlreadyClosed";
        case ResultChecksumError:
            return "ChecksumError";
        case ResultTooManyLookupRedirects:
            return "TooManyLookupRedirects";
        case ResultNotSupported:
            return "NotSupported";
        case ResultInvalidConfiguration:
            return "InvalidConfiguration";
        case ResultMessageTooBig:
            return "MessageTooBig";
    }
    return "UnknownError";
}

}
