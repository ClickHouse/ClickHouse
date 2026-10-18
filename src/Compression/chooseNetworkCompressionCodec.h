#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

struct Settings;

/// The codec for the compressed frames a peer originates over the network: the native protocol's
/// compressed packets on both ends, and the response of an HTTP request made with `compress=1`.
/// Reads the network compression settings by value, regardless of their `changed` flags — in
/// particular, values derived from `compatibility` apply even though they are not serialized to the
/// server (see `ClientBase::settingsWithoutCompatibilityDerived`). With no settings, the built-in
/// default codec is used.
CompressionCodecPtr chooseNetworkCompressionCodec(const Settings * settings);

/// The same, from the values of `network_compression_method` and `network_zstd_compression_level`, for
/// callers that resolve the codec after the settings object is gone.
CompressionCodecPtr chooseNetworkCompressionCodec(const String & network_compression_method, Int64 network_zstd_compression_level);

}
