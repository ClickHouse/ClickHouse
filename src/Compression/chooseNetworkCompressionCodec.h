#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

struct Settings;

/// Conservative default for skipping compression of tiny `Native` frames; the setting can override it.
inline constexpr size_t DEFAULT_NETWORK_COMPRESSION_MIN_BYTES = 128;

/// The codec for the compressed frames a peer originates over the network: the native protocol's
/// compressed packets on both ends, and the response of an HTTP request made with `compress=1`.
/// Reads the network compression settings by value, regardless of their `changed` flags — in
/// particular, values derived from `compatibility` apply even though they are not serialized to the
/// server (see `ClientBase::settingsWithoutCompatibilityDerived`). With no settings, the built-in
/// default codec is used.
CompressionCodecPtr chooseNetworkCompressionCodec(const Settings * settings);

}
