#pragma once

#include <base/types.h>
#include <string_view>
#include <utility>

namespace DB
{

/// Encodes a map (key, value) pair into a single text-index token:
///   token = key ‖ value ‖ revvarint(key.size())
/// The key length lives in a reverse-decodable trailer (not a prefix), so tokens sort by their
/// `key || value` bytes and no two distinct pairs collide. Keeping the key at the front lets
/// similar keys cluster for front-coding and keeps key-prefix scans usable.
String encodeMapKeyValueToken(std::string_view key, std::string_view value);

/// Splits a token produced by encodeMapKeyValueToken back into (key, value) views.
std::pair<std::string_view, std::string_view> decodeMapKeyValueToken(std::string_view token);

}
