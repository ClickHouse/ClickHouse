/// Stand-in for protobuf's `google/protobuf/wire_format_lite_inl.h`.
///
/// The SDK includes this header in `tunnel/serialize.cpp`, `tunnel/coded_checksum.h`
/// and the two `tunnel/arrow_record_*.h`, because the protobuf 3.7.1 it bundles still
/// had it. protobuf 3.8 merged its contents into `wire_format_lite.h` and removed the
/// file, so this shim forwards there. Every `WireFormatLite` symbol the SDK uses
/// (`ReadPrimitive`, `Write*NoTag`, `Write*ToArray`, ...) is declared in that header.
///
/// This directory is on the SDK target's include path ahead of protobuf's, and holds
/// only this one file, so every other `google/protobuf/...` include falls through to
/// the real protobuf headers.
///
/// If protobuf ever does ship the header again -- or if ClickHouse moves to a protobuf
/// older than 3.8 -- defer to it instead of shadowing it, because in those versions the
/// file carried inline definitions that `wire_format_lite.h` alone did not.
///
/// See contrib/odps-sdk-cpp-cmake/README.md.

#pragma once

#if defined(__has_include_next) && __has_include_next(<google/protobuf/wire_format_lite_inl.h>)
#    include_next <google/protobuf/wire_format_lite_inl.h>
#else
#    include <google/protobuf/wire_format_lite.h>
#endif
