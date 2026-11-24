#pragma once

// Since protobuf 36, `google/protobuf/descriptor.h` (pulled in by every generated `*.pb.h`)
// includes `absl/log/log.h`, which defines an unprefixed `LOG(severity)` macro. `libhdfs3` has its own
// `LOG(severity, fmt, ...)` macro in `common/Logger.h`, and the two clash depending on the include order.
// Process the Abseil header up front and drop its macro: the include guard prevents it from being
// defined again later, and `libhdfs3` does not use Abseil logging.
#include <absl/log/log.h>
#undef LOG
