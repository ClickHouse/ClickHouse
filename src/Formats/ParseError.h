#pragma once

namespace DB
{

/// Returns true if the error code corresponds to a recoverable error in the input data
/// (a parsing error) rather than a fatal one (e.g. MEMORY_LIMIT_EXCEEDED).
///
/// It is used to decide whether a value can be treated as not parsed (skipped, replaced
/// with a default, etc.) or whether the exception must propagate. In particular the
/// deserialization code that restores a column after a failed read must rethrow
/// non-parse errors instead of swallowing them.
bool isParseError(int code);

/// To be called from a `catch (...)` block of a `tryDeserialize` "try-pattern": if the
/// currently handled exception is not a parse error (e.g. it is MEMORY_LIMIT_EXCEEDED),
/// rethrow it, so that only genuine parse failures are reported as a not-parsed value.
/// Must be called only while an exception is being handled.
void rethrowIfNotParseError();

}
