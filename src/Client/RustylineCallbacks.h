#pragma once

/// Callbacks invoked from the Rust `rustyline-bridge` crate
/// (`rust/workspace/rustyline/src/lib.rs`) back into ClickHouse.
///
/// This header is included verbatim by the cxx-generated bridge stub, so
/// it must stay self-contained (no ClickHouse-internal headers) — and it
/// must NOT redefine any shared structs from the bridge: those are
/// emitted by cxx in the same translation unit and a duplicate definition
/// here causes a hard redefinition error.

#include "rust/cxx.h"

#include <cstddef>
#include <string>
#include <vector>

namespace DB::rustyline
{

::rust::String cb_highlight(const ::std::string & line, ::std::size_t pos);

/// Compute the byte offset at which the to-be-completed token starts.
::std::size_t cb_complete_start(const ::std::string & line, ::std::size_t pos);

/// Compute candidate completions for the token starting at
/// `cb_complete_start(line, pos)`.
::rust::Vec<::rust::String> cb_complete_candidates(const ::std::string & line, ::std::size_t pos);

::rust::String cb_open_editor(const ::std::string & buf, bool format_query);

}
