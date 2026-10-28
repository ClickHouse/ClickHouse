# Memory addresses reaching users

Trigger: the diff formats, stores, returns, or derives an identifier from a pointer, a runtime
address, a captured stack, an allocator or profiler output, a memory map, or symbol and debug
information; or it changes how existing addresses are normalized, symbolized, or gated.

## Why it matters

The server parses responses from servers the user chooses (drivers, client libraries, object
storage, external dictionaries), so a memory-safety bug in that code is reachable by a user. ASLR
is what keeps such a bug from becoming reliable code execution, and a single published address of
code, a library, the heap, a stack, or a mapping defeats it. Publishing the layout of the binary
(symbols, offsets) is the other half: with it, one leaked address locates everything.

## The invariant

Nothing a user can observe contains a runtime address. The only address-like values that may
leave the process are offsets inside the main executable, and only through surfaces gated as
introspection. Anything else (an address in another loaded object, in JIT code, on the heap or a
stack, or in an unknown mapping) is hidden, not published raw. Because clients never receive real
addresses, interfaces that accept addresses back from users need to accept offsets only; an
interface that also accepts real addresses lets a user test guesses.

## Carriers to follow

An address leaves the process through more channels than the one the diff touches. For each value
the diff introduces or changes, trace every path that can render it:

- Text: exception messages (sent to the client and stored in query logs), log lines (which reach
  the client through user-settable log forwarding and reach the text log table), debug dumps,
  `EXPLAIN` output, fatal-error reports.
- Values: system table columns, query results, function results, aggregate states.
- Identifiers derived from addresses: object ids, node names in graphs, hashes or counters seeded
  from a pointer whenever the address can be recovered from the result.
- Files and endpoints: profile or heap dumps, files whose path is returned to the user, HTTP and
  other non-SQL handlers, which may not authenticate at all.
- Third-party output: allocator, profiler, unwinder, and library reports embed addresses and memory
  maps in their own formats; passing them through publishes them.

Check normalization at each sink independently. Two sinks fed from one source often normalize
differently, and a text form that is safe does not make the numeric form of the same data safe,
or the reverse. A value that is normalized for the main executable but kept raw for every other
object is still a leak.

## Gates are not a fix

A privilege check reduces who can read an address; it does not make publishing it correct.
Monitoring roles routinely hold grants on system tables, and some configurations make all system
tables readable. Prefer hiding the value over gating it. Where data about the binary layout is
legitimately exposed (symbol names, offsets, symbolization), verify it sits behind the
introspection grant and setting on every entrypoint, including paths that symbolize on the server
side and return only the result.

## Severity

A runtime address reachable by a user is a **Blocker**, whatever privilege gates the surface,
including the introspection grant, and however unlikely the triggering path looks. Binary layout data
(symbols, offsets) exposed without the introspection gate is a **Blocker** as well. A new interface
that accepts real addresses as input, or inconsistent normalization between sinks of the same data,
is a **Major**.
