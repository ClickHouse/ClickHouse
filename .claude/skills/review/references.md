# Review references

Detailed procedures for triggered expansions that fire on a small fraction of PRs.
Read the relevant section only when its trigger appears in the diff; the base checklist
in `SKILL.md` names the trigger and points here.

## Build flag that drops a global side-effect or weakens numeric semantics {#build-flags}

Trigger: the diff changes compiler/build flags that remove a documented side-effect or relax
FP behavior across many translation units — `-fno-math-errno` (libm stops setting `errno`),
`-ffast-math` / `-ffp-contract=fast` / `-fassociative-math` / `-freciprocal-math` /
`-fno-signed-zeros` (reordered/contracted/round-changed FP → non-reproducible results),
`-fno-trapping-math`, strict-aliasing relaxation, and similar.

The PR's implicit promise is "no code relying on the old behavior is affected"; treat that as
the contract and verify it. A grep over the affected symbol is not proof, because a match only
matters if it is **(a) compiled with the flag** and **(b) actually depends on the specific
behavior the flag removes**, not merely mentions the symbol.

- **(a) build graph (general):** a match in code not compiled with the flag cannot be affected —
  `rust_vendor`/cargo crates, `EXCLUDE_FROM_ALL`, disabled `ENABLE_*` options, or an uncompiled
  part of a contrib (e.g. a vendored DB's server backend when only its client library is built).
- **(b) what a true consumer looks like is flag-specific** — work it out per flag, don't reuse
  one flag's shape for another. For `-fno-math-errno` the affected behavior is `errno` after a
  `<math.h>` call, so most matches are false: code that *sets* errno (a libm's own
  `errno = EDOM` / `with_errno(...)`, an error-code table, `return ERANGE`) is not a consumer —
  only a *read* after a math call is; and a read whose producer the flag does not touch
  (`strtod`/`strtol`/`scanf`, a syscall) is unaffected. For reproducibility-affecting FP flags
  (`-ffast-math`/contract/associative/signed-zeros) there is no "setter/reader" split at all —
  the consumer is anything that assumes bit-identical results: cross-platform/cross-ISA
  reproducibility, distributed-merge/aggregation consistency, and committed test references — so
  the cost is acknowledged and reasoned about, not grep-audited.

Also check **scope intent:** flags appended to `COMPILER_FLAGS` before `add_subdirectory(contrib …)`
propagate into vendored libraries. Contrib coverage may be deliberate (often it is where the
speedup lives) or unsafe, but it must be a conscious, stated decision with any exceptions carved
out per target. A real consumer that silently loses the behavior is a correctness/compatibility
**Blocker**; an unstated contrib-wide reproducibility change is at least a **Major**.

## Native protocol / native format spec sync {#spec-sync}

Trigger (protocol): the diff touches the native TCP protocol — `src/Core/Protocol.h`,
`src/Core/ProtocolDefines.h`, `src/Core/Protocol.cpp`, packet handling in `src/Server/TCPHandler.*`
or `src/Client/Connection.*`, the `DBMS_TCP_PROTOCOL_VERSION` / `DBMS_MIN_REVISION_*` constants,
packet types, handshake/version negotiation, or the wire layout of any non-`Block` message.

Trigger (format): the diff changes the `Native` format — its wire/serialization format, type
encodings (`LowCardinality`, `Array`, `Map`, `Variant`, `Dynamic`, `JSON`), the block/column
structure, the compression frame, `NativeReader`/`NativeWriter`, or `docs/en/interfaces/formats/Native.md`.

Verify the corresponding specification is updated in the **same PR**:
`docs/en/interfaces/specs/NativeProtocol.md` for protocol changes,
`docs/en/interfaces/specs/NativeFormat.md` for format changes. The spec is the canonical reference
third-party native clients (`ch-go`, `clickhouse-go`) are built against; letting it drift forces
re-deriving the protocol/format from C++ source. This applies to new features, bug fixes, and
behavior changes alike. Flag a missing or stale spec section as a **Major**, naming the
packet/version/field/encoding the diff changed.
