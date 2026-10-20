# Native protocol / native format spec sync

Fires only when the diff changes the bytes on the wire: a new or changed packet type or handshake
field, a `DBMS_TCP_PROTOCOL_VERSION` / `DBMS_MIN_REVISION_*` bump, or a new or changed column
encoding in `NativeReader` / `NativeWriter`. Refactors, bug fixes that keep the layout, and edits to
`TCPHandler` / `Connection` that do not change what is sent do not trigger it.

When it fires, check that the matching spec is updated: `docs/reference/interfaces/specs/NativeProtocol.mdx`
for the protocol, `docs/reference/interfaces/specs/NativeFormat.mdx` for the format. Third-party
clients are built against these specs. A missing update is a **Major** finding naming what changed.
