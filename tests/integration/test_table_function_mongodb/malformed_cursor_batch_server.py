#!/usr/bin/env python3

"""
A fake MongoDB server that answers `find` and `getMore` with hand-encoded cursor replies.

The collection name of the `find` command selects the reply, see `SCENARIOS`.
The cursor ID of the `getMore` command selects the next batch.

Usage: malformed_cursor_batch_server.py <port>
"""

import socketserver
import struct
import sys


# Encode BSON directly so the server can send malformed embedded documents.
def document(*elements):
    body = b"".join(elements)
    return struct.pack("<i", len(body) + 5) + body + b"\0"


def element(kind, name, payload):
    return bytes([kind]) + name.encode() + b"\0" + payload


def string(value):
    data = value.encode() + b"\0"
    return struct.pack("<i", len(data)) + data


OK = element(0x01, "ok", struct.pack("<d", 1.0))
HELLO = document(
    element(0x08, "ismaster", b"\1"),
    element(0x08, "helloOk", b"\1"),
    element(0x10, "minWireVersion", struct.pack("<i", 0)),
    element(0x10, "maxWireVersion", struct.pack("<i", 21)),
    OK,
)
VALID = document(element(0x02, "v", string("hello")))
SHORT = b"\x04\x00\x00\x00"
NONNUL = b"\x05\x00\x00\x00\x01"

# Collection name -> (first batch, cursor ID, next batch).
# A nonzero cursor ID keeps the cursor open. Each open cursor has a different ID.
# If the next batch is `None`, the server does not expect `getMore`.
SCENARIOS = {
    "valid": ([VALID, VALID], 0, None),
    "empty": ([], 0, None),
    # Neither a short length nor a missing NUL terminator must abort the process.
    "short": ([SHORT], 0, None),
    "nonnul": ([NONNUL], 0, None),
    # A bad document after a valid one, while the cursor is still open.
    "after_valid": ([VALID, SHORT], 1, None),
    # A bad document in a `getMore` reply.
    "next_batch": ([VALID], 2, [NONNUL]),
}
NEXT_BATCHES = {
    cursor_id: (collection, next_batch)
    for collection, (_, cursor_id, next_batch) in SCENARIOS.items()
    if cursor_id != 0
}


def cursor_reply(collection, documents, batch, cursor_id):
    return document(
        element(
            0x03,
            "cursor",
            document(
                element(0x12, "id", struct.pack("<q", cursor_id)),
                element(0x02, "ns", string(f"db.{collection}")),
                element(
                    0x04,
                    batch,
                    document(
                        *(element(0x03, str(i), doc) for i, doc in enumerate(documents))
                    ),
                ),
            ),
        ),
        OK,
    )


class MongoHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while header := self.rfile.read(16):
            length, request_id, _, opcode = struct.unpack("<iiii", header)
            body = self.rfile.read(length - 16)
            if opcode == 2004:  # `OP_QUERY`: flags, namespace, skip, return, command.
                command_doc = body[body.index(b"\0", 4) + 9 :]
            else:
                # `OP_MSG`, section kind 0.
                assert opcode == 2013 and body[4] == 0, (opcode, body)
                command_doc = body[5:]

            # The first element of the command document: the command name and its value.
            name_end = command_doc.index(b"\0", 5)
            command = command_doc[5:name_end].decode()
            value = command_doc[name_end + 1 :]

            if command.lower() in ("hello", "ismaster"):
                reply = HELLO
            elif command == "find":
                # The value is a string: the length with the NUL terminator, then the bytes.
                (size,) = struct.unpack_from("<i", value)
                collection = value[4 : 4 + size - 1].decode()
                first_batch, cursor_id, _ = SCENARIOS[collection]
                reply = cursor_reply(collection, first_batch, "firstBatch", cursor_id)
            elif command == "getMore":
                # The value is the cursor ID.
                (cursor_id,) = struct.unpack_from("<q", value)
                collection, next_batch = NEXT_BATCHES[cursor_id]
                assert next_batch is not None, f"Unexpected getMore for {collection}"
                reply = cursor_reply(collection, next_batch, "nextBatch", 0)
            elif command == "killCursors":
                reply = document(OK)
            else:
                raise AssertionError(f"Unexpected command: {command}")

            if opcode == 2004:
                # `OP_REPLY`: flags, cursor ID, starting offset, document count.
                payload = struct.pack("<iqii", 0, 0, 0, 1) + reply
                reply_opcode = 1
            else:
                payload = struct.pack("<I", 0) + b"\0" + reply
                reply_opcode = 2013
            self.request.sendall(
                struct.pack(
                    "<iiii", 16 + len(payload), request_id + 1, request_id, reply_opcode
                )
                + payload
            )


class MongoServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with MongoServer(("127.0.0.1", int(sys.argv[1])), MongoHandler) as server:
        server.serve_forever()
