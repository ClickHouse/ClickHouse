#!/usr/bin/env python3
"""A TCP server that speaks just enough LDAP to stall one kind of operation.

Usage: fake_ldap_server.py <port> <bind|search>

  bind    never answers anything: the connection is established and the bind request is
          sent, so only `operation_timeout` can end the wait for its result.
  search  answers every bind request with success and nothing else, so the search that
          follows the bind can only end through `search_timeout`.

Connections stay open until the client closes them.
"""
import socket
import sys
import threading

BIND_REQUEST = 0x60
UNBIND_REQUEST = 0x42


def recv_exact(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def read_message(conn):
    """Reads one BER-encoded LDAPMessage; returns (message_id, protocol_op_tag), or None at EOF."""
    head = recv_exact(conn, 2)
    if head is None:
        return None
    length = head[1]
    if length & 0x80:
        raw = recv_exact(conn, length & 0x7F)
        if raw is None:
            return None
        length = int.from_bytes(raw, "big")
    body = recv_exact(conn, length)
    if body is None:
        return None
    # body: INTEGER messageID (02 <len> <bytes>), then the protocol op with its tag.
    id_len = body[1]
    message_id = int.from_bytes(body[2 : 2 + id_len], "big")
    return message_id, body[2 + id_len]


def ber_integer(value):
    # One byte more than the bit length needs keeps the top bit clear: BER integers are signed.
    raw = value.to_bytes((value.bit_length() + 8) // 8, "big")
    return b"\x02" + bytes([len(raw)]) + raw


def bind_response(message_id):
    # bindResponse { resultCode success, matchedDN "", diagnosticMessage "" }
    operation = b"\x61\x07\x0a\x01\x00\x04\x00\x04\x00"
    payload = ber_integer(message_id) + operation
    return b"\x30" + bytes([len(payload)]) + payload


def serve(conn, mode):
    try:
        while True:
            message = read_message(conn)
            if message is None:
                return
            message_id, operation = message
            if operation == UNBIND_REQUEST:
                return
            if operation == BIND_REQUEST and mode == "search":
                conn.sendall(bind_response(message_id))
            # Every other request is left without an answer.
    finally:
        conn.close()


def main():
    port, mode = int(sys.argv[1]), sys.argv[2]
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(16)
    print(f"listening on {port} ({mode})", flush=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=serve, args=(conn, mode), daemon=True).start()


if __name__ == "__main__":
    main()
