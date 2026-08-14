#!/usr/bin/env python3
"""A fake Keeper four letter command endpoint which responds to `mntr`,
`srvr`, `dirs` and `lgif` with values above 2^31 - 1, as on a Keeper which has
committed more than 2^31 - 1 transactions."""

import socket

RESPONSES = {
    b"ruok": "imok",
    b"isro": "rw",
    b"mntr": (
        "zk_version\tv26.6.1.1-testing\n"
        "zk_avg_latency\t0\n"
        "zk_packets_received\t3000000000\n"
        "zk_packets_sent\t3000000000\n"
        "zk_open_file_descriptor_count\t-1\n"
        "zk_max_file_descriptor_count\t18446744073709551615\n"
    ),
    b"srvr": (
        "ClickHouse Keeper version: v26.6.1.1-testing\n"
        "Latency min/avg/max: 0/0/0\n"
        "Received: 0\n"
        "Sent: 0\n"
        "Connections: 1\n"
        "Outstanding: 0\n"
        "Zxid: 0x80000000\n"
        "Mode: leader\n"
        "Node count: 5\n"
    ),
    b"dirs": (
        "snapshot_dir_size: 3000000000\n"
        "log_dir_size: 4000000000\n"
    ),
    b"lgif": (
        "first_log_idx\t1\n"
        "first_log_term\t1\n"
        "last_log_idx\t5000000000\n"
        "last_log_term\t1\n"
        "last_committed_log_idx\t5000000000\n"
        "leader_committed_log_idx\t5000000000\n"
        "target_committed_log_idx\t5000000000\n"
        "last_snapshot_idx\t2500000000\n"
    ),
}


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 9998))
    server.listen(10)
    while True:
        connection, _ = server.accept()
        try:
            command = connection.recv(4)
            connection.sendall(RESPONSES.get(command, "").encode())
        finally:
            connection.close()


if __name__ == "__main__":
    main()
