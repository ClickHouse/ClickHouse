#!/usr/bin/env python3
# Tags: no-ordinary-database, no-fasttest, no-parallel-replicas
# Transactions need MergeTree tables in a non-Ordinary database, and the fast test config does not enable them.

# The `ResetSession` packet is rejected inside a transaction, so that the transaction is not rolled back
# without notice. The transaction stays usable after the rejected reset.

import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from native_session_client import NativeSessionClient, expect_error


def main():
    with NativeSessionClient() as client:
        client.query("DROP TABLE IF EXISTS reset_session_txn")
        client.query("CREATE TABLE reset_session_txn (x Int32) ENGINE = MergeTree ORDER BY x")

        client.query("BEGIN TRANSACTION")
        client.query("INSERT INTO reset_session_txn SETTINGS async_insert = 0 SELECT 1")
        expect_error("INVALID_TRANSACTION", client.reset_session)
        print("rows in the transaction:", client.value("SELECT toString(count()) FROM reset_session_txn"))
        client.query("COMMIT")

        client.reset_session()
        print("rows after commit:", client.value("SELECT toString(count()) FROM reset_session_txn"))
        client.query("DROP TABLE reset_session_txn")


main()
