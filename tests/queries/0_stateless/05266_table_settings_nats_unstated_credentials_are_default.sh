#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no `NATS`.
#
# The `NATS` factory expands macros into its credential and certificate settings in place. It used to assign every
# one of them, which marked a setting nobody stated as changed, so a table stating none of them reported each as
# `other` - a value the engine chose - rather than `default`. Only a value a macro changes is assigned now.
#
# `clickhouse-local` with `message_queue_disable_insertion`, a server setting: a `NATS` table otherwise tries to
# reach its broker when it is created.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
CREATE TABLE n (a UInt64) ENGINE = NATS SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 's', nats_format = 'CSV';
SELECT name, value, source FROM system.table_settings
WHERE table = 'n' AND name IN ('nats_username', 'nats_password', 'nats_token', 'nats_credential_file', 'nats_credentials',
                               'nats_ca_file', 'nats_client_cert_file', 'nats_client_key_file')
ORDER BY name;" -- --message_queue_disable_insertion=1
