#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A pooled region keeps what the last request left in it, and the pool serves everybody: over the
# pipes a command only ever saw what it was sent, here it could read the tail of another user's
# query for free. The server therefore zeroes the region - but only when the borrower changes: the
# same borrower again sees its own leftovers, which keeps the cost off the common path.
#
# The pool lives as long as the process does, so every borrower has to come to the same one: each
# scenario runs one `clickhouse-local` that listens on HTTP and sends itself the queries of the other
# users. `http_make_head_request` is off, because a `HEAD` request runs the query too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function shm_function()
{
    # name, the options that make it what it is, the command
    echo "<function><type>executable_pool</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>$2<command>$3</command></function>"
}

{
    shm_function shm_peek "" shm_udf_peek.py
    shm_function shm_peek_extended "<shared_memory_max_size>262144</shared_memory_max_size>" \
        "shm_udf_peek.py --extend-to 131072 --peek-at 65536"
} | shm_functions

# The query, run as the user.
function as_user()
{
    echo "SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port'))
        || '/?user=$1&query=' || encodeURLComponent('$2'), TSV, 'result String') SETTINGS http_make_head_request = 0;"
}

SCRUBBED="SELECT sum(value) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryScrubbedBytes';"

function shm_local_listening()
{
    shm_local "
        CREATE USER other IDENTIFIED WITH no_password;
        GRANT SELECT ON *.* TO other;
        SYSTEM START LISTEN HTTP;
        $1" --listen_host 127.0.0.1 --http_port 0
}

echo "--- between users"
# The first request finds a fresh region and dirties 4 KiB past its input. The same user again: the
# leftovers are still there, nothing was scrubbed. Another user: the region is clean again, and the
# whole region was scrubbed, not just what the server knew it had used - the command wrote its 4 KiB
# where the server never looked. And back: the first user does not get the other's leftovers either.
shm_local_listening "
    SELECT shm_peek(1);
    SELECT shm_peek(1);
    $SCRUBBED
    $(as_user other "SELECT shm_peek(1)")
    $SCRUBBED
    SELECT shm_peek(1);
"

echo "--- between roles of one user"
# The boundary is the borrower's identity, not the login: the roles a query runs with can be tied to
# different row policies, so what the same user's query saw under one set of roles must not be
# readable by the command while it serves that user under another - the same line the query result
# cache draws. Roles are switched through the user's default roles, which every request picks up.
shm_local_listening "
    CREATE ROLE role_a;
    CREATE ROLE role_b;
    GRANT SELECT ON *.* TO role_a, role_b;
    GRANT role_a, role_b TO other;
    SET DEFAULT ROLE role_a TO other;
    $(as_user other "SELECT shm_peek(1)")
    $(as_user other "SELECT shm_peek(1)")
    $SCRUBBED
    SET DEFAULT ROLE role_b TO other;
    $(as_user other "SELECT shm_peek(1)")
    $SCRUBBED
    $(as_user other "SELECT shm_peek(1)")
    $SCRUBBED
"

echo "--- between users, a tail the server never mapped"
# The file behind a pooled region can be longer than what the server has mapped - a command extended
# it (only shrinking is sealed). The command maps the whole file, so a stale tail beyond the server's
# mapping is as readable as the rest, and a scrub that only covered the mapping would leave exactly
# the bytes another user's command could still read. Here the command extends a 64 KiB region to
# 128 KiB and probes the tail at 64 KiB. The cap is 256 KiB: the page the command dirties in the tail
# is a page the server did not commit, and the hand-back takes such pages for pages past the end of
# the file until it maps the file and sees; at a cap of 128 KiB that would cost the worker its place
# in the pool, and the tail with it. The scrub covers the whole 128 KiB file.
shm_local_listening "
    SELECT shm_peek_extended(1);
    SELECT shm_peek_extended(1);
    $SCRUBBED
    $(as_user other "SELECT shm_peek_extended(1)")
    $SCRUBBED
    SELECT shm_peek_extended(1);
"
