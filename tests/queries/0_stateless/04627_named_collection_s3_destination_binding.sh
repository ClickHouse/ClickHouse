#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OWN="http://localhost:11111/test"
OTHER="http://127.0.0.1:11112/test"

c() { echo "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1"; }

DATA="${CLICKHOUSE_TEST_UNIQUE_NAME}_row.csv"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION
    s3('$OWN/$DATA', 'test', 'testtest', 'CSV', 'a String') SELECT 'payload'"

# For the refusal arms: the check either fires or it does not, and nothing downstream can produce this
# message.
run() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 \
        | grep -qF "Override not allowed for 'url'" && echo refused || echo allowed
}

# For the arms that must stay allowed. `run` reports "allowed" on any downstream failure, so a
# compatibility arm asserted that way cannot redden when the check becomes too broad. These assert the
# row instead: a real round trip to the collection's own origin.
allowed_reads() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 | grep -qxF payload && echo payload || echo "NOT-READ"
}

# For an arm that must pass the check but cannot complete: assert the *specific* downstream error, and
# assert the refusal is absent, so a refusal whose text happens to contain the pattern cannot pass.
allowed_fails_with() {
    local out
    out=$(${CLICKHOUSE_CLIENT} -m --query "$2" 2>&1)
    if grep -qF "Override not allowed for 'url'" <<< "$out"; then echo "REFUSED"
    elif grep -qF "$1" <<< "$out"; then echo "passed-check"
    else echo "NOT-REACHED"; fi
}

# For an arm whose credentials cannot read anything anywhere (an anonymous client against a bucket
# that requires auth): assert that a request was nevertheless issued. The check throws before any S3
# client is built, so an S3-level outcome of any kind proves it let the request through.
allowed_reaches_s3() {
    ${CLICKHOUSE_CLIENT} -m --query "$1" 2>&1 | grep -qE 'payload|S3_ERROR' && echo "reached-s3" || echo "NOT-REACHED"
}

# For the `no_sign_request` arm, whose whole claim is that nothing signs: any S3-level outcome is too
# weak, because those stored keys are valid for the collection's own origin, so a silently re-enabled
# signature would read the row and still look green. Send the request to a listener instead and assert
# on the bytes: the key must not appear, and a request must have arrived at all.
CAPTURE_PY="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_capture.py"
cat > "$CAPTURE_PY" <<'PY'
import socket, sys, threading
out, portfile = sys.argv[1], sys.argv[2]
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", 0)); srv.listen(8); srv.settimeout(60)
with open(portfile, "w") as f:
    f.write(str(srv.getsockname()[1]))
sink = open(out, "ab", buffering=0)
def serve(conn):
    try:
        conn.settimeout(5)
        sink.write(conn.recv(65536))
        conn.sendall(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    except Exception:
        pass
    finally:
        conn.close()
while True:
    try:
        conn, _ = srv.accept()
    except Exception:
        break
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
PY

# Runs one query against a throwaway listener and reports what reached the wire.
#   $1 = the stored secret that must NOT appear    $2 = SQL, with __CAPTURE__ standing in for the origin
capture_must_not_leak() {
    local secret="$1" sql="$2"
    local cap="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_cap.txt"
    local portfile="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_port.txt"
    : > "$cap"; : > "$portfile"
    python3 "$CAPTURE_PY" "$cap" "$portfile" &
    local pid=$!
    local port=""
    for _ in $(seq 1 80); do
        port=$(cat "$portfile" 2>/dev/null)
        [ -n "$port" ] && break
        sleep 0.25
    done
    if [ -z "$port" ]; then kill "$pid" 2>/dev/null; echo "NO-LISTENER"; return; fi
    ${CLICKHOUSE_CLIENT} -m --query "${sql//__CAPTURE__/http://127.0.0.1:$port}" > /dev/null 2>&1
    sleep 1
    kill "$pid" 2>/dev/null
    # The refusal happens before any client is built, so an empty capture is the check having fired.
    if [ ! -s "$cap" ]; then echo "NO-REQUEST"
    elif grep -qF "$secret" "$cap"; then echo "key-on-the-wire"
    else echo "no-key-on-the-wire"; fi
}

echo '--- credentialed collection, url moved to another origin'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c keys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c keys) AS
    url = '$OWN/', access_key_id = 'test', secret_access_key = 'testtest'"
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- same origin, different path: still allowed'
allowed_reads "SELECT * FROM s3($(c keys), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- each origin component alone is enough to refuse'
# `$OWN` and `$OTHER` differ in host and port at once, so without these three no arm shows which
# component the comparison reads.
run "SELECT * FROM s3($(c keys), url = 'http://127.0.0.1:11111/test/x.csv', format = 'CSV', structure = 'a String')"
run "SELECT * FROM s3($(c keys), url = 'http://localhost:11112/test/x.csv', format = 'CSV', structure = 'a String')"
run "SELECT * FROM s3($(c keys), url = 'https://localhost:11111/test/x.csv', format = 'CSV', structure = 'a String')"

echo '--- a url that writes no port compares against the scheme default'
# `Poco::URI::getPort` substitutes the well-known port, so a stored `http://localhost/` authorises :80.
# Nothing listens there, so the passing half asserts the request went out rather than a row coming back;
# the retry cap keeps that from taking minutes.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c dflt)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c dflt) AS
    url = 'http://localhost/', access_key_id = 'test', secret_access_key = 'testtest'"
allowed_reaches_s3 "SELECT * FROM s3($(c dflt), url = 'http://localhost:80/x.csv', format = 'CSV',
    structure = 'a String') SETTINGS s3_retry_attempts = 1, max_execution_time = 25"
run "SELECT * FROM s3($(c dflt), url = 'http://localhost:81/x.csv', format = 'CSV',
    structure = 'a String') SETTINGS s3_retry_attempts = 1, max_execution_time = 25"

echo '--- credential-free collection keeps full override freedom'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c anon)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c anon) AS url = '$OTHER/'"
# A credential-free collection reads anonymously, which this bucket refuses, so assert the request was
# issued rather than a row returned.
allowed_reaches_s3 "SELECT * FROM s3($(c anon), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- explicit OVERRIDABLE wins over the credential binding'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c open)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c open) AS
    url = '$OTHER/' OVERRIDABLE, access_key_id = 'test', secret_access_key = 'testtest'"
allowed_reads "SELECT * FROM s3($(c open), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- query replaces the whole key pair: the collection supplies nothing, so no binding'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c otherkeys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c otherkeys) AS
    url = '$OTHER/', access_key_id = 'stored', secret_access_key = 'storedsecret'"
allowed_reads "SELECT * FROM s3($(c otherkeys), url = '$OWN/$DATA',
    access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV', structure = 'a String')"

echo '--- partial replacement: the stored secret_access_key still signs'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    access_key_id = 'other', format = 'CSV', structure = 'a String')"

echo '--- query-supplied role_arn: the collection keys are dropped, the query role authenticates'
# No STS endpoint answers here, so this arm cannot complete; assert the credential-resolution failure
# that is only reachable once the destination check has let the request through.
allowed_fails_with "role" "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    role_arn = 'arn:aws:iam::111111111111:role/r', format = 'CSV', structure = 'a String')"

echo '--- gcp_oauth sends a bearer token, so its ADC secrets bind the destination too'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c gcp)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c gcp) AS
    url = '$OWN/', http_client = 'gcp_oauth',
    google_adc_client_id = 'cid', google_adc_client_secret = 'csecret', google_adc_refresh_token = 'rtoken'"
run "SELECT * FROM s3($(c gcp), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- no_sign_request disables SigV4 only, the bearer token still goes out'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c gcpnosign)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c gcpnosign) AS
    url = '$OWN/', http_client = 'gcp_oauth', no_sign_request = 1,
    google_adc_client_id = 'cid', google_adc_client_secret = 'csecret', google_adc_refresh_token = 'rtoken'"
run "SELECT * FROM s3($(c gcpnosign), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

echo '--- partial ADC replacement keeps the binding'
run "SELECT * FROM s3($(c gcp), url = '$OTHER/x.csv',
    google_adc_client_id = 'other', format = 'CSV', structure = 'a String')"

echo '--- under gcp_oauth the stored AWS keys are inert, so a complete ADC replacement releases'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c gcpkeys)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c gcpkeys) AS
    url = '$OWN/', http_client = 'gcp_oauth',
    access_key_id = 'test', secret_access_key = 'testtest',
    google_adc_client_id = 'cid', google_adc_client_secret = 'csecret', google_adc_refresh_token = 'rtoken'"
run "SELECT * FROM s3($(c gcpkeys), url = '$OTHER/x.csv',
    google_adc_client_id = 'own', google_adc_client_secret = 'ownsecret',
    google_adc_refresh_token = 'owntoken', format = 'CSV', structure = 'a String')"

echo '--- no_sign_request with static keys: nothing signs, so the destination is not bound'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c nosign)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c nosign) AS
    url = '$OTHER/', access_key_id = 'AKIAIOSFODNN7EXAMPLE', secret_access_key = 'testtest', no_sign_request = 1"
# Two path segments: `S3::URI` reads the first as the bucket, so a single-segment path leaves no key
# and no request is ever issued.
capture_must_not_leak AKIAIOSFODNN7EXAMPLE \
    "SELECT * FROM s3($(c nosign), url = '__CAPTURE__/test/$DATA', format = 'CSV', structure = 'a String')"

echo '--- control: the same listener does see the key once SigV4 is on, so the arm above can fail'
# Without this, "no key on the wire" would also be the reading for a listener nothing ever reaches.
# `OVERRIDABLE` is what lets the destination move at all here; the credentials are otherwise identical.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c signing)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c signing) AS
    url = '$OTHER/' OVERRIDABLE, access_key_id = 'AKIAIOSFODNN7EXAMPLE', secret_access_key = 'testtest'"
capture_must_not_leak AKIAIOSFODNN7EXAMPLE \
    "SELECT * FROM s3($(c signing), url = '__CAPTURE__/test/$DATA', format = 'CSV', structure = 'a String')"

echo '--- a collection that stores no url authorises no destination for its keys'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c keysonly)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c keysonly) AS
    access_key_id = 'test', secret_access_key = 'testtest'"
run "SELECT * FROM s3($(c keysonly), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- the same rule reaches DatabaseS3, where an absent url means every table name is a full url'
# `getFullUrl` returns the bare table name when the collection stores no url prefix, so each query
# picks the destination for the collection's keys. Refused for the same reason as the arm above; the
# changelog names this shape because it used to be accepted.
run "CREATE DATABASE ${CLICKHOUSE_DATABASE}_kodb ENGINE = S3($(c keysonly))"

echo '--- a fresh credentialed cross-origin CREATE is refused for both S3 engines'
# Each engine reaches the check through its own creator, so the replay predicate each one reads needs its
# own arm; the exemption arms below would otherwise be the only thing exercising either.
run "CREATE TABLE ${CLICKHOUSE_DATABASE}.freshtbl (a String)
    ENGINE = S3($(c keys), url = '$OTHER/x.csv', format = 'CSV')"
run "CREATE TABLE ${CLICKHOUSE_DATABASE}.freshq (a String)
    ENGINE = S3Queue($(c keys), url = '$OTHER/q/*.csv', format = 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '/clickhouse/${CLICKHOUSE_TEST_UNIQUE_NAME}_q'"

echo '--- a JSON-AST payload cannot claim the definition came from stored metadata'
# The replay exemption keys on `attach_short_syntax`, which no SQL syntax sets, so the `clickhouse_json`
# dialect must not accept it as an input. An `ATTACH TABLE ... ENGINE =` form only reaches the check in a
# `Memory`-engine database; `Atomic` rejects that shape upstream, which would measure nothing.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_mem"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_mem ENGINE = Memory"
FORGE_SQL="ATTACH TABLE ${CLICKHOUSE_DATABASE}_mem.forged (a String) ENGINE = S3($(c keys), url = '$OTHER/x.csv', format = 'CSV')"
# TabSeparatedRaw: the default JSON envelope escapes the slashes, and the escaped payload then fails
# upstream with `Host is empty in S3 URI` on both arms.
FORGE_JSON=$(${CLICKHOUSE_CLIENT} --format=TabSeparatedRaw -q \
    "SELECT replace(parseQueryToJSON('${FORGE_SQL//\'/\\\'}'), '\"attach_short_syntax\":false', '\"attach_short_syntax\":true')")
# The flag has to be in the payload for the arm to assert anything.
echo "flagset $(grep -c '"attach_short_syntax":true' <<< "$FORGE_JSON")"
${CLICKHOUSE_CLIENT} --enable_json_ast_dialect=1 --dialect=clickhouse_json --query="$FORGE_JSON" 2>&1 \
    | grep -qF "'attach_short_syntax' is not accepted" && echo refused || echo allowed
echo "created $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}_mem' AND name = 'forged'")"

echo '--- filename cannot move the origin: an absolute value is rejected before any request'
# `path::operator/` replaces the left operand when the right is absolute, so pin the rejection: were
# `S3::URI` ever to accept such a value, the destination would move and this arm must be revisited.
for f in '//127.0.0.1:11112/test/x.csv' '/steal/x.csv'; do
    ${CLICKHOUSE_CLIENT} -m --query "SELECT * FROM s3($(c keys), filename = '$f',
        format = 'CSV', structure = 'a String')" 2>&1 \
        | grep -qF "Host is empty in S3 URI" && echo "no-host" || echo "REACHED-HOST"
done

echo '--- backups: BackupInfo does not go through findOverrideForbiddingKey, so the seam is its own'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t; CREATE TABLE ${CLICKHOUSE_DATABASE}.t (a UInt8) ENGINE = Memory"
run "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO S3($(c keys), url = '$OTHER/bk')"

echo '--- DatabaseS3: getTableImpl rebuilds positional s3() args, so provenance is gone downstream'
run "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db ENGINE = S3($(c keys), url = '$OTHER/')"

echo '--- a relative stored url declares no origin, so a materialized s3_base replay still attaches'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c rel)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c rel) AS
    url = '${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION
    s3('$OWN/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', 'test', 'testtest', 'CSV', 'a UInt8') SELECT 1"
${CLICKHOUSE_CLIENT} -q "SET s3_base = '$OWN/';
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.replay;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.replay (a UInt8) ENGINE = S3($(c rel))"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.replay"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.replay"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.replay"

echo '--- a definition persisted before this rule loads with a warning instead of aborting startup'
# Built with supported statements only: create the table while the collection has no credentials (so
# the cross-origin override is allowed), then add them. That is the upgrade shape - a definition that
# was legal when written and is not now - and for a table loaded at startup a refusal here is the
# server failing to start rather than one unreadable table.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c later)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c later) AS url = '$OTHER/', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.persisted;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.persisted (a String) ENGINE = S3($(c later), url = '$OWN/$DATA')"
${CLICKHOUSE_CLIENT} -q "ALTER NAMED COLLECTION $(c later)
    SET access_key_id = 'test', secret_access_key = 'testtest'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.persisted"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.persisted" 2>&1 \
    | grep -qF "Override not allowed for 'url'" && echo "REFUSED" || echo "loaded"
allowed_reads "SELECT * FROM ${CLICKHOUSE_DATABASE}.persisted"

echo '--- the replay exemption is not reachable from a fresh query against the same collection'
run "SELECT * FROM s3($(c later), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- RESTORE replays the stored definition too, so it loads rather than failing the restore'
# A local `Disk` destination, so this arm exercises the restore replay and not the backup seam. The
# restore runs the stored CREATE at strictness SECONDARY_CREATE, which is neither FORCE_* nor a short
# ATTACH, so `mode` alone does not mark it.
BK="${CLICKHOUSE_TEST_UNIQUE_NAME}_bk"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE ${CLICKHOUSE_DATABASE}.persisted TO Disk('backups', '$BK')" > /dev/null
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.persisted"
allowed_fails_with RESTORED "RESTORE TABLE ${CLICKHOUSE_DATABASE}.persisted FROM Disk('backups', '$BK')"
allowed_reads "SELECT * FROM ${CLICKHOUSE_DATABASE}.persisted"

echo '--- and a fresh query against that same collection is still refused'
run "SELECT * FROM s3($(c later), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

echo '--- the replay warning names the destination without disclosing a url credential'
# A stored url may carry userinfo or a presigned signature, and the server log sits outside the
# `SHOW_NAMED_COLLECTIONS_SECRETS` grant that reading the collection needs.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c userinfo)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c userinfo) AS
    url = 'http://u:${CLICKHOUSE_TEST_UNIQUE_NAME}_pw@127.0.0.1:11112/test/', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.masked;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.masked (a String) ENGINE = S3($(c userinfo), url = '$OWN/$DATA')"
${CLICKHOUSE_CLIENT} -q "ALTER NAMED COLLECTION $(c userinfo)
    SET access_key_id = 'test', secret_access_key = 'testtest'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.masked"
MASK_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_mask"
${CLICKHOUSE_CLIENT} --query_id "$MASK_QID" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.masked" > /dev/null 2>&1
# Both assertions in one arm: a warning that vanished would otherwise read as a redaction. Scoped by
# query_id and logger_name, so the probe query's own text is not what is being counted.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "SELECT 'warning', count() >= 1 FROM system.text_log
    WHERE query_id = '$MASK_QID' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning';
    SELECT 'secret', count() FROM system.text_log
    WHERE query_id = '$MASK_QID' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'
      AND position(message, '${CLICKHOUSE_TEST_UNIQUE_NAME}_pw') > 0"

echo '--- and the destination it names is masked too, not only the origin it compares against'
# The two sides of that message are redacted independently: the origin cannot hold a credential by
# construction, the reported destination can, so each needs its own arm.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c effuserinfo)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c effuserinfo) AS url = '$OTHER/', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.maskedeff;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.maskedeff (a String)
    ENGINE = S3($(c effuserinfo), url = 'http://u:${CLICKHOUSE_TEST_UNIQUE_NAME}_pw2@localhost:11111/test/$DATA')"
${CLICKHOUSE_CLIENT} -q "ALTER NAMED COLLECTION $(c effuserinfo)
    SET access_key_id = 'test', secret_access_key = 'testtest'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.maskedeff"
MASK_QID2="${CLICKHOUSE_TEST_UNIQUE_NAME}_maskeff"
${CLICKHOUSE_CLIENT} --query_id "$MASK_QID2" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.maskedeff" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "SELECT 'warning', count() >= 1 FROM system.text_log
    WHERE query_id = '$MASK_QID2' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning';
    SELECT 'secret', count() FROM system.text_log
    WHERE query_id = '$MASK_QID2' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'
      AND position(message, '${CLICKHOUSE_TEST_UNIQUE_NAME}_pw2') > 0"

echo '--- a presigned signature in the destination is masked as well as userinfo'
# `maskedForLog` runs two independent scans, and userinfo is the only one the arms above reach, so a
# dropped presigned scan would leave them green.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c presigned)"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION $(c presigned) AS url = '$OTHER/', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.presigned;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.presigned (a String) ENGINE = S3($(c presigned),
    url = '$OWN/$DATA?X-Amz-Signature=${CLICKHOUSE_TEST_UNIQUE_NAME}_sig&X-Amz-Credential=x')"
${CLICKHOUSE_CLIENT} -q "ALTER NAMED COLLECTION $(c presigned)
    SET access_key_id = 'test', secret_access_key = 'testtest'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.presigned"
MASK_QID3="${CLICKHOUSE_TEST_UNIQUE_NAME}_maskpre"
${CLICKHOUSE_CLIENT} --query_id "$MASK_QID3" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.presigned" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "SELECT 'warning', count() >= 1 FROM system.text_log
    WHERE query_id = '$MASK_QID3' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning';
    SELECT 'secret', count() FROM system.text_log
    WHERE query_id = '$MASK_QID3' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'
      AND position(message, '${CLICKHOUSE_TEST_UNIQUE_NAME}_sig') > 0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.presigned"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.freshtbl"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.freshq"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_mem"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.maskedeff"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.masked"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.persisted"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_kodb"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.replay"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t"
for n in keys anon open otherkeys gcp gcpnosign gcpkeys nosign signing keysonly rel later userinfo effuserinfo dflt presigned; do
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS $(c $n)"
done
