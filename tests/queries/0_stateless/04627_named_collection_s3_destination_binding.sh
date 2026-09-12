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

# Every fixture whose position in the file does not matter, in one invocation.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3('$OWN/$DATA', 'test', 'testtest', 'CSV', 'a String') SELECT 'payload';
    INSERT INTO FUNCTION
        s3('$OWN/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', 'test', 'testtest', 'CSV', 'a UInt8') SELECT 1;
    DROP NAMED COLLECTION IF EXISTS $(c keys);
    CREATE NAMED COLLECTION $(c keys) AS
        url = '$OWN/', access_key_id = 'test', secret_access_key = 'testtest';
    DROP NAMED COLLECTION IF EXISTS $(c dflt);
    CREATE NAMED COLLECTION $(c dflt) AS
        url = 'http://localhost/', access_key_id = 'test', secret_access_key = 'testtest';
    DROP NAMED COLLECTION IF EXISTS $(c anon);
    CREATE NAMED COLLECTION $(c anon) AS url = '$OTHER/';
    DROP NAMED COLLECTION IF EXISTS $(c open);
    CREATE NAMED COLLECTION $(c open) AS
        url = '$OTHER/' OVERRIDABLE, access_key_id = 'test', secret_access_key = 'testtest';
    DROP NAMED COLLECTION IF EXISTS $(c otherkeys);
    CREATE NAMED COLLECTION $(c otherkeys) AS
        url = '$OTHER/', access_key_id = 'stored', secret_access_key = 'storedsecret';
    DROP NAMED COLLECTION IF EXISTS $(c gcp);
    CREATE NAMED COLLECTION $(c gcp) AS
        url = '$OWN/', http_client = 'gcp_oauth',
        google_adc_client_id = 'cid', google_adc_client_secret = 'csecret',
        google_adc_refresh_token = 'rtoken';
    DROP NAMED COLLECTION IF EXISTS $(c gcpnosign);
    CREATE NAMED COLLECTION $(c gcpnosign) AS
        url = '$OWN/', http_client = 'gcp_oauth', no_sign_request = 1,
        google_adc_client_id = 'cid', google_adc_client_secret = 'csecret',
        google_adc_refresh_token = 'rtoken';
    DROP NAMED COLLECTION IF EXISTS $(c gcpkeys);
    CREATE NAMED COLLECTION $(c gcpkeys) AS
        url = '$OWN/', http_client = 'gcp_oauth',
        access_key_id = 'test', secret_access_key = 'testtest',
        google_adc_client_id = 'cid', google_adc_client_secret = 'csecret',
        google_adc_refresh_token = 'rtoken';
    DROP NAMED COLLECTION IF EXISTS $(c xid);
    CREATE NAMED COLLECTION $(c xid) AS url = '$OTHER/',
        access_key_id = 'stored', secret_access_key = 'storedsecret', external_id = 'xid';
    DROP NAMED COLLECTION IF EXISTS $(c gcpmeta);
    CREATE NAMED COLLECTION $(c gcpmeta) AS
        url = '$OTHER/', http_client = 'gcp_oauth', service_account = 'sa',
        metadata_service = '127.0.0.1:11114', request_token_path = '/computeMetadata/v1';
    DROP NAMED COLLECTION IF EXISTS $(c nosign);
    CREATE NAMED COLLECTION $(c nosign) AS url = '$OTHER/',
        access_key_id = 'AKIAIOSFODNN7EXAMPLE', secret_access_key = 'testtest', no_sign_request = 1;
    DROP NAMED COLLECTION IF EXISTS $(c signing);
    CREATE NAMED COLLECTION $(c signing) AS url = '$OTHER/' OVERRIDABLE,
        access_key_id = 'AKIAIOSFODNN7EXAMPLE', secret_access_key = 'testtest';
    DROP NAMED COLLECTION IF EXISTS $(c keysonly);
    CREATE NAMED COLLECTION $(c keysonly) AS access_key_id = 'test', secret_access_key = 'testtest';
    DROP NAMED COLLECTION IF EXISTS $(c keyidonly);
    CREATE NAMED COLLECTION $(c keyidonly) AS url = '$OWN/', access_key_id = 'test';
    DROP NAMED COLLECTION IF EXISTS $(c secretonly);
    CREATE NAMED COLLECTION $(c secretonly) AS url = '$OWN/', secret_access_key = 'testtest';
    DROP NAMED COLLECTION IF EXISTS $(c rel);
    CREATE NAMED COLLECTION $(c rel) AS url = '${CLICKHOUSE_TEST_UNIQUE_NAME}.csv',
        access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV';
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (a UInt8) ENGINE = Memory;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_mem;
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_mem ENGINE = Memory"

# Arms are queued and run as one multi-statement invocation. `--ignore-error` is what makes that
# sound: every statement runs, and a failing one reports inline instead of ending the invocation.
#
# An arm's own output is still its whole oracle. `MARK` delimits the arms in the merged stream, and
# `flush` hands each arm exactly the lines between its own marker and the next one.
MARK='@@arm'
QUEUE=''
KINDS=()
NEEDLES=()

# Headers are queued alongside the arms rather than echoed, so a flushed batch keeps headers and
# results interleaved.
h() { KINDS+=(echo); NEEDLES+=("$1"); }

# $1 = classifier, $2 = SQL, $3 = the pattern `expect` needs
push() {
    QUEUE+="SELECT '$MARK';
$2;
"
    KINDS+=("$1")
    NEEDLES+=("${3-}")
}

# For the refusal arms: the check either fires or it does not, and nothing downstream can produce this
# message.
run() { push refused "$1"; }

# For the arms that must stay allowed. `refused` reports "allowed" on any downstream failure, so a
# compatibility arm asserted that way cannot redden when the check becomes too broad. These assert the
# row instead: a real round trip to the collection's own origin.
allowed_reads() { push payload "$1"; }

# For an arm that must pass the check but cannot complete: assert the *specific* downstream error, and
# assert the refusal is absent, so a refusal whose text happens to contain the pattern cannot pass. The
# client echoes the failing statement, so a needle that occurs in the statement itself always matches.
allowed_fails_with() { push expect "$2" "$1"; }

# For an arm whose credentials cannot read anything anywhere (an anonymous client against a bucket
# that requires auth): assert that a request was nevertheless issued. The check throws before any S3
# client is built, so an S3-level outcome of any kind proves it let the request through.
allowed_reaches_s3() { push reached "$1"; }

# For a statement whose result is the assertion as it stands (a count, a label the query itself
# formats): its slice goes to stdout as it came back.
raw() { push raw "$1"; }

# For fixture statements, which assert nothing and must produce no output.
stmt() { push none "$1"; }

# Applies each queued arm's classifier to that arm's own slice of the output, in order.
flush() {
    [ "${#KINDS[@]}" -gt 0 ] || return 0
    local out='' i=0 arm=0 slice
    if [ -n "$QUEUE" ]; then
        out=$(${CLICKHOUSE_CLIENT} --ignore-error -m --query "$QUEUE" 2>&1)
        # A missing marker would silently shift every later arm onto the wrong slice, so read the
        # count back rather than trusting the run to have produced one per arm.
        local want seen
        want=$(grep -c "^SELECT '$MARK';$" <<< "$QUEUE")
        seen=$(grep -cxF "$MARK" <<< "$out")
        if [ "$seen" != "$want" ]; then
            echo "MARKER-MISMATCH want $want got $seen"
            QUEUE=''; KINDS=(); NEEDLES=(); return
        fi
    fi
    QUEUE=''
    while [ "$i" -lt "${#KINDS[@]}" ]; do
        if [ "${KINDS[$i]}" = echo ]; then
            echo "${NEEDLES[$i]}"
            i=$((i + 1)); continue
        fi
        arm=$((arm + 1))
        slice=$(awk -v m="$MARK" -v want="$arm" '
            $0 == m { n++; next }
            n == want' <<< "$out")
        case "${KINDS[$i]}" in
            refused)
                grep -qF "Override not allowed for 'url'" <<< "$slice" \
                    && echo refused || echo allowed ;;
            payload)
                grep -qxF payload <<< "$slice" && echo payload || echo "NOT-READ" ;;
            reached)
                grep -qE 'payload|S3_ERROR' <<< "$slice" \
                    && echo "reached-s3" || echo "NOT-REACHED" ;;
            expect)
                if grep -qF "Override not allowed for 'url'" <<< "$slice"; then echo "REFUSED"
                elif grep -qF "${NEEDLES[$i]}" <<< "$slice"; then echo "passed-check"
                else echo "NOT-REACHED"; fi ;;
            raw)
                # The error stream is merged in so the refusal arms can read it, but a failing
                # statement's message is not asserted output: the runner fails any test whose
                # stdout holds the word Exception.
                if grep -qF 'DB::Exception' <<< "$slice"
                then printf '%s\n' "$slice" >&2; echo "RAW-FAILED"
                else printf '%s\n' "$slice"; fi ;;
            loaded)
                grep -qF "Override not allowed for 'url'" <<< "$slice" \
                    && echo "REFUSED" || echo "loaded" ;;
            nohost)
                grep -qF "Host is empty in S3 URI" <<< "$slice" \
                    && echo "no-host" || echo "REACHED-HOST" ;;
            none)
                # A fixture that failed would otherwise be silent until a later arm read the state.
                if grep -qF 'DB::Exception' <<< "$slice"
                then printf '%s\n' "$slice" >&2; echo "FIXTURE-FAILED"; fi ;;
        esac
        i=$((i + 1))
    done
    KINDS=(); NEEDLES=()
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
    flush
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

h '--- credentialed collection, url moved to another origin'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

h '--- same origin, different path: still allowed'
allowed_reads "SELECT * FROM s3($(c keys), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

h '--- each origin component alone is enough to refuse'
# `$OWN` and `$OTHER` differ in host and port at once, so without these three no arm shows which
# component the comparison reads.
run "SELECT * FROM s3($(c keys), url = 'http://127.0.0.1:11111/test/x.csv', format = 'CSV', structure = 'a String')"
run "SELECT * FROM s3($(c keys), url = 'http://localhost:11112/test/x.csv', format = 'CSV', structure = 'a String')"
run "SELECT * FROM s3($(c keys), url = 'https://localhost:11111/test/x.csv', format = 'CSV', structure = 'a String')"

h '--- a url that writes no port compares against the scheme default'
# `Poco::URI::getPort` substitutes the well-known port, so a stored `http://localhost/` authorises :80.
# Nothing listens there, so the passing half asserts the request went out rather than a row coming back;
# the retry cap keeps that from taking minutes.
allowed_reaches_s3 "SELECT * FROM s3($(c dflt), url = 'http://localhost:80/x.csv', format = 'CSV',
    structure = 'a String') SETTINGS s3_retry_attempts = 1, max_execution_time = 25"
run "SELECT * FROM s3($(c dflt), url = 'http://localhost:81/x.csv', format = 'CSV',
    structure = 'a String') SETTINGS s3_retry_attempts = 1, max_execution_time = 25"

h '--- credential-free collection keeps full override freedom'
# A credential-free collection reads anonymously, which this bucket refuses, so assert the request was
# issued rather than a row returned.
allowed_reaches_s3 "SELECT * FROM s3($(c anon), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

h '--- explicit OVERRIDABLE wins over the credential binding'
allowed_reads "SELECT * FROM s3($(c open), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

h '--- query replaces the whole key pair: the collection supplies nothing, so no binding'
allowed_reads "SELECT * FROM s3($(c otherkeys), url = '$OWN/$DATA',
    access_key_id = 'test', secret_access_key = 'testtest', format = 'CSV', structure = 'a String')"

h '--- partial replacement: the stored secret_access_key still signs'
run "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    access_key_id = 'other', format = 'CSV', structure = 'a String')"

h '--- half a key pair is never sent, so it authorises nothing and binds nothing'
# The arm above is the control: there the query completes the pair, so the stored half signs and binds.
# Nothing listens on that destination, so the retry cap keeps the passing half from taking minutes.
allowed_reaches_s3 "SELECT * FROM s3($(c keyidonly), url = '$OTHER/x.csv', format = 'CSV',
    structure = 'a String') SETTINGS s3_retry_attempts = 1, max_execution_time = 25"
allowed_reaches_s3 "SELECT * FROM s3($(c secretonly), url = '$OTHER/x.csv', format = 'CSV',
    structure = 'a String') SETTINGS s3_retry_attempts = 1, max_execution_time = 25"

h '--- query-supplied role_arn: the collection keys are dropped, the query role authenticates'
# Nothing serves that object, so an S3-level outcome is what shows the destination check let the request
# through; a needle naming the role would only match the echoed statement. The retry cap bounds a
# request that cannot succeed.
allowed_fails_with "S3_ERROR" "SELECT * FROM s3($(c keys), url = '$OTHER/x.csv',
    role_arn = 'arn:aws:iam::111111111111:role/r', format = 'CSV', structure = 'a String')
    SETTINGS s3_retry_attempts = 1"

h '--- gcp_oauth sends a bearer token, so its ADC secrets bind the destination too'
run "SELECT * FROM s3($(c gcp), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

h '--- no_sign_request disables SigV4 only, the bearer token still goes out'
run "SELECT * FROM s3($(c gcpnosign), url = '$OTHER/x.csv', format = 'CSV', structure = 'a String')"

h '--- partial ADC replacement keeps the binding'
run "SELECT * FROM s3($(c gcp), url = '$OTHER/x.csv',
    google_adc_client_id = 'other', format = 'CSV', structure = 'a String')"

h '--- under gcp_oauth the stored AWS keys are inert, so a complete ADC replacement releases'
run "SELECT * FROM s3($(c gcpkeys), url = '$OTHER/x.csv',
    google_adc_client_id = 'own', google_adc_client_secret = 'ownsecret',
    google_adc_refresh_token = 'owntoken', format = 'CSV', structure = 'a String')"

h '--- a stored external_id with no role to assume is read by nothing, so it binds nothing'
allowed_reads "SELECT * FROM s3($(c xid), url = '$OWN/$DATA', access_key_id = 'test',
    secret_access_key = 'testtest', format = 'CSV', structure = 'a String')"

h '--- control: alongside a surviving role_arn the same stored external_id is the STS secret and binds'
# The restricted path drops a stored `external_id` under a query-supplied role, so this is the arm that
# needs the restriction off to reach the case where the STS wrapper reads one.
run "SELECT * FROM s3($(c xid), url = '$OWN/$DATA', access_key_id = 'test',
    secret_access_key = 'testtest', role_arn = 'arn:aws:iam::111111111111:role/r',
    format = 'CSV', structure = 'a String')
    SETTINGS s3_allow_server_credentials_in_user_queries = 1"

h '--- a complete query ADC triple mints the token, so the stored metadata fields bind nothing'
# Nothing answers the ADC token endpoint, so what this asserts is that the check let the request
# through, not a round trip; the execution cap bounds an exchange that cannot succeed.
run "SELECT * FROM s3($(c gcpmeta), url = '$OWN/$DATA',
    google_adc_client_id = 'own', google_adc_client_secret = 'ownsecret',
    google_adc_refresh_token = 'owntoken', format = 'CSV', structure = 'a String')
    SETTINGS max_execution_time = 25"

h '--- control: an incomplete ADC triple leaves the metadata fields minting, so they still bind'
run "SELECT * FROM s3($(c gcpmeta), url = '$OWN/$DATA',
    google_adc_client_id = 'own', google_adc_client_secret = 'ownsecret',
    format = 'CSV', structure = 'a String')"

h '--- no_sign_request with static keys: nothing signs, so the destination is not bound'
# Two path segments: `S3::URI` reads the first as the bucket, so a single-segment path leaves no key
# and no request is ever issued.
capture_must_not_leak AKIAIOSFODNN7EXAMPLE \
    "SELECT * FROM s3($(c nosign), url = '__CAPTURE__/test/$DATA', format = 'CSV', structure = 'a String')"

h '--- control: the same listener does see the key once SigV4 is on, so the arm above can fail'
# Without this, "no key on the wire" would also be the reading for a listener nothing ever reaches.
# `OVERRIDABLE` is what lets the destination move at all here; the credentials are otherwise identical.
capture_must_not_leak AKIAIOSFODNN7EXAMPLE \
    "SELECT * FROM s3($(c signing), url = '__CAPTURE__/test/$DATA', format = 'CSV', structure = 'a String')"

h '--- a collection that stores no url authorises no destination for its keys'
run "SELECT * FROM s3($(c keysonly), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

h '--- the same rule reaches DatabaseS3, where an absent url means every table name is a full url'
# `getFullUrl` returns the bare table name when the collection stores no url prefix, so each query
# picks the destination for the collection's keys. Refused for the same reason as the arm above; the
# changelog names this shape because it used to be accepted.
run "CREATE DATABASE ${CLICKHOUSE_DATABASE}_kodb ENGINE = S3($(c keysonly))"

h '--- the replay exemption cannot hand that shape back: with no prefix to grandfather, load anonymously'
# The upgrade shape for a database: created while the collection declared its own origin, replayed after
# the collection lost it. `no-key-on-the-wire` is the load-bearing value, because it needs a request to
# have reached the listener - a fixture that broke earlier reads as NO-REQUEST instead of passing.
stmt "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_replaydb"
stmt "DROP NAMED COLLECTION IF EXISTS $(c droppedurl)"
stmt "CREATE NAMED COLLECTION $(c droppedurl) AS url = '$OWN/',
        access_key_id = 'AKIAIOSFODNN7EXAMPLE', secret_access_key = 'testtest'"
stmt "CREATE DATABASE ${CLICKHOUSE_DATABASE}_replaydb ENGINE = S3($(c droppedurl))"
stmt "ALTER NAMED COLLECTION $(c droppedurl) DELETE url"
stmt "DETACH DATABASE ${CLICKHOUSE_DATABASE}_replaydb"
push loaded "ATTACH DATABASE ${CLICKHOUSE_DATABASE}_replaydb"
capture_must_not_leak AKIAIOSFODNN7EXAMPLE \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}_replaydb.\`__CAPTURE__/test/$DATA\`"

h '--- control: a database whose collection does declare a url prefix still reads with its keys'
# The row is in a bucket an anonymous client cannot read, so `payload` is what separates "the keys are
# still attached" from "everything credentialed now loads anonymously".
stmt "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_bounddb"
stmt "CREATE DATABASE ${CLICKHOUSE_DATABASE}_bounddb ENGINE = S3($(c keys))"
allowed_reads "SELECT * FROM ${CLICKHOUSE_DATABASE}_bounddb.\`$DATA\`"

h '--- control: a grandfathered database whose stored prefix does name a destination keeps its keys'
# The same upgrade shape as above but with a prefix persisted in the statement, so one destination is
# grandfathered and there is something to grandfather it to.
stmt "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_pinneddb"
stmt "DROP NAMED COLLECTION IF EXISTS $(c pinnedurl)"
stmt "CREATE NAMED COLLECTION $(c pinnedurl) AS url = '$OTHER/'"
stmt "CREATE DATABASE ${CLICKHOUSE_DATABASE}_pinneddb ENGINE = S3($(c pinnedurl), url = '$OWN/')"
stmt "ALTER NAMED COLLECTION $(c pinnedurl) SET access_key_id = 'test', secret_access_key = 'testtest'"
stmt "DETACH DATABASE ${CLICKHOUSE_DATABASE}_pinneddb"
push loaded "ATTACH DATABASE ${CLICKHOUSE_DATABASE}_pinneddb"
allowed_reads "SELECT * FROM ${CLICKHOUSE_DATABASE}_pinneddb.\`$DATA\`"

h '--- a fresh credentialed cross-origin CREATE is refused for both S3 engines'
# Each engine reaches the check through its own creator, so the replay predicate each one reads needs its
# own arm; the exemption arms below would otherwise be the only thing exercising either.
run "CREATE TABLE ${CLICKHOUSE_DATABASE}.freshtbl (a String)
    ENGINE = S3($(c keys), url = '$OTHER/x.csv', format = 'CSV')"
run "CREATE TABLE ${CLICKHOUSE_DATABASE}.freshq (a String)
    ENGINE = S3Queue($(c keys), url = '$OTHER/q/*.csv', format = 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '/clickhouse/${CLICKHOUSE_TEST_UNIQUE_NAME}_q'"

h '--- a JSON-AST payload cannot claim the definition came from stored metadata'
# The replay exemption keys on `attach_short_syntax`, which no SQL syntax sets, so the `clickhouse_json`
# dialect must not accept it as an input. An `ATTACH TABLE ... ENGINE =` form only reaches the check in a
# `Memory`-engine database; `Atomic` rejects that shape upstream, which would measure nothing.
FORGE_SQL="ATTACH TABLE ${CLICKHOUSE_DATABASE}_mem.forged (a String) ENGINE = S3($(c keys), url = '$OTHER/x.csv', format = 'CSV')"
# TabSeparatedRaw: the default JSON envelope escapes the slashes, and the escaped payload then fails
# upstream with `Host is empty in S3 URI` on both arms.
flush
# The flag is never written, so it is inserted ahead of the key that follows it in the payload.
FORGE_JSON=$(${CLICKHOUSE_CLIENT} --format=TabSeparatedRaw -q \
    "SELECT replace(parseQueryToJSON('${FORGE_SQL//\'/\\\'}'), '\"replace_table\":false', '\"attach_short_syntax\":true,\"replace_table\":false')")
# The flag has to be in the payload for the arm to assert anything.
echo "flagset $(grep -c '"attach_short_syntax":true' <<< "$FORGE_JSON")"
flush
${CLICKHOUSE_CLIENT} --enable_json_ast_dialect=1 --dialect=clickhouse_json --query="$FORGE_JSON" 2>&1 \
    | grep -qF "'attach_short_syntax' is internal-only" && echo refused || echo allowed
raw "SELECT 'created ' || toString(count()) FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}_mem' AND name = 'forged'"

h '--- PARALLEL WITH runs its children internally, which must not read as a stored definition'
# The replay exemption is what a fresh cross-origin ATTACH must not reach, and `internal` alone does not
# separate them: a wrapped statement arrives internal and at mode ATTACH, exactly like the startup replay.
# The second assertion is the load-bearing one - the refusal is reported before the database is created,
# so an exemption that came back would leave the first line unchanged.
run "ATTACH DATABASE ${CLICKHOUSE_DATABASE}_wrapped ENGINE = S3($(c keys), url = '$OTHER/')
    PARALLEL WITH CREATE DATABASE ${CLICKHOUSE_DATABASE}_wrapsink ENGINE = Memory"
raw "SELECT 'wrapped ' || toString(count()) FROM system.databases
    WHERE name = '${CLICKHOUSE_DATABASE}_wrapped'"

h '--- filename cannot move the origin: an absolute value is rejected before any request'
# `path::operator/` replaces the left operand when the right is absolute, so pin the rejection: were
# `S3::URI` ever to accept such a value, the destination would move and this arm must be revisited.
for f in '//127.0.0.1:11112/test/x.csv' '/steal/x.csv'; do
    push nohost "SELECT * FROM s3($(c keys), filename = '$f',
        format = 'CSV', structure = 'a String')"
done

h '--- backups: BackupInfo does not go through findOverrideForbiddingKey, so the seam is its own'
run "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO S3($(c keys), url = '$OTHER/bk')"

h '--- DatabaseS3: getTableImpl rebuilds positional s3() args, so provenance is gone downstream'
run "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db ENGINE = S3($(c keys), url = '$OTHER/')"

h '--- a relative stored url declares no origin, so a materialized s3_base replay still attaches'
# `s3_base` is set only for the CREATE, so the url the replay reads is the one materialized there.
flush
# `s3_base` has to stay confined to this invocation, so this pair is not queued with the arms.
${CLICKHOUSE_CLIENT} -q "SET s3_base = '$OWN/';
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.replay;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.replay (a UInt8) ENGINE = S3($(c rel))"
stmt "DETACH TABLE ${CLICKHOUSE_DATABASE}.replay"
stmt "ATTACH TABLE ${CLICKHOUSE_DATABASE}.replay"
raw "SELECT count() FROM ${CLICKHOUSE_DATABASE}.replay"

h '--- a definition persisted before this rule loads with a warning instead of aborting startup'
# Built with supported statements only: create the table while the collection has no credentials (so
# the cross-origin override is allowed), then add them. That is the upgrade shape - a definition that
# was legal when written and is not now - and for a table loaded at startup a refusal here is the
# server failing to start rather than one unreadable table.
stmt "DROP NAMED COLLECTION IF EXISTS $(c later)"
stmt "CREATE NAMED COLLECTION $(c later) AS url = '$OTHER/', format = 'CSV'"
stmt "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.persisted"
stmt "CREATE TABLE ${CLICKHOUSE_DATABASE}.persisted (a String) ENGINE = S3($(c later), url = '$OWN/$DATA')"
stmt "ALTER NAMED COLLECTION $(c later) SET access_key_id = 'test', secret_access_key = 'testtest'"
stmt "DETACH TABLE ${CLICKHOUSE_DATABASE}.persisted"
# Inverted against the refusal arms: here the refusal is the failure, so it is spelled out rather than
# routed through `run`.
push loaded "ATTACH TABLE ${CLICKHOUSE_DATABASE}.persisted"
allowed_reads "SELECT * FROM ${CLICKHOUSE_DATABASE}.persisted"

h '--- the replay exemption is not reachable from a fresh query against the same collection'
run "SELECT * FROM s3($(c later), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

h '--- RESTORE replays the stored definition too, so it loads rather than failing the restore'
# A local `Disk` destination, so this arm exercises the restore replay and not the backup seam. The
# restore runs the stored CREATE at strictness SECONDARY_CREATE, which is neither FORCE_* nor a short
# ATTACH, so `mode` alone does not mark it.
BK="${CLICKHOUSE_TEST_UNIQUE_NAME}_bk"
# The BACKUP reports the backup id on success, which is output this test does not assert.
stmt "BACKUP TABLE ${CLICKHOUSE_DATABASE}.persisted TO Disk('backups', '$BK') FORMAT Null"
stmt "DROP TABLE ${CLICKHOUSE_DATABASE}.persisted"
allowed_fails_with RESTORED "RESTORE TABLE ${CLICKHOUSE_DATABASE}.persisted FROM Disk('backups', '$BK')"
allowed_reads "SELECT * FROM ${CLICKHOUSE_DATABASE}.persisted"

h '--- and a fresh query against that same collection is still refused'
run "SELECT * FROM s3($(c later), url = '$OWN/$DATA', format = 'CSV', structure = 'a String')"

h '--- the replay warning names the destination without disclosing a url credential'
# A stored url may carry userinfo or a presigned signature, and the server log sits outside the
# `SHOW_NAMED_COLLECTIONS_SECRETS` grant that reading the collection needs.
MASK_QID="${CLICKHOUSE_TEST_UNIQUE_NAME}_mask"
stmt "DROP NAMED COLLECTION IF EXISTS $(c userinfo)"
stmt "CREATE NAMED COLLECTION $(c userinfo) AS
        url = 'http://u:${CLICKHOUSE_TEST_UNIQUE_NAME}_pw@127.0.0.1:11112/test/', format = 'CSV'"
stmt "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.masked"
stmt "CREATE TABLE ${CLICKHOUSE_DATABASE}.masked (a String) ENGINE = S3($(c userinfo), url = '$OWN/$DATA')"
stmt "ALTER NAMED COLLECTION $(c userinfo) SET access_key_id = 'test', secret_access_key = 'testtest'"
stmt "DETACH TABLE ${CLICKHOUSE_DATABASE}.masked"
flush
${CLICKHOUSE_CLIENT} --query_id "$MASK_QID" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.masked" > /dev/null 2>&1
# Both assertions in one arm: a warning that vanished would otherwise read as a redaction. Scoped by
# query_id and logger_name, so the probe query's own text is not what is being counted.
stmt "SYSTEM FLUSH LOGS text_log"
raw "SELECT 'warning', count() >= 1 FROM system.text_log
    WHERE query_id = '$MASK_QID' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'"
raw "SELECT 'secret', count() FROM system.text_log
    WHERE query_id = '$MASK_QID' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'
      AND position(message, '${CLICKHOUSE_TEST_UNIQUE_NAME}_pw') > 0"

h '--- and the destination it names is masked too, not only the origin it compares against'
# The two sides of that message are redacted independently: the origin cannot hold a credential by
# construction, the reported destination can, so each needs its own arm.
MASK_QID2="${CLICKHOUSE_TEST_UNIQUE_NAME}_maskeff"
stmt "DROP NAMED COLLECTION IF EXISTS $(c effuserinfo)"
stmt "CREATE NAMED COLLECTION $(c effuserinfo) AS url = '$OTHER/', format = 'CSV'"
stmt "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.maskedeff"
stmt "CREATE TABLE ${CLICKHOUSE_DATABASE}.maskedeff (a String)
        ENGINE = S3($(c effuserinfo), url = 'http://u:${CLICKHOUSE_TEST_UNIQUE_NAME}_pw2@localhost:11111/test/$DATA')"
stmt "ALTER NAMED COLLECTION $(c effuserinfo) SET access_key_id = 'test', secret_access_key = 'testtest'"
stmt "DETACH TABLE ${CLICKHOUSE_DATABASE}.maskedeff"
flush
${CLICKHOUSE_CLIENT} --query_id "$MASK_QID2" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.maskedeff" > /dev/null 2>&1
stmt "SYSTEM FLUSH LOGS text_log"
raw "SELECT 'warning', count() >= 1 FROM system.text_log
    WHERE query_id = '$MASK_QID2' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'"
raw "SELECT 'secret', count() FROM system.text_log
    WHERE query_id = '$MASK_QID2' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'
      AND position(message, '${CLICKHOUSE_TEST_UNIQUE_NAME}_pw2') > 0"

h '--- a presigned signature in the destination is masked as well as userinfo'
# `maskedForLog` runs two independent scans, and userinfo is the only one the arms above reach, so a
# dropped presigned scan would leave them green.
MASK_QID3="${CLICKHOUSE_TEST_UNIQUE_NAME}_maskpre"
stmt "DROP NAMED COLLECTION IF EXISTS $(c presigned)"
stmt "CREATE NAMED COLLECTION $(c presigned) AS url = '$OTHER/', format = 'CSV'"
stmt "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.presigned"
stmt "CREATE TABLE ${CLICKHOUSE_DATABASE}.presigned (a String) ENGINE = S3($(c presigned),
        url = '$OWN/$DATA?X-Amz-Signature=${CLICKHOUSE_TEST_UNIQUE_NAME}_sig&X-Amz-Credential=x')"
stmt "ALTER NAMED COLLECTION $(c presigned) SET access_key_id = 'test', secret_access_key = 'testtest'"
stmt "DETACH TABLE ${CLICKHOUSE_DATABASE}.presigned"
flush
${CLICKHOUSE_CLIENT} --query_id "$MASK_QID3" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.presigned" > /dev/null 2>&1
stmt "SYSTEM FLUSH LOGS text_log"
raw "SELECT 'warning', count() >= 1 FROM system.text_log
    WHERE query_id = '$MASK_QID3' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'"
raw "SELECT 'secret', count() FROM system.text_log
    WHERE query_id = '$MASK_QID3' AND logger_name = 'NamedCollectionDestinationBinding' AND level = 'Warning'
      AND position(message, '${CLICKHOUSE_TEST_UNIQUE_NAME}_sig') > 0"

flush
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.presigned;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.freshtbl;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.freshq;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_mem;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.maskedeff;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.masked;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.persisted;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_kodb;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_wrapped;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_wrapsink;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_replaydb;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_bounddb;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_pinneddb;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.replay;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
    DROP NAMED COLLECTION IF EXISTS $(c keys);
    DROP NAMED COLLECTION IF EXISTS $(c anon);
    DROP NAMED COLLECTION IF EXISTS $(c open);
    DROP NAMED COLLECTION IF EXISTS $(c otherkeys);
    DROP NAMED COLLECTION IF EXISTS $(c gcp);
    DROP NAMED COLLECTION IF EXISTS $(c gcpnosign);
    DROP NAMED COLLECTION IF EXISTS $(c gcpkeys);
    DROP NAMED COLLECTION IF EXISTS $(c xid);
    DROP NAMED COLLECTION IF EXISTS $(c gcpmeta);
    DROP NAMED COLLECTION IF EXISTS $(c nosign);
    DROP NAMED COLLECTION IF EXISTS $(c signing);
    DROP NAMED COLLECTION IF EXISTS $(c keysonly);
    DROP NAMED COLLECTION IF EXISTS $(c keyidonly);
    DROP NAMED COLLECTION IF EXISTS $(c secretonly);
    DROP NAMED COLLECTION IF EXISTS $(c rel);
    DROP NAMED COLLECTION IF EXISTS $(c later);
    DROP NAMED COLLECTION IF EXISTS $(c userinfo);
    DROP NAMED COLLECTION IF EXISTS $(c effuserinfo);
    DROP NAMED COLLECTION IF EXISTS $(c dflt);
    DROP NAMED COLLECTION IF EXISTS $(c droppedurl);
    DROP NAMED COLLECTION IF EXISTS $(c pinnedurl);
    DROP NAMED COLLECTION IF EXISTS $(c presigned)"
