#! /bin/bash

set -euxf -o pipefail

FDB_VERSION="7.1.41"
FDBSERVER_BIN_URL="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/fdbserver.x86_64"
FDBSERVER_BIN_SHA256="500aec0fa6408d3be2928130cc2bbfb059daa33e2af83fa656d643f8fb6456e8"
FDBCLI_BIN_URL="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/fdbcli.x86_64"
FDBCLI_BIN_SHA256="8519a398676e5e27dc6f19c949c22878eb810964b8127bbd37b2d270fb6b7301"

FDB_PORT_TRANSACTION="4511"
FDB_PORT_STORAGE_1="4600"
FDB_PORT_STORAGE_2="4601"
FDB_PORT_STATELESS_1="4602"
FDB_PORT_STATELESS_2="4603"
FDB_PORT_STATELESS_3="4604"

FDB_HOME=/tmp/foundationdb
FDB_CLUSTER="/etc/foundationdb/fdb.cluster"
export FDB_CLUSTER
FDBSERVER="$FDB_HOME/fdbserver"
FDBCLI="$FDB_HOME/fdbcli"

if [ "$(uname -m)" != "x86_64" ]; then
    echo "Only x86_64 is supported" >&2
    exit 1
fi

[ -d "$FDB_HOME" ] || mkdir -p "$FDB_HOME"
cd "$FDB_HOME"
mkdir -p log data

# Download exectuables
download_executable () {
    _tgt="$1"
    _url="$2"
    _sha256="$3"

    if [ ! -f "$_tgt" ]; then
        echo "Downloading $_tgt" >&2
        wget -O "$_tgt" "$_url"
    else
        echo "Skip download $_tgt" >&2
    fi
    _actual_sha256="$(sha256sum "$_tgt" | sed 's/\s.*$//')"
    if [ "$_actual_sha256" != "$_sha256" ]; then
        echo "SHA256 mismatch. Expected: $_sha256, Actual: $_actual_sha256" >&2
        exit 1
    fi
    chmod +x "$_tgt"
}
download_executable "$FDBSERVER" "$FDBSERVER_BIN_URL" "$FDBSERVER_BIN_SHA256"
download_executable "$FDBCLI" "$FDBCLI_BIN_URL" "$FDBCLI_BIN_SHA256"

# Prepare conf
cat <<EOF > "$FDB_CLUSTER"
test:test@127.0.0.1:$FDB_PORT_TRANSACTION,127.0.0.1:$FDB_PORT_STORAGE_1,127.0.0.1:$FDB_PORT_STORAGE_2,127.0.0.1:$FDB_PORT_STATELESS_1,127.0.0.1:$FDB_PORT_STATELESS_2,127.0.0.1:$FDB_PORT_STATELESS_3
EOF

start_fdb_server () {
    _fdb_port="$1"
    _role="$2"

    mkdir -p data/"$_fdb_port"
    mkdir -p log/"$_fdb_port"

    nohup $FDBSERVER \
    -l "127.0.0.1:$_fdb_port" \
    -p "127.0.0.1:$_fdb_port" \
    -C "$FDB_CLUSTER" \
    -c "$_role" \
    --datadir "$FDB_HOME/data/$_fdb_port" \
    --logdir "$FDB_HOME/log/$_fdb_port" \
    >> "$FDB_HOME/log/$_fdb_port/fdbserver.out" \
    2>> "$FDB_HOME/log/$_fdb_port/fdbserver.err" \
    &
}

start_fdb_server "$FDB_PORT_TRANSACTION" "transaction"
start_fdb_server "$FDB_PORT_STORAGE_1" "storage"
start_fdb_server "$FDB_PORT_STORAGE_2"  "storage"
start_fdb_server "$FDB_PORT_STATELESS_1" "stateless"
start_fdb_server "$FDB_PORT_STATELESS_2" "stateless"
start_fdb_server "$FDB_PORT_STATELESS_3" "stateless"

sleep 2s
$FDBCLI --exec "configure new single ssd"
