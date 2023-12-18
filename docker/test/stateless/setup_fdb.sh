#! /bin/bash

set -euxf -o pipefail

FDB_VERSION="7.1.41"
FDBSERVER_BIN_URL="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/fdbserver.x86_64"
FDBSERVER_BIN_SHA256="500aec0fa6408d3be2928130cc2bbfb059daa33e2af83fa656d643f8fb6456e8"
FDBCLI_BIN_URL="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/fdbcli.x86_64"
FDBCLI_BIN_SHA256="8519a398676e5e27dc6f19c949c22878eb810964b8127bbd37b2d270fb6b7301"

FDB_PORT="4511"

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
test:test@127.0.0.1:$FDB_PORT
EOF

nohup $FDBSERVER \
    -l "127.0.0.1:$FDB_PORT" \
    -p "127.0.0.1:$FDB_PORT" \
    -C "$FDB_CLUSTER" \
    --datadir "$FDB_HOME/data" \
    --logdir "$FDB_HOME/log" \
    >> "$FDB_HOME/log/fdbserver.out" \
    2>> "$FDB_HOME/log/fdbserver.err" \
    &
echo $! > fdbserver.pid

sleep 2s
$FDBCLI --exec "configure new single ssd"
