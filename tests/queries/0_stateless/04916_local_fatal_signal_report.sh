#!/usr/bin/env bash
# Tags: long, no-fasttest, no-msan, no-tsan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_TMP:?}/04916_${CLICKHOUSE_DATABASE:?}"
rm -rf "${PREFIX:?}".d
mkdir -p "$PREFIX".d

# $1 = file, $2 = pattern. A destination that was never created counts zero rather than
# writing to this script's stderr, which the test runner treats as a failure regardless of
# the assertion.
count_in()
{
    if [ -f "$1" ]; then
        # A count of zero is itself an answer, and grep reports it with a non-zero status.
        grep -c "$2" "$1" || true
    else
        echo 0
    fi
}

# $1 = arm name, remaining args go after the settings separator. Client options, which are
# rejected there, go in ABORT_PRE_ARGS.
# A deliberate abort must not leave a core dump behind: the run directory is the one CI
# collects client cores from, and it keeps only the first three by name.
ABORT_PRE_ARGS=()
abort_arm()
{
    local arm="$1"; shift
    mkdir -p "$PREFIX".d/"$arm"
    (
        cd "$PREFIX".d/"$arm" || exit 1
        ulimit -c 0
        # Idle on one thread until signalled, and outlive an unsignalled arm by at most
        # the row count in seconds.
        exec $CLICKHOUSE_LOCAL "${ABORT_PRE_ARGS[@]}" --max_threads=1 \
            --query "SELECT 'ready'; SELECT sleep(1) FROM numbers(60) SETTINGS max_block_size = 1 FORMAT Null" \
            >stdout 2>stderr -- "$@"
    ) &
    local pid=$!

    local i
    for ((i = 0; i < 600; i++)); do
        grep -q ready "$PREFIX".d/"$arm"/stdout 2>/dev/null && break
        sleep 0.1
    done

    # Reaping the job inside this group keeps bash's own "Aborted" notice off the script's
    # stderr, which the test runner treats as a failure.
    { kill -ABRT "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; } 2>"$PREFIX".d/"$arm"/jobcontrol
    echo "$arm signalled $?"
    echo "$arm jobcontrol quiet $([ ! -s "$PREFIX".d/"$arm"/jobcontrol ] && echo 1 || echo 0)"
    echo "$arm cores $(find "$PREFIX".d/"$arm" -maxdepth 1 -name 'core.*' | wc -l)"
    echo "$arm stderr report $(count_in "$PREFIX".d/"$arm"/stderr 'Short fault info')"
    echo "$arm stderr signal $(count_in "$PREFIX".d/"$arm"/stderr 'Signal description: Aborted')"
    ABORT_PRE_ARGS=()
}

abort_arm plain

# A destination configured for the logger keeps the report, and stderr gains it.
abort_arm logfile --logger.log="$PREFIX".d/logfile/server.log --logger.level=fatal
echo "logfile file report $(count_in "$PREFIX".d/logfile/server.log 'Short fault info')"

# The client's own fatal log file is a separate destination from any the logger configures.
mkdir -p "$PREFIX".d/clientlog
ABORT_PRE_ARGS=(--client_logs_file="$PREFIX".d/clientlog/client.log)
abort_arm clientlog
echo "clientlog file report $(count_in "$PREFIX".d/clientlog/client.log 'Short fault info')"

# A destination under a directory that does not exist accepts no record. Destinations are
# written one after another and the first failure ends the round, so stderr must not be
# waiting behind this one. The path stays absent, which is what makes the arm measure the
# failing destination rather than a writable one.
ABORT_PRE_ARGS=(--client_logs_file="$PREFIX".d/absent/client.log)
abort_arm clientlog_unwritable
echo "clientlog_unwritable file report $(count_in "$PREFIX".d/absent/client.log 'Short fault info')"

# The same with a console destination the logger configures, where the client drops its own.
# Without it the client's console destination stays, and stays first, so this pair covers both
# outcomes of that choice rather than only the one that reorders anything.
ABORT_PRE_ARGS=(--client_logs_file="$PREFIX".d/absent/client.log)
abort_arm clientlog_unwritable_console --logger.console=1
echo "clientlog_unwritable_console file report $(count_in "$PREFIX".d/absent/client.log 'Short fault info')"

# Logging inside the thread calling LOG rather than in a background thread.
abort_arm sync --logger.async=0

# A console destination carries the report, so the count stays at one rather than two.
abort_arm console --logger.console=1

# A console destination whose level excludes fatal carries nothing, so the report needs the
# route the client built for it.
abort_arm console_none --logger.console=1 --logger.console_log_level=none

# A text_log destination is attached without a channel, so records must reach it while the
# channel list is empty.
mkdir -p "$PREFIX".d/textlog
cat > "$PREFIX".d/textlog/config.xml <<EOF
<clickhouse>
    <logger>
        <async>0</async>
        <console>0</console>
        <level>none</level>
        <levels><executeQuery>trace</executeQuery></levels>
    </logger>
    <text_log>
        <level>trace</level>
        <flush_interval_milliseconds>100</flush_interval_milliseconds>
    </text_log>
</clickhouse>
EOF
echo "textlog rows $($CLICKHOUSE_LOCAL --config-file="$PREFIX".d/textlog/config.xml \
    --path="$PREFIX".d/textlog/db \
    --query "SELECT 1 FORMAT Null; SYSTEM FLUSH LOGS text_log; SELECT count() > 0 FROM system.text_log WHERE logger_name = 'executeQuery'")"
echo "textlog channels 0 $(find "$PREFIX".d/textlog -maxdepth 1 -name '*.log' | wc -l)"

# Below fatal severity nothing reaches stderr, which is the program's own output.
$CLICKHOUSE_LOCAL --query "SELECT 42" >"$PREFIX".d/ok.out 2>"$PREFIX".d/ok.err
echo "success stdout $(cat "$PREFIX".d/ok.out)"
echo "success stderr empty $([ ! -s "$PREFIX".d/ok.err ] && echo 1 || echo 0)"

rm -rf "${PREFIX:?}".d
