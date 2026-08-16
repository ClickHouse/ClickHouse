#!/usr/bin/env bash
# Tags: long, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_TMP:?}/04916_${CLICKHOUSE_DATABASE:?}"
rm -rf "${PREFIX:?}".d
mkdir -p "$PREFIX".d

# $1 = arm name, remaining args go after the settings separator.
# A deliberate abort must not leave a core dump behind: the run directory is the one CI
# collects client cores from, and it keeps only the first three by name.
abort_arm()
{
    local arm="$1"; shift
    mkdir -p "$PREFIX".d/"$arm"
    (
        cd "$PREFIX".d/"$arm" || exit 1
        ulimit -c 0
        exec $CLICKHOUSE_LOCAL \
            --query "SELECT 'ready'; SELECT sum(sipHash64(number)) FROM numbers_mt(100000000000)" \
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
    echo "$arm stderr report $(grep -c 'Short fault info' "$PREFIX".d/"$arm"/stderr)"
    echo "$arm stderr signal $(grep -c 'Signal description: Aborted' "$PREFIX".d/"$arm"/stderr)"
}

abort_arm plain

# A destination configured for the logger keeps the report, and stderr gains it.
abort_arm logfile --logger.log="$PREFIX".d/logfile/server.log --logger.level=fatal
echo "logfile file report $(grep -c 'Short fault info' "$PREFIX".d/logfile/server.log)"

# Logging inside the thread calling LOG rather than in a background thread.
abort_arm sync --logger.async=0

# Below fatal severity nothing reaches stderr, which is the program's own output.
$CLICKHOUSE_LOCAL --query "SELECT 42" >"$PREFIX".d/ok.out 2>"$PREFIX".d/ok.err
echo "success stdout $(cat "$PREFIX".d/ok.out)"
echo "success stderr empty $([ ! -s "$PREFIX".d/ok.err ] && echo 1 || echo 0)"

rm -rf "${PREFIX:?}".d
