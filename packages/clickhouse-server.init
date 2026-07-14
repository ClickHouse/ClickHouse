#!/bin/sh
### BEGIN INIT INFO
# Provides:          clickhouse-server
# Required-Start:    $network
# Required-Stop:     $network
# Should-Start:      $time
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: clickhouse-server daemon
### END INIT INFO
#
# NOTES:
# - Should-* -- script can start if the listed facilities are missing, unlike Required-*
#
# For the documentation [1]:
#
#   [1]: https://wiki.debian.org/LSBInitScripts

CLICKHOUSE_USER=clickhouse
CLICKHOUSE_GROUP=${CLICKHOUSE_USER}
SHELL=/bin/bash
PROGRAM=clickhouse-server
CLICKHOUSE_GENERIC_PROGRAM=clickhouse
CLICKHOUSE_PROGRAM_ENV=""
EXTRACT_FROM_CONFIG=${CLICKHOUSE_GENERIC_PROGRAM}-extract-from-config
CLICKHOUSE_CONFDIR=/etc/$PROGRAM
CLICKHOUSE_LOGDIR=/var/log/clickhouse-server
CLICKHOUSE_LOGDIR_USER=root
CLICKHOUSE_DATADIR=/var/lib/clickhouse
if [ -d "/var/lock" ]; then
    LOCALSTATEDIR=/var/lock
else
    LOCALSTATEDIR=/run/lock
fi

if [ ! -d "$LOCALSTATEDIR" ]; then
    mkdir -p "$LOCALSTATEDIR"
fi

CLICKHOUSE_BINDIR=/usr/bin
CLICKHOUSE_CRONFILE=/etc/cron.d/clickhouse-server
CLICKHOUSE_CONFIG=$CLICKHOUSE_CONFDIR/config.xml
LOCKFILE=$LOCALSTATEDIR/$PROGRAM
CLICKHOUSE_PIDDIR=/var/run/$PROGRAM
CLICKHOUSE_PIDFILE="$CLICKHOUSE_PIDDIR/$PROGRAM.pid"
# CLICKHOUSE_STOP_TIMEOUT=60 # Disabled by default. Place to /etc/default/clickhouse if you need.

# Some systems lack "flock"
command -v flock >/dev/null && FLOCK=flock

# Override defaults from optional config file and export them automatically
set -a
test -f /etc/default/clickhouse && . /etc/default/clickhouse
set +a

die()
{
    echo $1 >&2
    exit 1
}


# Check that configuration file is Ok.
check_config()
{
    if [ -x "$CLICKHOUSE_BINDIR/$EXTRACT_FROM_CONFIG" ]; then
        su -s $SHELL ${CLICKHOUSE_USER} -c "$CLICKHOUSE_BINDIR/$EXTRACT_FROM_CONFIG --config-file=\"$CLICKHOUSE_CONFIG\" --key=path" >/dev/null || die "Configuration file ${CLICKHOUSE_CONFIG} doesn't parse successfully. Won't restart server. You may use forcerestart if you are sure.";
    fi
}


initdb()
{
    ${CLICKHOUSE_GENERIC_PROGRAM} install --user "${CLICKHOUSE_USER}" --pid-path "${CLICKHOUSE_PIDDIR}" --config-path "${CLICKHOUSE_CONFDIR}" --binary-path "${CLICKHOUSE_BINDIR}"
}


start()
{
    ${CLICKHOUSE_GENERIC_PROGRAM} start --user "${CLICKHOUSE_USER}" --pid-path "${CLICKHOUSE_PIDDIR}" --config-path "${CLICKHOUSE_CONFDIR}" --binary-path "${CLICKHOUSE_BINDIR}"
}


stop()
{
    ${CLICKHOUSE_GENERIC_PROGRAM} stop --pid-path "${CLICKHOUSE_PIDDIR}"
}


restart()
{
    ${CLICKHOUSE_GENERIC_PROGRAM} restart --user "${CLICKHOUSE_USER}" --pid-path "${CLICKHOUSE_PIDDIR}" --config-path "${CLICKHOUSE_CONFDIR}" --binary-path "${CLICKHOUSE_BINDIR}"
}


forcestop()
{
    ${CLICKHOUSE_GENERIC_PROGRAM} stop --force --pid-path "${CLICKHOUSE_PIDDIR}"
}


service_or_func()
{
    if [ -x "/bin/systemctl" ] && [ -f /etc/systemd/system/clickhouse-server.service ] && [ -d /run/systemd/system ]; then
        systemctl $1 $PROGRAM
    else
        $1
    fi
}

forcerestart()
{
    forcestop
    # Should not use 'start' function if systemd active
    service_or_func start
}

use_cron()
{
    # 1. running systemd
    if [ -x "/bin/systemctl" ] && [ -f /etc/systemd/system/clickhouse-server.service ] && [ -d /run/systemd/system ]; then
        return 1
    fi
    # 2. checking whether the config is existed
    if [ ! -f "$CLICKHOUSE_CRONFILE" ]; then
        return 1
    fi
    # 3. disabled by config
    if [ -z "$CLICKHOUSE_CRONFILE" ]; then
        return 2
    fi
    return 0
}
# returns false if cron disabled (with systemd)
enable_cron()
{
    use_cron && sed -i 's/^#*//' "$CLICKHOUSE_CRONFILE"
}
# returns false if cron disabled (with systemd)
disable_cron()
{
    use_cron && sed -i 's/^#*/#/' "$CLICKHOUSE_CRONFILE"
}


is_cron_disabled()
{
    use_cron || return 0

    # Assumes that either no lines are commented or all lines are commented.
    # Also please note, that currently cron file for ClickHouse has only one line (but some time ago there was more).
    grep -q -E '^#' "$CLICKHOUSE_CRONFILE";
}


main()
{
    # See how we were called.
    EXIT_STATUS=0
    case "$1" in
    start)
        service_or_func start && enable_cron
        ;;
    stop)
        disable_cron
        service_or_func stop
        ;;
    restart)
        service_or_func restart && enable_cron
        ;;
    forcestop)
        disable_cron
        forcestop
        ;;
    forcerestart)
        forcerestart && enable_cron
        ;;
    reload)
        service_or_func restart
        ;;
    condstart)
        service_or_func start
        ;;
    condstop)
        service_or_func stop
        ;;
    condrestart)
        service_or_func restart
        ;;
    condreload)
        service_or_func restart
        ;;
    initdb)
        initdb
        ;;
    enable_cron)
        enable_cron
        ;;
    disable_cron)
        disable_cron
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart|forcestop|forcerestart|reload|condstart|condstop|condrestart|condreload|initdb}"
        exit 2
        ;;
    esac

    exit $EXIT_STATUS
}


status()
{
    ${CLICKHOUSE_GENERIC_PROGRAM} status --pid-path "${CLICKHOUSE_PIDDIR}"
}


# Running commands without need of locking
case "$1" in
status)
    status
    exit 0
    ;;
esac


(
    if $FLOCK -n 9; then
        main "$@"
    else
        echo "Init script is already running" && exit 1
    fi
) 9> $LOCKFILE
