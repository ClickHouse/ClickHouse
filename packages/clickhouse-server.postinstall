#!/bin/sh
set -e
# set -x

PROGRAM=clickhouse-server
CLICKHOUSE_USER=${CLICKHOUSE_USER:-clickhouse}
CLICKHOUSE_GROUP=${CLICKHOUSE_GROUP:-${CLICKHOUSE_USER}}
# Please note that we don't support paths with whitespaces. This is rather ignorant.
CLICKHOUSE_CONFDIR=${CLICKHOUSE_CONFDIR:-/etc/clickhouse-server}
CLICKHOUSE_DATADIR=${CLICKHOUSE_DATADIR:-/var/lib/clickhouse}
CLICKHOUSE_LOGDIR=${CLICKHOUSE_LOGDIR:-/var/log/clickhouse-server}
CLICKHOUSE_BINDIR=${CLICKHOUSE_BINDIR:-/usr/bin}
CLICKHOUSE_GENERIC_PROGRAM=${CLICKHOUSE_GENERIC_PROGRAM:-clickhouse}
CLICKHOUSE_PIDDIR=/var/run/$PROGRAM

# Provide clickhouse-keeper
KEEPER_CONFDIR=${KEEPER_CONFDIR:-/etc/clickhouse-keeper}
KEEPER_DATADIR=${KEEPER_DATADIR:-/var/lib/clickhouse}
KEEPER_LOGDIR=${KEEPER_LOGDIR:-/var/log/clickhouse-keeper}

[ -f /usr/share/debconf/confmodule ] && . /usr/share/debconf/confmodule
[ -f /etc/default/clickhouse ] && . /etc/default/clickhouse

if [ ! -f "/etc/debian_version" ]; then
    not_deb_os=1
fi

if [ "$1" = configure ] || [ -n "$not_deb_os" ]; then

    ${CLICKHOUSE_GENERIC_PROGRAM} install --user "${CLICKHOUSE_USER}" --group "${CLICKHOUSE_GROUP}" --pid-path "${CLICKHOUSE_PIDDIR}" --config-path "${CLICKHOUSE_CONFDIR}" --binary-path "${CLICKHOUSE_BINDIR}" --log-path "${CLICKHOUSE_LOGDIR}" --data-path "${CLICKHOUSE_DATADIR}"

    if [ -x "/bin/systemctl" ] && [ -f /lib/systemd/system/clickhouse-server.service ] && [ -d /run/systemd/system ]; then
        # if old rc.d service present - remove it
        if [ -x "/etc/init.d/clickhouse-server" ] && [ -x "/usr/sbin/update-rc.d" ]; then
            /usr/sbin/update-rc.d clickhouse-server remove
        fi

        /bin/systemctl daemon-reload
        /bin/systemctl enable clickhouse-server
    else
        # If you downgrading to version older than 1.1.54336 run: systemctl disable clickhouse-server
        if [ -x "/etc/init.d/clickhouse-server" ]; then
            if [ -x "/usr/sbin/update-rc.d" ]; then
                /usr/sbin/update-rc.d clickhouse-server defaults 19 19 >/dev/null || exit $?
            else
                echo # Other OS
            fi
        fi
    fi

    # /etc/systemd/system/clickhouse-server.service shouldn't be distributed by the package, but it was
    # here we delete the service file if it was from our package
    if [ -f /etc/systemd/system/clickhouse-server.service ]; then
      SHA256=$(sha256sum /etc/systemd/system/clickhouse-server.service | cut -d' ' -f1)
      for ref_sum in 7769a14773e811a56f67fd70f7960147217f5e68f746010aec96722e24d289bb 22890012047ea84fbfcebd6e291fe2ef2185cbfdd94a0294e13c8bf9959f58f8 b7790ae57156663c723f92e75ac2508453bf0a7b7e8313bb8081da99e5e88cd3 d1dcc1dbe92dab3ae17baa395f36abf1876b4513df272bf021484923e0111eef ac29ddd32a02eb31670bf5f0018c5d8a3cc006ca7ea572dcf717cb42310dcad7 c62d23052532a70115414833b500b266647d3924eb006a6f3eb673ff0d55f8fa b6b200ffb517afc2b9cf9e25ad8a4afdc0dad5a045bddbfb0174f84cc5a959ed; do
        if [ "$SHA256" = "$ref_sum" ]; then
          rm /etc/systemd/system/clickhouse-server.service
          break
        fi
      done
    fi

    # Setup clickhouse-keeper directories
    chown -R "${CLICKHOUSE_USER}:${CLICKHOUSE_GROUP}" "${KEEPER_CONFDIR}"
    chmod 0755 "${KEEPER_CONFDIR}"

    if ! [ -d "${KEEPER_DATADIR}" ]; then
        mkdir -p "${KEEPER_DATADIR}"
        chown -R "${CLICKHOUSE_USER}:${CLICKHOUSE_GROUP}" "${KEEPER_DATADIR}"
        chmod 0700 "${KEEPER_DATADIR}"
    fi

    if ! [ -d "${KEEPER_LOGDIR}" ]; then
        mkdir -p "${KEEPER_LOGDIR}"
        chown -R "${CLICKHOUSE_USER}:${CLICKHOUSE_GROUP}" "${KEEPER_LOGDIR}"
        chmod 0770 "${KEEPER_LOGDIR}"
    fi
fi
