#!/bin/sh
set -e
# set -x

PROGRAM=clickhouse-keeper
KEEPER_USER=${KEEPER_USER:-clickhouse}
KEEPER_GROUP=${KEEPER_GROUP:-clickhouse}
# Please note that we don't support paths with whitespaces. This is rather ignorant.
KEEPER_CONFDIR=${KEEPER_CONFDIR:-/etc/$PROGRAM}
KEEPER_DATADIR=${KEEPER_DATADIR:-/var/lib/clickhouse}
KEEPER_LOGDIR=${KEEPER_LOGDIR:-/var/log/$PROGRAM}

[ -f /usr/share/debconf/confmodule ] && . /usr/share/debconf/confmodule
[ -f /etc/default/clickhouse-keeper ] && . /etc/default/clickhouse-keeper

if [ ! -f "/etc/debian_version" ]; then
    not_deb_os=1
fi

if [ "$1" = configure ] || [ -n "$not_deb_os" ]; then
    if ! getent group "${KEEPER_GROUP}" > /dev/null 2>&1 ; then
        groupadd --system "${KEEPER_GROUP}"
    fi
    GID=$(getent group "${KEEPER_GROUP}" | cut -d: -f 3)
    if ! id "${KEEPER_USER}" > /dev/null 2>&1 ; then
        adduser --system --home /dev/null --no-create-home \
            --gid "${GID}" --shell /bin/false \
            "${KEEPER_USER}"
    fi

    chown -R "${KEEPER_USER}:${KEEPER_GROUP}" "${KEEPER_CONFDIR}"
    chmod 0755 "${KEEPER_CONFDIR}"

    if ! [ -d "${KEEPER_DATADIR}" ]; then
        mkdir -p "${KEEPER_DATADIR}"
        chown -R "${KEEPER_USER}:${KEEPER_GROUP}" "${KEEPER_DATADIR}"
        chmod 0700 "${KEEPER_DATADIR}"
    fi

    if ! [ -d "${KEEPER_LOGDIR}" ]; then
        mkdir -p "${KEEPER_LOGDIR}"
        chown -R "${KEEPER_USER}:${KEEPER_GROUP}" "${KEEPER_LOGDIR}"
        chmod 0770 "${KEEPER_LOGDIR}"
    fi
fi
# vim: ts=4: sw=4: sts=4: expandtab
