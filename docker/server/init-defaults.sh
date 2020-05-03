#!/usr/bin/env sh

# define defaults
CLICKHOUSE_DEFAULT_DB="${CLICKHOUSE_DEFAULT_DB:-}";
CLICKHOUSE_DEFAULT_USER="${CLICKHOUSE_DEFAULT_USER:-}";
CLICKHOUSE_DEFAULT_PASSWORD="${CLICKHOUSE_DEFAULT_PASSWORD:-secret}";

# if default user is defined - create it
if [ -n "$CLICKHOUSE_DEFAULT_USER" ]; then
  cat <<EOT >> /etc/clickhouse-server/users.d/default-user.xml
  <yandex>
    <!-- Docs: <https://clickhouse.tech/docs/en/operations/settings/settings_users/> -->
    <users>
      <${CLICKHOUSE_DEFAULT_USER}>
        <profile>default</profile>
        <networks>
          <ip>::/0</ip>
        </networks>
        <password>${CLICKHOUSE_DEFAULT_PASSWORD}</password>
        <quota>default</quota>
      </${CLICKHOUSE_DEFAULT_USER}>
    </users>
  </yandex>
EOT
fi

# create default database, if defined
if [ -n "$CLICKHOUSE_DEFAULT_DB" ]; then
  clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DEFAULT_DB}";
fi
