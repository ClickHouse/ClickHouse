#*/10 * * * * root (which service > /dev/null && (service clickhouse-server condstart || true)) || /etc/init.d/clickhouse-server condstart 1>/dev/null 2>&1
