#*/10 * * * * root (which service > /dev/null 2>&1 && (service clickhouse-server condstart || true)) || /etc/init.d/clickhouse-server condstart > /dev/null 2>&1
