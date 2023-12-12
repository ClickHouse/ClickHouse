#! /bin/bash

set -ex

# Always overwrite fdb.cluser since public_ip may be changed in some cni
public_ip=$(hostname -i | awk '{print $1}')
if [[ "$public_ip" == *:* ]]; then
    # ipv6
    public_ip="[$public_ip]"
fi

extra_conf_dir=/etc/foundationdb
fdb_cluster_realpath=${extra_conf_dir}/fdb.cluster

[ -d "${extra_conf_dir}" ] || mkdir -p "${extra_conf_dir}"

echo "test:test@${public_ip}:4500" > "$fdb_cluster_realpath"
chmod 0664 "$fdb_cluster_realpath"

if [ -z "$(ls /var/lib/foundationdb/data)" ]; then
    echo "Setup new fdb"
    /etc/init.d/foundationdb start
    sleep 2s
    if ! fdbcli --exec "configure new single memory; status" --timeout 10; then 
        echo "Unable to configure new FDB cluster."
    fi
    /etc/init.d/foundationdb stop
    sleep 2s
fi

echo "Start fdbmointor"
nohup /usr/lib/foundationdb/fdbmonitor > /etc/foundationdb/fdb_log 2>&1 &
