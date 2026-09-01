#!/usr/bin/env bash
# Praktika CI DB node bootstrap. Single-node OSS ClickHouse with embedded
# Keeper on AL2023. Runs as EC2 user_data; intended to be idempotent enough
# that re-running on reboot reapplies the schema.
#
# Placeholders are substituted at deploy time by user_data.cidb_user_data():
#   _VPC_CIDR_                 e.g. "10.0.0.0/16" - networks ACL for users
#   _ADMIN_PASSWORD_SSM_NAME_  SSM parameter name holding admin user password
#   _SCHEMA_SQL_B64_           gzip+base64-encoded SQL script bootstrapping
#                              /etc/praktika/cidb_schema.sql
#   _REPLICA_NAME_             unique macro for ReplicatedMergeTree
set -xeuo pipefail

echo "=== CIDB node bootstrap ==="

VPC_CIDR='__VPC_CIDR__'
ADMIN_PASSWORD_SSM_NAME='__ADMIN_PASSWORD_SSM_NAME__'
REPLICA_NAME='__REPLICA_NAME__'

dnf install -y awscli jq

# IMDS for region/instance-id (also used as a fallback replica name).
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region)
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)

if [ -z "$REPLICA_NAME" ]; then
  REPLICA_NAME="$INSTANCE_ID"
fi

# --- Data volume ----------------------------------------------------------
# Find the first unformatted disk that isn't the root nvme0. We mount it at
# /var/lib/clickhouse BEFORE installing clickhouse-server so the package
# post-install creates its dirs with correct ownership on the data volume.
DATA_DEV=""
for candidate in $(lsblk -ndo NAME,TYPE | awk '$2=="disk" && $1!~/^nvme0/ {print "/dev/"$1}'); do
  if ! lsblk -no MOUNTPOINT "$candidate" | grep -q .; then
    DATA_DEV="$candidate"
    break
  fi
done
if [ -z "$DATA_DEV" ]; then
  echo "ERROR: no unmounted data volume found" >&2
  lsblk
  exit 1
fi
echo "Data volume: $DATA_DEV"

if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
  mkfs.ext4 -L praktika-cidb "$DATA_DEV"
fi

mkdir -p /var/lib/clickhouse
if ! mountpoint -q /var/lib/clickhouse; then
  mount "$DATA_DEV" /var/lib/clickhouse
fi
if ! grep -q "/var/lib/clickhouse" /etc/fstab; then
  UUID=$(blkid -s UUID -o value "$DATA_DEV")
  echo "UUID=$UUID /var/lib/clickhouse ext4 defaults,nofail 0 2" >> /etc/fstab
fi

# --- Install ClickHouse OSS ----------------------------------------------
cat > /etc/yum.repos.d/clickhouse.repo <<'EOF'
[clickhouse-stable]
name=ClickHouse - Stable Repository
baseurl=https://packages.clickhouse.com/rpm/stable/
enabled=1
gpgcheck=0
EOF

dnf install -y clickhouse-server clickhouse-client

# Make /var/lib/clickhouse owned by the CH user (the package may have created
# it before our mount was in place if cloud-init re-runs).
chown -R clickhouse:clickhouse /var/lib/clickhouse

# --- ClickHouse configuration --------------------------------------------
mkdir -p /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d /etc/praktika

cat > /etc/clickhouse-server/config.d/listen.xml <<'EOF'
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
</clickhouse>
EOF

# Embedded ClickHouse Keeper. Single-node quorum (server_id=1, only itself in
# raft_configuration). The <zookeeper> block points the same server at its own
# Keeper on localhost so ReplicatedMergeTree can register paths.
cat > /etc/clickhouse-server/config.d/keeper.xml <<'EOF'
<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <coordination_settings>
      <operation_timeout_ms>10000</operation_timeout_ms>
      <session_timeout_ms>30000</session_timeout_ms>
      <raft_logs_level>information</raft_logs_level>
    </coordination_settings>
    <raft_configuration>
      <server>
        <id>1</id>
        <hostname>localhost</hostname>
        <port>9234</port>
      </server>
    </raft_configuration>
  </keeper_server>

  <zookeeper>
    <node>
      <host>localhost</host>
      <port>9181</port>
    </node>
  </zookeeper>
</clickhouse>
EOF

cat > /etc/clickhouse-server/config.d/macros.xml <<EOF
<clickhouse>
  <macros>
    <shard>01</shard>
    <replica>${REPLICA_NAME}</replica>
  </macros>
</clickhouse>
EOF

# --- Users config --------------------------------------------------------
# - runner: passwordless, restricted to the VPC CIDR. Used by praktika
#   runners (cidb.py) so they can insert without managing credentials.
# - admin: password from SSM, restricted to the VPC CIDR. Intended for
#   human access bridged into the VPC (e.g. via Tailscale subnet router).
# - default: existing privileged user is locked down to localhost so the
#   schema bootstrap below still works without a password.
ADMIN_PASSWORD=$(aws ssm get-parameter --name "$ADMIN_PASSWORD_SSM_NAME" --with-decryption --region "$REGION" --query 'Parameter.Value' --output text)
ADMIN_PASSWORD_SHA256=$(printf '%s' "$ADMIN_PASSWORD" | sha256sum | awk '{print $1}')
unset ADMIN_PASSWORD

cat > /etc/clickhouse-server/users.d/praktika.xml <<EOF
<clickhouse>
  <users>
    <default>
      <networks replace="replace">
        <ip>::1</ip>
        <ip>127.0.0.1</ip>
      </networks>
    </default>

    <runner>
      <no_password/>
      <networks>
        <ip>${VPC_CIDR}</ip>
      </networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>0</access_management>
    </runner>

    <admin>
      <password_sha256_hex>${ADMIN_PASSWORD_SHA256}</password_sha256_hex>
      <networks>
        <ip>${VPC_CIDR}</ip>
      </networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </admin>
  </users>
</clickhouse>
EOF

# --- Schema --------------------------------------------------------------
echo '__SCHEMA_SQL_B64__' | base64 -d | gunzip > /etc/praktika/cidb_schema.sql

# Oneshot service that re-applies the schema after CH is healthy. Runs on
# every boot so schema additions ship by re-deploying the launch template
# (which forces an instance refresh) or by `systemctl restart cidb-bootstrap`.
cat > /usr/local/bin/cidb-bootstrap.sh <<'EOF'
#!/usr/bin/env bash
set -xeuo pipefail
for _ in $(seq 1 60); do
  if clickhouse-client --user=default --query="SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
clickhouse-client --user=default --multiquery < /etc/praktika/cidb_schema.sql
EOF
chmod +x /usr/local/bin/cidb-bootstrap.sh

cat > /etc/systemd/system/cidb-bootstrap.service <<'EOF'
[Unit]
Description=Praktika CI DB schema bootstrap
After=clickhouse-server.service
Requires=clickhouse-server.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/cidb-bootstrap.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now clickhouse-server
systemctl enable --now cidb-bootstrap

echo "=== CIDB node ready ==="
