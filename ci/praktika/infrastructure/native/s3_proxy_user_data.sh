#!/usr/bin/env bash
# Praktika S3 report proxy bootstrap (Amazon Linux 2023).
#
# Serves the project's PRIVATE S3 report buckets to Tailscale users, read-only:
#   Tailscale user --[tailnet]--> Caddy (:443 TLS, :8080 HTTP, GET/HEAD only)
#                                   --> signer.py (SigV4, EC2 instance role)
#                                   --> S3 (buckets stay fully private)
#
# The instance joins the tailnet with an ephemeral, tagged auth key minted at
# boot from a Tailscale OAuth client stored in SSM. No static S3 or Tailscale
# credentials are written to disk.
set -xeuo pipefail

# --- Resolve region from IMDSv2 (launch template forces HttpTokens=required) ---
IMDS_TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
REGION=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
  http://169.254.169.254/latest/meta-data/placement/region)
export AWS_DEFAULT_REGION="$REGION"

# --- Packages: jq for JSON, pip for boto3 (used by the signer) ---
dnf install -y jq python3-pip
pip3 install --quiet boto3

# --- Read the Tailscale OAuth client from SSM (granted by the instance role) ---
TS_CLIENT_ID=$(aws ssm get-parameter --with-decryption \
  --name "__TS_OAUTH_CLIENT_ID_SSM__" --query Parameter.Value --output text)
TS_CLIENT_SECRET=$(aws ssm get-parameter --with-decryption \
  --name "__TS_OAUTH_CLIENT_SECRET_SSM__" --query Parameter.Value --output text)

# --- Mint an ephemeral, pre-authorized, tagged auth key via the Tailscale API ---
TS_ACCESS_TOKEN=$(curl -s https://api.tailscale.com/api/v2/oauth/token \
  -d "client_id=$TS_CLIENT_ID" -d "client_secret=$TS_CLIENT_SECRET" | jq -r .access_token)
TS_AUTHKEY=$(curl -s "https://api.tailscale.com/api/v2/tailnet/-/keys" \
  -H "Authorization: Bearer $TS_ACCESS_TOKEN" -H "Content-Type: application/json" \
  -d '{"capabilities":{"devices":{"create":{"reusable":false,"ephemeral":true,"preauthorized":true,"tags":["__TS_TAG__"]}}},"expirySeconds":600}' \
  | jq -r .key)

# --- Install Tailscale and join the tailnet ---
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --ssh --auth-key="$TS_AUTHKEY" --hostname="__TS_HOSTNAME__"
TS_FQDN=$(tailscale status --json | jq -r '.Self.DNSName' | sed 's/\.$//')

# --- TLS certificate from Tailscale (renewed by a daily systemd timer below) ---
mkdir -p /etc/caddy/tls
tailscale cert --cert-file "/etc/caddy/tls/${TS_FQDN}.crt" \
  --key-file "/etc/caddy/tls/${TS_FQDN}.key" "$TS_FQDN"

# --- SigV4 signing proxy: signs with the instance role, streams from S3 ---
install -d /opt/praktika-s3-proxy
base64 -d > /opt/praktika-s3-proxy/signer.py <<'SIGNER_B64'
__SIGNER_PY_B64__
SIGNER_B64
cat > /etc/systemd/system/praktika-s3-signer.service <<UNIT
[Unit]
Description=Praktika S3 SigV4 signing proxy
After=network-online.target
Wants=network-online.target
[Service]
Environment=PRAKTIKA_S3_PROXY_BUCKETS=__PROXIED_BUCKETS__
Environment=AWS_DEFAULT_REGION=${REGION}
ExecStart=/usr/bin/python3 /opt/praktika-s3-proxy/signer.py 127.0.0.1 8081
Restart=always
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now praktika-s3-signer

# --- Caddy (static binary) fronting the signer: TLS :443 + HTTP :8080, GET/HEAD only ---
case "$(uname -m)" in
  aarch64) CADDY_ARCH=arm64 ;;
  x86_64)  CADDY_ARCH=amd64 ;;
  *)       CADDY_ARCH=amd64 ;;
esac
curl -fsSL -o /usr/local/bin/caddy "https://caddyserver.com/api/download?os=linux&arch=${CADDY_ARCH}"
chmod +x /usr/local/bin/caddy
id caddy &>/dev/null || useradd --system --home /var/lib/caddy --shell /sbin/nologin caddy
chown -R caddy:caddy /etc/caddy/tls

cat > /etc/caddy/Caddyfile <<CADDY
{
	admin off
}
(handlers) {
	@get_head method GET HEAD
	handle @get_head {
		reverse_proxy 127.0.0.1:8081
	}
	# Anything that is not GET/HEAD falls through to this mutually-exclusive
	# handle block and is rejected.
	handle {
		respond 405
	}
}
:8080 {
	import handlers
}
${TS_FQDN}:443 {
	tls /etc/caddy/tls/${TS_FQDN}.crt /etc/caddy/tls/${TS_FQDN}.key
	import handlers
}
CADDY

cat > /etc/systemd/system/caddy.service <<UNIT
[Unit]
Description=Caddy
After=network-online.target praktika-s3-signer.service
Wants=network-online.target
[Service]
User=caddy
Group=caddy
ExecStart=/usr/local/bin/caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
ExecReload=/usr/local/bin/caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
Restart=always
RestartSec=2
AmbientCapabilities=CAP_NET_BIND_SERVICE
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now caddy

# --- Renew the Tailscale TLS cert daily (certs expire ~90d; ASG also refreshes
#     the cert whenever it replaces the instance) ---
cat > /usr/local/bin/praktika-renew-tailscale-cert <<'RENEW'
#!/usr/bin/env bash
set -euo pipefail
TS_FQDN=$(tailscale status --json | jq -r '.Self.DNSName' | sed 's/\.$//')
tailscale cert --cert-file "/etc/caddy/tls/${TS_FQDN}.crt" \
  --key-file "/etc/caddy/tls/${TS_FQDN}.key" "$TS_FQDN"
chown -R caddy:caddy /etc/caddy/tls
systemctl reload caddy
RENEW
chmod +x /usr/local/bin/praktika-renew-tailscale-cert
cat > /etc/systemd/system/praktika-renew-tailscale-cert.service <<UNIT
[Unit]
Description=Renew Tailscale TLS certificate for the Praktika S3 proxy
[Service]
Type=oneshot
ExecStart=/usr/local/bin/praktika-renew-tailscale-cert
UNIT
cat > /etc/systemd/system/praktika-renew-tailscale-cert.timer <<UNIT
[Unit]
Description=Daily Tailscale TLS certificate renewal
[Timer]
OnCalendar=daily
Persistent=true
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now praktika-renew-tailscale-cert.timer
