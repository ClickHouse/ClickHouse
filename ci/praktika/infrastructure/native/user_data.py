from pathlib import Path

_HERE = Path(__file__).parent


def cidb_user_data(vpc_cidr, admin_password_ssm_name, replica_name):
    """Render the CI DB node bootstrap script.

    Inlines the schema SQL (gzip+base64-encoded) so the resulting script is
    self-contained — the EC2 instance does not need to reach back into S3
    or any praktika package on first boot.
    """
    import base64
    import gzip

    schema_sql = (_HERE / "cidb_schema.sql").read_text()
    template = (_HERE / "user_data_cidb.sh").read_text()
    placeholders = (
        "__VPC_CIDR__",
        "__ADMIN_PASSWORD_SSM_NAME__",
        "__SCHEMA_SQL_B64__",
        "__REPLICA_NAME__",
    )
    for ph in placeholders:
        if ph not in template:
            raise RuntimeError(f"user_data_cidb.sh is missing {ph}")
    schema_b64 = base64.b64encode(
        gzip.compress(schema_sql.encode("utf-8"), mtime=0)
    ).decode("ascii")
    return (
        template
        .replace("__VPC_CIDR__", vpc_cidr)
        .replace("__ADMIN_PASSWORD_SSM_NAME__", admin_password_ssm_name)
        .replace("__SCHEMA_SQL_B64__", schema_b64)
        .replace("__REPLICA_NAME__", replica_name)
    )


def s3_proxy_user_data(
    hostname,
    tailscale_tag,
    oauth_client_id_ssm,
    oauth_client_secret_ssm,
    proxied_buckets,
):
    """Render the S3 report proxy bootstrap script.

    Inlines the SigV4 signer (base64-encoded) so the instance is self-contained
    on first boot. The proxied bucket allowlist is passed to the signer via an
    environment variable rendered into its systemd unit.
    """
    import base64

    signer_py = (_HERE / "s3_proxy_signer.py").read_text()
    template = (_HERE / "s3_proxy_user_data.sh").read_text()
    placeholders = (
        "__TS_OAUTH_CLIENT_ID_SSM__",
        "__TS_OAUTH_CLIENT_SECRET_SSM__",
        "__TS_TAG__",
        "__TS_HOSTNAME__",
        "__PROXIED_BUCKETS__",
        "__SIGNER_PY_B64__",
    )
    for ph in placeholders:
        if ph not in template:
            raise RuntimeError(f"s3_proxy_user_data.sh is missing {ph}")
    signer_b64 = base64.b64encode(signer_py.encode("utf-8")).decode("ascii")
    return (
        template
        .replace("__TS_OAUTH_CLIENT_ID_SSM__", oauth_client_id_ssm)
        .replace("__TS_OAUTH_CLIENT_SECRET_SSM__", oauth_client_secret_ssm)
        .replace("__TS_TAG__", tailscale_tag)
        .replace("__TS_HOSTNAME__", hostname)
        .replace("__PROXIED_BUCKETS__", " ".join(proxied_buckets))
        .replace("__SIGNER_PY_B64__", signer_b64)
    )
