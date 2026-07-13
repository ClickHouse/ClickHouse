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
