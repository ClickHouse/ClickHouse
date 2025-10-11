import pytest
import subprocess

from helpers.cluster import ClickHouseCluster, QueryRuntimeException


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/openssl-fips.cnf",
        "configs/ssl_config.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
    ],
    env_variables={
        "OPENSSL_CONF": "/etc/clickhouse-server/config.d/openssl-fips.cnf",
        "OPENSSL_MODULES": "/etc/ssl",
    },
)
use_openssl_fips = False


@pytest.fixture(scope="module")
def start_cluster():
    result = subprocess.run(
        [
            cluster.server_bin_path,
            "local",
            "-q",
            "SELECT value FROM system.build_options WHERE name = 'USE_OPENSSL_FIPS'",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    global use_openssl_fips
    use_openssl_fips = result.stdout.strip() == "1"
    try:
        cluster.start()
        yield cluster
    except Exception:
        if use_openssl_fips:
            raise
        assert instance.contains_in_log(
            "error:0A0000A1:SSL routines::library has no ciphers", from_host=True
        )
        yield None
    finally:
        cluster.shutdown()


def maybe_skip():
    if not use_openssl_fips:
        pytest.skip("ClickHouse was built without OpenSSL FIPS support")


@pytest.mark.parametrize(
    "port",
    [8443, 9440],
)
def test_tls_curves(start_cluster, port):
    maybe_skip()
    # X25519 should be rejected
    cipher = "ECDHE-RSA-AES256-GCM-SHA384"
    base_cmd = [
        "openssl",
        "s_client",
        "-connect",
        f"{instance.ip_address}:{port}",
        "-cipher",
        cipher,
        "-groups",
    ]
    result = subprocess.run(
        base_cmd + ["X25519"], capture_output=True, text=True, input=""
    )
    assert result.returncode == 1
    assert "SSL alert number 40" in result.stderr
    assert instance.contains_in_log(
        "error:0A000065:SSL routines::no suitable key share"
    )
    # P-256 should be accepted
    result = subprocess.run(
        base_cmd + ["P-256"], capture_output=True, text=True, input="", check=True
    )
    assert "Server Temp Key: ECDH, prime256v1, 256 bits" in result.stdout


def test_hash_function(start_cluster):
    maybe_skip()
    with pytest.raises(QueryRuntimeException) as e:
        instance.query("SELECT MD4('hello')")
    assert "Function MD4 is not available in FIPS mode" in str(e.value)
