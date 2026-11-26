import base64
import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[os.path.join("configs", "config.xml")],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# A bogus `Authorization` header, as a browser would send after remembering some other
# credentials for the same origin.
BOGUS_AUTH = (
    "Basic " + base64.b64encode(b"remembered_user:remembered_password").decode()
)


def test_config_credentials_used():
    # The handler has its own configured credentials, so a plain request runs as that user.
    response = node.http_request("auth_config")
    assert response.status_code == 200
    assert response.text.strip() == "default"


def test_config_credentials_with_url_params_ok():
    # URL parameters do not conflict with configured credentials: the configured user wins,
    # and the request is not rejected (this behaviour is unchanged).
    response = node.http_request(
        "auth_config", params={"user": "default", "password": ""}
    )
    assert response.status_code == 200
    assert response.text.strip() == "default"


def test_config_credentials_with_authorization_header_rejected():
    # Mixing an `Authorization` header with configured credentials is rejected.
    response = node.http_request("auth_config", headers={"Authorization": BOGUS_AUTH})
    assert response.status_code == 403
    assert "Authorization HTTP header" in response.text
    assert "authentication set in config" in response.text


def test_config_credentials_with_url_params_and_header_rejected():
    # URL parameters take precedence over the `Authorization` header, but that precedence
    # must not let the header bypass the no-mixing rule for configured credentials. Before
    # the fix this request was accepted as the configured user.
    response = node.http_request(
        "auth_config",
        params={"user": "default", "password": ""},
        headers={"Authorization": BOGUS_AUTH},
    )
    assert response.status_code == 403
    assert "Authorization HTTP header" in response.text
    assert "authentication set in config" in response.text
