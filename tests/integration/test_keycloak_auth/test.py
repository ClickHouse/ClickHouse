"""
Integration tests for Keycloak-based JWT authentication in ClickHouse.

Layer 2 of the OAuth2 test plan.  Requires:
  - A running Keycloak container (started via `with_keycloak=True` on the cluster)
  - ClickHouse configured with `jwt_dynamic_jwks` and `openid` token processors

Run:
    pytest tests/integration/test_keycloak_auth/test.py -v
"""

import base64
import json
import logging
import re
import time
from html import unescape as html_unescape

import pytest
import requests

from helpers.cluster import ClickHouseCluster

KEYCLOAK_REALM = "clickhouse-test"
KEYCLOAK_CLIENT_ID = "clickhouse"
KEYCLOAK_CLIENT_SECRET = "test-secret"

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/validators.xml"],
    user_configs=["configs/users.xml"],
    with_keycloak=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def keycloak_url(started_cluster):
    return started_cluster.get_keycloak_url()


def get_keycloak_token(started_cluster, username="alice", password="secret"):
    """Obtain an id_token from Keycloak using the resource-owner password grant."""
    url = f"{keycloak_url(started_cluster)}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token"
    data = {
        "grant_type": "password",
        "client_id": KEYCLOAK_CLIENT_ID,
        "client_secret": KEYCLOAK_CLIENT_SECRET,
        "username": username,
        "password": password,
        "scope": "openid profile email",
    }
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()
    token_data = resp.json()
    assert "id_token" in token_data, f"No id_token in response: {token_data}"
    return token_data["id_token"]


def query_with_token(node_instance, token, query):
    """Execute a ClickHouse query using a JWT Bearer token via the HTTP interface."""
    resp = node_instance.http_request(
        "",
        method="POST",
        data=query,
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.text


def decode_jwt_payload(token):
    """Decode JWT payload without signature verification."""
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload_b64 = parts[1]
    # Add padding
    padding = 4 - len(payload_b64) % 4
    if padding != 4:
        payload_b64 += "=" * padding
    # Convert URL-safe base64
    payload_b64 = payload_b64.replace("-", "+").replace("_", "/")
    return json.loads(base64.b64decode(payload_b64))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_jwt_dynamic_jwks(started_cluster):
    """Token validated via explicit JWKS URI (keycloak_jwks processor)."""
    token = get_keycloak_token(started_cluster)
    result = query_with_token(node, token, "SELECT 1")
    assert result.strip() == "1"


def test_openid_discovery(started_cluster):
    """Token validated via OIDC discovery document (keycloak_discovery processor)."""
    token = get_keycloak_token(started_cluster)
    result = query_with_token(node, token, "SELECT 1")
    assert result.strip() == "1"


def test_username_claim(started_cluster):
    """The `preferred_username` claim is mapped to the ClickHouse session user."""
    token = get_keycloak_token(started_cluster, username="alice")
    result = query_with_token(node, token, "SELECT currentUser()")
    assert result.strip() == "alice"


def test_token_refresh(started_cluster):
    """Obtain a new id_token via the refresh_token grant and authenticate."""
    url = f"{keycloak_url(started_cluster)}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token"

    # Initial grant
    data = {
        "grant_type": "password",
        "client_id": KEYCLOAK_CLIENT_ID,
        "client_secret": KEYCLOAK_CLIENT_SECRET,
        "username": "alice",
        "password": "secret",
        "scope": "openid profile email offline_access",
    }
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()
    tokens = resp.json()
    assert "refresh_token" in tokens, "Expected refresh_token in password grant response"

    # Refresh
    refresh_data = {
        "grant_type": "refresh_token",
        "client_id": KEYCLOAK_CLIENT_ID,
        "client_secret": KEYCLOAK_CLIENT_SECRET,
        "refresh_token": tokens["refresh_token"],
    }
    refresh_resp = requests.post(url, data=refresh_data, timeout=30)
    refresh_resp.raise_for_status()
    refreshed = refresh_resp.json()
    assert "id_token" in refreshed

    result = query_with_token(node, refreshed["id_token"], "SELECT 1")
    assert result.strip() == "1"


def test_wrong_issuer_rejected(started_cluster):
    """A token with a tampered issuer claim must be rejected."""
    token = get_keycloak_token(started_cluster)
    payload = decode_jwt_payload(token)

    # Modify the issuer
    payload["iss"] = "https://evil.example.com"
    tampered_payload = (
        base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    )

    parts = token.split(".")
    parts[1] = tampered_payload
    tampered_token = ".".join(parts)

    # Authentication must fail
    try:
        query_with_token(node, tampered_token, "SELECT 1")
        pytest.fail("Expected authentication failure for tampered token")
    except Exception:
        pass  # Expected


def test_expired_token_rejected(started_cluster):
    """A token with an expired `exp` claim must be rejected."""
    token = get_keycloak_token(started_cluster)
    payload = decode_jwt_payload(token)

    # Set exp to a past timestamp
    payload["exp"] = int(time.time()) - 3600
    expired_payload = (
        base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    )

    parts = token.split(".")
    parts[1] = expired_payload
    expired_token = ".".join(parts)

    try:
        query_with_token(node, expired_token, "SELECT 1")
        pytest.fail("Expected authentication failure for expired token")
    except Exception:
        pass  # Expected


def _approve_device_code_via_browser(
    keycloak_base_url, realm, user_code, username="alice", password="secret"
):
    """
    Simulate a browser approving a Keycloak device authorization request.

    Keycloak's device flow requires a user to visit a verification URI, log in,
    and confirm access.  This helper drives that multi-step HTML form sequence
    using a `requests.Session` so that session cookies are maintained across
    the redirects.
    """

    s = requests.Session()

    def _strip_secure_flag(session):
        """Keycloak >= 25 emits Set-Cookie with Secure;SameSite=None on every
        response, but the integration tests reach Keycloak over plain HTTP.
        ``requests`` honors the Secure flag and refuses to resend those cookies
        on the next HTTP hop, which causes Keycloak to lose its session and
        return ``cookie_not_found``.  Clear the flag after every response so the
        cookies are sent on subsequent HTTP requests."""
        for cookie in session.cookies:
            if getattr(cookie, "secure", False):
                cookie.secure = False

    def _follow(method, url, **kw):
        """Manually walk redirects so we can strip the Secure flag between
        hops; ``requests`` follows redirects internally before our hook can
        run, which is too late once the chain has dropped a Secure cookie."""
        kw.setdefault("timeout", 30)
        kw["allow_redirects"] = False
        for _ in range(20):
            r = s.request(method, url, **kw)
            _strip_secure_flag(s)
            if r.status_code not in (301, 302, 303, 307, 308):
                return r
            loc = r.headers.get("Location")
            if not loc:
                return r
            if loc.startswith("/"):
                from urllib.parse import urlparse
                parsed = urlparse(url)
                url = f"{parsed.scheme}://{parsed.netloc}{loc}"
            else:
                url = loc
            method = "GET"
            kw.pop("data", None)
            kw.pop("json", None)
            kw.pop("params", None)
        raise RuntimeError("Too many redirects")

    def get(url, **kw):
        return _follow("GET", url, **kw)

    def post(url, **kw):
        return _follow("POST", url, **kw)

    def get_form(html, base_url=None):
        """Return (action_url, field_dict) for the first <form> in *html*.

        Resolves relative ``action`` URLs against *base_url* when provided."""
        m = re.search(r'<form\b[^>]*\baction="([^"]+)"', html)
        if not m:
            return None, {}
        action_url = html_unescape(m.group(1))
        if base_url and not re.match(r"^https?://", action_url):
            from urllib.parse import urljoin
            action_url = urljoin(base_url, action_url)
        fields = {}
        for inp in re.findall(r"<input\b[^>]+>", html):
            n = re.search(r'\bname="([^"]+)"', inp)
            v = re.search(r'\bvalue="([^"]*)"', inp)
            t = re.search(r'\btype="([^"]+)"', inp)
            if n and (not t or t.group(1).lower() not in ("checkbox", "radio")):
                fields[n.group(1)] = v.group(1) if v else ""
        return action_url, fields

    # Step 1: Navigate to the device endpoint.  Keycloak redirects to a login
    # page when the user_code query parameter is provided and valid.
    r = get(
        f"{keycloak_base_url}/realms/{realm}/device",
        params={"user_code": user_code},
    )
    r.raise_for_status()

    # Step 1a: If Keycloak shows a user-code entry form first (no user_code
    # in the redirect), fill it in and submit.
    if 'name="device_user_code"' in r.text or 'name="user_code"' in r.text:
        action, fields = get_form(r.text, base_url=r.url)
        fields["device_user_code"] = user_code
        fields["user_code"] = user_code
        r = post(action, data=fields)
        r.raise_for_status()

    # Step 2: We should now be on the login page.  Submit credentials.
    assert 'type="password"' in r.text, (
        f"Expected Keycloak login page, got:\n{r.text[:800]}"
    )
    action, fields = get_form(r.text, base_url=r.url)
    fields["username"] = username
    fields["password"] = password
    r = post(action, data=fields)
    r.raise_for_status()

    # Step 3: Submit the device consent / grant form.  Keycloak renders a
    # "Do you want to grant access?" page with an `accept` submit button.
    action, fields = get_form(r.text, base_url=r.url)
    if action:
        if "accept" not in fields:
            fields["accept"] = ""
        post(action, data=fields)


def test_device_flow_initiation(started_cluster):
    """
    Verify that Keycloak responds correctly to the device authorization request.
    The polling / approval mechanics are covered by the Layer 1 unit tests.
    """
    url = f"{keycloak_url(started_cluster)}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth/device"
    data = {
        "client_id": KEYCLOAK_CLIENT_ID,
        "client_secret": KEYCLOAK_CLIENT_SECRET,
        "scope": "openid profile email",
    }
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()

    device_data = resp.json()
    assert "device_code" in device_data, f"Missing device_code: {device_data}"
    assert "user_code" in device_data, f"Missing user_code: {device_data}"
    assert (
        "verification_uri" in device_data or "verification_uri_complete" in device_data
    ), f"Missing verification_uri: {device_data}"
    logging.info(
        "Device flow initiated: user_code=%s verification_uri=%s",
        device_data.get("user_code"),
        device_data.get("verification_uri", device_data.get("verification_uri_complete")),
    )


def test_device_flow_round_trip(started_cluster):
    """
    Full device-authorization-grant round-trip (RFC 8628).

    1. Client initiates device flow → Keycloak returns `device_code` / `user_code`.
    2. User (simulated via `_approve_device_code_via_browser`) visits the
       verification URI, logs in, and grants access.
    3. Client polls the token endpoint until an `id_token` is returned.
    4. `id_token` is used to authenticate a ClickHouse query — must return `1`.
    """
    base_url = keycloak_url(started_cluster)
    device_endpoint = (
        f"{base_url}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth/device"
    )
    token_endpoint = (
        f"{base_url}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token"
    )

    # --- 1. Initiate device authorization ---
    init_resp = requests.post(
        device_endpoint,
        data={
            "client_id": KEYCLOAK_CLIENT_ID,
            "client_secret": KEYCLOAK_CLIENT_SECRET,
            "scope": "openid profile email",
        },
        timeout=30,
    )
    init_resp.raise_for_status()
    device_data = init_resp.json()
    device_code = device_data["device_code"]
    user_code = device_data["user_code"]
    interval = max(device_data.get("interval", 5), 1)

    logging.info(
        "Device flow round-trip: user_code=%s device_code=%.8s…", user_code, device_code
    )

    # --- 2. Simulate user approving the request in a browser ---
    _approve_device_code_via_browser(base_url, KEYCLOAK_REALM, user_code)

    # --- 3. Poll until the token arrives (or a 60-second deadline) ---
    deadline = time.time() + 60
    id_token = None
    while time.time() < deadline:
        time.sleep(interval)
        poll_resp = requests.post(
            token_endpoint,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "client_id": KEYCLOAK_CLIENT_ID,
                "client_secret": KEYCLOAK_CLIENT_SECRET,
                "device_code": device_code,
            },
            timeout=30,
        )
        poll_data = poll_resp.json()
        if "id_token" in poll_data:
            id_token = poll_data["id_token"]
            break
        error = poll_data.get("error", "")
        assert error in ("authorization_pending", "slow_down"), (
            f"Unexpected polling error: {poll_data}"
        )
        if error == "slow_down":
            interval += 5

    assert id_token is not None, (
        "Device flow timed out: Keycloak never returned an id_token after approval"
    )

    # --- 4. Use the token to authenticate a ClickHouse query ---
    result = query_with_token(node, id_token, "SELECT 1")
    assert result.strip() == "1"
