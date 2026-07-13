import threading
import time
import json
from datetime import datetime

import requests

try:
    import jwt  # From pyjwt

    assert hasattr(jwt, "encode"), "Invalid jwt module, 'encode' not found"
    USING_PYJWT = True
except (ImportError, AssertionError):
    USING_PYJWT = False
    print(
        "Warning: pyjwt not available. Falling back to 'jwt' module (not recommended)"
    )
    from jwt import jwk_from_pem, JWT

from praktika.utils import Shell


_PERMISSION_LEVELS = {
    "none": 0,
    "read": 1,
    "write": 2,
    "admin": 3,
}


class GHAuth:
    @classmethod
    def _validate_permissions(cls, permissions, required_permissions):
        if not required_permissions:
            return

        missing = []
        permissions = permissions or {}
        for name, required in required_permissions.items():
            actual = permissions.get(name)
            actual_level = _PERMISSION_LEVELS.get(actual, -1)
            required_level = _PERMISSION_LEVELS.get(required, -1)
            if actual_level < required_level:
                missing.append(f"{name}={required} (actual: {actual or 'missing'})")

        if missing:
            raise RuntimeError(
                "GH auth token lacks required permissions: "
                f"{', '.join(missing)}. Update the GitHub App/token minter "
                "permissions and redeploy the token minter."
            )

    @classmethod
    def _get_lambda_token_with_expiry(cls, required_permissions=None):
        import boto3
        from .settings import Settings

        region = Settings.GH_AUTH_LAMBDA_REGION or Settings.AWS_REGION
        if not region:
            raise RuntimeError("GH_AUTH_LAMBDA_REGION or AWS_REGION must be set")
        if not Settings.GH_AUTH_LAMBDA_NAME:
            raise RuntimeError("GH_AUTH_LAMBDA_NAME is not configured")

        client = boto3.client("lambda", region_name=region)
        response = client.invoke(
            FunctionName=Settings.GH_AUTH_LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=b"{}",
        )
        payload = response["Payload"].read().decode("utf-8")
        data = json.loads(payload)
        if "FunctionError" in response:
            raise RuntimeError("GH auth lambda failed (payload redacted)")
        if isinstance(data, dict) and "statusCode" in data:
            if int(data.get("statusCode", 500)) >= 400:
                raise RuntimeError(
                    f"GH auth lambda returned statusCode={data.get('statusCode')} "
                    "(body redacted)"
                )
            body = data.get("body", "{}")
            data = json.loads(body) if isinstance(body, str) else body
        token = data.get("token")
        expires_at_iso = data.get("expires_at")
        if not token:
            raise RuntimeError("GH auth lambda returned no token (payload redacted)")
        cls._validate_permissions(data.get("permissions"), required_permissions)
        if expires_at_iso:
            expires_at = datetime.fromisoformat(
                expires_at_iso.replace("Z", "+00:00")
            ).timestamp()
        else:
            expires_at = time.time() + 3600
        return token, expires_at


    @classmethod
    def _post_installation_token(cls, jwt_token: str, installation_id: int):
        """POST the JWT for an installation access token; return (token, expires_at_epoch).

        GitHub returns the token plus an ISO 8601 ``expires_at`` (~1h ahead).
        We surface both so ``GHTokenProvider`` can refresh ahead of expiry.
        """
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = requests.post(
            f"https://api.github.com/app/installations/{installation_id}/access_tokens",
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        token = data["token"]
        expires_at_iso = data.get("expires_at")
        if expires_at_iso:
            expires_at = datetime.fromisoformat(
                expires_at_iso.replace("Z", "+00:00")
            ).timestamp()
        else:
            expires_at = time.time() + 3600
        return token, expires_at

    @classmethod
    def _get_access_token_by_jwt(cls, jwt_token: str, installation_id: int) -> str:
        token, _ = cls._post_installation_token(jwt_token, installation_id)
        return token

    @classmethod
    def _get_access_token(cls, private_key: str, app_id: str, installation_id: int) -> str:
        payload = {
            "iat": int(time.time()) - 60,
            "exp": int(time.time()) + (10 * 60),
            "iss": app_id,
        }

        jwt_instance = jwt.PyJWT()
        encoded_jwt = jwt_instance.encode(payload, private_key, algorithm="RS256")
        return cls._get_access_token_by_jwt(encoded_jwt, installation_id)

    @classmethod
    def _get_access_token_deprecated(cls, app_key, app_id, installation_id: int):
        def _generate_jwt(client_id, pem):
            pem = str.encode(pem)
            signing_key = jwk_from_pem(pem)
            payload = {
                "iat": int(time.time()),
                "exp": int(time.time()) + 600,
                "iss": client_id,
            }
            # Create JWT
            jwt_instance = JWT()
            encoded_jwt = jwt_instance.encode(payload, signing_key, alg="RS256")
            return encoded_jwt

        jwt_token = _generate_jwt(app_id, app_key)
        return cls._get_access_token_by_jwt(jwt_token, installation_id)

    @classmethod
    def auth(cls, app_id, app_key, installation_id: int) -> None:
        if USING_PYJWT:
            access_token = cls._get_access_token(app_key, app_id, installation_id)
        else:
            access_token = cls._get_access_token_deprecated(app_key, app_id, installation_id)
        Shell.check(
            "gh auth login --with-token",
            stdin_str=f"{access_token}\n",
            strict=True,
        )

    @classmethod
    def _read_app_credentials(cls):
        """Read (app_id, pem, installation_id) from Secrets Manager via the
        Settings.SECRET_GH_APP entry. Shared by ``get_installation_token`` and
        ``GHTokenProvider`` so both go through the same secret."""
        from praktika.secret import Secret
        from praktika.settings import Settings

        app_id, pem, installation_id = Secret.Config(
            name=[
                f"{Settings.SECRET_GH_APP}.app-id",
                f"{Settings.SECRET_GH_APP}.app-key",
                f"{Settings.SECRET_GH_APP}.app-installation-id",
            ],
            type=Secret.Type.AWS_SSM_SECRET,
            region=Settings.AWS_REGION,
        ).get_value()
        return app_id, pem, int(installation_id)

    @classmethod
    def get_installation_token(cls, required_permissions=None) -> str:
        """Return a raw GitHub App installation access token."""
        from .settings import Settings

        if Settings.GH_AUTH_LAMBDA_NAME:
            token, _ = cls._get_lambda_token_with_expiry(
                required_permissions=required_permissions
            )
            return token
        app_id, pem, installation_id = cls._read_app_credentials()
        return cls._get_access_token(pem, app_id, installation_id)

    @classmethod
    def get_installation_token_with_expiry(cls, required_permissions=None):
        """Like ``get_installation_token`` but returns ``(token, expires_at_epoch)``."""
        from .settings import Settings

        if Settings.GH_AUTH_LAMBDA_NAME:
            return cls._get_lambda_token_with_expiry(
                required_permissions=required_permissions
            )
        app_id, pem, installation_id = cls._read_app_credentials()
        payload = {
            "iat": int(time.time()) - 60,
            "exp": int(time.time()) + (10 * 60),
            "iss": app_id,
        }
        encoded_jwt = jwt.PyJWT().encode(payload, pem, algorithm="RS256")
        return cls._post_installation_token(encoded_jwt, installation_id)

    @classmethod
    def auth_from_settings(cls) -> None:
        Shell.check(
            "gh auth login --with-token",
            stdin_str=f"{cls.get_installation_token()}\n",
            strict=True,
        )


class GHTokenProvider:
    """Auto-refreshing GitHub App installation token cache.

    Installation tokens have a fixed ~1h lifetime (GitHub-side, not
    configurable), so any process that outlives a single token must
    re-mint. This provider caches the most recent token and re-mints
    transparently on the next ``get()`` once the cached token is within
    ``refresh_margin`` seconds of expiry. Thread-safe.

    Pass instances where a token *callable* is expected (``CheckRun``,
    ``JobCheckRun``); call sites resolve via ``__call__`` on every API hit
    and never see a stale token. Callers that want a one-shot string can
    use ``GHAuth.get_installation_token()`` directly.
    """

    def __init__(self, refresh_margin: int = 300):
        self._refresh_margin = refresh_margin
        self._token = None
        self._expires_at = 0.0
        self._lock = threading.Lock()

    def get(self) -> str:
        with self._lock:
            if self._token and time.time() < self._expires_at - self._refresh_margin:
                return self._token
            self._token, self._expires_at = GHAuth.get_installation_token_with_expiry()
            return self._token

    def __call__(self) -> str:
        return self.get()


# if __name__ == "__main__":
#     from praktika.secret import Secret
#
#     pem = Secret.Config(
#         name="woolenwolf_gh_app.clickhouse-app-key",
#         type=Secret.Type.AWS_SSM_SECRET,
#     ).get_value()
#     app_id = Secret.Config(
#         name="woolenwolf_gh_app.clickhouse-app-id",
#         type=Secret.Type.AWS_SSM_SECRET,
#     ).get_value()
#     print(app_id, pem)
#     GHAuth.auth(app_id, pem)
