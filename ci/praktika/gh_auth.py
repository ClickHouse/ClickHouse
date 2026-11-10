import threading
import time
import json
from datetime import datetime

from praktika.utils import Shell

# `gh auth login --with-token` validates the token against api.github.com. A single
# transient GitHub API 5xx/timeout there would otherwise hard-fail the whole job.
# Retry only on transport-class errors; auth errors (e.g. HTTP 401 bad token) stay fatal.
_GH_AUTH_RETRY_ERRORS = [
    "HTTP 500",
    "HTTP 502",
    "HTTP 503",
    "HTTP 504",
    "HTTP 429",
    "Service Unavailable",
    "Bad Gateway",
    "Gateway Timeout",
    "Too Many Requests",
    "Internal Server Error",
    "i/o timeout",
    "TLS handshake timeout",
    "connection reset by peer",
    "connection refused",
    "EOF",
]


_PERMISSION_LEVELS = {
    "none": 0,
    "read": 1,
    "write": 2,
    "admin": 3,
}


class GHAuth:
    # Set once a token has been minted, so it is done at most once per process.
    _authenticated = False

    @staticmethod
    def _describe_lambda_failure(response, data) -> str:
        details = []
        function_error = response.get("FunctionError")
        if function_error:
            details.append(f"FunctionError={function_error}")
        status_code = response.get("StatusCode")
        if status_code is not None:
            details.append(f"StatusCode={status_code}")
        executed_version = response.get("ExecutedVersion")
        if executed_version:
            details.append(f"ExecutedVersion={executed_version}")

        if isinstance(data, dict):
            if data.get("statusCode") is not None:
                details.append(f"statusCode={data.get('statusCode')}")
            if data.get("errorType"):
                details.append(f"errorType={data.get('errorType')}")
            if data.get("errorMessage"):
                details.append(f"errorMessage={data.get('errorMessage')}")
            if data.get("stackTrace"):
                details.append("stackTrace=present")

        return ", ".join(details) if details else "no diagnostic details available"

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
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                "GH auth lambda returned non-JSON payload "
                f"[{payload[:200]}]: {e}"
            ) from e
        if "FunctionError" in response:
            raise RuntimeError(
                "GH auth lambda failed "
                f"({cls._describe_lambda_failure(response, data)})"
            )
        if isinstance(data, dict) and "statusCode" in data:
            if int(data.get("statusCode", 500)) >= 400:
                raise RuntimeError(
                    "GH auth lambda returned error "
                    f"({cls._describe_lambda_failure(response, data)})"
                )
            body = data.get("body", "{}")
            data = json.loads(body) if isinstance(body, str) else body
        token = data.get("token")
        expires_at_iso = data.get("expires_at")
        if not token:
            raise RuntimeError(
                "GH auth lambda returned no token "
                f"({cls._describe_lambda_failure(response, data)})"
            )
        cls._validate_permissions(data.get("permissions"), required_permissions)
        if expires_at_iso:
            expires_at = datetime.fromisoformat(
                expires_at_iso.replace("Z", "+00:00")
            ).timestamp()
        else:
            expires_at = time.time() + 3600
        return token, expires_at

    @classmethod
    def _get_access_token_from_lambda(cls, lambda_name: str, region: str) -> str:
        import boto3  # type: ignore

        client = boto3.session.Session().client(
            service_name="lambda", region_name=region or None
        )
        response = client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=b"{}",
        )
        if response.get("FunctionError"):
            raise RuntimeError(
                f"Lambda {lambda_name} returned FunctionError (payload redacted)"
            )
        result = json.loads(response["Payload"].read())
        status_code = result.get("statusCode")
        if status_code != 200:
            raise RuntimeError(
                f"Lambda {lambda_name} returned statusCode={status_code} (body redacted)"
            )
        body = json.loads(result["body"])
        return body["token"]

    @classmethod
    def auth_with_lambda(
        cls, lambda_name: str, region: str = "", no_strict: bool = False
    ) -> bool:
        """
        Authenticate `gh` with a token minted by the given AWS Lambda.

        By default an authentication failure raises; pass `no_strict=True` to
        instead print a warning and return False.
        """
        try:
            print(f"Mint GitHub token via lambda [{lambda_name}]")
            access_token = cls._get_access_token_from_lambda(lambda_name, region)
            return Shell.check(
                "gh auth login --with-token",
                stdin_str=f"{access_token}\n",
                strict=not no_strict,
                retries=4,
                retry_errors=_GH_AUTH_RETRY_ERRORS,
            )
        except Exception as e:
            if not no_strict:
                raise
            print(f"WARNING: GH auth failed: {e}")
            return False

    @classmethod
    def auth(cls, workflow=None, force=False, no_strict: bool = False) -> bool:
        """
        Authenticate `gh` for GitHub API calls and return whether `gh` is usable.

        A token is minted from the AWS Lambda configured for the workflow
        (Workflow.Config.gh_auth_lambda_name) or globally
        (Settings.GH_AUTH_LAMBDA_NAME). When no lambda is configured, the
        ambient `gh` token is assumed and this is a no-op.

        The token is minted at most once per process unless `force` is set.

        By default an authentication failure raises. Pass `no_strict=True` to
        instead print a warning and return False (the historical behaviour).
        """
        from praktika.settings import Settings

        if cls._authenticated and not force:
            return True

        lambda_name = (
            workflow.gh_auth_lambda_name if workflow else ""
        ) or Settings.GH_AUTH_LAMBDA_NAME

        if not lambda_name:
            # No lambda configured - rely on the ambient gh token.
            return True

        try:
            authenticated = cls.auth_with_lambda(
                lambda_name, Settings.GH_AUTH_LAMBDA_REGION, no_strict=no_strict
            )
        except Exception as e:
            if not no_strict:
                raise
            print(f"WARNING: GH auth failed: {e}")
            authenticated = False

        cls._authenticated = authenticated
        return authenticated

    @classmethod
    def get_installation_token(cls, required_permissions=None) -> str:
        """Return a raw GitHub App installation access token minted via the
        GH_AUTH_LAMBDA_NAME lambda (raises if no lambda is configured)."""
        token, _ = cls._get_lambda_token_with_expiry(
            required_permissions=required_permissions
        )
        return token

    @classmethod
    def get_installation_token_with_expiry(cls, required_permissions=None):
        """Like ``get_installation_token`` but returns ``(token, expires_at_epoch)``.

        Minted via the GH_AUTH_LAMBDA_NAME lambda (raises if not configured)."""
        return cls._get_lambda_token_with_expiry(
            required_permissions=required_permissions
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
