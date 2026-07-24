import json
import time

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


class GHAuth:
    # Set once a token has been minted, so it is done at most once per process.
    _authenticated = False

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
    def _get_access_token_by_jwt(cls, jwt_token: str, installation_id: int) -> str:
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
        token = response.json()["token"]  # type: str
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
    def auth_with_app(
        cls, app_id, app_key, installation_id: int, no_strict: bool = False
    ) -> bool:
        """
        Authenticate `gh` with a token minted from the GitHub App secrets.

        By default an authentication failure raises; pass `no_strict=True` to
        instead print a warning and return False.
        """
        try:
            if USING_PYJWT:
                access_token = cls._get_access_token(app_key, app_id, installation_id)
            else:
                access_token = cls._get_access_token_deprecated(app_key, app_id, installation_id)
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
        (Settings.GH_AUTH_LAMBDA_NAME); if no lambda is set, the GitHub App
        secrets (SECRET_GH_APP_*) are used instead. When neither is configured,
        the ambient `gh` token is assumed and this is a no-op.

        The token is minted at most once per process unless `force` is set.

        By default an authentication failure raises. Pass `no_strict=True` to
        instead print a warning and return False (the historical behaviour).
        """
        from praktika.secret import Secret
        from praktika.settings import Settings

        if cls._authenticated and not force:
            return True

        lambda_name = (
            workflow.gh_auth_lambda_name if workflow else ""
        ) or Settings.GH_AUTH_LAMBDA_NAME

        try:
            if lambda_name:
                authenticated = cls.auth_with_lambda(
                    lambda_name, Settings.GH_AUTH_LAMBDA_REGION, no_strict=no_strict
                )
            elif Settings.SECRET_GH_APP_ID:
                app_id, pem, installation_id = (
                    Secret.Config(
                        name=Settings.SECRET_GH_APP_ID,
                        type=Secret.Type.AWS_SSM_SECRET,
                        region=Settings.SECRET_GH_APP_REGION,
                    )
                    .join_with(
                        Secret.Config(
                            name=Settings.SECRET_GH_APP_PEM_KEY,
                            type=Secret.Type.AWS_SSM_SECRET,
                            region=Settings.SECRET_GH_APP_REGION,
                        )
                    )
                    .join_with(
                        Secret.Config(
                            name=Settings.SECRET_GH_APP_INSTALLATION_ID,
                            type=Secret.Type.AWS_SSM_SECRET,
                            region=Settings.SECRET_GH_APP_REGION,
                        )
                    )
                    .get_value()
                )
                authenticated = cls.auth_with_app(
                    app_id=app_id,
                    app_key=pem,
                    installation_id=int(installation_id),
                    no_strict=no_strict,
                )
            else:
                # No custom auth configured - rely on the ambient gh token.
                return True
        except Exception as e:
            if not no_strict:
                raise
            print(f"WARNING: GH auth failed: {e}")
            authenticated = False

        cls._authenticated = authenticated
        return authenticated


# if __name__ == "__main__":
#     from ci.praktika.secret import Secret
#
#     pem = Secret.Config(
#         name="/github-app/clickhouse-gh.clickhouse-app-key",
#         type=Secret.Type.AWS_SSM_SECRET,
#     ).get_value()
#     app_id = Secret.Config(
#         name="/github-app/clickhouse-gh.clickhouse-app-id",
#         type=Secret.Type.AWS_SSM_SECRET,
#     ).get_value()
#     print(app_id, pem)
#     GHAuth.auth_with_app(app_id, pem)
