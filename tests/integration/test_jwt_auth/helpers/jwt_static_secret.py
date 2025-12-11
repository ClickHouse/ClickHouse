import jwt
import datetime


def create_jwt(
    payload: dict, secret: str, algorithm: str = "HS256", expiration_minutes: int = None
) -> str:
    """
    Create a JWT using a static secret and a specified encryption algorithm.

    :param payload: The payload to include in the JWT (as a dictionary).
    :param secret: The secret key used to sign the JWT.
    :param algorithm: The encryption algorithm to use (default is 'HS256').
    :param expiration_minutes: The time until the token expires (default is 60 minutes).
    :return: The encoded JWT as a string.
    """
    if expiration_minutes:
        expiration = datetime.datetime.utcnow() + datetime.timedelta(
            minutes=expiration_minutes
        )
        payload["exp"] = expiration

    return jwt.encode(payload, secret, algorithm=algorithm)


if __name__ == "__main__":
    secret = "my_secret"
    payload = {"sub": "jwt_user"}  # `sub` must contain user name

    """
    Supported algorithms:
      | HMSC  | RSA   | ECDSA  | PSS   | EdDSA   |
      | ----- | ----- | ------ | ----- | ------- |
      | HS256 | RS256 | ES256  | PS256 | Ed25519 |
      | HS384 | RS384 | ES384  | PS384 | Ed448   |
      | HS512 | RS512 | ES512  | PS512 |         |
      |       |       | ES256K |       |         |
      And None
    """
    algorithm = "HS256"

    token = create_jwt(payload, secret, algorithm)
    print(f"Generated JWT: {token}")
