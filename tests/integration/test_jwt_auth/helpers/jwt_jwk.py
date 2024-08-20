from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

import base64
import json
import jwt


"""
Only RS* family algorithms are supported!!!
"""
with open("./jwtRS256.key", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )


public_key = private_key.public_key()


def to_base64_url(data):
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def rsa_key_to_jwk(private_key=None, public_key=None):
    if private_key:
        # Convert the private key to its components
        private_numbers = private_key.private_numbers()
        public_numbers = private_key.public_key().public_numbers()

        jwk = {
            "kty": "RSA",
            "alg": "RS256",
            "kid": "mykid",
            "n": to_base64_url(
                public_numbers.n.to_bytes(
                    (public_numbers.n.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "e": to_base64_url(
                public_numbers.e.to_bytes(
                    (public_numbers.e.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "d": to_base64_url(
                private_numbers.d.to_bytes(
                    (private_numbers.d.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "p": to_base64_url(
                private_numbers.p.to_bytes(
                    (private_numbers.p.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "q": to_base64_url(
                private_numbers.q.to_bytes(
                    (private_numbers.q.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "dp": to_base64_url(
                private_numbers.dmp1.to_bytes(
                    (private_numbers.dmp1.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "dq": to_base64_url(
                private_numbers.dmq1.to_bytes(
                    (private_numbers.dmq1.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "qi": to_base64_url(
                private_numbers.iqmp.to_bytes(
                    (private_numbers.iqmp.bit_length() + 7) // 8, byteorder="big"
                )
            ),
        }
    elif public_key:
        # Convert the public key to its components
        public_numbers = public_key.public_numbers()

        jwk = {
            "kty": "RSA",
            "alg": "RS256",
            "kid": "mykid",
            "n": to_base64_url(
                public_numbers.n.to_bytes(
                    (public_numbers.n.bit_length() + 7) // 8, byteorder="big"
                )
            ),
            "e": to_base64_url(
                public_numbers.e.to_bytes(
                    (public_numbers.e.bit_length() + 7) // 8, byteorder="big"
                )
            ),
        }
    else:
        raise ValueError("You must provide either a private or public key.")

    return jwk


# Convert to JWK
jwk_private = rsa_key_to_jwk(private_key=private_key)
jwk_public = rsa_key_to_jwk(public_key=public_key)

print(f"Private JWK:\n{json.dumps(jwk_private)}\n")
print(f"Public JWK:\n{json.dumps(jwk_public)}\n")

payload = {"sub": "jwt_user", "iss": "test_iss"}

# Create a JWT
token = jwt.encode(payload, private_key, headers={"kid": "mykid"}, algorithm="RS256")
print(f"JWT:\n{token}")
