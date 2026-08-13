from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Generate RSA private key
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,  # Key size of 2048 bits
    backend=default_backend()
)

# Save the private key to a PEM file
pem_private_key = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption()  # You can add encryption if needed
)

# Write the private key to a file
with open("new_private_key", "wb") as pem_file:
    pem_file.write(pem_private_key)
