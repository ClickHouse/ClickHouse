from cryptography import x509
from cryptography.hazmat.primitives import serialization

from certificates import generate_self_signed_cert, generate_certificate

def _handle_certificate_get_request(csr_map, hostname, order_id):
    private_key, _ = generate_self_signed_cert(hostname)

    # For ACME we're using URL-safe base64 encoded bytes
    # No PEM header, no newlines, nothing
    almost_der = csr_map[order_id]
    almost_der = almost_der.replace("-", "+").replace("_", "/")
    formatted_csr = "\n".join(
        almost_der[i : i + 64] for i in range(0, len(almost_der), 64)
    )
    header = "-----BEGIN CERTIFICATE REQUEST-----"
    footer = "-----END CERTIFICATE REQUEST-----"

    csr = f"{header}\n{formatted_csr}\n{footer}"
    print(csr)

    x509_csr = x509.load_pem_x509_csr(csr.encode())

    certificate = generate_certificate(x509_csr, private_key)
    return certificate.public_bytes(serialization.Encoding.PEM).decode(), 200
