import datetime
import OpenSSL

# Load the certificate file
with open('/etc/clickhouse-server/server.crt', 'r') as f:
    cert = f.read()

# Parse the certificate and get the expiration date
x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)
print(datetime.datetime.strptime(x509.get_notAfter().decode('ascii'), '%Y%m%d%H%M%SZ'))
