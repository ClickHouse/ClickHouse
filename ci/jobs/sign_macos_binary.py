import argparse
import json
import os
import struct
import subprocess
from pathlib import Path

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils

RCODESIGN = "/rust/cargo/bin/rcodesign"
PKCS11_PROXY = "/usr/lib/x86_64-linux-gnu/p11-kit-proxy.so"

ASSETS_DIR = Path("./ci/jobs/scripts/sign_macos_binary")
CERTIFICATE_CHAIN = ASSETS_DIR / "apple-developer-id-chain.pem"
ENTITLEMENTS = ASSETS_DIR / "entitlements.plist"

TEMP_DIR = Path(f"{Utils.cwd()}/ci/tmp")
INPUT_BINARY = TEMP_DIR / "clickhouse"
SIGNED_BINARY = TEMP_DIR / "signed" / "clickhouse"
SIGNED_ZIP = TEMP_DIR / "clickhouse-macos.zip"
EMPTY_CONFIG = TEMP_DIR / "rcodesign-empty.toml"
NOTARY_P8 = TEMP_DIR / "notary_key.p8"
NOTARY_API_KEY_JSON = TEMP_DIR / "notary_api_key.json"

KMS_KEY_ARN = (
    "arn:aws:kms:us-east-1:445567100269:key/mrk-9742178ad5054c8ba662fb073b62aac8"
)
KMS_REGION = "us-east-1"
SIGNING_ROLE_ARN = "arn:aws:iam::445567100269:role/release_signing"
PKCS11_TOKEN_LABEL = "clickhouse_macos_signing"

APPLE_TEAM_ID = "ZNDB5FJ8ZW"
LEAF_COMMON_NAME = f"Developer ID Application: ClickHouse Inc. ({APPLE_TEAM_ID})"
TIMESTAMP_URL = "http://timestamp.apple.com/ts01"

_NOTARY_KEY_SECRET = Secret.Config(
    name="/release/apple-notary/notary_key",
    type=Secret.Type.AWS_SSM_PARAMETER,
)
NOTARY_ISSUER_ID = "b17e8d0b-6b5d-4063-9a1d-24d9baa80daf"
NOTARY_KEY_ID = "99QTC6XVSW"

MAX_SIZE_GROWTH_BYTES = 32 * 1024 * 1024


def assume_signing_role():
    # Assume the signing role in the security account. This job runs on the
    # dedicated release-runner pool, whose instance role (release_runner) is
    # the only principal release_signing trusts. We just swap the env creds for
    # the short-lived signing creds; the instance role stays reachable via IMDS.
    creds = json.loads(
        subprocess.check_output(
            [
                "aws",
                "sts",
                "assume-role",
                "--role-arn",
                SIGNING_ROLE_ARN,
                "--role-session-name",
                "macos-signing",
                "--region",
                KMS_REGION,
                "--query",
                "Credentials",
                "--output",
                "json",
            ]
        )
    )
    os.environ["AWS_ACCESS_KEY_ID"] = creds["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["SecretAccessKey"]
    os.environ["AWS_SESSION_TOKEN"] = creds["SessionToken"]
    print("Assumed signing role")


def report_tooling():
    ok = Shell.check(f"{RCODESIGN} --version", verbose=True)
    if not Path(PKCS11_PROXY).exists():
        print(f"ERROR: no p11-kit proxy at [{PKCS11_PROXY}]")
        return False
    print(f"p11-kit proxy: {PKCS11_PROXY}")
    Shell.check("p11-kit list-modules", verbose=True)
    return ok


def looks_like_self_extracting_archive(path):
    size = path.stat().st_size
    if size < 16:
        return False
    with open(path, "rb") as f:
        f.seek(size - 16)
        number_of_files, start_of_files_data = struct.unpack("<QQ", f.read(16))
    return (
        1 <= number_of_files <= 64
        and 0 < start_of_files_data < size
        and size - start_of_files_data < 64 * 1024
    )


def check_input_binary():
    if not INPUT_BINARY.exists():
        print(f"ERROR: no input binary at [{INPUT_BINARY}]")
        return False

    with open(INPUT_BINARY, "rb") as f:
        magic = f.read(4)
    if magic not in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf"):
        print(f"ERROR: [{INPUT_BINARY}] is not a 64-bit Mach-O, magic {magic!r}")
        return False

    if looks_like_self_extracting_archive(INPUT_BINARY):
        print(
            f"ERROR: [{INPUT_BINARY}] is a self-extracting archive, not a plain "
            "binary. Signing it would discard the payload."
        )
        return False

    print(f"input is a plain Mach-O of {INPUT_BINARY.stat().st_size} bytes")
    return True


def configure_kms_module():
    config = {
        "slots": [
            {
                "label": PKCS11_TOKEN_LABEL,
                "kms_key_id": KMS_KEY_ARN,
                "aws_region": KMS_REGION,
            }
        ]
    }
    for directory in ("/etc/aws-kms-pkcs11", f"{Path.home()}/.config/aws-kms-pkcs11"):
        Path(directory).mkdir(parents=True, exist_ok=True)
        Path(f"{directory}/config.json").write_text(json.dumps(config, indent=2))
    print(json.dumps(config, indent=2))
    return Shell.check(
        f"p11-kit list-modules | grep -q '{PKCS11_TOKEN_LABEL}'", verbose=True
    )


def check_certificate_matches_key():
    kms_pub = TEMP_DIR / "kms_pub.pem"
    cert_pub = TEMP_DIR / "cert_pub.pem"
    if not Shell.check(
        f"aws kms get-public-key --key-id {KMS_KEY_ARN} --region {KMS_REGION}"
        " --query PublicKey --output text"
        f" | base64 -d | openssl pkey -pubin -inform DER -pubout > {kms_pub}",
        verbose=True,
    ):
        return False
    if not Shell.check(
        f"openssl x509 -in {CERTIFICATE_CHAIN} -noout -pubkey > {cert_pub}",
        verbose=True,
    ):
        return False
    if kms_pub.read_text() != cert_pub.read_text():
        print("ERROR: the certificate public key does not match the KMS key")
        return False
    print("the certificate public key matches the KMS key")

    subject = Shell.get_output(f"openssl x509 -in {CERTIFICATE_CHAIN} -noout -subject")
    print(f"leaf subject: {subject}")
    if LEAF_COMMON_NAME not in subject:
        print(f"ERROR: the leaf certificate is not [{LEAF_COMMON_NAME}]")
        return False
    return True


def sign():
    SIGNED_BINARY.parent.mkdir(parents=True, exist_ok=True)
    EMPTY_CONFIG.write_text("")
    return Shell.check(
        f"{RCODESIGN} -v --config-file {EMPTY_CONFIG} sign"
        f" --pkcs11-library {PKCS11_PROXY}"
        f" --pkcs11-token-label {PKCS11_TOKEN_LABEL}"
        f" --pkcs11-certificate-file {CERTIFICATE_CHAIN}"
        f" --entitlements-xml-file {ENTITLEMENTS}"
        " --code-signature-flags runtime"
        f" --timestamp-url {TIMESTAMP_URL}"
        f" {INPUT_BINARY} {SIGNED_BINARY}",
        verbose=True,
    )


def check_signed_output():
    if not SIGNED_BINARY.exists():
        print(f"ERROR: no signed binary at [{SIGNED_BINARY}]")
        return False
    before = INPUT_BINARY.stat().st_size
    after = SIGNED_BINARY.stat().st_size
    print(f"size before: {before}, after: {after}, delta: {after - before}")
    if after < before:
        print(f"ERROR: the signed binary lost {before - after} bytes")
        return False
    if after - before > MAX_SIZE_GROWTH_BYTES:
        print(f"ERROR: the signed binary grew by {after - before} bytes, too much")
        return False
    return True


def package():
    if SIGNED_ZIP.exists():
        SIGNED_ZIP.unlink()
    return Shell.check(f"zip -j {SIGNED_ZIP} {SIGNED_BINARY}", verbose=True)


def write_notary_api_key():
    fd = os.open(NOTARY_P8, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(_NOTARY_KEY_SECRET.get_value())
    os.chmod(NOTARY_P8, 0o600)

    ok = Shell.check(
        f"{RCODESIGN} encode-app-store-connect-api-key -o {NOTARY_API_KEY_JSON}"
        f" {NOTARY_ISSUER_ID} {NOTARY_KEY_ID} {NOTARY_P8}",
        verbose=True,
    )
    NOTARY_P8.unlink(missing_ok=True)
    if not ok:
        return False
    os.chmod(NOTARY_API_KEY_JSON, 0o600)
    return True


def notarize():
    return Shell.check(
        f"{RCODESIGN} notary-submit --api-key-file {NOTARY_API_KEY_JSON}"
        f" --wait --max-wait-seconds 1800 {SIGNED_ZIP}",
        verbose=True,
    )


def verify():
    ok = Shell.check(f"{RCODESIGN} verify {SIGNED_BINARY}", verbose=True)
    info = subprocess.run(
        f"{RCODESIGN} print-signature-info {SIGNED_BINARY}",
        shell=True,
        capture_output=True,
        text=True,
    ).stdout
    for line in info.splitlines():
        if any(
            key in line
            for key in (
                "apple_team_id",
                "identifier:",
                "flags:",
                "signed_with_algorithm",
            )
        ):
            print(line.strip())
    if f"apple_team_id: {APPLE_TEAM_ID}" not in info:
        print(f"ERROR: the signature does not carry team id {APPLE_TEAM_ID}")
        return False
    return ok


def parse_args():
    parser = argparse.ArgumentParser("Sign a macOS binary")
    parser.add_argument("--build-type", required=True, help="amd_darwin or arm_darwin")
    return parser.parse_args()


def main():
    stopwatch = Utils.Stopwatch()
    args = parse_args()
    print(f"Build type [{args.build_type}], job [{Info().job_name}]")

    steps = [
        # read the notary key with the runner's own creds BEFORE assuming the
        # signing role (release_signing can only kms:Sign, not read SSM).
        ("write notary api key", write_notary_api_key),
        ("assume signing role", assume_signing_role),
        ("report tooling", report_tooling),
        ("configure KMS module", configure_kms_module),
        ("check certificate against key", check_certificate_matches_key),
        ("check input", check_input_binary),
        ("sign", sign),
        ("check output", check_signed_output),
        ("package", package),
        ("verify", verify),
        ("notarize", notarize),
    ]

    results = []
    try:
        for name, command in steps:
            results.append(Result.from_commands_run(name=name, command=command))
            if not results[-1].is_ok():
                print(f"Step [{name}] failed, stopping")
                break
    finally:
        # never leave the App Store Connect credential in the reused self-hosted
        # workspace, even if a step before notarize failed.
        NOTARY_P8.unlink(missing_ok=True)
        NOTARY_API_KEY_JSON.unlink(missing_ok=True)

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
