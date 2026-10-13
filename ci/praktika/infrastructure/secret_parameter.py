from ._utils import aws_client
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


class SecretParameter:

    @dataclass
    class Config:
        """An AWS SSM Parameter Store SecureString entry.

        Idempotent: if the parameter already exists it is never overwritten,
        so manual rotations and out-of-band updates are preserved.

        If generate_random=True and the parameter does not exist, a random
        secret is generated, stored in SSM, and written to a local file under
        the infra config directory for initial reference. The file is only
        written on first creation — subsequent deploys skip both SSM and the
        file if the parameter is already present.
        """

        name: str
        description: str = ""
        generate_random: bool = False
        region: str = ""
        ext: Dict[str, Any] = field(default_factory=dict)

        def deploy(self):
            import secrets


            ssm = aws_client("ssm", self.region, self.name)

            try:
                ssm.get_parameter(Name=self.name, WithDecryption=False)
                print(f"Secret parameter '{self.name}' already exists, skipping")
                return self
            except ssm.exceptions.ParameterNotFound:
                pass

            if not self.generate_random:
                raise ValueError(
                    f"Secret parameter '{self.name}' does not exist and "
                    f"generate_random=False — create it manually or set generate_random=True"
                )

            value = secrets.token_urlsafe(32)
            ssm.put_parameter(
                Name=self.name,
                Value=value,
                Type="SecureString",
                Description=self.description or f"Auto-generated secret for {self.name}",
            )
            print(f"Generated and stored secret parameter '{self.name}'")
            self._dump_secret(value)
            return self

        def _dump_secret(self, value: str):
            from ..settings import Settings

            out_dir = Path(Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH).parent
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{self.name}.secret"
            out_file.write_text(f"{value}\n")
            print(f"Secret value written to {out_file} (keep this file safe)")

        def delete(self):
            ssm = aws_client("ssm", self.region, self.name)
            try:
                ssm.delete_parameter(Name=self.name)
                print(f"Deleted secret parameter '{self.name}'")
            except ssm.exceptions.ParameterNotFound:
                print(f"Secret parameter '{self.name}' does not exist, skipping")
