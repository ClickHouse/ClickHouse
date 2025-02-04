import dataclasses
import os

from praktika.utils import Shell


class Secret:
    class Type:
        AWS_SSM_VAR = "aws parameter"
        AWS_SSM_SECRET = "aws secret"
        GH_SECRET = "gh secret"

    @dataclasses.dataclass
    class Config:
        name: str
        type: str

        def is_gh(self):
            return self.type == Secret.Type.GH_SECRET

        def get_value(self):
            if self.type == Secret.Type.AWS_SSM_VAR:
                return self.get_aws_ssm_var()
            if self.type == Secret.Type.AWS_SSM_SECRET:
                return self.get_aws_ssm_secret()
            elif self.type == Secret.Type.GH_SECRET:
                return self.get_gh_secret()
            else:
                assert False, f"Not supported secret type, secret [{self}]"

        def get_aws_ssm_var(self):
            res = Shell.get_output(
                f"aws ssm get-parameter --name {self.name} --with-decryption --output text --query Parameter.Value",
            )
            if not res:
                print(f"ERROR: Failed to get secret [{self.name}]")
                raise RuntimeError()
            return res

        def get_aws_ssm_secret(self):
            name, secret_key_name = self.name, ""
            if "." in self.name:
                name, secret_key_name = self.name.split(".")
            cmd = f"aws secretsmanager get-secret-value --secret-id  {name} --query SecretString --output text"
            if secret_key_name:
                cmd += f" | jq -r '.[\"{secret_key_name}\"]'"
            res = Shell.get_output(cmd, verbose=True)
            if not res:
                print(f"ERROR: Failed to get secret [{self.name}]")
                raise RuntimeError()
            return res

        def get_gh_secret(self):
            res = os.getenv(f"{self.name}")
            if not res:
                print(f"ERROR: Failed to get secret [{self.name}]")
                raise RuntimeError()
            return res

        def __repr__(self):
            return self.name
