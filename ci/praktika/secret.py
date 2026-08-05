import dataclasses
import json
import os
from typing import List, Union

from .utils import Shell


class Secret:

    class Type:
        AWS_SSM_PARAMETER = "aws parameter"
        AWS_SSM_SECRET = "aws secret"
        GH_SECRET = "gh secret"
        GH_VAR = "gh var"

    @dataclasses.dataclass
    class Config:
        name: Union[List[str], str]
        type: str
        region: str = ""

        def is_gh_secret(self):
            return self.type == Secret.Type.GH_SECRET

        def is_gh_var(self):
            return self.type == Secret.Type.GH_VAR

        def get_value(self):
            if self.type == Secret.Type.AWS_SSM_PARAMETER:
                if isinstance(self.name, list):
                    return self.get_aws_ssm_parameters()
                else:
                    return self.get_aws_ssm_parameter()
            if self.type == Secret.Type.AWS_SSM_SECRET:
                if isinstance(self.name, list):
                    return self.get_aws_ssm_secrets_batched()
                else:
                    return self.get_aws_ssm_secret()
            elif self.type in (Secret.Type.GH_SECRET, Secret.Type.GH_VAR):
                if isinstance(self.name, list):
                    res = []
                    for name in self.name:
                        res.append(Secret.Config(name=name, type=self.type).get_value())
                    return res
                else:
                    return self.get_gh_secret()
            else:
                assert False, f"Not supported secret type, secret [{self}]"

        def get_aws_ssm_parameter(self):
            region = ""
            if self.region:
                region = f" --region {self.region}"
            res = Shell.get_output(
                f"aws ssm get-parameter --name {self.name} --with-decryption --output text --query Parameter.Value {region}",
                strict=True,
            )
            return res

        def get_aws_ssm_parameters(self):
            """
            Request multiple parameters at once to avoid rate limiting
            """
            region = ""
            if self.region:
                region = f" --region {self.region}"
            assert isinstance(self.name, list)
            res = Shell.get_output(
                f"aws ssm get-parameters --names {' '.join(self.name)} --with-decryption --output text --query 'Parameters[*].[Name,Value]' {region}",
                strict=True,
            )
            name_value_pairs = res.split("\n")
            names = [n.split("\t")[0].strip() for n in name_value_pairs]
            values = [n.split("\t")[1].strip() for n in name_value_pairs]

            for n in self.name:
                if n not in names:
                    raise RuntimeError(f"Failed to get value for parameter [{n}]")

            # Sort to match requested order and validate values:
            name_value_pairs = list(zip(names, values))
            name_value_pairs.sort(key=lambda x: self.name.index(x[0]))

            for name, value in name_value_pairs:
                if not value:
                    raise RuntimeError(f"Empty value for parameter [{name}]")

            values = [pair[1] for pair in name_value_pairs]
            return values

        def get_aws_ssm_secret(self):
            name, secret_key_name = self.name, ""
            if "." in self.name:
                name, secret_key_name = self.name.split(".", 1)
            region = ""
            if self.region:
                region = f" --region {self.region}"
            cmd = f"aws secretsmanager get-secret-value --secret-id  {name} --query SecretString --output text {region}"
            if secret_key_name:
                cmd += f" | jq -r '.[\"{secret_key_name}\"]'"
            res = Shell.get_output(cmd, verbose=True, strict=True)
            return res

        def get_aws_ssm_secrets_batched(self):
            """
            Fetch multiple secrets efficiently, making one CLI call per unique root
            secret. Secrets sharing the same root (e.g. "vault.key1" and "vault.key2")
            are resolved from a single get_secret_value response parsed in Python,
            which correctly handles multi-line values such as PEM keys.
            """
            assert isinstance(self.name, list)

            region = f" --region {self.region}" if self.region else ""

            # Parse each name into (root, key); key is None when there is no dot
            parsed = [(n.split(".", 1) if "." in n else (n, None)) for n in self.name]

            # Group indices by root, preserving insertion order
            root_to_indices: dict = {}
            for i, (root, _) in enumerate(parsed):
                root_to_indices.setdefault(root, []).append(i)

            results = [None] * len(self.name)

            for root, indices in root_to_indices.items():
                cmd = f"aws secretsmanager get-secret-value --secret-id {root} --query SecretString --output text{region}"
                secret_string = Shell.get_output(cmd, verbose=True, strict=True)
                keys = [parsed[idx][1] for idx in indices]
                # Only parse JSON when at least one entry requests a specific key;
                # keyless requests return the raw secret string to stay compatible
                # with non-JSON secrets.
                secret_data = json.loads(secret_string) if any(k is not None for k in keys) else None
                for idx, key in zip(indices, keys):
                    results[idx] = secret_data[key] if key is not None else secret_string

            return results

        def get_gh_secret(self):
            res = os.getenv(f"{self.name}")
            if not res:
                print(f"ERROR: Failed to get secret [{self.name}]")
                raise RuntimeError()
            return res

        def join_with(self, other):
            """
            Join secrets of the same type and region, to allow requesting all at once and save on api calls if applicable
            """
            assert self.type == other.type or all(
                type_ in (Secret.Type.GH_SECRET, Secret.Type.GH_VAR)
                for type_ in (self.type, other.type)
            ), f"Secrets must have the same type [{self.type}] and [{other.type}]"
            assert (
                self.region == other.region
            ), f"Secrets must have the same region [{self.region}] and [{other.region}]"
            assert (
                self.name != other.name
            ), f"Secrets must have different names [{self.name}] and [{other.name}]"
            assert isinstance(
                other.name, str
            ), f"Secret [{other.name}] must be single name"

            names = list(self.name) if isinstance(self.name, list) else [self.name]
            names.append(other.name)
            return Secret.Config(name=names, type=self.type, region=self.region)

        def __repr__(self):
            return self.name
