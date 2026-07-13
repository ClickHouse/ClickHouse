import dataclasses
import json
import os
from typing import List, Union


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
            import boto3

            client = boto3.client(
                "ssm",
                region_name=self.region or None,
            )
            res = client.get_parameter(
                Name=self.name,
                WithDecryption=True,
            )
            value = res.get("Parameter", {}).get("Value", "")
            if not value:
                raise RuntimeError(f"Empty value for parameter [{self.name}]")
            return value

        def get_aws_ssm_parameters(self):
            """
            Request multiple parameters at once to avoid rate limiting
            """
            import boto3

            assert isinstance(self.name, list)
            client = boto3.client(
                "ssm",
                region_name=self.region or None,
            )
            res = client.get_parameters(
                Names=self.name,
                WithDecryption=True,
            )
            name_to_value = {
                parameter.get("Name", ""): parameter.get("Value", "")
                for parameter in res.get("Parameters", [])
            }

            for n in self.name:
                if n not in name_to_value:
                    raise RuntimeError(f"Failed to get value for parameter [{n}]")
                if not name_to_value[n]:
                    raise RuntimeError(f"Empty value for parameter [{n}]")

            return [name_to_value[name] for name in self.name]

        def get_aws_ssm_secret(self):
            import boto3

            name, secret_key_name = self.name, ""
            if "." in self.name:
                name, secret_key_name = self.name.split(".", 1)
            client = boto3.client(
                "secretsmanager",
                region_name=self.region or None,
            )
            res = client.get_secret_value(SecretId=name)
            secret_string = res.get("SecretString", "")
            if not secret_string:
                raise RuntimeError(f"Empty value for secret [{name}]")
            if secret_key_name:
                return json.loads(secret_string)[secret_key_name]
            return secret_string

        def get_aws_ssm_secrets_batched(self):
            """
            Fetch multiple secrets efficiently, making one CLI call per unique root
            secret. Secrets sharing the same root (e.g. "vault.key1" and "vault.key2")
            are resolved from a single get_secret_value response parsed in Python,
            which correctly handles multi-line values such as PEM keys.
            """
            import boto3

            assert isinstance(self.name, list)

            # Parse each name into (root, key); key is None when there is no dot
            parsed = [(n.split(".", 1) if "." in n else (n, None)) for n in self.name]

            # Group indices by root, preserving insertion order
            root_to_indices: dict = {}
            for i, (root, _) in enumerate(parsed):
                root_to_indices.setdefault(root, []).append(i)

            results = [None] * len(self.name)
            client = boto3.client(
                "secretsmanager",
                region_name=self.region or None,
            )

            for root, indices in root_to_indices.items():
                res = client.get_secret_value(SecretId=root)
                secret_string = res.get("SecretString", "")
                if not secret_string:
                    raise RuntimeError(f"Empty value for secret [{root}]")
                keys = [parsed[idx][1] for idx in indices]
                # Only parse JSON when at least one entry requests a specific key;
                # keyless requests return the raw secret string to stay compatible
                # with non-JSON secrets.
                secret_data = (
                    json.loads(secret_string)
                    if any(k is not None for k in keys)
                    else None
                )
                for idx, key in zip(indices, keys):
                    results[idx] = secret_data[key] if key is not None else secret_string

            return results

        def get_gh_secret(self):
            res = os.getenv(f"{self.name}")
            if not res:
                raise RuntimeError(
                    f"Failed to get GH_SECRET [{self.name}]: env var is unset or empty"
                )
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
