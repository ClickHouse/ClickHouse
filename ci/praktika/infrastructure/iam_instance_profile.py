from ._utils import aws_client
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class IAMInstanceProfile:

    @dataclass
    class Config:
        """Wraps an IAM role in an EC2 instance profile.

        Responsibilities:
        - Verify the role exists (fail explicitly if not)
        - Create the instance profile if missing
        - Attach the role to the profile

        Policy management is intentionally out of scope — use IAMRole.Config
        to define and deploy the role with its policies before this.
        """

        name: str
        role_name: str
        region: str = ""
        tags: Dict[str, str] = field(default_factory=dict)
        ext: Dict[str, Any] = field(default_factory=dict)

        def deploy(self):
            import boto3

            if not self.role_name:
                raise ValueError(f"role_name must be set for IAMInstanceProfile '{self.name}'")

            iam = aws_client("iam", self.region, self.name)

            # Verify role exists — fail explicitly rather than silently creating it
            try:
                role = iam.get_role(RoleName=self.role_name)["Role"]
                self.ext["role_arn"] = role["Arn"]
            except iam.exceptions.NoSuchEntityException:
                raise RuntimeError(
                    f"IAMInstanceProfile '{self.name}': role '{self.role_name}' does not exist. "
                    f"Deploy the IAMRole first."
                )

            # Create profile if missing
            try:
                ip = iam.get_instance_profile(InstanceProfileName=self.name)["InstanceProfile"]
            except iam.exceptions.NoSuchEntityException:
                resp = iam.create_instance_profile(
                    InstanceProfileName=self.name,
                    Tags=[{"Key": k, "Value": v} for k, v in (self.tags or {}).items()],
                )
                ip = resp["InstanceProfile"]
                print(f"Created instance profile '{self.name}'")

            ip_arn = ip.get("Arn", "")
            if ip_arn:
                self.ext["instance_profile_arn"] = ip_arn

            # Attach role if not already attached
            attached = [r.get("RoleName") for r in (ip.get("Roles") or [])]
            if self.role_name not in attached:
                iam.add_role_to_instance_profile(
                    InstanceProfileName=self.name,
                    RoleName=self.role_name,
                )
                print(f"Attached role '{self.role_name}' to profile '{self.name}'")
            else:
                print(f"Instance profile '{self.name}' already up to date, skipping")
                return self

            # IAM is eventually consistent — wait until profile is visible
            last_exc: Optional[Exception] = None
            for _ in range(30):
                try:
                    ip = iam.get_instance_profile(InstanceProfileName=self.name)["InstanceProfile"]
                    if ip.get("InstanceProfileName"):
                        break
                except Exception as e:
                    last_exc = e
                time.sleep(2)
            else:
                if last_exc:
                    raise last_exc

            print(f"Successfully deployed instance profile '{self.name}' (role='{self.role_name}')")
            return self

        def delete(self):
            import boto3
            iam = aws_client("iam", self.region, self.name)
            try:
                ip = iam.get_instance_profile(InstanceProfileName=self.name)["InstanceProfile"]
                for role in ip.get("Roles") or []:
                    iam.remove_role_from_instance_profile(
                        InstanceProfileName=self.name,
                        RoleName=role["RoleName"],
                    )
                iam.delete_instance_profile(InstanceProfileName=self.name)
                print(f"Deleted instance profile '{self.name}'")
            except iam.exceptions.NoSuchEntityException:
                print(f"Instance profile '{self.name}' does not exist, skipping")
