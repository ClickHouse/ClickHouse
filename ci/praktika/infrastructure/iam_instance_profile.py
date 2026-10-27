import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


class IAMInstanceProfile:

    @dataclass
    class Config:
        name: (
            str  # IAM Instance Profile name (also used as a stable logical identifier)
        )
        region: str = ""  # AWS region (empty means boto3 default)

        role_name: str = ""  # IAM Role name to associate with the Instance Profile
        policy_arns: List[str] = field(
            default_factory=list
        )  # Policy ARNs to attach to the role
        inline_policies: Dict[str, Dict[str, Any]] = field(
            default_factory=dict
        )  # Inline policies to put on the role (name -> policy document)
        tags: Dict[str, str] = field(
            default_factory=dict
        )  # Tags applied to the role and instance profile

        ext: Dict[str, Any] = field(
            default_factory=dict
        )  # Runtime/fetched fields (ARNs, etc.)

        def _is_up_to_date(self, iam) -> bool:
            """Return True if the role, policies, and instance profile all match the desired config."""
            from urllib.parse import unquote

            try:
                iam.get_role(RoleName=self.role_name)
            except Exception:
                print(f"  Role '{self.role_name}' does not exist yet")
                return False

            try:
                current_arns: set = set()
                paginator = iam.get_paginator("list_attached_role_policies")
                for page in paginator.paginate(RoleName=self.role_name):
                    for attached in page.get("AttachedPolicies") or []:
                        arn = attached.get("PolicyArn")
                        if arn:
                            current_arns.add(arn)
                desired_arns = {p for p in (self.policy_arns or []) if p}
                if current_arns != desired_arns:
                    print(
                        f"  Managed policies differ:"
                        f" added={desired_arns - current_arns},"
                        f" removed={current_arns - desired_arns}"
                    )
                    return False
            except Exception as e:
                print(f"  Failed to check managed policies: {e}")
                return False

            try:
                desired_inline = {
                    n: d
                    for n, d in (self.inline_policies or {}).items()
                    if n and d
                }
                current_inline_names: set = set()
                paginator = iam.get_paginator("list_role_policies")
                for page in paginator.paginate(RoleName=self.role_name):
                    for policy_name in page.get("PolicyNames") or []:
                        if policy_name:
                            current_inline_names.add(policy_name)
                if current_inline_names != set(desired_inline.keys()):
                    print(
                        f"  Inline policy names differ:"
                        f" current={current_inline_names},"
                        f" desired={set(desired_inline.keys())}"
                    )
                    return False
                for policy_name, desired_doc in desired_inline.items():
                    resp = iam.get_role_policy(
                        RoleName=self.role_name, PolicyName=policy_name
                    )
                    raw = resp.get("PolicyDocument", "{}")
                    current_doc = (
                        raw if isinstance(raw, dict) else json.loads(unquote(raw))
                    )
                    if current_doc != desired_doc:
                        print(f"  Inline policy '{policy_name}' content differs")
                        return False
            except Exception as e:
                print(f"  Failed to check inline policies: {e}")
                return False

            try:
                ip = iam.get_instance_profile(
                    InstanceProfileName=self.name
                ).get("InstanceProfile", {})
                role_names = [
                    r.get("RoleName")
                    for r in (ip.get("Roles") or [])
                    if r.get("RoleName")
                ]
                if self.role_name not in role_names:
                    print(
                        f"  Role '{self.role_name}' not yet attached to instance profile '{self.name}'"
                    )
                    return False
            except Exception as e:
                print(f"  Instance profile '{self.name}' check failed: {e}")
                return False

            return True

        def deploy(self):
            import time

            import boto3

            if not self.role_name:
                raise ValueError(
                    f"role_name must be set for IAMInstanceProfile '{self.name}'"
                )
            if not self.name:
                raise ValueError("name must be set for IAMInstanceProfile")

            instance_profile_name = self.name

            iam = boto3.client("iam")

            if self._is_up_to_date(iam):
                print(
                    f"IAM instance profile '{instance_profile_name}' is already up to date, skipping"
                )
                return self

            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "ec2.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }

            try:
                role = iam.get_role(RoleName=self.role_name).get("Role", {})
            except Exception:
                resp = iam.create_role(
                    RoleName=self.role_name,
                    AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                    Tags=[{"Key": k, "Value": v} for k, v in (self.tags or {}).items()],
                )
                role = resp.get("Role", {})

            role_arn = role.get("Arn", "")
            if role_arn:
                self.ext["role_arn"] = role_arn

            desired_policy_arns = {p for p in (self.policy_arns or []) if p}
            desired_inline_policy_names = {
                n for n in (self.inline_policies or {}).keys() if n
            }

            try:
                paginator = iam.get_paginator("list_attached_role_policies")
                for page in paginator.paginate(RoleName=self.role_name):
                    for attached in page.get("AttachedPolicies") or []:
                        policy_arn = attached.get("PolicyArn")
                        if policy_arn and policy_arn not in desired_policy_arns:
                            try:
                                iam.detach_role_policy(
                                    RoleName=self.role_name, PolicyArn=policy_arn
                                )
                            except Exception as e:
                                print(
                                    f"Warning: Failed to detach policy {policy_arn} from {self.role_name}: {e}"
                                )
            except Exception as e:
                print(
                    f"Warning: Failed to list/detach managed policies for {self.role_name}: {e}"
                )

            try:
                paginator = iam.get_paginator("list_role_policies")
                for page in paginator.paginate(RoleName=self.role_name):
                    for policy_name in page.get("PolicyNames") or []:
                        if (
                            policy_name
                            and policy_name not in desired_inline_policy_names
                        ):
                            try:
                                iam.delete_role_policy(
                                    RoleName=self.role_name, PolicyName=policy_name
                                )
                            except Exception as e:
                                print(
                                    f"Warning: Failed to delete inline policy {policy_name} from {self.role_name}: {e}"
                                )
            except Exception as e:
                print(
                    f"Warning: Failed to list/delete inline policies for {self.role_name}: {e}"
                )

            for policy_arn in self.policy_arns or []:
                if not policy_arn:
                    continue
                try:
                    iam.attach_role_policy(
                        RoleName=self.role_name, PolicyArn=policy_arn
                    )
                except Exception as e:
                    print(
                        f"Warning: Failed to attach policy {policy_arn} to {self.role_name}: {e}"
                    )

            for policy_name, policy_document in (self.inline_policies or {}).items():
                if not policy_name or not policy_document:
                    continue
                try:
                    iam.put_role_policy(
                        RoleName=self.role_name,
                        PolicyName=policy_name,
                        PolicyDocument=json.dumps(policy_document),
                    )
                except Exception as e:
                    print(
                        f"Warning: Failed to put inline policy {policy_name} on {self.role_name}: {e}"
                    )

            try:
                ip = iam.get_instance_profile(
                    InstanceProfileName=instance_profile_name
                ).get("InstanceProfile", {})
            except Exception:
                resp = iam.create_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    Tags=[{"Key": k, "Value": v} for k, v in (self.tags or {}).items()],
                )
                ip = resp.get("InstanceProfile", {})

            ip_arn = ip.get("Arn", "")
            if ip_arn:
                self.ext["instance_profile_arn"] = ip_arn

            role_names = [
                r.get("RoleName") for r in (ip.get("Roles") or []) if r.get("RoleName")
            ]
            if self.role_name not in role_names:
                iam.add_role_to_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=self.role_name,
                )

            # IAM is eventually consistent. Wait until instance profile is visible.
            last_exc: Optional[Exception] = None
            for _ in range(30):
                try:
                    ip = iam.get_instance_profile(
                        InstanceProfileName=instance_profile_name
                    ).get("InstanceProfile", {})
                    if ip.get("InstanceProfileName"):
                        break
                except Exception as e:
                    last_exc = e
                time.sleep(2)
            else:
                if last_exc:
                    raise last_exc

            print(
                f"Successfully deployed IAM instance profile: {instance_profile_name} (role={self.role_name})"
            )
            return self
