from ._utils import aws_client
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class IAMRole:

    @dataclass
    class Config:
        name: str
        trust_service: str  # e.g. "lambda.amazonaws.com" or "ec2.amazonaws.com"
        region: str = ""
        policy_arns: List[str] = field(default_factory=list)
        inline_policies: Dict[str, Dict[str, Any]] = field(default_factory=dict)
        tags: Dict[str, str] = field(default_factory=dict)
        ext: Dict[str, Any] = field(default_factory=dict)

        def _is_up_to_date(self, iam) -> bool:
            from urllib.parse import unquote

            try:
                iam.get_role(RoleName=self.name)
            except Exception:
                return False

            try:
                current_arns: set = set()
                paginator = iam.get_paginator("list_attached_role_policies")
                for page in paginator.paginate(RoleName=self.name):
                    for attached in page.get("AttachedPolicies") or []:
                        arn = attached.get("PolicyArn")
                        if arn:
                            current_arns.add(arn)
                desired_arns = {p for p in (self.policy_arns or []) if p}
                if current_arns != desired_arns:
                    return False
            except Exception:
                return False

            try:
                desired_inline = {n: d for n, d in (self.inline_policies or {}).items() if n and d}
                current_inline_names: set = set()
                paginator = iam.get_paginator("list_role_policies")
                for page in paginator.paginate(RoleName=self.name):
                    for policy_name in page.get("PolicyNames") or []:
                        if policy_name:
                            current_inline_names.add(policy_name)
                if current_inline_names != set(desired_inline.keys()):
                    return False
                for policy_name, desired_doc in desired_inline.items():
                    resp = iam.get_role_policy(RoleName=self.name, PolicyName=policy_name)
                    raw = resp.get("PolicyDocument", "{}")
                    current_doc = raw if isinstance(raw, dict) else json.loads(unquote(raw))
                    if current_doc != desired_doc:
                        return False
            except Exception:
                return False

            return True

        def deploy(self):
            import boto3

            iam = aws_client("iam", self.region, self.name)

            if self._is_up_to_date(iam):
                print(f"IAM role '{self.name}' is already up to date, skipping")
                role_arn = iam.get_role(RoleName=self.name)["Role"]["Arn"]
                self.ext["role_arn"] = role_arn
                return self

            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": self.trust_service},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }

            try:
                role = iam.get_role(RoleName=self.name).get("Role", {})
            except Exception:
                resp = iam.create_role(
                    RoleName=self.name,
                    AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                    Tags=[{"Key": k, "Value": v} for k, v in (self.tags or {}).items()],
                )
                role = resp.get("Role", {})

            role_arn = role.get("Arn", "")
            if role_arn:
                self.ext["role_arn"] = role_arn

            desired_policy_arns = {p for p in (self.policy_arns or []) if p}
            desired_inline_names = {n for n in (self.inline_policies or {}).keys() if n}

            try:
                paginator = iam.get_paginator("list_attached_role_policies")
                for page in paginator.paginate(RoleName=self.name):
                    for attached in page.get("AttachedPolicies") or []:
                        policy_arn = attached.get("PolicyArn")
                        if policy_arn and policy_arn not in desired_policy_arns:
                            try:
                                iam.detach_role_policy(RoleName=self.name, PolicyArn=policy_arn)
                            except Exception as e:
                                print(f"Warning: Failed to detach policy {policy_arn}: {e}")
            except Exception as e:
                print(f"Warning: Failed to list managed policies for {self.name}: {e}")

            try:
                paginator = iam.get_paginator("list_role_policies")
                for page in paginator.paginate(RoleName=self.name):
                    for policy_name in page.get("PolicyNames") or []:
                        if policy_name and policy_name not in desired_inline_names:
                            try:
                                iam.delete_role_policy(RoleName=self.name, PolicyName=policy_name)
                            except Exception as e:
                                print(f"Warning: Failed to delete inline policy {policy_name}: {e}")
            except Exception as e:
                print(f"Warning: Failed to list inline policies for {self.name}: {e}")

            for policy_arn in self.policy_arns or []:
                if not policy_arn:
                    continue
                try:
                    iam.attach_role_policy(RoleName=self.name, PolicyArn=policy_arn)
                except Exception as e:
                    print(f"Warning: Failed to attach policy {policy_arn}: {e}")

            for policy_name, policy_document in (self.inline_policies or {}).items():
                if not policy_name or not policy_document:
                    continue
                try:
                    iam.put_role_policy(
                        RoleName=self.name,
                        PolicyName=policy_name,
                        PolicyDocument=json.dumps(policy_document),
                    )
                except Exception as e:
                    print(f"Warning: Failed to put inline policy {policy_name}: {e}")

            print(f"Successfully deployed IAM role: {self.name}")
            return self

        def delete(self):
            import boto3
            iam = aws_client("iam", self.region, self.name)
            try:
                for page in iam.get_paginator("list_attached_role_policies").paginate(RoleName=self.name):
                    for p in page.get("AttachedPolicies") or []:
                        iam.detach_role_policy(RoleName=self.name, PolicyArn=p["PolicyArn"])
                for page in iam.get_paginator("list_role_policies").paginate(RoleName=self.name):
                    for name in page.get("PolicyNames") or []:
                        iam.delete_role_policy(RoleName=self.name, PolicyName=name)
                iam.delete_role(RoleName=self.name)
                print(f"Deleted IAM role '{self.name}'")
            except iam.exceptions.NoSuchEntityException:
                print(f"IAM role '{self.name}' does not exist, skipping")
