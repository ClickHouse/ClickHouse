import base64
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ._utils import aws_client

if TYPE_CHECKING:
    from .image_builder import ImageBuilder


class LaunchTemplate:

    @dataclass
    class Config:
        # Launch Template name
        name: str
        region: str = ""

        # Mandatory runner identification fields
        praktika_resource_tag: str = (
            ""  # Praktika resource tag (e.g., "mac") - tagged as "praktika_resource_tag"
        )
        runner_type: str = (
            ""  # GitHub runner type (e.g., "arm_macos_small") - tagged as "github:runner-type"
        )

        # High-level fields (optional). If `data` is provided, it is used as-is.
        image_id: str = ""
        image_builder: Optional["ImageBuilder.Config"] = None
        image_builder_pipeline_name: str = ""
        instance_type: str = ""
        # If set, will be base64-encoded and applied as LaunchTemplateData.UserData
        user_data: str = ""
        security_group_ids: List[str] = field(default_factory=list)
        # Resolved to IDs at deploy time by looking up SG names in the VPC.
        # Requires `vpc_name` so the lookup is scoped (SG names are unique
        # within a VPC, not globally).
        security_group_names: List[str] = field(default_factory=list)
        vpc_name: str = ""
        iam_instance_profile_name: str = ""
        tenancy: str = ""  # e.g. "host"
        host_id: str = ""

        # Root EBS volume override. The root device name is resolved from the
        # selected AMI at deploy time so Ubuntu (/dev/sda1) and AL2023
        # (/dev/xvda) both get the requested size.
        root_volume_size_gb: int = 0
        root_volume_type: str = "gp3"
        root_volume_delete_on_termination: bool = True

        # Extra block device mappings (passed through as LaunchTemplateData.BlockDeviceMappings).
        # Example: [{"DeviceName": "/dev/xvda",
        #            "Ebs": {"VolumeSize": 30, "VolumeType": "gp3",
        #                    "DeleteOnTermination": True}}]
        block_device_mappings: List[Dict[str, Any]] = field(default_factory=list)
        tags: Dict[str, str] = field(default_factory=dict)

        # Raw launch template data (passed directly to EC2 API as LaunchTemplateData)
        data: Dict[str, Any] = field(default_factory=dict)

        # If set, update will create a new version; if False, existing LT must not exist
        create_new_version: bool = True

        # If True, after creating a new version, automatically set it as the default version.
        set_default_version_to_latest: bool = False

        # Extra fetched/derived properties
        ext: Dict[str, Any] = field(default_factory=dict)

        def fetch(self):
            """
            Fetch Launch Template configuration from AWS and store in ext.

            Raises:
                Exception: If launch template does not exist or AWS API call fails
            """
            import boto3

            ec2 = aws_client("ec2", self.region, self.name)

            resp = ec2.describe_launch_templates(LaunchTemplateNames=[self.name])
            lts = resp.get("LaunchTemplates", [])
            if not lts:
                raise Exception(f"Launch Template '{self.name}' not found in AWS")

            lt = lts[0]

            self.ext["launch_template_id"] = lt.get("LaunchTemplateId")
            self.ext["launch_template_name"] = lt.get("LaunchTemplateName")
            self.ext["latest_version_number"] = lt.get("LatestVersionNumber")
            self.ext["default_version_number"] = lt.get("DefaultVersionNumber")
            self.ext["created_time"] = lt.get("CreateTime")

            print(
                f"Successfully fetched configuration for Launch Template: {self.name}"
            )
            return self

        def _resolve_launch_template_id(self) -> str:
            if self.ext.get("launch_template_id"):
                return self.ext["launch_template_id"]
            self.fetch()
            if not self.ext.get("launch_template_id"):
                raise Exception(
                    f"Failed to resolve Launch Template id for '{self.name}'"
                )
            return self.ext["launch_template_id"]

        def _resolve_image_id(self) -> str:
            def _resolve_ready_ami_from_pipeline(client, pipeline_arn: str, label: str) -> str:
                resp = client.list_image_pipeline_images(
                    imagePipelineArn=pipeline_arn,
                    maxResults=25,
                )
                images = resp.get("imageSummaryList", []) or []
                if not images:
                    raise Exception(
                        f"No ready AMI found for Image Builder pipeline '{label}'. "
                        "Rerun deploy after the image is ready."
                    )

                images.sort(key=lambda s: s.get("dateCreated", "") or "", reverse=True)
                for summary in images:
                    image_arn = summary.get("arn", "")
                    if not image_arn:
                        continue

                    image_resp = client.get_image(imageBuildVersionArn=image_arn)
                    image = image_resp.get("image") or {}

                    for output in image.get("outputResources", {}).get("amis", []) or []:
                        if output.get("region") == self.region and output.get("image"):
                            return output["image"]

                    for output in image.get("outputResources", {}).get("amis", []) or []:
                        if output.get("image"):
                            return output["image"]

                raise Exception(
                    f"No ready AMI found for Image Builder pipeline '{label}'. "
                    "Rerun deploy after the image is ready."
                )

            if self.image_id:
                return self.image_id

            if self.image_builder:
                if not self.image_builder.region:
                    self.image_builder.region = self.region
                self.image_id = self.image_builder.resolve_latest_ami_id()
                return self.image_id

            if self.image_builder_pipeline_name:
                client = aws_client("imagebuilder", self.region, self.name)

                pipeline_arn = ""
                paginator = client.get_paginator("list_image_pipelines")
                for page in paginator.paginate():
                    for item in page.get("imagePipelineList", []) or []:
                        if item.get(
                            "name"
                        ) == self.image_builder_pipeline_name and item.get("arn"):
                            pipeline_arn = item["arn"]
                            break
                    if pipeline_arn:
                        break

                if not pipeline_arn:
                    raise Exception(
                        f"Failed to resolve Image Builder pipeline ARN for '{self.image_builder_pipeline_name}'"
                    )

                self.image_id = _resolve_ready_ami_from_pipeline(
                    client,
                    pipeline_arn,
                    self.image_builder_pipeline_name,
                )
                return self.image_id

            # Detect architecture from instance type: Graviton families end in 'g'
            # (t4g, m6g, c6g, r6g, ...). Everything else is x86_64.
            family = (self.instance_type or "").split(".")[0]
            is_arm = family.endswith("g")
            if is_arm:
                from .native.configs import resolve_al2023_arm64_ami
                self.image_id = resolve_al2023_arm64_ami(self.region)
            else:
                from .native.configs import resolve_al2023_x86_64_ami
                self.image_id = resolve_al2023_x86_64_ami(self.region)
            return self.image_id

        def _current_launch_template_image_id(self, ec2, lt_id: str) -> str:
            resp = ec2.describe_launch_template_versions(
                LaunchTemplateId=lt_id,
                Versions=["$Latest"],
            )
            versions = resp.get("LaunchTemplateVersions", [])
            if not versions:
                raise Exception(
                    f"Launch Template '{self.name}' has no latest version to reuse"
                )
            image_id = versions[0].get("LaunchTemplateData", {}).get("ImageId", "")
            if not image_id:
                raise Exception(
                    f"Launch Template '{self.name}' latest version has no ImageId"
                )
            return image_id

        def _resolve_root_device_name(self, image_id: str) -> str:
            cache_key = f"root_device_name:{image_id}"
            if self.ext.get(cache_key):
                return self.ext[cache_key]

            ec2 = aws_client("ec2", self.region, self.name)
            resp = ec2.describe_images(ImageIds=[image_id])
            images = resp.get("Images", []) or []
            root = (images[0] if images else {}).get("RootDeviceName", "")
            if not root:
                raise Exception(
                    f"Failed to resolve root device name for Launch Template "
                    f"'{self.name}' image_id={image_id}"
                )
            self.ext[cache_key] = root
            return root

        def _build_launch_template_data(self) -> Dict[str, Any]:
            if self.data:
                return self.data

            resolved_image_id = self._resolve_image_id()
            if not self.instance_type:
                raise ValueError(
                    f"instance_type must be set for Launch Template '{self.name}'"
                )

            lt_data: Dict[str, Any] = {
                "ImageId": resolved_image_id,
                "InstanceType": self.instance_type,
            }

            # Add mandatory runner identification tags to instances launched from this template
            tag_specs = []
            instance_tags = {"praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                instance_tags["praktika_resource_tag"] = self.praktika_resource_tag
            if self.runner_type:
                instance_tags["github:runner-type"] = self.runner_type

            if instance_tags:
                instance_tags.update(self.tags or {})
                tag_specs.append(
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": k, "Value": v} for k, v in instance_tags.items()
                        ],
                    }
                )
                lt_data["TagSpecifications"] = tag_specs
            lt_data["MetadataOptions"] = {
                "HttpTokens": "required",
                "InstanceMetadataTags": "enabled",
            }

            sg_ids = list(self.security_group_ids)
            if self.security_group_names:
                if not self.vpc_name:
                    raise ValueError(
                        f"LaunchTemplate '{self.name}' has security_group_names but no vpc_name; "
                        f"set vpc_name to scope the SG lookup."
                    )
                from .vpc import VPC
                lookup = VPC.Lookup(name=self.vpc_name, region=self.region)
                sg_ids.extend(lookup.resolve_security_group_ids(self.security_group_names))
            if sg_ids:
                lt_data["SecurityGroupIds"] = sg_ids

            if self.iam_instance_profile_name:
                lt_data["IamInstanceProfile"] = {
                    "Name": self.iam_instance_profile_name,
                }

            if self.user_data:
                lt_data["UserData"] = base64.b64encode(
                    self.user_data.encode("utf-8")
                ).decode("utf-8")

            block_device_mappings = []
            if self.root_volume_size_gb:
                ebs = {
                    "VolumeSize": int(self.root_volume_size_gb),
                    "DeleteOnTermination": bool(
                        self.root_volume_delete_on_termination
                    ),
                }
                if self.root_volume_type:
                    ebs["VolumeType"] = self.root_volume_type
                block_device_mappings.append(
                    {
                        "DeviceName": self._resolve_root_device_name(
                            resolved_image_id
                        ),
                        "Ebs": ebs,
                    }
                )
            if self.block_device_mappings:
                block_device_mappings.extend(self.block_device_mappings)
            if block_device_mappings:
                lt_data["BlockDeviceMappings"] = block_device_mappings

            if self.tenancy:
                lt_data.setdefault("Placement", {})
                lt_data["Placement"]["Tenancy"] = self.tenancy

            if self.host_id:
                lt_data["Placement"] = {
                    "Tenancy": "host",
                    "HostId": self.host_id,
                }
                return lt_data

            return lt_data

        def _is_current_version_up_to_date(self, ec2, lt_id: str, desired: dict) -> bool:
            try:
                resp = ec2.describe_launch_template_versions(
                    LaunchTemplateId=lt_id, Versions=["$Latest"]
                )
                versions = resp.get("LaunchTemplateVersions", [])
                if not versions:
                    return False
                current = versions[0].get("LaunchTemplateData", {})
            except Exception:
                return False

            def _norm(data: dict) -> dict:
                out = {}
                for key in ("ImageId", "InstanceType"):
                    if key in data:
                        out[key] = data[key]
                # UserData: decode both sides before comparing — AWS may return
                # different base64 padding/line breaks than what we encoded.
                if "UserData" in desired:
                    def _decode(s):
                        try:
                            return base64.b64decode(s).decode("utf-8") if s else ""
                        except Exception:
                            return s
                    out["UserData"] = _decode(desired["UserData"])
                    out["_current_UserData"] = _decode(current.get("UserData", ""))
                # Security groups
                if "SecurityGroupIds" in desired:
                    out["SecurityGroupIds"] = sorted(desired["SecurityGroupIds"])
                    out["_current_SecurityGroupIds"] = sorted(
                        current.get("NetworkInterfaces", [{}])[0].get("Groups", [{}])
                        if current.get("NetworkInterfaces")
                        else current.get("SecurityGroupIds", [])
                    )
                # IAM profile: compare by name
                dp = desired.get("IamInstanceProfile", {})
                cp = current.get("IamInstanceProfile", {})
                if dp:
                    out["IamProfileName"] = dp.get("Name", "")
                    out["_current_IamProfileName"] = cp.get("Arn", "").split("/")[-1] if cp.get("Arn") else cp.get("Name", "")
                if "MetadataOptions" in desired:
                    # We force IMDS tags on so bootstrap agents can read their
                    # own pool/asg/scaling metadata without extra config files.
                    out["MetadataOptions"] = desired.get("MetadataOptions", {})
                    out["_current_MetadataOptions"] = current.get("MetadataOptions", {})
                if "BlockDeviceMappings" in desired:
                    out["BlockDeviceMappings"] = desired.get("BlockDeviceMappings", [])
                    out["_current_BlockDeviceMappings"] = current.get(
                        "BlockDeviceMappings", []
                    )
                desired_tags = []
                for spec in desired.get("TagSpecifications", []) or []:
                    if spec.get("ResourceType") != "instance":
                        continue
                    desired_tags.extend(spec.get("Tags", []) or [])
                if desired_tags:
                    current_tags = []
                    for spec in current.get("TagSpecifications", []) or []:
                        if spec.get("ResourceType") != "instance":
                            continue
                        current_tags.extend(spec.get("Tags", []) or [])
                    out["InstanceTags"] = sorted(
                        desired_tags, key=lambda t: (t.get("Key", ""), t.get("Value", ""))
                    )
                    out["_current_InstanceTags"] = sorted(
                        current_tags, key=lambda t: (t.get("Key", ""), t.get("Value", ""))
                    )
                return out

            d = _norm(desired)
            for key, val in d.items():
                if key.startswith("_current_"):
                    continue
                current_val = d.get(f"_current_{key}", current.get(key))
                if key == "UserData":
                    current_val = d["_current_UserData"]
                if val != current_val:
                    print(f"  Launch Template field changed: {key}")
                    return False
            return True

        def deploy(self):
            """
            Create or update (create new version) an EC2 Launch Template.

            Notes:
                - This component expects `data` to be a valid EC2 LaunchTemplateData dict.
                - It intentionally does not attempt to diff/merge existing template data.
            """
            import boto3

            ec2 = aws_client("ec2", self.region, self.name)

            # Determine if LT exists
            exists = False
            try:
                self.fetch()
                exists = True
                print(
                    f"Fetched existing configuration for Launch Template: {self.name}"
                )
            except Exception:
                print(
                    f"Launch Template {self.name} does not exist yet, will create new"
                )

            try:
                launch_template_data = self._build_launch_template_data()
            except Exception as e:
                message = str(e)
                missing_pipeline_image = (
                    "No ready AMI found for Image Builder pipeline" in message
                    or "Image Builder pipeline" in message and "not found" in message
                )
                if not missing_pipeline_image:
                    raise
                raise SystemExit(
                    f"Image Builder output is not ready yet for Launch Template '{self.name}'. "
                    "This can happen on the first deploy while Image Builder is still "
                    "building the first AMI. Rerun deploy after the image is ready."
                )

            if not exists:
                resp = ec2.create_launch_template(
                    LaunchTemplateName=self.name,
                    LaunchTemplateData=launch_template_data,
                )
                lt = resp.get("LaunchTemplate", {})
                self.ext["launch_template_id"] = lt.get("LaunchTemplateId")
                self.ext["latest_version_number"] = lt.get("LatestVersionNumber")
                self.ext["default_version_number"] = lt.get("DefaultVersionNumber")
                print(f"Successfully created Launch Template: {self.name}")
                return self

            # Exists — check if current version matches desired config
            if not self.create_new_version:
                raise ValueError(
                    f"Launch Template '{self.name}' already exists and create_new_version=False"
                )

            lt_id = self._resolve_launch_template_id()

            if self._is_current_version_up_to_date(ec2, lt_id, launch_template_data):
                print(f"Launch Template '{self.name}' is already up to date, skipping")
                self.ext["version_updated"] = False
                return self

            resp = ec2.create_launch_template_version(
                LaunchTemplateId=lt_id,
                LaunchTemplateData=launch_template_data,
            )

            version = resp.get("LaunchTemplateVersion", {})
            new_version_number: Optional[int] = version.get("VersionNumber")
            if new_version_number is not None:
                self.ext["latest_version_number"] = new_version_number

            if self.set_default_version_to_latest and new_version_number is not None:
                ec2.modify_launch_template(
                    LaunchTemplateId=lt_id,
                    DefaultVersion=str(new_version_number),
                )
                self.ext["default_version_number"] = new_version_number

            self.ext["version_updated"] = True
            print(
                f"Successfully created new version for Launch Template: {self.name} (version={new_version_number})"
            )
            return self

        def delete(self):
            import boto3
            client = aws_client("ec2", self.region, self.name)
            try:
                client.delete_launch_template(LaunchTemplateName=self.name)
                print(f"Deleted Launch Template '{self.name}'")
            except client.exceptions.ClientError as e:
                if "does not exist" in str(e).lower() or "InvalidLaunchTemplateName" in str(e):
                    print(f"Launch Template '{self.name}' does not exist, skipping")
                else:
                    raise
