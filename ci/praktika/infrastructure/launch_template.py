import base64
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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
        image_builder_pipeline_name: str = ""
        instance_type: str = ""
        # If set, will be base64-encoded and applied as LaunchTemplateData.UserData
        user_data: str = ""
        security_group_ids: List[str] = field(default_factory=list)
        tenancy: str = ""  # e.g. "host"
        host_id: str = ""

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

            ec2 = boto3.client("ec2", region_name=self.region)

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
            if self.image_id:
                return self.image_id

            if not self.image_builder_pipeline_name:
                return ""

            import boto3

            client = boto3.client("imagebuilder", region_name=self.region)

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

            resp = client.list_image_pipeline_images(
                imagePipelineArn=pipeline_arn,
                maxResults=25,
            )
            images = resp.get("imageSummaryList", []) or []
            if not images:
                raise Exception(
                    f"No images found for Image Builder pipeline '{self.image_builder_pipeline_name}'"
                )

            images.sort(key=lambda s: s.get("dateCreated", "") or "", reverse=True)
            image_arn = images[0].get("arn", "")
            if not image_arn:
                raise Exception(
                    f"Failed to resolve latest image ARN for pipeline '{self.image_builder_pipeline_name}'"
                )

            image_resp = client.get_image(imageBuildVersionArn=image_arn)
            image = image_resp.get("image") or {}

            for output in image.get("outputResources", {}).get("amis", []) or []:
                if output.get("region") == self.region and output.get("image"):
                    self.image_id = output["image"]
                    return self.image_id

            for output in image.get("outputResources", {}).get("amis", []) or []:
                if output.get("image"):
                    self.image_id = output["image"]
                    return self.image_id

            raise Exception(
                f"Failed to resolve AMI id for pipeline '{self.image_builder_pipeline_name}'"
            )

        def _build_launch_template_data(self) -> Dict[str, Any]:
            if self.data:
                return self.data

            resolved_image_id = self._resolve_image_id()
            if not resolved_image_id or not self.instance_type:
                raise ValueError(
                    f"Either data must be provided, or image_id + instance_type must be set for Launch Template '{self.name}'"
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
                tag_specs.append(
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": k, "Value": v} for k, v in instance_tags.items()
                        ],
                    }
                )
                lt_data["TagSpecifications"] = tag_specs

            if self.security_group_ids:
                lt_data["SecurityGroupIds"] = list(self.security_group_ids)

            if self.user_data:
                lt_data["UserData"] = base64.b64encode(
                    self.user_data.encode("utf-8")
                ).decode("utf-8")

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

        def deploy(self):
            """
            Create or update (create new version) an EC2 Launch Template.

            Notes:
                - This component expects `data` to be a valid EC2 LaunchTemplateData dict.
                - It intentionally does not attempt to diff/merge existing template data.
            """
            import boto3

            launch_template_data = self._build_launch_template_data()

            ec2 = boto3.client("ec2", region_name=self.region)

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

            # Exists
            if not self.create_new_version:
                raise ValueError(
                    f"Launch Template '{self.name}' already exists and create_new_version=False"
                )

            lt_id = self._resolve_launch_template_id()

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

            print(
                f"Successfully created new version for Launch Template: {self.name} (version={new_version_number})"
            )
            return self
