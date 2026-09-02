from dataclasses import dataclass, field
import json
import re
import shlex
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from ._utils import aws_client

if TYPE_CHECKING:
    from .launch_template import LaunchTemplate


class ImageBuilder:
    @dataclass
    class PrebuiltVenv:
        name: str
        packages: List[str] = field(default_factory=list)
        python: str = "python3.12"
        path: str = ""
        version: str = ""
        description: str = ""

    @dataclass
    class Config:
        name: str
        region: str = ""

        image_recipe_version: str = "1.0.0"
        parent_image: str = ""  # AMI id or Image Builder managed image ARN
        parent_image_resolver: Optional[Callable[[str], str]] = None
        components: List[str] = field(default_factory=list)  # list of component ARNs
        inline_components: List[Dict[str, Any]] = field(default_factory=list)
        prebuilt_venvs: List["ImageBuilder.PrebuiltVenv"] = field(default_factory=list)
        working_directory: str = ""
        block_device_mappings: List[Dict[str, Any]] = field(default_factory=list)

        instance_profile_name: str = ""
        instance_types: List[str] = field(default_factory=list)
        subnet_id: str = ""
        security_group_ids: List[str] = field(default_factory=list)
        security_group_names: List[str] = field(default_factory=list)
        vpc_name: str = ""
        key_pair: str = ""
        terminate_instance_on_failure: bool = True
        sns_topic_arn: str = ""

        ami_launch_permission: Dict[str, Any] = field(default_factory=dict)
        regions: List[str] = field(default_factory=list)
        launch_templates: List["LaunchTemplate.Config"] = field(default_factory=list)
        set_launch_template_default_version: bool = True

        enabled: bool = True
        schedule_expression: str = ""
        image_tests_enabled: Optional[bool] = None
        image_tests_timeout_minutes: int = 60

        recipe: Dict[str, Any] = field(default_factory=dict)
        infrastructure_configuration: Dict[str, Any] = field(default_factory=dict)
        distribution_configuration: Dict[str, Any] = field(default_factory=dict)
        pipeline: Dict[str, Any] = field(default_factory=dict)

        ext: Dict[str, Any] = field(default_factory=dict)

        image_recipe_name: str = field(init=False, default="")
        infrastructure_configuration_name: str = field(init=False, default="")
        distribution_configuration_name: str = field(init=False, default="")
        ami_name: str = field(init=False, default="")
        image_pipeline_name: str = field(init=False, default="")

        def __post_init__(self):
            self.refresh_derived_names()

        def refresh_derived_names(self):
            base = (
                self.name[: -len("-image")]
                if self.name.endswith("-image")
                else self.name
            )
            self.image_recipe_name = f"{self.name}-recipe" if self.name else ""
            self.infrastructure_configuration_name = (
                f"{base}-imagebuilder-infra" if base else ""
            )
            self.distribution_configuration_name = (
                f"{base}-imagebuilder-dist" if base else ""
            )
            self.ami_name = f"{base}-{{{{ imagebuilder:buildDate }}}}" if base else ""
            self.image_pipeline_name = f"{base}-imagebuilder-pipeline" if base else ""

        def _client(self):
            return aws_client("imagebuilder", self.region, self.name)

        def _split_commands(self, script: str) -> List[str]:
            return [
                line.strip() for line in (script or "").splitlines() if line.strip()
            ]

        def _normalize_component_platform(self, platform: str) -> str:
            p = (platform or "").strip()
            if not p:
                return "Linux"
            low = p.lower()
            if low in {"macos", "macosx", "mac", "osx"}:
                return "macOS"
            if low in {"linux"}:
                return "Linux"
            if low in {"windows", "win"}:
                return "Windows"
            if p in {"macOS", "Linux", "Windows"}:
                return p
            return p

        def _account_id(self) -> str:
            if self.ext.get("account_id"):
                return self.ext["account_id"]

            sts = aws_client("sts", self.region, self.name)
            account_id = sts.get_caller_identity().get("Account", "")
            if not account_id:
                raise Exception("Failed to resolve AWS account id via STS")
            self.ext["account_id"] = account_id
            return account_id

        def _imagebuilder_arn(self, resource_type: str, name: str) -> str:
            if not name:
                raise ValueError(
                    f"name must be set to build ARN for ImageBuilder '{self.name}'"
                )
            # AWS keeps the configured display name but normalizes underscores
            # to dashes in the ARN resource path for Image Builder resources.
            arn_name = name.replace("_", "-")
            return f"arn:aws:imagebuilder:{self.region}:{self._account_id()}:{resource_type}/{arn_name}"

        def _inline_component_document(
            self,
            commands: List[str],
            *,
            phase: str = "build",
        ) -> str:
            escaped = [c.replace('"', '\\"') for c in (commands or [])]
            component_phase = (phase or "build").strip() or "build"
            lines = [
                "name: InlineInstall",
                "description: Inline install commands",
                "schemaVersion: 1.0",
                "phases:",
                f"  - name: {component_phase}",
                "    steps:",
                "      - name: install",
                "        action: ExecuteBash",
                "        inputs:",
                "          commands:",
                '            - "set -e -o pipefail"',
            ]
            for cmd in escaped:
                lines.append(f'            - "{cmd}"')
            return "\n".join(lines) + "\n"

        def _component_version(self, raw_version: Any) -> str:
            version = str(raw_version or "").strip()
            if version:
                return version
            return str(self.image_recipe_version or "").strip()

        def _component_resource_name(self, raw_name: Any) -> str:
            name = re.sub(r"[^-_A-Za-z0-9 ]+", "-", str(raw_name or "").strip())
            name = re.sub(r"[- ]{2,}", "-", name).strip(" -")
            if len(name) > 128:
                name = name[:128].rstrip(" -")
            if not name:
                return ""
            while len(name) < 3:
                name = f"{name}-x"
            return name

        def _prebuilt_venv_component_specs(self) -> List[Dict[str, Any]]:
            specs: List[Dict[str, Any]] = []
            for venv in self.prebuilt_venvs:
                if not venv.name:
                    raise ValueError(
                        f"prebuilt_venvs entries must have name for ImageBuilder '{self.name}'"
                    )
                path = venv.path or f"/opt/praktika/base-venvs/{venv.name}"
                python = venv.python or "python3.12"
                commands = [
                    f"mkdir -p {shlex.quote(path.rsplit('/', 1)[0] if '/' in path else '.')}",
                    f"if [ ! -x {shlex.quote(path)}/bin/python ]; then {shlex.quote(python)} -m venv {shlex.quote(path)}; fi",
                    f"{shlex.quote(path)}/bin/python -m pip install --upgrade pip setuptools wheel",
                ]
                if venv.packages:
                    pkg_list = " ".join(shlex.quote(pkg) for pkg in venv.packages)
                    commands.append(
                        f"{shlex.quote(path)}/bin/python -m pip install {pkg_list}"
                    )
                specs.append(
                    {
                        "name": self._component_resource_name(
                            f"{self.name}-{venv.name}-venv"
                        ),
                        "version": self._component_version(venv.version),
                        "platform": "Linux",
                        "description": venv.description
                        or f"Create prebaked Python venv '{venv.name}'",
                        "commands": commands,
                    }
                )
            return specs

        def _ensure_inline_components(self) -> List[str]:
            specs_to_create = [*self.inline_components, *self._prebuilt_venv_component_specs()]
            if not specs_to_create:
                return []

            client = self._client()
            created_arns: List[str] = []

            for spec in specs_to_create:
                name = self._component_resource_name(spec.get("name", ""))
                version = self._component_version(spec.get("version"))
                platform = self._normalize_component_platform(
                    str(spec.get("platform", "macOS"))
                )
                description = str(spec.get("description", "")).strip()

                commands: List[str] = []
                if isinstance(spec.get("commands"), list):
                    commands = [str(x) for x in spec.get("commands") if str(x).strip()]
                elif spec.get("script"):
                    commands = self._split_commands(str(spec.get("script")))
                phase = str(spec.get("phase", "build")).strip() or "build"

                if not name or not version:
                    raise ValueError(
                        f"inline_components entries must have name and version for ImageBuilder '{self.name}'"
                    )
                if not commands:
                    raise ValueError(
                        f"inline component '{name}' has no commands/script for ImageBuilder '{self.name}'"
                    )

                existing_arn = ""

                token: str = ""
                while True:
                    req: Dict[str, Any] = {"owner": "Self"}
                    if token:
                        req["nextToken"] = token

                    page = client.list_components(**req)
                    for item in page.get("componentVersionList", []) or []:
                        if (
                            item.get("name") == name
                            and (item.get("semanticVersion") or item.get("version"))
                            == version
                            and item.get("arn")
                        ):
                            existing_arn = item["arn"]
                            break
                    if existing_arn:
                        break

                    token = page.get("nextToken", "") or ""
                    if not token:
                        break

                if existing_arn:
                    created_arns.append(existing_arn)
                    continue

                data = spec.get("data")
                if not data:
                    data = self._inline_component_document(commands, phase=phase)

                req: Dict[str, Any] = {
                    "name": name,
                    "platform": platform,
                    "semanticVersion": version,
                    "data": data,
                }
                if description:
                    req["description"] = description

                arn = self._create_component_or_get_existing(req)
                created_arns.append(arn)

            return created_arns

        def _create_component_or_get_existing(self, component_req: Dict[str, Any]) -> str:
            client = self._client()
            try:
                resp = client.create_component(**component_req)
                arn = resp.get("componentBuildVersionArn", "")
                if not arn:
                    raise Exception("Failed to create Image Builder component")
                return arn
            except Exception as e:
                if e.__class__.__name__ != "ResourceAlreadyExistsException":
                    raise

                name = component_req.get("name", "")
                version = component_req.get("semanticVersion") or component_req.get(
                    "version"
                )
                if not name or not version:
                    raise

                return self._imagebuilder_arn("component", name) + f"/{version}/1"

        def _find_arn_by_name(self, list_op: str, name_key: str, name: str) -> str:
            client = self._client()
            token: str = ""
            while True:
                req: Dict[str, Any] = {"maxResults": 25}
                if token:
                    req["nextToken"] = token

                page = getattr(client, list_op)(**req)
                for item in page.get(name_key, []) or []:
                    if item.get("name") == name and item.get("arn"):
                        return item["arn"]

                token = page.get("nextToken", "") or ""
                if not token:
                    break
            return ""

        def _canonicalize(self, value: Any) -> Any:
            if isinstance(value, dict):
                return {
                    str(k): self._canonicalize(v)
                    for k, v in sorted(value.items(), key=lambda item: str(item[0]))
                }
            if isinstance(value, list):
                normalized_items = [self._canonicalize(v) for v in value]
                return sorted(
                    normalized_items,
                    key=lambda item: json.dumps(item, sort_keys=True),
                )
            return value

        def _same_config(self, current: Dict[str, Any], desired: Dict[str, Any]) -> bool:
            return self._canonicalize(current) == self._canonicalize(desired)

        def _cloudwatch_log_group_name(self) -> str:
            if not self.image_recipe_name:
                return ""
            return f"/aws/imagebuilder/{self.image_recipe_name}"

        def _cloudwatch_log_stream_name(self, image_build_version_arn: str = "") -> str:
            parts = [part for part in (image_build_version_arn or "").split("/") if part]
            if len(parts) >= 2:
                return "/".join(parts[-2:])
            if self.image_recipe_version:
                return f"{self.image_recipe_version}/1"
            return ""

        def _get_or_create_recipe_arn(self) -> str:
            client = self._client()

            if self.recipe:
                recipe_req = dict(self.recipe)
                if "version" in recipe_req and "semanticVersion" not in recipe_req:
                    recipe_req["semanticVersion"] = recipe_req.pop("version")
                resp = client.create_image_recipe(**recipe_req)
                arn = resp.get("imageRecipeArn", "")
                if not arn:
                    raise Exception("Failed to create image recipe")
                return arn

            if not self.image_recipe_name:
                raise ValueError(
                    f"image_recipe_name must be set for ImageBuilder '{self.name}'"
                )
            if not self.parent_image:
                if self.parent_image_resolver:
                    self.parent_image = self.parent_image_resolver(self.region)
                    if not self.parent_image:
                        raise Exception(
                            f"parent_image_resolver returned empty AMI for ImageBuilder '{self.name}'"
                        )
                else:
                    if not self.instance_types:
                        raise ValueError(
                            f"parent_image or instance_types must be set for ImageBuilder '{self.name}'"
                        )
                    family = (self.instance_types[0] or "").split(".")[0]
                    is_arm = family.endswith("g")
                    if is_arm:
                        from .native.configs import resolve_al2023_arm64_ami

                        self.parent_image = resolve_al2023_arm64_ami(self.region)
                    else:
                        from .native.configs import resolve_al2023_x86_64_ami

                        self.parent_image = resolve_al2023_x86_64_ami(self.region)

            token: str = ""
            while True:
                req: Dict[str, Any] = {}
                if token:
                    req["nextToken"] = token

                page = client.list_image_recipes(**req)
                for item in page.get("imageRecipeSummaryList", []) or []:
                    if (
                        item.get("name") == self.image_recipe_name
                        and (item.get("semanticVersion") or item.get("version"))
                        == self.image_recipe_version
                        and item.get("arn")
                    ):
                        return item["arn"]

                token = page.get("nextToken", "") or ""
                if not token:
                    break

            recipe_req: Dict[str, Any] = {
                "name": self.image_recipe_name,
                "semanticVersion": self.image_recipe_version,
                "parentImage": self.parent_image,
                "components": [
                    {"componentArn": c}
                    for c in [*self.components, *self._ensure_inline_components()]
                ],
            }

            if self.working_directory:
                recipe_req["workingDirectory"] = self.working_directory

            if self.block_device_mappings:
                recipe_req["blockDeviceMappings"] = self.block_device_mappings

            return self._create_image_recipe_or_get_existing(recipe_req)

        def _create_image_recipe_or_get_existing(
            self, recipe_req: Dict[str, Any]
        ) -> str:
            client = self._client()
            try:
                resp = client.create_image_recipe(**recipe_req)
                arn = resp.get("imageRecipeArn", "")
                if not arn:
                    raise Exception("Failed to create image recipe")
                return arn
            except Exception as e:
                if e.__class__.__name__ != "ResourceAlreadyExistsException":
                    raise

                name = recipe_req.get("name", "")
                version = recipe_req.get("semanticVersion") or recipe_req.get("version")
                if not name or not version:
                    raise

                return self._imagebuilder_arn("image-recipe", name) + f"/{version}"

        def _get_or_create_infrastructure_configuration_arn(self) -> str:
            client = self._client()

            if self.infrastructure_configuration:
                name = self.infrastructure_configuration.get("name", "")
                req = dict(self.infrastructure_configuration)
                try:
                    resp = client.create_infrastructure_configuration(**req)
                    arn = resp.get("infrastructureConfigurationArn", "")
                    if not arn:
                        raise Exception("Failed to create infrastructure configuration")
                    return arn
                except Exception as e:
                    if (
                        e.__class__.__name__ != "ResourceAlreadyExistsException"
                        or not name
                    ):
                        raise
                    arn = self._imagebuilder_arn("infrastructure-configuration", name)
                    client.update_infrastructure_configuration(
                        infrastructureConfigurationArn=arn,
                        **{k: v for k, v in req.items() if k != "name"},
                    )
                    return arn

            if not self.infrastructure_configuration_name:
                raise ValueError(
                    f"infrastructure_configuration_name must be set for ImageBuilder '{self.name}'"
                )
            if not self.instance_profile_name:
                raise ValueError(
                    f"instance_profile_name must be set for ImageBuilder '{self.name}'"
                )

            req: Dict[str, Any] = {
                "name": self.infrastructure_configuration_name,
                "instanceProfileName": self.instance_profile_name,
                "terminateInstanceOnFailure": self.terminate_instance_on_failure,
            }

            if self.instance_types:
                req["instanceTypes"] = list(self.instance_types)
            subnet_id = self.subnet_id
            security_group_ids = list(self.security_group_ids)
            if self.security_group_names:
                if not self.vpc_name:
                    raise ValueError(
                        f"ImageBuilder '{self.name}' has security_group_names but no vpc_name"
                    )
                from .vpc import VPC

                lookup = VPC.Lookup(name=self.vpc_name, region=self.region)
                if not subnet_id:
                    subnet_id = lookup.first_subnet_id()
                security_group_ids.extend(
                    lookup.resolve_security_group_ids(self.security_group_names)
                )
            if subnet_id:
                req["subnetId"] = subnet_id
            if security_group_ids:
                req["securityGroupIds"] = security_group_ids
            if self.key_pair:
                req["keyPair"] = self.key_pair
            if self.sns_topic_arn:
                req["snsTopicArn"] = self.sns_topic_arn

            import time

            last_exc: Optional[Exception] = None
            for attempt in range(15):
                try:
                    resp = client.create_infrastructure_configuration(**req)
                    arn = resp.get("infrastructureConfigurationArn", "")
                    if not arn:
                        raise Exception("Failed to create infrastructure configuration")
                    return arn
                except Exception as e:
                    last_exc = e
                    if e.__class__.__name__ == "ResourceAlreadyExistsException":
                        arn = self._imagebuilder_arn(
                            "infrastructure-configuration",
                            self.infrastructure_configuration_name,
                        )
                        current = client.get_infrastructure_configuration(
                            infrastructureConfigurationArn=arn
                        ).get("infrastructureConfiguration", {})
                        current_req: Dict[str, Any] = {
                            "instanceProfileName": current.get("instanceProfileName"),
                            "terminateInstanceOnFailure": current.get(
                                "terminateInstanceOnFailure"
                            ),
                        }
                        if current.get("instanceTypes"):
                            current_req["instanceTypes"] = current.get("instanceTypes")
                        if current.get("subnetId"):
                            current_req["subnetId"] = current.get("subnetId")
                        if current.get("securityGroupIds"):
                            current_req["securityGroupIds"] = current.get(
                                "securityGroupIds"
                            )
                        if current.get("keyPair"):
                            current_req["keyPair"] = current.get("keyPair")
                        if current.get("snsTopicArn"):
                            current_req["snsTopicArn"] = current.get("snsTopicArn")
                        desired_req: Dict[str, Any] = {
                            "instanceProfileName": req["instanceProfileName"],
                            "terminateInstanceOnFailure": req[
                                "terminateInstanceOnFailure"
                            ],
                        }
                        for key in (
                            "instanceTypes",
                            "subnetId",
                            "securityGroupIds",
                            "keyPair",
                            "snsTopicArn",
                        ):
                            if key in req:
                                desired_req[key] = req[key]
                        if self._same_config(current_req, desired_req):
                            return arn
                        client.update_infrastructure_configuration(
                            infrastructureConfigurationArn=arn,
                            **{k: v for k, v in req.items() if k != "name"},
                        )
                        return arn

                    msg = str(e)
                    if (
                        "instance profile" in msg.lower()
                        and "does not exist" in msg.lower()
                    ):
                        time.sleep(min(2**attempt, 30))
                        continue

                    raise

            if last_exc:
                raise last_exc
            raise Exception("Failed to create infrastructure configuration")

        def _get_or_create_distribution_configuration_arn(self) -> str:
            client = self._client()

            if self.distribution_configuration:
                name = self.distribution_configuration.get("name", "")
                req = dict(self.distribution_configuration)
                try:
                    resp = client.create_distribution_configuration(**req)
                    arn = resp.get("distributionConfigurationArn", "")
                    if not arn:
                        raise Exception("Failed to create distribution configuration")
                    return arn
                except Exception as e:
                    if (
                        e.__class__.__name__ != "ResourceAlreadyExistsException"
                        or not name
                    ):
                        raise
                    arn = self._imagebuilder_arn("distribution-configuration", name)
                    client.update_distribution_configuration(
                        distributionConfigurationArn=arn,
                        **{k: v for k, v in req.items() if k != "name"},
                    )
                    return arn

            if not self.distribution_configuration_name:
                raise ValueError(
                    f"distribution_configuration_name must be set for ImageBuilder '{self.name}'"
                )

            target_regions = self.regions or ([self.region] if self.region else [])
            if not target_regions:
                raise ValueError(
                    f"regions must be set (or region must be set) for ImageBuilder '{self.name}'"
                )

            if not self.ami_name:
                raise ValueError(f"ami_name must be set for ImageBuilder '{self.name}'")

            distributions = []
            launch_template_configurations = []
            for launch_template in self.launch_templates:
                if not launch_template.region:
                    launch_template.region = self.region
                launch_template_id = launch_template.ext.get("launch_template_id", "")
                if not launch_template_id:
                    try:
                        launch_template.fetch()
                    except Exception as e:
                        message = str(e)
                        if (
                            e.__class__.__name__ == "ClientError"
                            and "InvalidLaunchTemplateName.NotFoundException" in message
                        ) or "does not exist" in message or "not found" in message:
                            print(
                                f"Launch Template '{launch_template.name}' is not deployed yet; "
                                f"skipping Image Builder launch template propagation for '{self.name}'"
                            )
                            continue
                        raise
                    launch_template_id = launch_template.ext.get(
                        "launch_template_id", ""
                    )
                if not launch_template_id:
                    raise Exception(
                        f"Failed to resolve launch template id for ImageBuilder '{self.name}' from '{launch_template.name}'"
                    )
                launch_template_configurations.append(
                    {
                        "launchTemplateId": launch_template_id,
                        "setDefaultVersion": self.set_launch_template_default_version,
                    }
                )

            for r in target_regions:
                distribution = {
                    "region": r,
                    "amiDistributionConfiguration": {
                        "name": self.ami_name,
                    },
                }
                if self.ami_launch_permission:
                    distribution["amiDistributionConfiguration"][
                        "launchPermission"
                    ] = dict(self.ami_launch_permission)
                if launch_template_configurations:
                    distribution["launchTemplateConfigurations"] = list(
                        launch_template_configurations
                    )
                distributions.append(distribution)

            req = {
                "name": self.distribution_configuration_name,
                "distributions": distributions,
            }

            try:
                resp = client.create_distribution_configuration(**req)
                arn = resp.get("distributionConfigurationArn", "")
                if not arn:
                    raise Exception("Failed to create distribution configuration")
                return arn
            except Exception as e:
                if e.__class__.__name__ != "ResourceAlreadyExistsException":
                    raise
                arn = self._imagebuilder_arn(
                    "distribution-configuration", self.distribution_configuration_name
                )
                current = client.get_distribution_configuration(
                    distributionConfigurationArn=arn
                ).get("distributionConfiguration", {})
                current_req = {
                    "distributions": current.get("distributions", []),
                }
                desired_req = {
                    "distributions": distributions,
                }
                if self._same_config(current_req, desired_req):
                    return arn
                client.update_distribution_configuration(
                    distributionConfigurationArn=arn,
                    distributions=distributions,
                )
                return arn

        def _get_or_create_pipeline_arn(
            self,
            recipe_arn: str,
            infra_arn: str,
            dist_arn: str,
        ) -> str:
            client = self._client()

            if self.pipeline:
                name = self.pipeline.get("name", "")
                req = dict(self.pipeline)
                try:
                    resp = client.create_image_pipeline(**req)
                    arn = resp.get("imagePipelineArn", "")
                    if not arn:
                        raise Exception("Failed to create image pipeline")
                    return arn
                except Exception as e:
                    if (
                        e.__class__.__name__ != "ResourceAlreadyExistsException"
                        or not name
                    ):
                        raise
                    arn = self._imagebuilder_arn("image-pipeline", name)
                    client.update_image_pipeline(
                        imagePipelineArn=arn,
                        **{k: v for k, v in req.items() if k != "name"},
                    )
                    return arn

            if not self.image_pipeline_name:
                raise ValueError(
                    f"image_pipeline_name must be set for ImageBuilder '{self.name}'"
                )

            req: Dict[str, Any] = {
                "name": self.image_pipeline_name,
                "imageRecipeArn": recipe_arn,
                "infrastructureConfigurationArn": infra_arn,
                "distributionConfigurationArn": dist_arn,
                "status": "ENABLED" if self.enabled else "DISABLED",
            }

            if self.schedule_expression:
                req["schedule"] = {
                    "scheduleExpression": self.schedule_expression,
                    "pipelineExecutionStartCondition": "EXPRESSION_MATCH_ONLY",
                }
            if self.image_tests_enabled is not None:
                req["imageTestsConfiguration"] = {
                    "imageTestsEnabled": bool(self.image_tests_enabled),
                    "timeoutMinutes": self.image_tests_timeout_minutes,
                }

            try:
                resp = client.create_image_pipeline(**req)
                arn = resp.get("imagePipelineArn", "")
                if not arn:
                    raise Exception("Failed to create image pipeline")
                return arn
            except Exception as e:
                if e.__class__.__name__ != "ResourceAlreadyExistsException":
                    raise
                arn = self._imagebuilder_arn("image-pipeline", self.image_pipeline_name)
                update_req: Dict[str, Any] = {
                    "imagePipelineArn": arn,
                    "imageRecipeArn": recipe_arn,
                    "infrastructureConfigurationArn": infra_arn,
                    "distributionConfigurationArn": dist_arn,
                    "status": req["status"],
                }
                if "schedule" in req:
                    update_req["schedule"] = req["schedule"]
                if "imageTestsConfiguration" in req:
                    update_req["imageTestsConfiguration"] = req[
                        "imageTestsConfiguration"
                    ]
                current = client.get_image_pipeline(imagePipelineArn=arn).get(
                    "imagePipeline", {}
                )
                current_req: Dict[str, Any] = {
                    "imageRecipeArn": current.get("imageRecipeArn"),
                    "infrastructureConfigurationArn": current.get(
                        "infrastructureConfigurationArn"
                    ),
                    "distributionConfigurationArn": current.get(
                        "distributionConfigurationArn"
                    ),
                    "status": current.get("status"),
                }
                if current.get("schedule"):
                    current_req["schedule"] = current.get("schedule")
                if "imageTestsConfiguration" in req:
                    current_req["imageTestsConfiguration"] = current.get(
                        "imageTestsConfiguration"
                    )
                desired_req = {
                    "imageRecipeArn": recipe_arn,
                    "infrastructureConfigurationArn": infra_arn,
                    "distributionConfigurationArn": dist_arn,
                    "status": req["status"],
                }
                if "schedule" in req:
                    desired_req["schedule"] = req["schedule"]
                if "imageTestsConfiguration" in req:
                    desired_req["imageTestsConfiguration"] = req[
                        "imageTestsConfiguration"
                    ]
                if self._same_config(current_req, desired_req):
                    return arn
                client.update_image_pipeline(**update_req)
                self.ext["image_pipeline_updated"] = True
                return arn

        def fetch(self):
            client = self._client()

            if self.image_pipeline_name:
                arn = self._find_arn_by_name(
                    "list_image_pipelines",
                    "imagePipelineList",
                    self.image_pipeline_name,
                )
                if arn:
                    resp = client.get_image_pipeline(imagePipelineArn=arn)
                    pipeline = resp.get("imagePipeline") or {}
                    self.ext["image_pipeline_arn"] = pipeline.get("arn")
                    self.ext["image_recipe_arn"] = pipeline.get("imageRecipeArn")
                    self.ext["infrastructure_configuration_arn"] = pipeline.get(
                        "infrastructureConfigurationArn"
                    )
                    self.ext["distribution_configuration_arn"] = pipeline.get(
                        "distributionConfigurationArn"
                    )
                    return self

            raise Exception(
                f"Image Builder pipeline '{self.image_pipeline_name}' not found"
            )

        def resolve_latest_ami_id(self) -> str:
            if not self.image_pipeline_name:
                raise ValueError(
                    f"image_pipeline_name must be set to resolve AMI for ImageBuilder '{self.name}'"
                )

            client = self._client()
            pipeline_arn = self._find_arn_by_name(
                "list_image_pipelines",
                "imagePipelineList",
                self.image_pipeline_name,
            )
            if not pipeline_arn:
                raise Exception(
                    f"Image Builder pipeline '{self.image_pipeline_name}' not found"
                )

            resp = client.list_image_pipeline_images(
                imagePipelineArn=pipeline_arn,
                maxResults=25,
            )
            images = resp.get("imageSummaryList", []) or []
            if not images:
                raise Exception(
                    f"No ready AMI found for Image Builder pipeline '{self.image_pipeline_name}'. "
                    "Rerun deploy after the image is ready."
                )

            def _created_at(summary: Dict[str, Any]) -> str:
                return summary.get("dateCreated", "") or ""

            images.sort(key=_created_at, reverse=True)
            for summary in images:
                image_arn = summary.get("arn", "")
                if not image_arn:
                    continue

                image_resp = client.get_image(imageBuildVersionArn=image_arn)
                image = image_resp.get("image") or {}

                for output in image.get("outputResources", {}).get("amis", []) or []:
                    if output.get("region") == self.region and output.get("image"):
                        self.ext["latest_ami_id"] = output["image"]
                        return output["image"]

                for output in image.get("outputResources", {}).get("amis", []) or []:
                    if output.get("image"):
                        self.ext["latest_ami_id"] = output["image"]
                        return output["image"]

            raise Exception(
                f"No ready AMI found for Image Builder pipeline '{self.image_pipeline_name}'. "
                "Rerun deploy after the image is ready."
            )

        def _start_build_on_change(self, pipeline_arn: str) -> None:
            if not self.enabled:
                print(
                    f"Image Builder pipeline '{self.image_pipeline_name}' is disabled; "
                    "skipping build start"
                )
                return

            client = self._client()
            try:
                resp = client.start_image_pipeline_execution(
                    imagePipelineArn=pipeline_arn
                )
            except Exception as e:
                message = str(e)
                if (
                    e.__class__.__name__ == "ResourceInUseException"
                    or "ResourceInUseException" in message
                    or "in progress" in message.lower()
                    or "already running" in message.lower()
                ):
                    print(
                        f"Image Builder pipeline '{self.image_pipeline_name}' already "
                        "has a build in progress, skipping start"
                    )
                    return
                raise

            execution_arn = resp.get("imageBuildVersionArn", "")
            log_group_name = self._cloudwatch_log_group_name()
            log_stream_name = self._cloudwatch_log_stream_name(execution_arn)
            if log_group_name:
                self.ext["cloudwatch_log_group_name"] = log_group_name
            if log_stream_name:
                self.ext["cloudwatch_log_stream_name"] = log_stream_name
            if execution_arn:
                self.ext["last_started_build_arn"] = execution_arn
                print(
                    f"Started Image Builder build for '{self.image_pipeline_name}': "
                    f"{execution_arn}"
                )
            else:
                print(
                    f"Started Image Builder build for '{self.image_pipeline_name}'"
                )
            if log_group_name and log_stream_name:
                print(
                    "Image Builder CloudWatch logs: "
                    f"log group '{log_group_name}', log stream '{log_stream_name}'"
                )

        def deploy(self):
            try:
                self.fetch()
            except Exception:
                pass

            recipe_arn = self._get_or_create_recipe_arn()
            infra_arn = self._get_or_create_infrastructure_configuration_arn()
            dist_arn = self._get_or_create_distribution_configuration_arn()
            changed = False
            if (
                self.ext.get("image_recipe_arn") != recipe_arn
                or self.ext.get("infrastructure_configuration_arn") != infra_arn
                or self.ext.get("distribution_configuration_arn") != dist_arn
            ):
                changed = True
            pipeline_arn = self._get_or_create_pipeline_arn(
                recipe_arn, infra_arn, dist_arn
            )
            if self.ext.pop("image_pipeline_updated", False):
                changed = True
            if self.ext.get("image_pipeline_arn") != pipeline_arn:
                changed = True

            self.ext["image_recipe_arn"] = recipe_arn
            self.ext["infrastructure_configuration_arn"] = infra_arn
            self.ext["distribution_configuration_arn"] = dist_arn
            self.ext["image_pipeline_arn"] = pipeline_arn

            if not changed:
                print(
                    f"Image Builder '{self.image_pipeline_name}' is already up to date, skipping"
                )
                return self

            self._start_build_on_change(pipeline_arn)
            print(
                f"Successfully deployed Image Builder pipeline: {self.image_pipeline_name}"
            )
            return self

        def delete(self):
            client = self._client()

            def _ignore_missing(fn, *args, ignore_dependency: bool = False, **kwargs):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    message = str(e)
                    if (
                        e.__class__.__name__ in {
                            "ResourceNotFoundException",
                            "InvalidRequestException",
                            "InvalidParameterValueException",
                        }
                        or "not found" in message.lower()
                        or "does not exist" in message.lower()
                    ):
                        return None
                    if (
                        ignore_dependency
                        and e.__class__.__name__ == "ResourceDependencyException"
                    ) or "dependency" in message.lower():
                        return None
                    raise

            if self.image_pipeline_name:
                arn = self._find_arn_by_name(
                    "list_image_pipelines",
                    "imagePipelineList",
                    self.image_pipeline_name,
                )
                if arn:
                    _ignore_missing(
                        client.delete_image_pipeline,
                        imagePipelineArn=arn,
                    )
                    print(f"Deleted Image Builder pipeline '{self.image_pipeline_name}'")

            if self.image_recipe_name:
                token: str = ""
                found = False
                while True:
                    req: Dict[str, Any] = {}
                    if token:
                        req["nextToken"] = token
                    page = client.list_image_recipes(**req)
                    for item in page.get("imageRecipeSummaryList", []) or []:
                        if (
                            item.get("name") == self.image_recipe_name
                            and (item.get("semanticVersion") or item.get("version"))
                            == self.image_recipe_version
                            and item.get("arn")
                        ):
                            _ignore_missing(
                                client.delete_image_recipe,
                                imageRecipeArn=item["arn"],
                            )
                            print(f"Deleted Image Builder recipe '{self.image_recipe_name}'")
                            found = True
                            break
                    if found:
                        break
                    token = page.get("nextToken", "") or ""
                    if not token:
                        break

            if self.distribution_configuration_name:
                arn = self._find_arn_by_name(
                    "list_distribution_configurations",
                    "distributionConfigurationSummaryList",
                    self.distribution_configuration_name,
                )
                if arn:
                    _ignore_missing(
                        client.delete_distribution_configuration,
                        distributionConfigurationArn=arn,
                    )
                    print(
                        f"Deleted Image Builder distribution configuration '{self.distribution_configuration_name}'"
                    )

            if self.infrastructure_configuration_name:
                arn = self._find_arn_by_name(
                    "list_infrastructure_configurations",
                    "infrastructureConfigurationSummaryList",
                    self.infrastructure_configuration_name,
                )
                if arn:
                    _ignore_missing(
                        client.delete_infrastructure_configuration,
                        infrastructureConfigurationArn=arn,
                    )
                    print(
                        f"Deleted Image Builder infrastructure configuration '{self.infrastructure_configuration_name}'"
                    )

            component_specs = [*self.inline_components, *self._prebuilt_venv_component_specs()]
            for spec in component_specs:
                name = str(spec.get("name", "")).strip()
                version = self._component_version(spec.get("version"))
                if not name or not version:
                    continue
                token = ""
                while True:
                    req: Dict[str, Any] = {"owner": "Self"}
                    if token:
                        req["nextToken"] = token
                    page = client.list_components(**req)
                    found = False
                    for item in page.get("componentVersionList", []) or []:
                        if (
                            item.get("name") == name
                            and (item.get("semanticVersion") or item.get("version"))
                            == version
                            and item.get("arn")
                        ):
                            arn = item["arn"]
                            deleted = _ignore_missing(
                                client.delete_component,
                                componentBuildVersionArn=arn,
                                ignore_dependency=True,
                            )
                            if deleted is None:
                                _ignore_missing(
                                    client.delete_component,
                                    componentBuildVersionArn=f"{arn}/1",
                                    ignore_dependency=True,
                                )
                            print(
                                f"Deleted or skipped dependent Image Builder component '{name}'"
                            )
                            found = True
                            break
                    if found:
                        break
                    token = page.get("nextToken", "") or ""
                    if not token:
                        break
