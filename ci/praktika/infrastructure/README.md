# Praktika Infrastructure Module

**Status**: Under development

This module provides classes for configuring and deploying cloud infrastructure components for Praktika CI/CD workflows.

## Module Structure

### Core Configuration Classes

#### [cloud.py](cloud.py)
Top-level infrastructure configuration class `CloudInfrastructure.Config`:
- Aggregates cloud resources (Dedicated Hosts, Image Builder pipelines, Launch Templates, Auto Scaling Groups, Lambda functions)
- Entry point for infrastructure definition and deployment orchestration

#### [lambda_function.py](lambda_function.py)
Lambda function configuration class `Lambda.Config`:
- Defines and deploys AWS Lambda functions used by Praktika (including optional native components)

#### [launch_template.py](launch_template.py)
Launch Template configuration class `LaunchTemplate.Config`:
- Defines and deploys AWS EC2 Launch Templates (used by ASGs)

#### [autoscaling_group.py](autoscaling_group.py)
Auto Scaling Group configuration class `AutoScalingGroup.Config`:
- Defines and deploys AWS EC2 Auto Scaling Groups (ASG) using an existing Launch Template

#### [image_builder.py](image_builder.py)
Image Builder configuration class `ImageBuilder.Config`:
- Defines and deploys AWS EC2 Image Builder resources (Recipe, Infrastructure/Distribution configs, Pipeline)
- Can be used as an AMI source for `LaunchTemplate.Config` via `image_builder_pipeline_name`

#### [iam_instance_profile.py](iam_instance_profile.py)
IAM Instance Profile configuration class `IAMInstanceProfile.Config`:
- Defines and deploys IAM Role + Instance Profile (commonly used for EC2 Image Builder Infrastructure Configuration)

#### [dedicated_host.py](dedicated_host.py)
Dedicated Host configuration class `DedicatedHost.Config`:
- Defines and deploys EC2 Dedicated Hosts (required for macOS fleets)

### Native Components

See [native/README.md](native/README.md) for pre-built cloud components provided by Praktika (e.g., Slack app integration).

## Usage

### Define Infrastructure

Create a cloud configuration file (e.g., `ci/infra/cloud.py`):

```python
from praktika import CloudInfrastructure

CLOUD = CloudInfrastructure.Config(
    name="my_cloud_infra",
    lambda_functions=[
        # Add your Lambda.Config instances here
        *CloudInfrastructure.SLACK_APP_LAMBDAS  # Optional: include native components
    ],
    launch_templates=[
        # Add your LaunchTemplate.Config instances here
    ],
    autoscaling_groups=[
        # Add your AutoScalingGroup.Config instances here
    ],
)
```

### Configure Settings

Set the cloud configuration path in your Praktika settings:

```python
from praktika import Settings

Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH = "./ci/infra/cloud.py"
Settings.AWS_REGION = "us-east-1"
Settings.EVENT_FEED_S3_PATH = "my-bucket/events"  # For Slack feed storage
```

### Deploy

Deploy your infrastructure to AWS:

```bash
praktika infrastructure --deploy
```

Deploy only selected component types:

```bash
praktika infrastructure --deploy --only ImageBuilder
praktika infrastructure --deploy --only ImageBuilder LaunchTemplate
praktika infrastructure --deploy --only DedicatedHost
```

This command:
1. Loads configuration from `Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH`
2. Deploys all Dedicated Hosts defined in `CLOUD.dedicated_hosts`
3. Deploys all IAM Instance Profiles defined in `CLOUD.iam_instance_profiles`
4. Deploys all Image Builder pipelines defined in `CLOUD.image_builders`
5. Deploys all Launch Templates defined in `CLOUD.launch_templates`
6. Deploys all Auto Scaling Groups defined in `CLOUD.autoscaling_groups`
7. Deploys all Lambda functions defined in `CLOUD.lambda_functions`
8. Fetches secrets from AWS Parameter Store (for Lambda environment injection)

### Shutdown

Terminate running EC2 instances:

```bash
praktika infrastructure --shutdown --only EC2Instance
```

Release Dedicated Hosts:

```bash
praktika infrastructure --shutdown --only DedicatedHost
```

## macOS Auto Scaling Group (Dedicated Hosts)

To run macOS instances in an ASG you must use EC2 Dedicated Hosts.

Praktika configuration is split into three objects:

1. `DedicatedHost.Config`: allocates/maintains the required mac Dedicated Hosts.
2. `LaunchTemplate.Config`: defines how instances are launched (AMI, instance type, SGs, placement).
3. `AutoScalingGroup.Config`: defines scaling + networking and references the launch template.

Minimal working requirements for macOS ASG in Praktika:

- **Dedicated hosts are required**
  - Configure `DedicatedHost.Config(instance_type="mac2-m2.metal" | "mac2.metal")`.
  - **MUST set `auto_placement="on"`** (this is enforced with an assertion). This allows EC2 to automatically place instances with `tenancy="host"` onto available hosts in this pool.
  - With `auto_placement="on"`, EC2 instances do NOT need to specify `host_resource_group_name` - AWS automatically handles placement based on instance type and availability zone matching.
- **Launch template must use host tenancy**
  - Configure `LaunchTemplate.Config(tenancy="host")`.
  - Do not pin to a specific `host_id` for ASG usage.
- **ASG must be created in subnets (VPC)**
  - Either provide `subnet_ids`, or provide `vpc_id`/`vpc_name` and optionally `availability_zones` for discovery.
- **Start with desired capacity 0**
  - Use `min_size=0` and either omit `desired_capacity` or set it to `0`.

### How Auto-Placement Works

When you configure:
- `DedicatedHost.Config(auto_placement="on", instance_type="mac2-m2.metal")`
- `EC2Instance.Config(tenancy="host", instance_type="mac2-m2.metal")` or `LaunchTemplate.Config(tenancy="host")`

AWS automatically places the instance on an available dedicated host that:
1. Has `auto_placement="on"`
2. Matches the instance type
3. Is in the same availability zone
4. Has available capacity

**Note**: For more complex scenarios requiring explicit host targeting (e.g., multiple host pools with different purposes, specific placement policies), you would need to implement `ResourceGroup.Config` or enhance `EC2Instance.Config` to support explicit `host_resource_group_name` targeting. The current implementation supports the simple case where auto-placement handles all the routing.

Example (macOS, Dedicated Hosts + ASG):

```python
from praktika import CloudInfrastructure
from praktika.infrastructure.dedicated_host import DedicatedHost
from praktika.infrastructure.launch_template import LaunchTemplate
from praktika.infrastructure.autoscaling_group import AutoScalingGroup

MAC_HOST_POOL_NAME = "praktika-mac-hosts-us-east-1"
MAC_VPC_NAME = "ci-cd"

CLOUD = CloudInfrastructure.Config(
    name="cloud_infra",
    dedicated_hosts=[
        DedicatedHost.Config(
            name=MAC_HOST_POOL_NAME,
            availability_zones=["us-east-1a"],
            instance_type="mac2-m2.metal",
            auto_placement="on",
            quantity_per_az=1,
            tags={"praktika": "mac"},
        )
    ],
    launch_templates=[
        LaunchTemplate.Config(
            name="praktika-mac-lt",
            image_id="ami-...",
            instance_type="mac2-m2.metal",
            security_group_ids=["sg-..."],
            tenancy="host",
            user_data="#!/bin/bash\necho hello-world\n",
        )
    ],
    autoscaling_groups=[
        AutoScalingGroup.Config(
            name="praktika-mac-asg",
            vpc_name=MAC_VPC_NAME,
            availability_zones=["us-east-1a"],
            min_size=0,
            max_size=1,
            desired_capacity=0,
            launch_template_name="praktika-mac-lt",
        )
    ],
)
```

## Generic Auto Scaling Group configuration

`AutoScalingGroup.Config` is intentionally minimal: it manages the core set of ASG attributes needed for “runner-like” fleets.

Required fields:

- `name`
- `launch_template_id` or `launch_template_name`
- Networking: either `subnet_ids` or `vpc_id`/`vpc_name` (with optional `availability_zones` for subnet discovery)

Field reference (`AutoScalingGroup.Config`):

- **`name`**: ASG name.
- **`region`**: AWS region. Typically inherited from `Settings.AWS_REGION` by `CloudInfrastructure`.
- **`subnet_ids`**: Explicit subnet IDs for the ASG. If provided, no discovery is performed.
- **`vpc_id`**: Used for subnet discovery when `subnet_ids` is empty.
- **`vpc_name`**: VPC `Name` tag value used for subnet discovery when `vpc_id` is empty.
- **`availability_zones`**: Optional AZ filter used during subnet discovery (filters subnets by `availability-zone`).
- **`min_size` / `max_size` / `desired_capacity`**: Capacity settings. If `desired_capacity` is not set, Praktika uses `min_size`.
- **`health_check_type`**: `EC2` or `ELB`.
- **`health_check_grace_period_sec`**: Grace period for health checks.
- **`launch_template_id` / `launch_template_name`**: Which LT to use.
- **`launch_template_version`**: LT version string, default `$Latest`.
- **`target_group_arns`**: Optional list of ALB/NLB target group ARNs.
- **`tags`**: Dict of tags propagated to launched instances (`PropagateAtLaunch=True`).
- **`ext`**: Runtime/fetched fields (ARNs, resolved subnets, etc.). Not meant to be configured manually.

Related `LaunchTemplate.Config` fields you will typically set:

- **`image_id`** and **`instance_type`** (or provide raw `data`).
- **`security_group_ids`**.
- **`user_data`**.
- **`tenancy`** (for Dedicated Hosts use-cases).

## Image Builder (AMI Pipeline)

`ImageBuilder.Config` deploys an EC2 Image Builder pipeline that produces an AMI.

Minimal (Linux) example:

```python
from praktika import CloudInfrastructure
from praktika.infrastructure.image_builder import ImageBuilder
from praktika.infrastructure.launch_template import LaunchTemplate

CLOUD = CloudInfrastructure.Config(
    name="cloud_infra",
    image_builders=[
        ImageBuilder.Config(
            name="praktika-linux-ami",
            image_recipe_name="praktika-linux-recipe",
            image_recipe_version="1.0.0",
            parent_image="ami-...",  # base AMI
            components=[
                "arn:aws:imagebuilder:us-east-1:aws:component/update-linux/1.0.0",
            ],
            infrastructure_configuration_name="praktika-linux-ib-infra",
            instance_profile_name="praktika-ec2-instance-profile",
            instance_types=["c6a.large"],
            subnet_id="subnet-...",
            security_group_ids=["sg-..."],
            distribution_configuration_name="praktika-linux-ib-dist",
            ami_name="praktika-linux-{{imagebuilder:buildDate}}",
            ami_tags={"praktika": "true"},
            image_pipeline_name="praktika-linux-ib-pipeline",
            enabled=True,
        )
    ],
    launch_templates=[
        LaunchTemplate.Config(
            name="praktika-linux-lt",
            image_builder_pipeline_name="praktika-linux-ib-pipeline",
            instance_type="c6a.large",
            security_group_ids=["sg-..."],
        )
    ],
)
```

Notes:

- The pipeline must have produced at least one image build before `LaunchTemplate.Config` can resolve an AMI id.
- `LaunchTemplate.Config(image_id=...)` still works; `image_builder_pipeline_name` is only used when `image_id` is empty.

Inline components:

If you do not have pre-created component ARNs, you can define installation steps inline and Praktika will create Image Builder Components automatically:

```python
ImageBuilder.Config(
    name="my-ami",
    image_recipe_name="my-recipe",
    image_recipe_version="1.0.0",
    parent_image="ami-...",
    inline_components=[
        {
            "name": "my-install-tools",
            "version": "1.0.0",
            "platform": "Linux",
            "commands": [
                "set -euxo pipefail",
                "apt-get update",
                "apt-get install -y python3",
            ],
        }
    ],
    infrastructure_configuration_name="my-ib-infra",
    instance_profile_name="praktika-ec2-instance-profile",
    instance_types=["t3.large"],
    distribution_configuration_name="my-ib-dist",
    ami_name="my-ami-{{imagebuilder:buildDate}}",
    image_pipeline_name="my-ib-pipeline",
)
```

## Roadmap

The long-term goal is to provide functionality for configuring and deploying complete cloud CI/CD infrastructure from scratch, enabling teams to provision their entire workflow environment declaratively.

### Future Configuration Classes

- **S3Bucket.Config**: S3 bucket creation and lifecycle management
- **Policy.Config**: IAM policies attachable to Lambdas, roles, etc.
- **Image.Config / ImageBuilder.Config**: Container image or AMI configuration
