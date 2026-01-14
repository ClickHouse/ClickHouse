# Praktika Infrastructure Module

**Status**: Under development

This module provides classes for configuring and deploying cloud infrastructure components for Praktika CI/CD workflows.

## Module Structure

### Core Configuration Classes

#### [cloud.py](cloud.py)
Top-level infrastructure configuration class `CloudInfrastructure.Config`:
- Aggregates all cloud resources (Lambda functions, S3 buckets, etc.)
- Entry point for infrastructure definition
- Handles deployment orchestration

#### [lambda_function.py](lambda_function.py)
Lambda function configuration class `Lambda.Config`:
- Defines AWS Lambda function settings (handler, timeout, memory, environment)
- Manages Lambda deployment package bundling
- Handles secret injection from AWS Parameter Store
- Supports CloudWatch logs retrieval

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
    ]
)
```

### Configure Settings

Set the cloud configuration path in your Praktika settings:

```python
from praktika import Settings

Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH = "./ci/infra/cloud.py"
Settings.AWS_REGION = "us-east-1"
Settings.EVENTS_S3_PATH = "my-bucket/events"  # For Slack feed storage
```

### Deploy

Deploy your infrastructure to AWS:

```bash
praktika deploy
```

This command:
1. Loads configuration from `Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH`
2. Deploys all Lambda functions defined in `CLOUD.lambda_functions`
3. Fetches secrets from AWS Parameter Store
4. Updates function code and configuration

## Roadmap

The long-term goal is to provide functionality for configuring and deploying complete cloud CI/CD infrastructure from scratch, enabling teams to provision their entire workflow environment declaratively.

### Future Configuration Classes

- **S3Bucket.Config**: S3 bucket creation and lifecycle management
- **Policy.Config**: IAM policies attachable to Lambdas, roles, etc.
- **AutoScalingGroup.Config**: EC2 Auto Scaling groups for runners
- **Image.Config / ImageBuilder.Config**: Container image or AMI configuration
