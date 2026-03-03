from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

from ..settings import _Settings
from .lambda_function import lambda_app_config, lambda_worker_config

if TYPE_CHECKING:
    from .lambda_function import Lambda


class CloudInfrastructure:
    SLACK_APP_LAMBDAS = [lambda_app_config, lambda_worker_config]

    @dataclass
    class Config:
        name: str
        lambda_functions: List["Lambda.Config"] = field(default_factory=list)
        _settings: Optional[_Settings] = None

        def deploy(self, all=False):
            """
            Deploy Lambda functions.

            Args:
                all: If False, only deploy code (skip settings validation and IAM policies).
                     If True, deploy everything (validate settings, deploy code, attach IAM policies).
            """
            if not self.lambda_functions:
                print("No Lambda functions to deploy")
                return

            # Full deployment mode: validate settings and configure environments
            if all:
                if not self._settings:
                    raise ValueError(
                        "Settings not configured. Please set _settings before deploying."
                    )

                required_settings = {
                    "EVENT_FEED_S3_PATH": self._settings.EVENT_FEED_S3_PATH,
                    "AWS_REGION": self._settings.AWS_REGION,
                }

                missing_settings = [
                    name for name, value in required_settings.items() if not value
                ]
                if missing_settings:
                    raise ValueError(
                        f"Missing required settings for Lambda deployment: {', '.join(missing_settings)}"
                    )

            # Deploy all Lambdas (code only or with configuration)
            for lambda_config in self.lambda_functions:
                # Always set region if available (needed even for code-only deploys)
                if self._settings and self._settings.AWS_REGION:
                    lambda_config.region = self._settings.AWS_REGION

                # Only set environment variables in full deployment mode
                if all and self._settings:
                    if self._settings.EVENT_FEED_S3_PATH:
                        # Inject project-specific settings into Lambda environment
                        # EVENT_FEED_S3_PATH is required by Slack Lambdas for event feed storage
                        lambda_config.environments["EVENT_FEED_S3_PATH"] = (
                            self._settings.EVENT_FEED_S3_PATH
                        )

                print("\n" + "=" * 60)
                print(f"Deploying Lambda: {lambda_config.name}")
                print("=" * 60)
                lambda_config.deploy()

            # Only attach IAM policies in full deployment mode
            if all:
                print("\n" + "=" * 60)
                print("Attaching IAM policies...")
                print("=" * 60)

                for lambda_config in self.lambda_functions:
                    role_arn = lambda_config.ext.get("role_arn")
                    if not role_arn:
                        print(
                            f"Warning: No role_arn found for {lambda_config.name}, skipping policy attachment"
                        )
                        continue

                    # Attach policies based on Lambda name patterns
                    if "worker" in lambda_config.name:
                        # Worker Lambda needs S3 read/write and CloudWatch access
                        lambda_config._attach_s3_readwrite_policy(role_arn)
                        lambda_config._attach_cloudwatch_logs_policy(role_arn)
                    elif "app" in lambda_config.name:
                        # App Lambda needs to invoke worker
                        worker_name = next(
                            (
                                lc.name
                                for lc in self.lambda_functions
                                if "worker" in lc.name
                            ),
                            None,
                        )
                        if worker_name:
                            lambda_config._attach_worker_invoke_policy(
                                role_arn, worker_name
                            )

            print("\n" + "=" * 60)
            print("Lambda deployment completed!")
            print("=" * 60)
