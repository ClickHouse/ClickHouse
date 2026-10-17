import base64
import hashlib
import io
import json
import os
import sys
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List


class Lambda:

    @dataclass
    class Config:
        # lambda name
        name: str
        # path to code
        path: str
        handler: str
        region: str = ""
        # additional files to include in package (list of paths)
        include_files: List[str] = field(default_factory=list)
        # Map from AWS Parameter Store name to environment variable name
        secrets: Dict[str, str] = field(default_factory=dict)
        # Non-secret environment variables (plain key-value pairs)
        environments: Dict[str, str] = field(default_factory=dict)
        timeout_ms: int = 3 * 1000
        memory_size_mb: int = 128
        ext: Dict[str, Any] = field(default_factory=dict)

        def fetch(self):
            """
            Fetch Lambda function configuration from AWS and store in ext dictionary.

            Retrieves: role_arn, runtime, handler, memory_size, timeout, environment variables,
            description, and other configuration properties from the existing Lambda function.

            Raises:
                Exception: If Lambda function does not exist or AWS API call fails
            """
            import boto3

            lambda_client = boto3.client("lambda", region_name=self.region)

            try:
                # Get function configuration
                response = lambda_client.get_function(FunctionName=self.name)
                config = response["Configuration"]

                # Store all fetched properties in ext dictionary
                self.ext["role_arn"] = config.get("Role")
                self.ext["runtime"] = config.get("Runtime")
                self.ext["handler"] = config.get("Handler")
                self.ext["memory_size"] = config.get("MemorySize")
                self.ext["timeout"] = config.get("Timeout")
                self.ext["description"] = config.get("Description", "")
                self.ext["last_modified"] = config.get("LastModified")
                self.ext["code_size"] = config.get("CodeSize")
                self.ext["code_sha256"] = config.get("CodeSha256")
                self.ext["version"] = config.get("Version")
                self.ext["vpc_config"] = config.get("VpcConfig")
                self.ext["layers"] = config.get("Layers", [])
                self.ext["state"] = config.get("State")
                self.ext["architectures"] = config.get("Architectures", [])

                # Extract environment variables
                env_config = config.get("Environment", {})
                self.ext["environment"] = env_config.get("Variables", {})

                # Dead letter config
                dlq_config = config.get("DeadLetterConfig", {})
                self.ext["dead_letter_target_arn"] = dlq_config.get("TargetArn")

                # Tracing config
                tracing_config = config.get("TracingConfig", {})
                self.ext["tracing_mode"] = tracing_config.get("Mode")

                print(
                    f"Successfully fetched configuration for Lambda function: {self.name}"
                )

            except lambda_client.exceptions.ResourceNotFoundException:
                raise Exception(f"Lambda function '{self.name}' not found in AWS")
            return self

        def _fetch_secrets(self) -> Dict[str, str]:
            """
            Fetch secrets from AWS Systems Manager Parameter Store.

            Returns:
                Dict mapping environment variable names to their secret values
            """
            if not self.secrets:
                return {}

            import boto3

            ssm_client = boto3.client("ssm", region_name=self.region)
            env_vars = {}

            for param_name, env_var_name in self.secrets.items():
                try:
                    response = ssm_client.get_parameter(
                        Name=param_name, WithDecryption=True
                    )
                    env_vars[env_var_name] = response["Parameter"]["Value"]
                    print(f"Fetched secret: {param_name} -> {env_var_name}")
                except Exception as e:
                    print(f"Warning: Failed to fetch secret {param_name}: {e}")
                    raise

            return env_vars

        def _attach_worker_invoke_policy(
            self, role_arn: str, worker_function_name: str
        ):
            """
            Attach IAM policy to allow Lambda to invoke worker Lambda.

            Args:
                role_arn: Lambda execution role ARN
                worker_function_name: Worker Lambda function name to invoke
            """
            import boto3

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = boto3.client("iam", region_name=self.region)
            policy_name = "LambdaInvokeWorker"

            # Get worker Lambda function ARN
            lambda_client = boto3.client("lambda", region_name=self.region)
            try:
                response = lambda_client.get_function(FunctionName=worker_function_name)
                worker_arn = response["Configuration"]["FunctionArn"]
            except Exception as e:
                print(f"Warning: Could not get worker Lambda ARN: {e}")
                return

            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "lambda:InvokeFunction",
                        "Resource": worker_arn,
                    }
                ],
            }

            try:
                iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(policy_document),
                )
                print(
                    f"Attached IAM policy '{policy_name}' to role '{role_name}' to invoke worker Lambda"
                )
            except Exception as e:
                print(f"Warning: Failed to attach IAM policy: {e}")

        def _attach_s3_read_policy(self, role_arn: str):
            """
            Attach IAM policy to allow Lambda to read from S3.

            Args:
                role_arn: Lambda execution role ARN
            """
            import boto3

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = boto3.client("iam", region_name=self.region)
            policy_name = "LambdaS3ReadAccess"

            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:HeadObject"],
                        "Resource": "arn:aws:s3:::clickhouse-test-reports-private/*",
                    }
                ],
            }

            try:
                iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(policy_document),
                )
                print(
                    f"Attached IAM policy '{policy_name}' to role '{role_name}' for S3 read access"
                )
            except Exception as e:
                print(f"Warning: Failed to attach S3 policy: {e}")

        def _attach_s3_readwrite_policy(self, role_arn: str):
            """
            Attach IAM policy to allow Lambda to read and write to S3.

            Args:
                role_arn: Lambda execution role ARN
            """
            import boto3

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = boto3.client("iam", region_name=self.region)
            policy_name = "LambdaS3ReadWriteAccess"

            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:HeadObject", "s3:PutObject"],
                        "Resource": "arn:aws:s3:::clickhouse-test-reports-private/*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": "arn:aws:s3:::clickhouse-test-reports-private",
                    },
                ],
            }

            try:
                iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(policy_document),
                )
                print(
                    f"Attached IAM policy '{policy_name}' to role '{role_name}' for S3 read/write access"
                )
            except Exception as e:
                print(f"Warning: Failed to attach S3 policy: {e}")

        def _attach_cloudwatch_logs_policy(self, role_arn: str):
            """
            Attach IAM policy to allow Lambda to write CloudWatch Logs.

            Args:
                role_arn: Lambda execution role ARN
            """
            import boto3

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = boto3.client("iam", region_name=self.region)
            policy_name = "LambdaCloudWatchLogsAccess"

            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                        ],
                        "Resource": "arn:aws:logs:*:*:*",
                    }
                ],
            }

            try:
                iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(policy_document),
                )
                print(
                    f"Attached IAM policy '{policy_name}' to role '{role_name}' for CloudWatch Logs access"
                )
            except Exception as e:
                print(f"Warning: Failed to attach CloudWatch Logs policy: {e}")

        def deploy(self):
            """
            Deploy a Lambda function to AWS using self.ext configuration.
            If Lambda exists, fetches current configuration including role_arn.
            """
            import boto3

            lambda_client = boto3.client("lambda", region_name=self.region)

            # Try to fetch existing Lambda configuration first
            try:
                self.fetch()
                print(f"Fetched existing configuration for Lambda: {self.name}")
            except Exception:
                print(f"Lambda {self.name} does not exist yet, will create new")

            # Package the Lambda code
            zip_buffer = self._package_lambda_code(self.path, self.include_files)

            # Get Lambda function configuration from self.ext with defaults
            function_name = self.name
            runtime = self.ext.get("runtime", "python3.11")
            handler = self.handler
            role_arn = self.ext.get("role_arn")
            memory_size = self.memory_size_mb
            timeout = int(self.timeout_ms / 1000)
            environment = self.ext.get("environment", {})

            # Merge non-secret environment variables (plain configuration)
            if self.environments:
                environment = {**environment, **self.environments}
                print(
                    f"Added {len(self.environments)} non-secret environment variable(s)"
                )

            # Fetch secrets and merge with environment (secrets overwrite existing)
            if self.secrets:
                print(f"Fetching secrets from Parameter Store...")
                secrets_env = self._fetch_secrets()
                environment = {**environment, **secrets_env}
                print(
                    f"Environment variables updated with {len(secrets_env)} secret(s)"
                )

            if not role_arn:
                raise ValueError(
                    f"role_arn must be specified for Lambda function {function_name}"
                )

            # Check if function exists
            try:
                response = lambda_client.get_function(FunctionName=function_name)
                existing_code_sha256 = response["Configuration"].get("CodeSha256")

                # Calculate SHA256 of new code (base64 encoded to match AWS format)
                zip_data = zip_buffer.getvalue()
                new_code_sha256 = base64.b64encode(
                    hashlib.sha256(zip_data).digest()
                ).decode("utf-8")

                # Compare code hashes
                code_updated = False
                if existing_code_sha256 == new_code_sha256:
                    print(
                        f"Code unchanged for Lambda function: {function_name} (SHA256: {new_code_sha256})"
                    )
                    print(f"Skipping code update")
                else:
                    # Function exists, update it
                    print(f"Code changed for Lambda function: {function_name}")
                    print(f"  Old SHA256: {existing_code_sha256}")
                    print(f"  New SHA256: {new_code_sha256}")

                    # Update function code
                    lambda_client.update_function_code(
                        FunctionName=function_name, ZipFile=zip_data
                    )
                    code_updated = True
                    print(f"Successfully updated Lambda function code: {function_name}")

                # Update function configuration only if something changed
                current_env = self.ext.get("environment", {})
                config_changed = (
                    self.ext.get("handler") != handler
                    or self.ext.get("timeout") != timeout
                    or self.ext.get("memory_size") != memory_size
                    or current_env != environment
                )
                if config_changed:
                    if code_updated:
                        print(f"Waiting for code update to complete...")
                        waiter = lambda_client.get_waiter("function_updated")
                        waiter.wait(FunctionName=function_name)

                    print(
                        f"Updating Lambda configuration (timeout={timeout}s, memory={memory_size}MB, handler={handler})..."
                    )
                    lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Handler=handler,
                        Timeout=timeout,
                        MemorySize=memory_size,
                        # Runtime=runtime,
                        Environment={"Variables": environment},
                    )
                    print(
                        f"Successfully updated Lambda function configuration: {function_name}"
                    )
                elif not code_updated:
                    print(
                        f"Lambda '{function_name}' is already up to date, skipping"
                    )

            except lambda_client.exceptions.ResourceNotFoundException:
                # Function doesn't exist, create it
                print(f"Creating new Lambda function: {function_name}")

                lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime=runtime,
                    Role=role_arn,
                    Handler=handler,
                    Code={"ZipFile": zip_buffer.getvalue()},
                    MemorySize=memory_size,
                    Timeout=timeout,
                    Environment=(
                        {"Variables": environment} if environment else {"Variables": {}}
                    ),
                )

                print(f"Successfully created Lambda function: {function_name}")
            return self

        def _package_lambda_code(
            self, code_path: str, include_files: List[str] = None
        ) -> io.BytesIO:
            """
            Package Lambda code into a zip file.

            Args:
                code_path: Path to the Lambda code (file or directory)
                include_files: Additional files to include in the package

            Returns:
                BytesIO buffer containing the zipped code
            """
            zip_buffer = io.BytesIO()
            path = Path(code_path)
            include_files = include_files or []

            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                if path.is_file():
                    # Single file
                    zip_file.write(path, arcname=path.name)
                elif path.is_dir():
                    # Directory - recursively add all files
                    for root, dirs, files in os.walk(path):
                        for file in files:
                            file_path = Path(root) / file
                            arcname = file_path.relative_to(path)
                            zip_file.write(file_path, arcname=str(arcname))
                else:
                    raise ValueError(f"Invalid path: {code_path}")

                # Add additional files
                for include_path in include_files:
                    include_file = Path(include_path)
                    if include_file.is_file():
                        zip_file.write(include_file, arcname=include_file.name)
                        print(f"Including additional file: {include_file.name}")
                    else:
                        print(f"Warning: Include file not found: {include_path}")

            zip_buffer.seek(0)
            return zip_buffer

        def fetch_cloud_watch_logs(self, limit: int = 100, seconds: int = 1800):
            """
            Fetch CloudWatch logs for the Lambda function.

            Args:
                limit: Maximum number of log events to fetch (default: 100)
                seconds: Number of seconds to look back (default: 1800)

            Returns:
                List of log events with timestamp and message
            """
            import time

            import boto3

            logs_client = boto3.client("logs", region_name=self.region)
            log_group_name = f"/aws/lambda/{self.name}"

            # Calculate time range (in milliseconds)
            end_time = int(time.time() * 1000)
            start_time = end_time - (seconds * 1000)

            try:
                # Get log streams (sorted by last event time)
                streams_response = logs_client.describe_log_streams(
                    logGroupName=log_group_name,
                    orderBy="LastEventTime",
                    descending=True,
                    limit=5,  # Get last 5 streams
                )

                log_streams = streams_response.get("logStreams", [])

                if not log_streams:
                    print(f"No log streams found for Lambda function: {self.name}")
                    return []

                # Fetch log events from all streams
                all_events = []
                for stream in log_streams:
                    stream_name = stream["logStreamName"]

                    try:
                        events_response = logs_client.get_log_events(
                            logGroupName=log_group_name,
                            logStreamName=stream_name,
                            startTime=start_time,
                            endTime=end_time,
                            limit=limit,
                            startFromHead=False,  # Get most recent events first
                        )

                        events = events_response.get("events", [])
                        all_events.extend(events)

                    except Exception as e:
                        print(
                            f"Warning: Failed to fetch logs from stream {stream_name}: {e}"
                        )
                        continue

                # Sort by timestamp (most recent first) and limit
                all_events.sort(key=lambda x: x["timestamp"], reverse=True)
                all_events = all_events[:limit]

                print(
                    f"Fetched {len(all_events)} log events for Lambda function: {self.name}"
                )

                # Store in ext for later access
                self.ext["cloudwatch_logs"] = all_events

                return all_events

            except logs_client.exceptions.ResourceNotFoundException:
                print(f"Log group not found for Lambda function: {self.name}")
                print(f"Expected log group: {log_group_name}")
                return []
            except Exception as e:
                print(f"Error fetching CloudWatch logs: {e}")
                return []

        def invoke(self, payload: dict, invocation_type: str = "Event"):
            """
            Invoke this Lambda function.

            Args:
                payload: Dictionary payload to send to Lambda
                invocation_type: "Event" (async), "RequestResponse" (sync), or "DryRun"

            Returns:
                Response from Lambda invoke call
            """
            import boto3

            lambda_client = boto3.client("lambda", region_name=self.region)

            try:
                response = lambda_client.invoke(
                    FunctionName=self.name,
                    InvocationType=invocation_type,
                    Payload=json.dumps(payload),
                )
                print(
                    f"Invoked Lambda {self.name} with invocation type {invocation_type}, StatusCode: {response['StatusCode']}"
                )
                return response
            except Exception as e:
                print(f"Error invoking Lambda {self.name}: {e}")
                raise


# Main Slack app Lambda
lambda_app_config = Lambda.Config(
    name="praktika_slack_app",
    path=f"{os.path.dirname(__file__)}/native/lambda_slack_app.py",
    handler="lambda_slack_app.lambda_handler",
    secrets={
        "praktika_slack_app_signing_secret": "SIGN_SECRET",
        "praktika_slack_app_token": "SLACK_BOT_TOKEN",
    },
    timeout_ms=3 * 1000,
    memory_size_mb=128,
)

# Worker Lambda for S3 and Slack home view processing
lambda_worker_config = Lambda.Config(
    name="praktika_slack_worker",
    path=f"{os.path.dirname(__file__)}/native/lambda_slack_worker.py",
    handler="lambda_slack_worker.lambda_handler",
    include_files=[f"{os.path.dirname(__file__)}/../event.py"],
    secrets={
        "praktika_slack_app_token": "SLACK_BOT_TOKEN",
    },
    timeout_ms=30 * 1000,
    memory_size_mb=128,
)

# local tests and development
if __name__ == "__main__":
    lambda_worker_config.region = "us-east-1"
    lambda_app_config.region = "us-east-1"
    if "--logs" in sys.argv:
        if "--worker" in sys.argv:
            print("Worker Lambda logs:")
            print(lambda_worker_config.fetch_cloud_watch_logs(seconds=300))
        else:
            print("Main Lambda logs:")
            print(lambda_app_config.fetch_cloud_watch_logs(seconds=300))
    else:
        print("Usage: python lambda_function.py --logs [--worker]")
