from ._utils import aws_client
import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass, field
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
        # Python packages to vendor into the Lambda zip via pip install -t.
        python_dependencies: List[str] = field(default_factory=list)
        timeout_ms: int = 3 * 1000
        memory_size_mb: int = 128
        # Create an HTTP API Gateway for this Lambda (public HTTPS endpoint)
        api_gateway: bool = False
        # Optional EventBridge schedule expression, e.g. "rate(1 minute)"
        schedule_expression: str = ""
        # Inline IAM policies to attach to the Lambda execution role (name -> policy document)
        inline_policies: Dict[str, Any] = field(default_factory=dict)
        # IAM role name for the Lambda execution role; resolved to ARN before deploy
        role_name: str = ""
        ext: Dict[str, Any] = field(default_factory=dict)

        def fetch(self):
            """
            Fetch Lambda function configuration from AWS and store in ext dictionary.

            Retrieves: role_arn, runtime, handler, memory_size, timeout, environment variables,
            description, and other configuration properties from the existing Lambda function.

            Raises:
                Exception: If Lambda function does not exist or AWS API call fails
            """

            lambda_client = aws_client("lambda", self.region, self.name)

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


            ssm_client = aws_client("ssm", self.region, self.name)
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

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = aws_client("iam", self.region, self.name)
            policy_name = "LambdaInvokeWorker"

            # Get worker Lambda function ARN
            lambda_client = aws_client("lambda", self.region, self.name)
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

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = aws_client("iam", self.region, self.name)
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

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = aws_client("iam", self.region, self.name)
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

            # Extract role name from ARN (format: arn:aws:iam::account:role/role-name)
            role_name = role_arn.split("/")[-1]

            iam_client = aws_client("iam", self.region, self.name)
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

        def _validate_secrets(self):
            """Raise if any SSM Parameter Store secrets declared in self.secrets do not exist."""
            if not self.secrets:
                return
            ssm = aws_client("ssm", self.region, self.name)
            missing = []
            for param_name in self.secrets:
                try:
                    ssm.get_parameter(Name=param_name, WithDecryption=False)
                except ssm.exceptions.ParameterNotFound:
                    missing.append(param_name)
                except Exception as e:
                    print(f"Warning: Could not check secret '{param_name}': {e}")
            if missing:
                raise ValueError(
                    f"Lambda '{self.name}': the following SSM secrets are not set: {missing}"
                )
            print(f"All {len(self.secrets)} secret(s) validated in Parameter Store")

        def deploy(self):
            """
            Deploy a Lambda function to AWS using self.ext configuration.
            If Lambda exists, fetches current configuration including role_arn.
            """

            self._validate_secrets()

            lambda_client = aws_client("lambda", self.region, self.name)

            # Try to fetch existing Lambda configuration first
            try:
                self.fetch()
                print(f"Fetched existing configuration for Lambda: {self.name}")
            except Exception:
                print(f"Lambda {self.name} does not exist yet, will create new")

            # Package the Lambda code
            zip_buffer = self._package_lambda_code(
                self.path,
                self.include_files,
                self.python_dependencies,
            )

            # Get Lambda function configuration from self.ext with defaults
            function_name = self.name
            runtime = self.ext.get("runtime", "python3.11")
            handler = self.handler
            role_arn = self.ext.get("role_arn")
            memory_size = self.memory_size_mb
            timeout = int(self.timeout_ms / 1000)
            environment = dict(self.ext.get("environment", {}))

            # Merge non-secret environment variables (plain configuration)
            if self.environments:
                environment = {**environment, **self.environments}
                print(
                    f"Added {len(self.environments)} non-secret environment variable(s)"
                )

            # Fetch secrets and merge with environment (secrets overwrite existing)
            if self.secrets:
                print("Fetching secrets from Parameter Store...")
                secrets_env = self._fetch_secrets()
                environment = {**environment, **secrets_env}
                print(
                    f"Environment variables updated with {len(secrets_env)} secret(s)"
                )

            if not role_arn and self.role_name:
                iam = aws_client("iam", self.region, self.name)
                try:
                    role_arn = iam.get_role(RoleName=self.role_name)["Role"]["Arn"]
                    print(f"Resolved role_arn for '{function_name}' from role '{self.role_name}'")
                except Exception as e:
                    raise ValueError(
                        f"Failed to resolve role '{self.role_name}' for Lambda '{function_name}': {e}"
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
                    print("Skipping code update")
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
                current_env = dict(self.ext.get("environment", {}))
                changed_env_keys = sorted(
                    key
                    for key in set(current_env) | set(environment)
                    if current_env.get(key) != environment.get(key)
                )
                config_changed = (
                    self.ext.get("handler") != handler
                    or self.ext.get("timeout") != timeout
                    or self.ext.get("memory_size") != memory_size
                    or current_env != environment
                )
                if changed_env_keys:
                    print(
                        "Lambda environment changes detected for key(s): "
                        f"{', '.join(changed_env_keys)}"
                    )
                if config_changed or code_updated:
                    if code_updated:
                        print("Waiting for code update to complete...")
                        waiter = lambda_client.get_waiter("function_updated")
                        waiter.wait(FunctionName=function_name)

                    if config_changed:
                        print(
                            f"Updating Lambda configuration (timeout={timeout}s, memory={memory_size}MB, handler={handler})..."
                        )
                    else:
                        print(
                            "Refreshing Lambda configuration after code update "
                            f"(timeout={timeout}s, memory={memory_size}MB, handler={handler})..."
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

            # Set up API Gateway if requested
            if self.api_gateway:
                self._ensure_api_gateway(function_name)
            if self.schedule_expression:
                self._ensure_eventbridge_schedule(function_name)

            # Attach inline IAM policies to the execution role
            if self.inline_policies:
                role_arn = self.ext.get("role_arn")
                if role_arn:
                    self._attach_inline_policies(role_arn)

            return self

        def delete(self):

            if self.schedule_expression:
                self._delete_eventbridge_schedule()
            client = aws_client("lambda", self.region, self.name)
            try:
                client.delete_function(FunctionName=self.name)
                print(f"Deleted Lambda function '{self.name}'")
            except client.exceptions.ResourceNotFoundException:
                print(f"Lambda function '{self.name}' does not exist, skipping")

        def _attach_inline_policies(self, role_arn: str):
            """Attach inline IAM policies to the Lambda execution role."""

            role_name = role_arn.split("/")[-1]
            iam = aws_client("iam", self.region, self.name)

            for policy_name, policy_doc in self.inline_policies.items():
                try:
                    iam.put_role_policy(
                        RoleName=role_name,
                        PolicyName=policy_name,
                        PolicyDocument=json.dumps(policy_doc),
                    )
                    print(f"Attached inline policy '{policy_name}' to role '{role_name}'")
                except Exception as e:
                    print(f"Warning: Failed to attach policy '{policy_name}': {e}")

        def _ensure_api_gateway(self, function_name: str):
            """Create or verify an HTTP API Gateway for this Lambda."""

            apigw = aws_client("apigatewayv2", self.region, self.name)
            lambda_client = aws_client("lambda", self.region, self.name)
            api_name = f"{function_name}-API"

            # Get Lambda ARN. Even when the API already exists we still need to
            # ensure invoke permission is present, because the Lambda may have
            # been recreated after the API was left intact.
            func = lambda_client.get_function(FunctionName=function_name)
            lambda_arn = func["Configuration"]["FunctionArn"]
            account_id = lambda_arn.split(":")[4]

            # Check if API already exists
            apis = apigw.get_apis().get("Items", [])
            existing = [a for a in apis if a["Name"] == api_name]

            if existing:
                api = existing[0]
                api_id = api["ApiId"]
                endpoint = api["ApiEndpoint"]
                print(f"API Gateway already exists: {endpoint}")
                self.ext["api_endpoint"] = endpoint
                self._dump_api_endpoint(function_name, endpoint)
            else:
                # Create HTTP API with Lambda integration
                api = apigw.create_api(
                    Name=api_name,
                    ProtocolType="HTTP",
                    Target=lambda_arn,
                )
                api_id = api["ApiId"]
                endpoint = api["ApiEndpoint"]
                print(f"Created API Gateway: {endpoint}")
                self._dump_api_endpoint(function_name, endpoint)
                self.ext["api_endpoint"] = endpoint

            # Grant API Gateway permission to invoke the Lambda
            source_arn = f"arn:aws:execute-api:{self.region}:{account_id}:{api_id}/*/*"
            try:
                lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId="AllowAPIGatewayInvoke",
                    Action="lambda:InvokeFunction",
                    Principal="apigateway.amazonaws.com",
                    SourceArn=source_arn,
                )
                print("Added API Gateway invoke permission")
            except lambda_client.exceptions.ResourceConflictException:
                print("API Gateway invoke permission already exists")

        def _schedule_rule_name(self) -> str:
            return f"{self.name}-schedule"

        def _schedule_target_id(self) -> str:
            return f"{self.name}-target"

        def _ensure_eventbridge_schedule(self, function_name: str):
            """Create or update an EventBridge schedule that invokes this Lambda."""

            events = aws_client("events", self.region, self.name)
            lambda_client = aws_client("lambda", self.region, self.name)

            func = lambda_client.get_function(FunctionName=function_name)
            lambda_arn = func["Configuration"]["FunctionArn"]
            rule_name = self._schedule_rule_name()

            rule = events.put_rule(
                Name=rule_name,
                ScheduleExpression=self.schedule_expression,
                State="ENABLED",
                Description=f"Scheduled trigger for Lambda {function_name}",
            )
            rule_arn = rule["RuleArn"]
            print(
                f"Configured EventBridge schedule '{rule_name}' with expression {self.schedule_expression}"
            )

            events.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        "Id": self._schedule_target_id(),
                        "Arn": lambda_arn,
                    }
                ],
            )
            print(f"Configured EventBridge target for Lambda '{function_name}'")

            try:
                lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId="AllowEventBridgeInvoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=rule_arn,
                )
                print("Added EventBridge invoke permission")
            except lambda_client.exceptions.ResourceConflictException:
                print("EventBridge invoke permission already exists")

            self.ext["schedule_rule_arn"] = rule_arn

        def _delete_eventbridge_schedule(self):

            events = aws_client("events", self.region, self.name)
            rule_name = self._schedule_rule_name()
            try:
                events.remove_targets(
                    Rule=rule_name,
                    Ids=[self._schedule_target_id()],
                    Force=True,
                )
            except Exception as e:
                print(f"Warning: Failed to remove EventBridge targets for {rule_name}: {e}")
            try:
                events.delete_rule(Name=rule_name, Force=True)
                print(f"Deleted EventBridge schedule '{rule_name}'")
            except Exception as e:
                print(f"Warning: Failed to delete EventBridge schedule {rule_name}: {e}")

        def _dump_api_endpoint(self, function_name: str, endpoint: str):
            from ..settings import Settings
            out_dir = Path(Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH).parent
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{function_name}_api_gateway.txt"
            out_file.write_text(f"{endpoint}\n")
            print(f"API Gateway URL written to {out_file}")

        def _package_lambda_code(
            self,
            code_path: str,
            include_files: List[str] = None,
            python_dependencies: List[str] = None,
        ) -> io.BytesIO:
            """
            Package Lambda code into a zip file.

            Args:
                code_path: Path to the Lambda code (file or directory)
                include_files: Additional files to include in the package
                python_dependencies: Python packages to vendor into the zip

            Returns:
                BytesIO buffer containing the zipped code
            """
            zip_buffer = io.BytesIO()
            path = Path(code_path)
            include_files = include_files or []
            python_dependencies = python_dependencies or []

            with tempfile.TemporaryDirectory(prefix=f"{self.name}-lambda-") as staging_dir:
                staging = Path(staging_dir)
                if path.is_file():
                    shutil.copy2(path, staging / path.name)
                elif path.is_dir():
                    for root, dirs, files in os.walk(path):
                        root_path = Path(root)
                        rel_root = root_path.relative_to(path)
                        target_root = staging / rel_root
                        target_root.mkdir(parents=True, exist_ok=True)
                        for file in files:
                            file_path = root_path / file
                            shutil.copy2(file_path, target_root / file)
                else:
                    raise ValueError(f"Invalid path: {code_path}")

                for include_path in include_files:
                    include_file = Path(include_path)
                    if include_file.is_file():
                        shutil.copy2(include_file, staging / include_file.name)
                        print(f"Including additional file: {include_file.name}")
                    else:
                        print(f"Warning: Include file not found: {include_path}")

                if python_dependencies:
                    runtime = self.ext.get("runtime", "python3.11")
                    if not runtime.startswith("python3."):
                        raise ValueError(
                            f"Unsupported Lambda runtime for dependency vendoring: {runtime}"
                        )
                    py_version = runtime.removeprefix("python")
                    cmd = [
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
                        "--only-binary=:all:",
                        "--platform",
                        "manylinux2014_x86_64",
                        "--implementation",
                        "cp",
                        "--python-version",
                        py_version,
                        "--target",
                        str(staging),
                        *python_dependencies,
                    ]
                    print(
                        f"Vendoring {len(python_dependencies)} Lambda Python dependenc"
                        f"{'y' if len(python_dependencies) == 1 else 'ies'} for {self.name}"
                    )
                    subprocess.run(cmd, check=True)

                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                    for root, dirs, files in os.walk(staging):
                        root_path = Path(root)
                        for file in files:
                            file_path = root_path / file
                            arcname = file_path.relative_to(staging)
                            zip_file.write(file_path, arcname=str(arcname))

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


            logs_client = aws_client("logs", self.region, self.name)
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

            lambda_client = aws_client("lambda", self.region, self.name)

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

# CI engine Lambda - receives GitHub webhook events to trigger pipelines

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
