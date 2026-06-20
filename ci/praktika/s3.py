import dataclasses
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Any, Dict
from urllib.parse import quote

from ._environment import _Environment
from .settings import Settings
from .usage import StorageUsage
from .utils import Shell, Utils

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    ClientError = None
    NoCredentialsError = None


class S3:
    _boto3_client = None

    @classmethod
    def _get_boto3_client(cls):
        if not BOTO3_AVAILABLE:
            return None
        if cls._boto3_client is None:
            try:
                cls._boto3_client = boto3.client("s3")
            except Exception as e:
                print(f"WARNING: Failed to initialize boto3 client: {e}")
                return None
        return cls._boto3_client

    @classmethod
    def _retry_on_no_credentials(cls, func, retries=3, delay=5):
        """
        Retries a boto3 operation on NoCredentialsError.
        Credentials from the instance metadata service (IMDS) can be
        temporarily unavailable, so we reset the cached client and wait
        before each retry to give IMDS time to recover.
        """
        for attempt in range(retries):
            try:
                return func()
            except NoCredentialsError:
                if attempt + 1 < retries:
                    print(
                        f"WARNING: No AWS credentials available (attempt {attempt + 1}/{retries}), "
                        f"retrying in {delay}s..."
                    )
                    cls._boto3_client = None
                    time.sleep(delay)
                else:
                    raise

    @dataclasses.dataclass
    class Object:
        AcceptRanges: str
        Expiration: str
        LastModified: str
        ContentLength: int
        ETag: str
        ContentType: str
        ServerSideEncryption: str
        Metadata: Dict

        def has_tags(self, tags):
            meta = self.Metadata
            for k, v in tags.items():
                if k not in meta or meta[k] != v:
                    print(f"tag [{k}={v}] does not match meta [{meta}]")
                    return False
            return True

    @classmethod
    def clean_s3_directory(cls, s3_path, include=""):
        assert len(s3_path.split("/")) > 2, "check to not delete too much"
        cmd = f"aws s3 rm s3://{s3_path} --recursive"
        if include:
            cmd += f' --exclude "*" --include "{include}"'
        cls.run_command_with_retries(cmd, retries=1, with_stderr=True)
        return

    @classmethod
    def copy_file_to_s3(
        cls,
        s3_path,
        local_path,
        text=False,
        with_rename=False,
        no_strict=False,
        content_type="",
        content_encoding="",
        tags=None,
    ):
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"

        file_name = Path(local_path).name
        s3_full_path = s3_path
        if not s3_full_path.endswith(file_name) and not with_rename:
            s3_full_path = f"{s3_path}/{Path(local_path).name}"

        # Use boto3 if available, otherwise fall back to AWS CLI
        if BOTO3_AVAILABLE and cls._get_boto3_client():
            try:
                s3_full_path_clean = str(s3_full_path).removeprefix("s3://")
                bucket, key = s3_full_path_clean.split("/", maxsplit=1)

                # Prepare ExtraArgs for upload_file
                extra_args = {}

                inferred_content_type = ""
                if not content_type and not (text and not content_type):
                    inferred_content_type, _ = mimetypes.guess_type(key)

                if text and not content_type:
                    extra_args["ContentType"] = "text/plain"
                elif content_type:
                    extra_args["ContentType"] = content_type
                elif inferred_content_type:
                    extra_args["ContentType"] = inferred_content_type
                if content_encoding:
                    extra_args["ContentEncoding"] = content_encoding

                def _upload():
                    client = cls._get_boto3_client()
                    if extra_args:
                        client.upload_file(
                            str(local_path), bucket, key, ExtraArgs=extra_args
                        )
                    else:
                        client.upload_file(str(local_path), bucket, key)
                    if tags:
                        tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
                        client.put_object_tagging(
                            Bucket=bucket, Key=key, Tagging={"TagSet": tag_set}
                        )

                # Retry on transient credential failures
                cls._retry_on_no_credentials(_upload)

            except NoCredentialsError as e:
                print(
                    f"ERROR: Failed to upload to S3 using boto3 (no credentials): {e}"
                )
                if not no_strict:
                    raise
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                print(f"ERROR: Failed to upload to S3 using boto3: {error_code}")
                if not no_strict:
                    raise
            except Exception as e:
                print(f"ERROR: Failed to upload to S3 using boto3: {e}")
                if not no_strict:
                    raise
        else:
            # boto3 not available, use AWS CLI
            cmd = f"aws s3 cp {local_path} s3://{s3_full_path}"
            if text and not content_type:
                cmd += " --content-type text/plain"
            elif content_type:
                cmd += f" --content-type {content_type}"
            if content_encoding:
                cmd += f" --content-encoding {content_encoding}"
            _ = cls.run_command_with_retries(cmd, no_strict=no_strict)

            # Apply tags if provided
            if tags:
                bucket = s3_full_path.split("/")[0]
                key = "/".join(s3_full_path.split("/")[1:])
                # Use JSON format for tagging to ensure correct syntax
                tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
                tagging_json = json.dumps({"TagSet": tag_set})
                tag_cmd = f"aws s3api put-object-tagging --bucket {bucket} --key {key} --tagging '{tagging_json}'"
                cls.run_command_with_retries(tag_cmd, no_strict=True)

        # Common cleanup and return for both paths
        try:
            StorageUsage.add_uploaded(local_path)
        except Exception as e:
            print(f"WARNING: Failed to record upload usage for {local_path}: {e}")

        bucket = s3_path.split("/")[0]
        endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[bucket]
        assert endpoint
        return quote(f"https://{s3_full_path}".replace(bucket, endpoint), safe=":/?&=")

    @classmethod
    def put(
        cls,
        s3_path,
        local_path,
        text=False,
        metadata=None,
        if_none_matched=False,
        if_match=None,
        no_strict=False,
    ):
        """
        puts object via API PUT request
        :param s3_path:
        :param local_path:
        :param text:
        :param metadata:
        :param if_none_matched:
        :param if_match: ETag value for conditional PUT (use with version checking)
        :param no_strict:
        :return:
        """
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"
        s3_full_path = s3_path
        if s3_full_path.endswith("/"):
            s3_full_path = f"{s3_path}{Path(local_path).name}"

        s3_full_path = str(s3_full_path).removeprefix("s3://")
        bucket, key = s3_full_path.split("/", maxsplit=1)

        command = (
            f"aws s3api put-object --bucket {bucket} --key {key} --body {local_path}"
        )
        if if_none_matched:
            command += f' --if-none-match "*"'
        if if_match:
            command += f' --if-match "{if_match}"'
        if metadata:
            for k, v in metadata.items():
                command += f" --metadata {k}={v}"

        if text:
            command += " --content-type text/plain"
        res = cls.run_command_with_retries(command, no_strict=no_strict)
        if res:
            StorageUsage.add_uploaded(local_path)
        return res

    @classmethod
    def run_command_with_retries(
        cls,
        command,
        retries=Settings.MAX_RETRIES_S3,
        no_strict=False,
        with_stderr=False,
    ):
        i = 0
        res = False
        stderr = ""
        while not res and i < retries:
            i += 1
            ret_code, stdout, stderr = Shell.get_res_stdout_stderr(
                command, verbose=True
            )
            if "aws sso login" in stderr:
                print("ERROR: aws login expired")
                break
            elif "does not exist" in stderr:
                print("ERROR: requested file does not exist")
                break
            elif "Unknown options" in stderr:
                print("ERROR: Invalid AWS CLI command or CLI client version:")
                print(f"  | awc error: {stderr}")
                break
            elif "PreconditionFailed" in stderr:
                print("ERROR: AWS API Call Precondition Failed")
                print(f"  | awc error: {stderr}")
                break
            if ret_code != 0:
                print(
                    f"ERROR: aws s3 cp failed, stdout/stderr err: [{stderr}], out [{stdout}]"
                )
            elif with_stderr and (stdout or stderr):
                print(f"stdout: {stdout}\nstderr: {stderr}")
            res = ret_code == 0
        if not res and not no_strict:
            raise RuntimeError(f"s3 command failed: [{stderr}]")
        return res

    @classmethod
    def copy_file_from_s3(
        cls,
        s3_path,
        local_path,
        recursive=False,
        include_pattern="",
        _skip_download_counter=False,
        no_strict=False,
    ):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"

        if BOTO3_AVAILABLE and not recursive and not include_pattern:
            if cls._get_boto3_client():
                try:
                    s3_path_clean = str(s3_path).removeprefix("s3://")
                    bucket, key = s3_path_clean.split("/", maxsplit=1)

                    if Path(local_path).is_dir():
                        local_file = Path(local_path) / Path(key).name
                    else:
                        local_file = Path(local_path)
                        if not local_file.parent.is_dir():
                            assert (
                                False
                            ), f"Parent path for [{local_path}] does not exist"

                    local_file.parent.mkdir(parents=True, exist_ok=True)

                    def _download():
                        client = cls._get_boto3_client()
                        client.download_file(bucket, key, str(local_file))

                    # Retry on transient credential failures
                    cls._retry_on_no_credentials(_download)

                    if not _skip_download_counter:
                        StorageUsage.add_downloaded(local_file)

                    return True

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["404", "NoSuchKey"]:
                        if not no_strict:
                            print(f"ERROR: S3 object not found: {s3_path}")
                        return False
                    if not no_strict:
                        raise
                    return False
                except Exception as e:
                    print(f"ERROR: Failed to download S3 object [{s3_path}]: {e}")
                    if not no_strict:
                        raise
                    return False

        if Path(local_path).is_dir():
            pass
        else:
            assert Path(
                local_path
            ).parent.is_dir(), f"Parent path for [{local_path}] does not exist"
        cmd = f"aws s3 cp s3://{s3_path}  {local_path}"
        if recursive:
            cmd += " --recursive"
        if include_pattern:
            cmd += f' --exclude "*" --include "{include_pattern}"'
        res = cls.run_command_with_retries(cmd, no_strict=no_strict)
        if res and not _skip_download_counter:
            if not recursive:
                if Path(local_path).is_dir():
                    path = Path(local_path) / Path(s3_path).name
                else:
                    path = local_path
                StorageUsage.add_downloaded(path)
            else:
                print(
                    "TODO: support StorageUsage.add_downloaded with recursive download"
                )
        return res

    @classmethod
    def copy_file_from_s3_matching_pattern(
        cls, s3_path, local_path, include, exclude="*", no_strict=False
    ):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_dir(), f"Path [{local_path}] does not exist or not a directory"
        assert s3_path.endswith("/"), f"s3 path is invalid [{s3_path}]"
        cmd = f'aws s3 cp s3://{s3_path}  {local_path} --exclude "{exclude}" --include "{include}" --recursive'
        res = cls.run_command_with_retries(cmd, no_strict=no_strict, with_stderr=True)
        if res:
            print(
                "TODO: support StorageUsage.add_downloaded with matching pattern download"
            )
        return res

    @classmethod
    def head_object(cls, s3_path):
        s3_path = str(s3_path).removeprefix("s3://")
        bucket, key = s3_path.split("/", maxsplit=1)
        output = Shell.get_output(
            f"aws s3api head-object --bucket {bucket} --key {key}", verbose=True
        )
        if not output:
            return None
        else:
            return cls.Object(**json.loads(output))

    @classmethod
    def delete(cls, s3_path):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        return Shell.check(
            f"aws s3 rm s3://{s3_path}",
            verbose=True,
        )

    @classmethod
    def _upload_file_to_s3(
        cls, local_file_path, upload_to_s3: bool, text: bool = False, s3_subprefix=""
    ) -> str:
        if upload_to_s3:
            env = _Environment.get()
            s3_path = f"{Settings.HTML_S3_PATH}/{env.get_s3_prefix()}"
            if s3_subprefix:
                s3_subprefix = s3_subprefix.removeprefix("/").removesuffix("/")
                s3_path += f"/{s3_subprefix}"
            if text and Settings.COMPRESS_THRESHOLD_MB > 0:
                file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
                if file_size_mb > Settings.COMPRESS_THRESHOLD_MB:
                    print(
                        f"NOTE: File [{local_file_path}] exceeds threshold [Settings.COMPRESS_THRESHOLD_MB:{Settings.COMPRESS_THRESHOLD_MB}] - compress"
                    )
                    text = False
                    local_file_path = Utils.compress_file(local_file_path)
            html_link = S3.copy_file_to_s3(
                s3_path=s3_path, local_path=local_file_path, text=text
            )
            return html_link
        return f"file://{Path(local_file_path).absolute()}"

    @classmethod
    def _dump_urls(cls, s3_path):
        # TODO: add support for path with '*'
        bucket, name = s3_path.split("/")[0], s3_path.split("/")[-1]
        endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[bucket]

        with open(Settings.ARTIFACT_URLS_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    name: quote(
                        f"https://{s3_path}".replace(bucket, endpoint), safe=":/?&="
                    )
                },
                f,
            )

    @classmethod
    def upload_asset_streaming(cls, local_path: Path, s3_path: str):
        """
        Uploads assets using streaming gzip to AWS S3. Detects mimetypes automatically.
        """
        assert isinstance(local_path, Path)
        content_type, _ = mimetypes.guess_type(local_path)
        content_type = content_type or "application/octet-stream"

        compressible = [".html", ".css", ".js", ".json", ".svg", ".txt"]
        use_gzip = local_path.suffix.lower() in compressible

        if use_gzip:
            cmd = f'gzip -8c {local_path} | aws s3 cp - s3://{s3_path} --content-type {content_type} --content-encoding gzip --cache-control "max-age=604800, public"'
        else:
            cmd = f'aws s3 cp {local_path} s3://{s3_path} --content-type {content_type} --cache-control "max-age=604800, public"'

        print("Execute:", cmd)
        cls.run_command_with_retries(cmd, retries=3)

    @classmethod
    def copy_file_from_s3_with_version(cls, s3_path, local_path):
        """
        Downloads a file from S3 and returns its version from object metadata.
        Uses a single atomic GET operation to ensure version matches the downloaded content.

        :param s3_path: S3 path (with or without s3:// prefix)
        :param local_path: Local path to save the file
        :return: Version number from object metadata (guaranteed to match downloaded content)
        """
        # Use boto3 if available, otherwise AWS CLI
        if BOTO3_AVAILABLE and cls._get_boto3_client():
            s3_path_clean = str(s3_path).removeprefix("s3://")
            bucket, key = s3_path_clean.split("/", maxsplit=1)

            def _download():
                client = cls._get_boto3_client()
                response = client.get_object(Bucket=bucket, Key=key)
                version = int(response.get("Metadata", {}).get("version", "0"))
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(response["Body"].read())
                return version

            # Retry on transient credential failures (IMDS temporarily unreachable)
            version = cls._retry_on_no_credentials(_download)
            print(f"Downloaded file from S3 with version {version} using boto3")
            return version

        # AWS CLI approach: use get-object for atomic metadata+content download
        s3_path_clean = str(s3_path).removeprefix("s3://")
        bucket, key = s3_path_clean.split("/", maxsplit=1)

        # Use get-object to atomically download file and get metadata
        # This prevents race condition where object changes between HEAD and GET
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        output = Shell.get_output(
            f"aws s3api get-object --bucket {bucket} --key {key} {local_path}",
            strict=True,
            verbose=True,
        )
        metadata = json.loads(output)
        version = int(metadata.get("Metadata", {}).get("version", "0"))

        print(f"Downloaded file from S3 with version {version} using AWS CLI")
        return version

    @classmethod
    def copy_file_to_s3_with_version(
        cls, s3_path, local_path, version, text=True, no_strict=False
    ):
        """
        Uploads a file to S3 with version tracking in object metadata.
        Uses conditional PUT with ETag matching for concurrent write safety (optimistic locking).

        IMPORTANT: Version 0 is a destructive reset operation that overwrites without conditions.
        It MUST NOT be called concurrently - only use version 0 for initialization or when you
        have exclusive access. For concurrent updates, always use version > 0 with the retry pattern:
        read current version, attempt to write version+1, retry on failure.

        :param s3_path: S3 path (with or without s3:// prefix)
        :param local_path: Local file path to upload
        :param version: Version number to set in metadata (0 = destructive reset, >0 = conditional update)
        :param text: Whether to set content-type as text/plain
        :param no_strict: Whether to suppress unexpected errors (returns False instead of raising exception)
        :return: True if successful, False if concurrent write detected (object was modified by another process).
                 On unexpected errors: raises exception if no_strict=False, returns False if no_strict=True.
        """
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"

        # Warn about version 0 usage
        if version == 0:
            print(
                "WARNING: Version 0 is a destructive reset operation - ensure no concurrent writes are happening"
            )

        # Use boto3 if available, otherwise AWS CLI
        if BOTO3_AVAILABLE and cls._get_boto3_client():
            s3_path_clean = str(s3_path).removeprefix("s3://")
            bucket, key = s3_path_clean.split("/", maxsplit=1)

            try:

                def _upload_versioned():
                    client = cls._get_boto3_client()
                    extra_args = {
                        "ContentType": (
                            "text/plain" if text else "application/octet-stream"
                        ),
                        "Metadata": {"version": str(version)},
                    }

                    if version == 0:
                        # DESTRUCTIVE: Version 0 overwrites without conditions (NOT safe for concurrent use)
                        print(
                            f"Uploading file with version 0 (destructive reset) using boto3"
                        )
                        client.upload_file(
                            str(local_path), bucket, key, ExtraArgs=extra_args
                        )
                    else:
                        # For version > 0, use conditional PUT with If-Match (ETag check)
                        # First get current ETag
                        head_response = client.head_object(Bucket=bucket, Key=key)
                        current_etag = head_response.get("ETag", "").strip('"')
                        current_version = int(
                            head_response.get("Metadata", {}).get("version", "0")
                        )

                        # Verify we're updating from expected version
                        if current_version != version - 1:
                            print(
                                f"Version mismatch: expected {version - 1}, found {current_version} (concurrent write detected)"
                            )
                            return False

                        # Stream file content for put_object (needed for If-Match)
                        with open(local_path, "rb") as f:
                            client.put_object(
                                Bucket=bucket,
                                Key=key,
                                Body=f,
                                ContentType=(
                                    "text/plain"
                                    if text
                                    else "application/octet-stream"
                                ),
                                Metadata={"version": str(version)},
                                IfMatch=current_etag,
                            )
                    return True

                # Retry on transient credential failures
                result = cls._retry_on_no_credentials(_upload_versioned)
                if result is False:
                    return False

                print(f"Uploaded file with version {version} using boto3")
                StorageUsage.add_uploaded(local_path)
                return True

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "PreconditionFailed":
                    print("Precondition failed (concurrent write detected)")
                    return False
                print(f"ERROR: Failed to upload file using boto3: {error_code}")
                if not no_strict:
                    raise
                return False
            except Exception as e:
                print(f"ERROR: Failed to upload file using boto3: {e}")
                if not no_strict:
                    raise
                return False

        # AWS CLI approach: single file with version in metadata
        s3_path_clean = str(s3_path).removeprefix("s3://")
        bucket, key = s3_path_clean.split("/", maxsplit=1)

        try:
            if version == 0:
                # DESTRUCTIVE: Version 0 uploads without conditions (NOT safe for concurrent use)
                print(
                    f"Uploading file with version 0 (destructive reset) using AWS CLI"
                )
                result_uploaded = cls.put(
                    s3_path=s3_path,
                    local_path=local_path,
                    metadata={"version": str(version)},
                    no_strict=no_strict,
                    text=text,
                )
                if not result_uploaded:
                    print("Failed to put file")
                    return False
            else:
                # For version > 0, use conditional PUT with If-Match (ETag check)
                # First get current ETag and verify version
                head_output = Shell.get_output(
                    f"aws s3api head-object --bucket {bucket} --key {key}",
                    strict=True,
                    verbose=True,
                )
                head_data = json.loads(head_output)
                current_etag = head_data.get("ETag", "").strip('"')
                current_version = int(head_data.get("Metadata", {}).get("version", "0"))

                # Verify we're updating from expected version
                if current_version != version - 1:
                    print(
                        f"Version mismatch: expected {version - 1}, found {current_version} (concurrent write detected)"
                    )
                    return False

                # Upload with If-Match condition
                content_type = "text/plain" if text else "application/octet-stream"
                command = f'aws s3api put-object --bucket {bucket} --key {key} --body {local_path} --content-type {content_type} --metadata version={version} --if-match "{current_etag}"'
                res = cls.run_command_with_retries(command, no_strict=no_strict)
                if not res:
                    print("Failed to put file (precondition failed or other error)")
                    return False

            print(f"Uploaded file with version {version} using AWS CLI")
            StorageUsage.add_uploaded(local_path)
            return True

        except Exception as e:
            print(f"ERROR: Failed to upload file using AWS CLI: {e}")
            if not no_strict:
                raise
            return False
