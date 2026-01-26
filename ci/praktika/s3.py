import dataclasses
import json
import os
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
        cmd = f"aws s3 cp {local_path} s3://{s3_full_path}"
        if text and not content_type:
            cmd += " --content-type text/plain"
        elif content_type:
            cmd += f" --content-type {content_type}"
        if content_encoding:
            cmd += f" --content-encoding {content_encoding}"
        _ = cls.run_command_with_retries(cmd, no_strict=no_strict)
        try:
            StorageUsage.add_uploaded(local_path)
        except Exception as e:
            pass
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
        no_strict=False,
    ):
        """
        puts object via API PUT request
        :param s3_path:
        :param local_path:
        :param text:
        :param metadata:
        :param if_none_matched:
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
            client = cls._get_boto3_client()
            if client:
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

                    client.download_file(bucket, key, str(local_file))

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
                s3_subprefix.removeprefix("/").removesuffix("/")
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
