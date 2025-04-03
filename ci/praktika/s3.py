import dataclasses
import json
import os
from pathlib import Path
from typing import Any, Dict
from urllib.parse import quote

from ._environment import _Environment
from .settings import Settings
from .utils import MetaClasses, Shell, Utils


@dataclasses.dataclass
class StorageUsage(MetaClasses.SerializableSingleton):
    downloaded: int = 0
    uploaded: int = 0
    downloaded_details: Dict[str, int] = dataclasses.field(default_factory=dict)
    uploaded_details: Dict[str, int] = dataclasses.field(default_factory=dict)
    ext: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def merge_with(self, storage_usage: "StorageUsage"):
        self.downloaded += storage_usage.downloaded
        self.uploaded += storage_usage.uploaded
        for k, v in storage_usage.downloaded_details.items():
            if k in self.downloaded_details:
                self.downloaded_details[k] += v
            else:
                self.downloaded_details[k] = v
        for k, v in storage_usage.uploaded_details.items():
            if k in self.uploaded_details:
                self.uploaded_details[k] += v
            else:
                self.uploaded_details[k] = v
        return self

    @classmethod
    def file_name_static(cls):
        return f"{Settings.TEMP_DIR}/storage_usage.json"

    @classmethod
    def _init(cls):
        if not StorageUsage.exist():
            print("NOTE: UsageStorage data will be initialized")
            StorageUsage(
                downloaded=0, uploaded=0, downloaded_details={}, uploaded_details={}
            ).dump()

    @classmethod
    def add_downloaded(cls, file_path):
        cls._init()
        if not Path(file_path).exists():
            return
        file_name = str(file_path).split("/")[-1]
        usage = cls.from_fs()
        file_zize = cls.get_size_bytes(file_path)
        usage.downloaded += file_zize
        if file_name in usage.downloaded_details:
            print(f"WARNING: Duplicated download for filename [{file_name}]")
            usage.downloaded_details[file_name] += file_zize
        else:
            usage.downloaded_details[file_name] = file_zize
        usage.dump()

    @classmethod
    def add_uploaded(cls, file_path):
        cls._init()
        if not Path(file_path).exists():
            return
        file_name = str(file_path).split("/")[-1]
        usage = cls.from_fs()
        file_zize = cls.get_size_bytes(file_path)
        usage.uploaded += file_zize
        if file_name in usage.uploaded_details:
            print(f"WARNING: Duplicated upload for filename [{file_name}]")
            usage.uploaded_details[file_name] += file_zize
        else:
            usage.uploaded_details[file_name] = file_zize
        usage.dump()

    @classmethod
    def get_size_bytes(cls, file_path):
        return os.path.getsize(file_path)


class S3:
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
        cls.run_command_with_retries(cmd, retries=1)
        return

    @classmethod
    def copy_file_to_s3(cls, s3_path, local_path, text=False, with_rename=False):
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
        if text:
            cmd += " --content-type text/plain"
        res = cls.run_command_with_retries(cmd)
        if not res:
            raise RuntimeError()
        StorageUsage.add_uploaded(local_path)
        bucket = s3_path.split("/")[0]
        endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[bucket]
        assert endpoint
        return quote(f"https://{s3_full_path}".replace(bucket, endpoint), safe=":/?&=")

    @classmethod
    def put(cls, s3_path, local_path, text=False, metadata=None, if_none_matched=False):
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
        res = cls.run_command_with_retries(command)
        if res:
            StorageUsage.add_uploaded(local_path)
        return res

    @classmethod
    def run_command_with_retries(cls, command, retries=Settings.MAX_RETRIES_S3):
        i = 0
        res = False
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
            res = ret_code == 0
        return res

    @classmethod
    def copy_file_from_s3(
        cls,
        s3_path,
        local_path,
        recursive=False,
        include_pattern="",
        _skip_download_counter=False,
    ):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
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
        res = cls.run_command_with_retries(cmd)
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
        cls, s3_path, local_path, include, exclude="*"
    ):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_dir(), f"Path [{local_path}] does not exist or not a directory"
        assert s3_path.endswith("/"), f"s3 path is invalid [{s3_path}]"
        cmd = f'aws s3 cp s3://{s3_path}  {local_path} --exclude "{exclude}" --include "{include}" --recursive'
        res = cls.run_command_with_retries(cmd)
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
