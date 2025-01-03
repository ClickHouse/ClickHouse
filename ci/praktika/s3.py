import dataclasses
import json
import time
from pathlib import Path
from typing import Dict

from praktika._environment import _Environment
from praktika.settings import Settings
from praktika.utils import Shell, Utils


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
    def clean_s3_directory(cls, s3_path):
        assert len(s3_path.split("/")) > 2, "check to not delete too much"
        cmd = f"aws s3 rm s3://{s3_path} --recursive"
        cls.run_command_with_retries(cmd, retries=1)
        return

    @classmethod
    def copy_file_to_s3(cls, s3_path, local_path, text=False):
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"
        file_name = Path(local_path).name
        s3_full_path = s3_path
        if not s3_full_path.endswith(file_name):
            s3_full_path = f"{s3_path}/{Path(local_path).name}"
        cmd = f"aws s3 cp {local_path} s3://{s3_full_path}"
        if text:
            cmd += " --content-type text/plain"
        res = cls.run_command_with_retries(cmd)
        if not res:
            raise
        bucket = s3_path.split("/")[0]
        endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[bucket]
        assert endpoint
        return f"https://{s3_full_path}".replace(bucket, endpoint)

    @classmethod
    def put(cls, s3_path, local_path, text=False, metadata=None):
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"
        file_name = Path(local_path).name
        s3_full_path = s3_path
        if not s3_full_path.endswith(file_name):
            s3_full_path = f"{s3_path}/{Path(local_path).name}"

        s3_full_path = str(s3_full_path).removeprefix("s3://")
        bucket, key = s3_full_path.split("/", maxsplit=1)

        command = (
            f"aws s3api put-object --bucket {bucket} --key {key} --body {local_path}"
        )
        if metadata:
            for k, v in metadata.items():
                command += f" --metadata {k}={v}"

        cmd = f"aws s3 cp {local_path} s3://{s3_full_path}"
        if text:
            cmd += " --content-type text/plain"
        res = cls.run_command_with_retries(command)
        assert res

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
            if ret_code != 0:
                print(
                    f"ERROR: aws s3 cp failed, stdout/stderr err: [{stderr}], out [{stdout}]"
                )
            res = ret_code == 0
        return res

    @classmethod
    def get_link(cls, s3_path, local_path):
        s3_full_path = f"{s3_path}/{Path(local_path).name}"
        bucket = s3_path.split("/")[0]
        endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[bucket]
        return f"https://{s3_full_path}".replace(bucket, endpoint)

    @classmethod
    def copy_file_from_s3(cls, s3_path, local_path):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        if Path(local_path).is_dir():
            local_path = Path(local_path) / Path(s3_path).name
        else:
            assert Path(
                local_path
            ).parent.is_dir(), f"Parent path for [{local_path}] does not exist"
        cmd = f"aws s3 cp s3://{s3_path}  {local_path}"
        res = cls.run_command_with_retries(cmd)
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

    # TODO: apparently should be placed into separate file to be used only inside praktika
    #   keeping this module clean from importing Settings, Environment and etc, making it easy for use externally
    @classmethod
    def copy_result_to_s3(cls, result, unlock=True):
        result.dump()
        env = _Environment.get()
        s3_path = f"{Settings.HTML_S3_PATH}/{env.get_s3_prefix()}"
        s3_path_full = f"{s3_path}/{Path(result.file_name()).name}"
        url = S3.copy_file_to_s3(s3_path=s3_path, local_path=result.file_name())
        if env.PR_NUMBER:
            print("Duplicate Result for latest commit alias in PR")
            s3_path = f"{Settings.HTML_S3_PATH}/{env.get_s3_prefix(latest=True)}"
            url = S3.copy_file_to_s3(s3_path=s3_path, local_path=result.file_name())
        if unlock:
            if not cls.unlock(s3_path_full):
                print(f"ERROR: File [{s3_path_full}] unlock failure")
                assert False  # TODO: investigate
        return url

    @classmethod
    def copy_result_from_s3(cls, local_path, lock=True):
        env = _Environment.get()
        file_name = Path(local_path).name
        s3_path = f"{Settings.HTML_S3_PATH}/{env.get_s3_prefix()}/{file_name}"
        if lock:
            cls.lock(s3_path)
        if not S3.copy_file_from_s3(s3_path=s3_path, local_path=local_path):
            print(f"ERROR: failed to cp file [{s3_path}] from s3")
            raise

    @classmethod
    def lock(cls, s3_path, level=0):
        assert level < 3, "Never"
        env = _Environment.get()
        s3_path_lock = s3_path + f".lock"
        file_path_lock = f"{Settings.TEMP_DIR}/{Path(s3_path_lock).name}"
        assert Shell.check(
            f"echo '''{env.JOB_NAME}''' > {file_path_lock}", verbose=True
        ), "Never"

        i = 20
        meta = S3.head_object(s3_path_lock)
        while meta:
            print(f"WARNING: Failed to acquire lock, meta [{meta}] - wait")
            i -= 5
            if i < 0:
                info = f"ERROR: lock acquire failure - unlock forcefully"
                print(info)
                env.add_info(info)
                break
            time.sleep(5)

        metadata = {"job": Utils.to_base64(env.JOB_NAME)}
        S3.put(
            s3_path=s3_path_lock,
            local_path=file_path_lock,
            metadata=metadata,
        )
        time.sleep(1)
        obj = S3.head_object(s3_path_lock)
        if not obj or not obj.has_tags(tags=metadata):
            print(f"WARNING: locked by another job [{obj}]")
            env.add_info("S3 lock file failure")
            cls.lock(s3_path, level=level + 1)
        print("INFO: lock acquired")

    @classmethod
    def unlock(cls, s3_path):
        s3_path_lock = s3_path + ".lock"
        env = _Environment.get()
        obj = S3.head_object(s3_path_lock)
        if not obj:
            print("ERROR: lock file is removed")
            assert False  # investigate
        elif not obj.has_tags({"job": Utils.to_base64(env.JOB_NAME)}):
            print("ERROR: lock file was acquired by another job")
            assert False  # investigate

        if not S3.delete(s3_path_lock):
            print(f"ERROR: File [{s3_path_lock}] delete failure")
        print("INFO: lock released")
        return True

    @classmethod
    def get_result_link(cls, result):
        env = _Environment.get()
        s3_path = f"{Settings.HTML_S3_PATH}/{env.get_s3_prefix(latest=True if env.PR_NUMBER else False)}"
        return S3.get_link(s3_path=s3_path, local_path=result.file_name())

    @classmethod
    def clean_latest_result(cls):
        env = _Environment.get()
        env.SHA = "latest"
        assert env.PR_NUMBER
        s3_path = f"{Settings.HTML_S3_PATH}/{env.get_s3_prefix()}"
        S3.clean_s3_directory(s3_path=s3_path)

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
            html_link = S3.copy_file_to_s3(
                s3_path=s3_path, local_path=local_file_path, text=text
            )
            return html_link
        return f"file://{Path(local_file_path).absolute()}"

    @classmethod
    def upload_result_files_to_s3(cls, result):
        if result.results:
            for result_ in result.results:
                cls.upload_result_files_to_s3(result_)
        for file in result.files:
            if not Path(file).is_file():
                print(f"ERROR: Invalid file [{file}] in [{result.name}] - skip upload")
                result.info += f"\nWARNING: Result file [{file}] was not found"
                file_link = cls._upload_file_to_s3(file, upload_to_s3=False)
            else:
                is_text = False
                for text_file_suffix in Settings.TEXT_CONTENT_EXTENSIONS:
                    if file.endswith(text_file_suffix):
                        print(
                            f"File [{file}] matches Settings.TEXT_CONTENT_EXTENSIONS [{Settings.TEXT_CONTENT_EXTENSIONS}] - add text attribute for s3 object"
                        )
                        is_text = True
                        break
                file_link = cls._upload_file_to_s3(
                    file,
                    upload_to_s3=True,
                    text=is_text,
                    s3_subprefix=Utils.normalize_string(result.name),
                )
            result.links.append(file_link)
        if result.files:
            print(
                f"Result files [{result.files}] uploaded to s3 [{result.links[-len(result.files):]}] - clean files list"
            )
            result.files = []
        result.dump()
