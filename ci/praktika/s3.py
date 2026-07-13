"""S3 file transfer abstraction.

Two backends:

* **Real S3** (``boto3``) — used in CI and any environment with AWS
  credentials. All API calls go through a single cached ``boto3`` client.

* **Local filesystem** — used when running outside CI (no AWS, no SSO).
  Every S3 path is mapped to ``Settings.TEMP_DIR/s3_local/<bucket>/<key>``
  and methods become file ops. Object metadata (e.g. version stamps used
  by ``copy_file_*_with_version``) are persisted in an adjacent
  ``<file>.s3meta.json`` file so the conditional-PUT semantics still work.

Local mode is selected when ``PRAKTIKA_LOCAL_RUN=1`` is set in the
environment OR when the dumped ``_Environment`` has ``LOCAL_RUN=True``.
The orchestrator and the dev ``Runner.generate_local_run_environment``
both set the env var, so a single signal flips the whole module without
code-site changes elsewhere.
"""
import dataclasses
import json
import mimetypes
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

from .settings import Settings
from .usage import StorageUsage
from .utils import Utils

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    ClientError = None
    NoCredentialsError = None


def _is_local_run() -> bool:
    """True when S3 calls should be redirected to the local filesystem.

    Cheap path: env var ``PRAKTIKA_LOCAL_RUN=1`` (set by the orchestrator
    when dispatching jobs locally and by ``Runner.generate_local_run_environment``).
    Fallback: read ``LOCAL_RUN`` directly from the dumped environment file
    so jobs that rebuild the env mid-run still see local mode.
    """
    if os.environ.get("PRAKTIKA_LOCAL_RUN") == "1":
        return True
    env_path = Path(Settings.TEMP_DIR) / "environment.json"
    if env_path.is_file():
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                return bool(json.load(f).get("LOCAL_RUN", False))
        except Exception:
            return False
    return False


def _local_root() -> Path:
    return Path(Settings.TEMP_DIR) / "s3_local"


def _to_local_path(s3_path) -> Path:
    """Map ``[s3://]bucket/key`` → ``TEMP_DIR/s3_local/bucket/key``."""
    cleaned = str(s3_path).removeprefix("s3://").lstrip("/")
    return _local_root() / cleaned


def _local_meta_path(local_path: Path) -> Path:
    return local_path.with_name(local_path.name + ".s3meta.json")


def _read_local_meta(local_path: Path) -> Dict[str, Any]:
    meta_path = _local_meta_path(local_path)
    if not meta_path.is_file():
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_local_meta(local_path: Path, meta: Dict[str, Any]) -> None:
    meta_path = _local_meta_path(local_path)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f)


def _file_url(local_path: Path) -> str:
    return quote(f"file://{local_path.absolute()}", safe=":/?&=")


class S3:
    _boto3_client = None

    @classmethod
    def _get_boto3_client(cls):
        # Honor Settings.AWS_PROFILE on developer machines so SSO / named
        # profiles work without having to export AWS_PROFILE in the shell.
        # On EC2 runners the configured profile typically isn't present in
        # ~/.aws/config (the runner authenticates via its instance role),
        # so probe `available_profiles` and only pin the profile when it
        # actually exists; otherwise fall through to the default credentials
        # chain.
        if not BOTO3_AVAILABLE:
            return None
        if cls._boto3_client is None:
            region = Settings.AWS_REGION or None
            try:
                if Settings.AWS_PROFILE:
                    import botocore.session

                    if (
                        Settings.AWS_PROFILE
                        in botocore.session.Session().available_profiles
                    ):
                        cls._boto3_client = boto3.Session(
                            profile_name=Settings.AWS_PROFILE,
                            region_name=region,
                        ).client("s3")
                    else:
                        cls._boto3_client = boto3.client("s3", region_name=region)
                else:
                    cls._boto3_client = boto3.client("s3", region_name=region)
            except Exception as e:
                print(f"WARNING: Failed to initialize boto3 client: {e}")
                return None
        return cls._boto3_client

    @classmethod
    def _ensure_boto3(cls):
        client = cls._get_boto3_client()
        assert (
            BOTO3_AVAILABLE and client is not None
        ), "boto3 is required for S3 operations: install with `pip install boto3`"
        return client

    @classmethod
    def _retry_on_no_credentials(cls, func, retries=3, delay=5):
        """Retry on ``NoCredentialsError`` — IMDS can be briefly unreachable
        right after a runner starts, so we drop the cached client and wait
        before retrying.
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

    # ---- public API ----------------------------------------------------

    @classmethod
    def clean_s3_directory(cls, s3_path, include=""):
        """Recursively delete everything under ``s3_path``."""
        assert len(str(s3_path).split("/")) > 2, "check to not delete too much"

        if _is_local_run():
            local = _to_local_path(s3_path)
            if not local.exists():
                return
            if include:
                for f in local.rglob(include):
                    if f.is_file():
                        f.unlink()
                        meta = _local_meta_path(f)
                        if meta.is_file():
                            meta.unlink()
            else:
                shutil.rmtree(local, ignore_errors=True)
            return

        client = cls._ensure_boto3()
        cleaned = str(s3_path).removeprefix("s3://")
        bucket, prefix = cleaned.split("/", maxsplit=1)
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = []
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if include and not Path(key).match(include):
                    continue
                keys.append({"Key": key})
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})

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

        if _is_local_run():
            dst = _to_local_path(s3_full_path)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_path, dst)
            meta = {}
            if content_type:
                meta["ContentType"] = content_type
            elif text:
                meta["ContentType"] = "text/plain"
            if content_encoding:
                meta["ContentEncoding"] = content_encoding
            if tags:
                meta["Tags"] = dict(tags)
            if meta:
                _write_local_meta(dst, meta)
            try:
                StorageUsage.add_uploaded(local_path)
            except Exception as e:
                print(f"WARNING: Failed to record upload usage for {local_path}: {e}")
            return _file_url(dst)

        try:
            s3_full_path_clean = str(s3_full_path).removeprefix("s3://")
            bucket, key = s3_full_path_clean.split("/", maxsplit=1)

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
                client = cls._ensure_boto3()
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

            cls._retry_on_no_credentials(_upload)
        except NoCredentialsError as e:
            print(f"ERROR: Failed to upload to S3 (no credentials): {e}")
            if not no_strict:
                raise
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            print(f"ERROR: Failed to upload to S3: {error_code}")
            if not no_strict:
                raise
        except Exception as e:
            print(f"ERROR: Failed to upload to S3: {e}")
            if not no_strict:
                raise

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
        """Direct PUT (boto3 ``put_object``). Supports conditional writes via
        ``if_none_matched`` and ``if_match``.
        """
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"

        s3_full_path = s3_path
        if s3_full_path.endswith("/"):
            s3_full_path = f"{s3_path}{Path(local_path).name}"

        if _is_local_run():
            dst = _to_local_path(s3_full_path)
            existed = dst.is_file()
            if if_none_matched and existed:
                if not no_strict:
                    raise RuntimeError(
                        f"Local S3 PUT precondition failed: object exists at [{dst}]"
                    )
                return False
            if if_match is not None:
                current_etag = _read_local_meta(dst).get("ETag", "")
                if current_etag != if_match:
                    if not no_strict:
                        raise RuntimeError(
                            f"Local S3 PUT precondition failed: ETag mismatch (expected {if_match}, got {current_etag})"
                        )
                    return False
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_path, dst)
            meta_payload: Dict[str, Any] = {"ETag": str(int(time.time() * 1000))}
            if metadata:
                meta_payload["Metadata"] = dict(metadata)
            if text:
                meta_payload["ContentType"] = "text/plain"
            _write_local_meta(dst, meta_payload)
            try:
                StorageUsage.add_uploaded(local_path)
            except Exception as e:
                print(f"WARNING: Failed to record upload usage for {local_path}: {e}")
            return True

        cleaned = str(s3_full_path).removeprefix("s3://")
        bucket, key = cleaned.split("/", maxsplit=1)

        try:
            def _put():
                client = cls._ensure_boto3()
                kwargs = {"Bucket": bucket, "Key": key}
                with open(local_path, "rb") as f:
                    kwargs["Body"] = f.read()
                if metadata:
                    kwargs["Metadata"] = {str(k): str(v) for k, v in metadata.items()}
                if text:
                    kwargs["ContentType"] = "text/plain"
                if if_none_matched:
                    kwargs["IfNoneMatch"] = "*"
                if if_match:
                    kwargs["IfMatch"] = if_match
                client.put_object(**kwargs)
                return True

            cls._retry_on_no_credentials(_put)
            try:
                StorageUsage.add_uploaded(local_path)
            except Exception as e:
                print(f"WARNING: Failed to record upload usage for {local_path}: {e}")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("PreconditionFailed", "ConditionalRequestConflict"):
                print("Precondition failed on PUT (conditional write)")
                return False
            print(f"ERROR: Failed to PUT to S3: {error_code}")
            if not no_strict:
                raise
            return False
        except Exception as e:
            print(f"ERROR: Failed to PUT to S3: {e}")
            if not no_strict:
                raise
            return False

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

        if _is_local_run():
            return cls._copy_from_local(
                s3_path,
                local_path,
                recursive=recursive,
                include_pattern=include_pattern,
                _skip_download_counter=_skip_download_counter,
                no_strict=no_strict,
            )

        if not recursive and not include_pattern:
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
                    client = cls._ensure_boto3()
                    client.download_file(bucket, key, str(local_file))

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

        # Recursive / pattern download via list_objects_v2
        try:
            client = cls._ensure_boto3()
            s3_path_clean = str(s3_path).removeprefix("s3://")
            bucket, prefix = s3_path_clean.split("/", maxsplit=1)
            assert Path(local_path).is_dir(), f"Path [{local_path}] is not a directory"

            paginator = client.get_paginator("list_objects_v2")
            downloaded_any = False
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    name = Path(key).name
                    if include_pattern and not Path(name).match(include_pattern):
                        continue
                    rel = key[len(prefix):].lstrip("/") if recursive else name
                    dest = Path(local_path) / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    client.download_file(bucket, key, str(dest))
                    if not _skip_download_counter:
                        StorageUsage.add_downloaded(dest)
                    downloaded_any = True
            return downloaded_any
        except Exception as e:
            print(f"ERROR: Failed to download from S3 [{s3_path}]: {e}")
            if not no_strict:
                raise
            return False

    @classmethod
    def _copy_from_local(
        cls,
        s3_path,
        local_path,
        recursive=False,
        include_pattern="",
        _skip_download_counter=False,
        no_strict=False,
    ):
        src = _to_local_path(s3_path)
        if recursive or include_pattern:
            assert Path(local_path).is_dir(), f"Path [{local_path}] is not a directory"
            if not src.is_dir():
                if not no_strict:
                    print(f"ERROR: Local S3 directory not found: {src}")
                return False
            pattern = include_pattern or "*"
            matched = False
            for f in src.rglob(pattern):
                if not f.is_file() or f.name.endswith(".s3meta.json"):
                    continue
                rel = f.relative_to(src) if recursive else Path(f.name)
                dest = Path(local_path) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(f, dest)
                if not _skip_download_counter:
                    StorageUsage.add_downloaded(dest)
                matched = True
            return matched

        if not src.is_file():
            if not no_strict:
                print(f"ERROR: Local S3 object not found: {src}")
            return False

        if Path(local_path).is_dir():
            dest = Path(local_path) / src.name
        else:
            dest = Path(local_path)
            assert dest.parent.is_dir(), f"Parent path for [{local_path}] does not exist"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
        if not _skip_download_counter:
            StorageUsage.add_downloaded(dest)
        return True

    @classmethod
    def copy_file_from_s3_matching_pattern(
        cls, s3_path, local_path, include, exclude="*", no_strict=False
    ):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"
        assert Path(
            local_path
        ).is_dir(), f"Path [{local_path}] does not exist or not a directory"
        assert s3_path.endswith("/"), f"s3 path is invalid [{s3_path}]"
        return cls.copy_file_from_s3(
            s3_path=s3_path,
            local_path=local_path,
            recursive=True,
            include_pattern=include,
            no_strict=no_strict,
        )

    @classmethod
    def head_object(cls, s3_path) -> Optional["S3.Object"]:
        if _is_local_run():
            local = _to_local_path(s3_path)
            if not local.is_file():
                return None
            meta = _read_local_meta(local)
            stat = local.stat()
            return cls.Object(
                AcceptRanges="bytes",
                Expiration="",
                LastModified=time.strftime(
                    "%a, %d %b %Y %H:%M:%S GMT", time.gmtime(stat.st_mtime)
                ),
                ContentLength=stat.st_size,
                ETag=meta.get("ETag", ""),
                ContentType=meta.get("ContentType", "application/octet-stream"),
                ServerSideEncryption="",
                Metadata=meta.get("Metadata", {}),
            )

        cleaned = str(s3_path).removeprefix("s3://")
        bucket, key = cleaned.split("/", maxsplit=1)
        try:
            client = cls._ensure_boto3()
            response = client.head_object(Bucket=bucket, Key=key)
            last_modified = response.get("LastModified")
            return cls.Object(
                AcceptRanges=response.get("AcceptRanges", ""),
                Expiration=response.get("Expiration", ""),
                LastModified=(
                    last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")
                    if last_modified is not None
                    else ""
                ),
                ContentLength=response.get("ContentLength", 0),
                ETag=response.get("ETag", "").strip('"'),
                ContentType=response.get("ContentType", ""),
                ServerSideEncryption=response.get("ServerSideEncryption", ""),
                Metadata=response.get("Metadata", {}) or {},
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    @classmethod
    def delete(cls, s3_path):
        assert Path(s3_path), f"Invalid S3 Path [{s3_path}]"

        if _is_local_run():
            local = _to_local_path(s3_path)
            ok = True
            if local.is_file():
                local.unlink()
            else:
                ok = False
            meta_path = _local_meta_path(local)
            if meta_path.is_file():
                meta_path.unlink()
            return ok

        cleaned = str(s3_path).removeprefix("s3://")
        bucket, key = cleaned.split("/", maxsplit=1)
        try:
            client = cls._ensure_boto3()
            client.delete_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            print(f"ERROR: Failed to delete S3 object [{s3_path}]: {e}")
            return False

    @classmethod
    def _upload_file_to_s3(
        cls, local_file_path, upload_to_s3: bool, text: bool = False, s3_subprefix=""
    ) -> str:
        if upload_to_s3:
            from ._environment import _Environment

            env = _Environment.get()
            s3_path = f"{Settings.S3_REPORT_BUCKET}/{env.get_s3_prefix()}"
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
        if _is_local_run():
            url = _file_url(_to_local_path(s3_path))
        else:
            endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[bucket]
            url = quote(f"https://{s3_path}".replace(bucket, endpoint), safe=":/?&=")

        with open(Settings.ARTIFACT_URLS_FILE, "w", encoding="utf-8") as f:
            json.dump({name: url}, f)

    @classmethod
    def upload_asset_streaming(cls, local_path: Path, s3_path: str):
        """Upload a single asset, gzipping HTML/CSS/JS/JSON/SVG/TXT in
        memory before the PUT.
        """
        import gzip as _gzip

        assert isinstance(local_path, Path)
        content_type, _ = mimetypes.guess_type(local_path)
        content_type = content_type or "application/octet-stream"

        compressible = {".html", ".css", ".js", ".json", ".svg", ".txt"}
        use_gzip = local_path.suffix.lower() in compressible

        if _is_local_run():
            dst = _to_local_path(s3_path)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if use_gzip:
                data = _gzip.compress(local_path.read_bytes(), compresslevel=8)
                dst.write_bytes(data)
                _write_local_meta(
                    dst,
                    {
                        "ContentType": content_type,
                        "ContentEncoding": "gzip",
                        "CacheControl": "max-age=604800, public",
                    },
                )
            else:
                shutil.copyfile(local_path, dst)
                _write_local_meta(
                    dst,
                    {
                        "ContentType": content_type,
                        "CacheControl": "max-age=604800, public",
                    },
                )
            return

        s3_path_clean = str(s3_path).removeprefix("s3://")
        bucket, key = s3_path_clean.split("/", maxsplit=1)
        extra_args = {
            "ContentType": content_type,
            "CacheControl": "max-age=604800, public",
        }
        if use_gzip:
            data = _gzip.compress(local_path.read_bytes(), compresslevel=8)
            extra_args["ContentEncoding"] = "gzip"

            def _upload():
                cls._ensure_boto3().put_object(
                    Bucket=bucket, Key=key, Body=data, **extra_args
                )
        else:
            def _upload():
                cls._ensure_boto3().upload_file(
                    str(local_path), bucket, key, ExtraArgs=extra_args
                )

        cls._retry_on_no_credentials(_upload)

    @classmethod
    def copy_file_from_s3_with_version(cls, s3_path, local_path):
        """Atomic GET + version-from-metadata.

        Returns the integer version stamp the writer set in the object's
        ``Metadata``. In local mode the version is read from the
        ``.s3meta.json`` sidecar.
        """
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        if _is_local_run():
            src = _to_local_path(s3_path)
            assert src.is_file(), f"Local S3 object not found: {src}"
            shutil.copyfile(src, local_path)
            meta = _read_local_meta(src)
            version = int((meta.get("Metadata") or {}).get("version", "0"))
            StorageUsage.add_downloaded(local_path)
            print(f"Downloaded file from local S3 with version {version}")
            return version

        s3_path_clean = str(s3_path).removeprefix("s3://")
        bucket, key = s3_path_clean.split("/", maxsplit=1)

        def _download():
            client = cls._ensure_boto3()
            response = client.get_object(Bucket=bucket, Key=key)
            version = int(response.get("Metadata", {}).get("version", "0"))
            with open(local_path, "wb") as f:
                f.write(response["Body"].read())
            return version

        version = cls._retry_on_no_credentials(_download)
        StorageUsage.add_downloaded(local_path)
        print(f"Downloaded file from S3 with version {version}")
        return version

    @classmethod
    def copy_file_to_s3_with_version(
        cls, s3_path, local_path, version, text=True, no_strict=False
    ):
        """Conditional PUT with optimistic locking.

        ``version=0`` is a destructive reset (no precondition) and must
        only be issued by code that has exclusive access to the object —
        Concurrent reset writers will silently clobber each other. For
        concurrent updates, callers always go ``read → version+1 → retry``.

        Returns ``True`` on success, ``False`` if a concurrent write was
        detected (precondition failed). Raises on unexpected errors unless
        ``no_strict=True``, in which case unexpected errors return ``False``.
        """
        assert Path(local_path).exists(), f"Path [{local_path}] does not exist"
        assert Path(
            local_path
        ).is_file(), f"Path [{local_path}] is not file. Only files are supported"

        if version == 0:
            print(
                "WARNING: Version 0 is a destructive reset operation - ensure no concurrent writes are happening"
            )

        if _is_local_run():
            return cls._put_versioned_local(s3_path, local_path, version, text=text)

        s3_path_clean = str(s3_path).removeprefix("s3://")
        bucket, key = s3_path_clean.split("/", maxsplit=1)

        try:
            def _upload_versioned():
                client = cls._ensure_boto3()
                content_type = "text/plain" if text else "application/octet-stream"

                if version == 0:
                    print("Uploading file with version 0 (destructive reset)")
                    client.upload_file(
                        str(local_path),
                        bucket,
                        key,
                        ExtraArgs={
                            "ContentType": content_type,
                            "Metadata": {"version": str(version)},
                        },
                    )
                    return True

                head_response = client.head_object(Bucket=bucket, Key=key)
                current_etag = head_response.get("ETag", "").strip('"')
                current_version = int(
                    head_response.get("Metadata", {}).get("version", "0")
                )
                if current_version != version - 1:
                    print(
                        f"Version mismatch: expected {version - 1}, found {current_version} (concurrent write detected)"
                    )
                    return False
                with open(local_path, "rb") as f:
                    client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=f,
                        ContentType=content_type,
                        Metadata={"version": str(version)},
                        IfMatch=current_etag,
                    )
                return True

            result = cls._retry_on_no_credentials(_upload_versioned)
            if result is False:
                return False
            print(f"Uploaded file with version {version}")
            StorageUsage.add_uploaded(local_path)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "PreconditionFailed":
                print("Precondition failed (concurrent write detected)")
                return False
            print(f"ERROR: Failed to upload versioned file: {error_code}")
            if not no_strict:
                raise
            return False
        except Exception as e:
            print(f"ERROR: Failed to upload versioned file: {e}")
            if not no_strict:
                raise
            return False

    @classmethod
    def _put_versioned_local(cls, s3_path, local_path, version, text=True):
        dst = _to_local_path(s3_path)
        if dst.name == "" or str(s3_path).endswith("/"):
            dst = dst / Path(local_path).name
        if version != 0 and dst.is_file():
            current_version = int(
                (_read_local_meta(dst).get("Metadata") or {}).get("version", "0")
            )
            if current_version != version - 1:
                print(
                    f"Version mismatch: expected {version - 1}, found {current_version} (concurrent write detected)"
                )
                return False

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, dst)
        meta = {
            "ContentType": "text/plain" if text else "application/octet-stream",
            "Metadata": {"version": str(version)},
            "ETag": str(int(time.time() * 1000)),
        }
        _write_local_meta(dst, meta)
        StorageUsage.add_uploaded(local_path)
        print(f"Uploaded file with version {version} (local)")
        return True
