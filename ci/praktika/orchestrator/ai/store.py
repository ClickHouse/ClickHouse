"""Persistence backends for AI sessions.

A ``SessionStore`` is the thin IO layer underneath ``SessionManager`` — it knows
how to read/write JSON blobs and append to ordered event streams, nothing about
sessions or rounds. Two backends:

* ``LocalSessionStore`` — filesystem, used in local/dry-run mode and tests.
  Event streams are JSONL files (real appends).
* ``S3SessionStore`` — used in CI. S3 has no append, so each event in a stream
  is written as its own zero-padded object under a prefix and reassembled in
  order on read. Low-volume by design (a handful of turns per run).

Keys are POSIX-style relative paths, e.g.
``ai-sessions/<repo>/pr/<pr>/session.json``. The manager owns key layout; the
store just maps keys to files / S3 objects.
"""
import json
import os
from abc import ABC, abstractmethod

from praktika.settings import Settings


class SessionStore(ABC):
    @abstractmethod
    def read_json(self, key):
        """Return the parsed JSON at ``key``, or None if absent."""

    @abstractmethod
    def write_json(self, key, obj):
        """Write ``obj`` as JSON at ``key`` (overwrite)."""

    @abstractmethod
    def write_text(self, key, text):
        """Write raw text at ``key`` (overwrite)."""

    @abstractmethod
    def append_event(self, stream_key, record):
        """Append one record to the ordered stream at ``stream_key``."""

    @abstractmethod
    def read_events(self, stream_key):
        """Return all records of the stream at ``stream_key``, in order."""


class LocalSessionStore(SessionStore):
    """Filesystem backend. Streams are JSONL files (``<stream_key>.jsonl``)."""

    def __init__(self, root):
        self.root = root

    def _path(self, key):
        return os.path.join(self.root, key)

    def read_json(self, key):
        path = self._path(key)
        if not os.path.isfile(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_json(self, key, obj):
        self.write_text(key, json.dumps(obj, indent=2))

    def write_text(self, key, text):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def append_event(self, stream_key, record):
        path = self._path(stream_key + ".jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def read_events(self, stream_key):
        path = self._path(stream_key + ".jsonl")
        if not os.path.isfile(path):
            return []
        out = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out


class S3SessionStore(SessionStore):
    """S3 backend. Streams are prefixes of zero-padded per-event objects."""

    def __init__(self, bucket, client=None):
        self.bucket = bucket
        if client is None:
            import boto3

            region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            client = boto3.client("s3", region_name=region)
        self._s3 = client
        self._counts = {}  # stream_key -> next sequence number

    def read_json(self, key):
        try:
            obj = self._s3.get_object(Bucket=self.bucket, Key=key)
        except Exception:
            return None  # missing key or transient error — caller treats as absent
        return json.loads(obj["Body"].read())

    def write_json(self, key, obj):
        self.write_text(key, json.dumps(obj, indent=2))

    def write_text(self, key, text):
        self._s3.put_object(
            Bucket=self.bucket, Key=key, Body=text.encode("utf-8")
        )

    def _stream_keys(self, stream_key):
        prefix = stream_key + "/"
        keys = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self._s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                keys.append(item["Key"])
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return sorted(keys)  # zero-padded names => lexical order is event order

    def append_event(self, stream_key, record):
        if stream_key not in self._counts:
            self._counts[stream_key] = len(self._stream_keys(stream_key))
        seq = self._counts[stream_key]
        self._counts[stream_key] = seq + 1
        key = f"{stream_key}/{seq:06d}.json"
        self.write_text(key, json.dumps(record))

    def read_events(self, stream_key):
        out = []
        for key in self._stream_keys(stream_key):
            obj = self._s3.get_object(Bucket=self.bucket, Key=key)
            out.append(json.loads(obj["Body"].read()))
        return out


def make_store(local_mode):
    """Pick a backend: filesystem locally, S3 in CI.
    """
    if local_mode:
        # Keys already carry the "ai-sessions/" prefix (it's the S3 namespace,
        # where the bucket is the root), so the local root is just TEMP_DIR.
        return LocalSessionStore(Settings.TEMP_DIR)
    return S3SessionStore(Settings.S3_ARTIFACT_BUCKET)
