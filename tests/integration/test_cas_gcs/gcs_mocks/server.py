"""A deterministic in-memory fake of the Google Cloud Storage XML API.

The point of this fake is that a GCS *generation* and an S3 *ETag* are two disjoint token domains.
Every stored object therefore carries both, minted from independent sources that can never collide:

  - a generation is a 16-digit decimal number, so it never contains a letter;
  - an ETag always begins with the letter ``e``, so it can never parse as a generation.

A test that confused the two domains cannot pass here: the fake enforces preconditions in whichever
domain the request named, so a generation sent as an ordinary ``If-Match`` fails the ETag comparison
and an ETag sent as ``x-goog-if-generation-match`` fails the numeric parse.

Deliberately NOT supported. Each of these answers with an XML ``<Error>`` naming itself, so a failure
reads as "the fake refuses this" and never as a storage bug in ClickHouse:

  - multipart upload (``?uploads``, ``uploadId``, ``partNumber``);
  - object versioning (the versioning probe always answers "not enabled", and no noncurrent
    generation is ever retained);
  - server-side encryption, ACLs, lifecycle, requester-pays.

Control surface, reserved under the bucket name ``_control``:

  - ``GET  /_control/requests`` — the capture log as JSON: every request's method, bucket, key, query,
    headers, response status and the generation/ETag the response carried;
  - ``GET  /_control/minted`` — every generation and every ETag the fake has ever minted;
  - ``GET  /_control/counters`` — per-method request counts plus the counts of the two operations a
    token-producing write must never issue, ``CreateMultipartUpload`` and ``UploadPart``;
  - ``POST /_control/reset`` — drop the capture log and the counters (objects are kept);
  - ``POST /_control/mode?if_match=reject|ignore&omit_generation=0|1`` — select the adversarial
    behaviours below. Global, not per bucket: the client reuses connections across buckets and a
    per-bucket switch would invite a test to believe it had isolated something it had not.

Adversarial behaviours, each off by default:

  - ``if_match=ignore`` — a raw numeric ``If-Match`` is ignored instead of refused. The default
    ``reject`` answers ``400``. Neither mode is reachable by correct production code, which never puts
    a generation in the ETag domain; the pair exists to show what each kind of real service would do
    if it did, and therefore how much of the fixture's safety depends on the service being strict.
  - ``omit_generation=1`` — a successful object-write ``PUT`` answers without ``x-goog-generation``.
    A real GCS always sends one; this models the response a proxy or a future API version might
    return, which is the only input that can reach the "write succeeded but carried no valid
    generation" refusal in the CAS write path.

Usage: ``python3 server.py <port>``. Started by ``helpers.mock_servers.start_mock_servers``, which
probes ``GET /`` and expects the body ``OK``.
"""

import http.server
import json
import re
import sys
import threading
import time
import urllib.parse

# A generation looks like a real GCS one: 16 decimal digits. The stride is a prime rather than 1 so
# that a test cannot pass by predicting the next value from an ordinal.
_GENERATION_SEED = 1783078552147137
_GENERATION_STRIDE = 7919

_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

# Query parameters this service actually models. An unmodelled query key — `?acl`, `?lifecycle`,
# `?encryption`, `?requestPayment`, a subresource of any kind this fake has never heard of — is
# refused rather than silently treated as a listing or as an object write. An ALLOWLIST rather than a
# denylist on purpose: a denylist grows stale the moment the SDK learns a new subresource, and the
# failure mode of a stale denylist here is a half-served request that looks like a ClickHouse bug.
#
# The allowlist governs the query shape only. Two other shapes reach the same half-served failure
# without ever naming an unmodelled key, so each handler guards them separately: a MODELLED
# subresource addressed at the wrong level (`?tagging` on a bucket rather than an object), and a
# request carrying no object key at all where one is structurally required (`PUT /bucket`). Both are
# refused by the `_MODELLED_SUBRESOURCES`-remainder check and the keyless-write check in the handlers
# below.
_MODELLED_LIST_PARAMS = frozenset(
    {
        "list-type",
        "prefix",
        "delimiter",
        "max-keys",
        "marker",
        "continuation-token",
        "start-after",
        "encoding-type",
        "fetch-owner",
    }
)

# Subresources handled explicitly by their own branch in the handlers below.
_MODELLED_SUBRESOURCES = frozenset({"versioning", "location", "tagging", "delete", "uploads", "uploadId", "partNumber"})


def _unmodelled_params(query):
    """Query keys that are neither a modelled listing parameter nor a modelled subresource."""
    return sorted(set(query) - _MODELLED_LIST_PARAMS - _MODELLED_SUBRESOURCES)


def _unclaimed_subresources(query):
    """Modelled subresources still present after every branch that owns one has declined it.

    Each handler runs its explicit subresource branches first, each guarded to the level (bucket or
    object) the subresource exists at, then calls this. A non-empty result means the request named a
    subresource at a level no branch serves, which must be refused rather than falling through to the
    generic listing or object path below it.
    """
    return sorted(set(query) & _MODELLED_SUBRESOURCES)


_LOCK = threading.Lock()


class Store:
    """The whole mutable state, guarded by ``_LOCK``."""

    def __init__(self):
        self.objects = {}  # (bucket, key) -> dict(body, generation, etag, meta, tags, mtime)
        self.requests = []  # the capture log
        self.minted_generations = []
        self.minted_etags = []
        self.counters = {}
        # Adversarial behaviour, off by default — see the module docstring.
        self.if_match_mode = "reject"
        self.omit_generation = False
        self._next_generation = _GENERATION_SEED
        self._next_etag_ordinal = 1

    def count(self, name):
        self.counters[name] = self.counters.get(name, 0) + 1

    def mint_generation(self):
        value = str(self._next_generation)
        self._next_generation += _GENERATION_STRIDE
        self.minted_generations.append(value)
        return value

    def mint_etag(self):
        # Leading 'e' guarantees the value never parses as a generation.
        value = '"e%031x"' % (self._next_etag_ordinal * 0x9E3779B1)
        self._next_etag_ordinal += 1
        self.minted_etags.append(value)
        return value


STORE = Store()


def _xml_escape(text):
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _http_date(epoch):
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(epoch))


def _iso_date(epoch):
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(epoch))


def _error_xml(code, message):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Error><Code>{}</Code><Message>{}</Message></Error>".format(
            _xml_escape(code), _xml_escape(message)
        )
    ).encode()


class Reply:
    def __init__(self, status, body=b"", headers=None):
        self.status = status
        self.body = body if isinstance(body, bytes) else str(body).encode()
        self.headers = headers or {}


def _unsupported(what):
    return Reply(
        501,
        _error_xml(
            "NotImplemented",
            "This fake GCS XML service does not implement {}. It refuses the operation on purpose "
            "rather than serving it half-way.".format(what),
        ),
        {"Content-Type": "application/xml"},
    )


def _bad_request(message):
    return Reply(
        400, _error_xml("InvalidArgument", message), {"Content-Type": "application/xml"}
    )


def _precondition_failed(message):
    return Reply(
        412,
        _error_xml("PreconditionFailed", message),
        {"Content-Type": "application/xml"},
    )


def _no_such_key(key):
    return Reply(
        404,
        _error_xml("NoSuchKey", "No such object: {}".format(key)),
        {"Content-Type": "application/xml"},
    )


def _object_headers(entry):
    headers = {
        "ETag": entry["etag"],
        "x-goog-generation": entry["generation"],
        "x-goog-metageneration": "1",
        "x-goog-storage-class": "STANDARD",
        "Last-Modified": _http_date(entry["mtime"]),
        "Content-Type": entry.get("content_type", "application/octet-stream"),
    }
    # Only the Google prefix, exactly as real GCS answers: the AWS SDK parses no metadata out of
    # this response unless the native-conditional adapter maps the prefix first.
    for name, value in entry["meta"].items():
        headers["x-goog-meta-" + name] = value
    return headers


def _check_preconditions(entry, headers):
    """Enforce whichever token domain the request named. Returns a Reply on rejection, else None."""
    generation_match = headers.get("x-goog-if-generation-match")
    if_match = headers.get("if-match")
    if_none_match = headers.get("if-none-match")

    if generation_match is not None and (if_match is not None or if_none_match is not None):
        return _bad_request(
            "the request carries both a generation precondition ({}) and an ETag precondition "
            "(If-Match={!r}, If-None-Match={!r}); a translated request must carry only the "
            "generation form".format(generation_match, if_match, if_none_match)
        )

    if generation_match is not None:
        if not re.fullmatch(r"[0-9]+", generation_match):
            return _bad_request(
                "x-goog-if-generation-match is not a generation: {!r}. An ETag reached the "
                "generation domain.".format(generation_match)
            )
        if generation_match == "0":
            if entry is not None:
                return _precondition_failed(
                    "x-goog-if-generation-match: 0 but the object exists at generation "
                    + entry["generation"]
                )
        else:
            if entry is None:
                return _precondition_failed(
                    "x-goog-if-generation-match: {} but the object does not exist".format(
                        generation_match
                    )
                )
            if entry["generation"] != generation_match:
                return _precondition_failed(
                    "x-goog-if-generation-match: {} but the current generation is {}".format(
                        generation_match, entry["generation"]
                    )
                )
        return None

    if if_none_match is not None:
        if if_none_match.strip() == "*":
            if entry is not None:
                return _precondition_failed("If-None-Match: * but the object exists")
        elif entry is not None and if_none_match.strip().strip('"') == entry["etag"].strip('"'):
            return Reply(304)

    if if_match is not None:
        if re.fullmatch(r'"?[0-9]+"?', if_match.strip()):
            STORE.count("numeric_if_match")
            if STORE.if_match_mode == "ignore":
                # A permissive service: the precondition is dropped, so the request proceeds as if it
                # had carried none at all. This is the dangerous direction — the caller is told its
                # conditional operation succeeded conditionally when nothing was ever compared.
                return None
            return _bad_request(
                "If-Match is a generation, not an ETag: {!r}. A generation reached the ETag "
                "domain.".format(if_match)
            )
        if entry is None:
            return _precondition_failed("If-Match: {} but the object does not exist".format(if_match))
        if if_match.strip().strip('"') != entry["etag"].strip('"'):
            return _precondition_failed(
                "If-Match: {} but the current ETag is {}".format(if_match, entry["etag"])
            )

    return None


def _meta_from_request(headers):
    """Custom object metadata, taken from whichever prefix the request used."""
    meta = {}
    for name, value in headers.items():
        for prefix in ("x-goog-meta-", "x-amz-meta-"):
            if name.startswith(prefix):
                meta[name[len(prefix) :]] = value
    return meta


def _copy_source(headers):
    for name in ("x-goog-copy-source", "x-amz-copy-source"):
        if name in headers:
            return headers[name]
    return None


def _split_source(raw):
    source = urllib.parse.unquote(raw.lstrip("/"))
    if "/" not in source:
        return None, None
    bucket, key = source.split("/", 1)
    return bucket, key


def handle_put(bucket, key, query, headers, body):
    unmodelled = _unmodelled_params(query)
    if unmodelled:
        return _unsupported("PUT with the subresource/parameter " + ", ".join(unmodelled))
    if "uploadId" in query or "uploads" in query or "partNumber" in query:
        if "partNumber" in query:
            STORE.count("UploadPart")
        return _unsupported("multipart upload")
    if "tagging" in query and key:
        entry = STORE.objects.get((bucket, key))
        if entry is None:
            return _no_such_key(key)
        entry["tags"] = body.decode("utf-8", "replace")
        return Reply(200)
    if "versioning" in query:
        return _unsupported("changing the bucket versioning configuration")
    unclaimed = _unclaimed_subresources(query)
    if unclaimed:
        return _unsupported("PUT with the subresource " + ", ".join(unclaimed))
    if not key:
        # A keyless PUT is CreateBucket, which this fake does not model — its buckets are implicit.
        # Refused explicitly: falling through would mint an object at the empty key, and that phantom
        # would then appear in every later listing of the bucket.
        return _unsupported("creating a bucket")

    existing = STORE.objects.get((bucket, key))
    rejection = _check_preconditions(existing, headers)
    if rejection is not None:
        return rejection

    source = _copy_source(headers)
    if source is not None:
        source_bucket, source_key = _split_source(source)
        if source_bucket is None:
            return _bad_request("malformed copy source: {!r}".format(source))
        source_entry = STORE.objects.get((source_bucket, source_key))
        if source_entry is None:
            return _no_such_key(source_key)
        directive = headers.get(
            "x-goog-metadata-directive", headers.get("x-amz-metadata-directive", "COPY")
        )
        meta = (
            dict(source_entry["meta"]) if directive.upper() == "COPY" else _meta_from_request(headers)
        )
        entry = {
            "body": source_entry["body"],
            "generation": STORE.mint_generation(),
            "etag": STORE.mint_etag(),
            "meta": meta,
            "tags": "",
            "mtime": time.time(),
            "content_type": source_entry.get("content_type", "application/octet-stream"),
        }
        STORE.objects[(bucket, key)] = entry
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<CopyObjectResult xmlns="{}"><LastModified>{}</LastModified><ETag>{}</ETag>'
            "</CopyObjectResult>".format(
                _XMLNS, _iso_date(entry["mtime"]), _xml_escape(entry["etag"])
            )
        ).encode()
        return Reply(
            200,
            payload,
            {
                "Content-Type": "application/xml",
                "ETag": entry["etag"],
                "x-goog-generation": entry["generation"],
            },
        )

    entry = {
        "body": body,
        "generation": STORE.mint_generation(),
        "etag": STORE.mint_etag(),
        "meta": _meta_from_request(headers),
        "tags": "",
        "mtime": time.time(),
        "content_type": headers.get("content-type", "application/octet-stream"),
    }
    STORE.objects[(bucket, key)] = entry
    headers = {"ETag": entry["etag"]}
    if not STORE.omit_generation:
        headers["x-goog-generation"] = entry["generation"]
    return Reply(200, b"", headers)


def handle_delete(bucket, key, query, headers):
    if "uploadId" in query:
        return _unsupported("aborting a multipart upload")
    # DELETE was the last handler without this check, so `?acl` and friends used to delete the object
    # while the allowlist above promised refusal. Nothing CAS sends today carries an unmodelled
    # parameter on a DELETE, so this refuses nothing that currently happens — it is here so that a
    # future delete shape acquiring one is refused rather than silently served, which is the whole
    # point of the allowlist being an allowlist.
    unmodelled = _unmodelled_params(query)
    if unmodelled:
        return _unsupported("DELETE with the subresource/parameter " + ", ".join(unmodelled))
    unclaimed = _unclaimed_subresources(query)
    if unclaimed:
        return _unsupported("DELETE with the subresource " + ", ".join(unclaimed))
    if not key:
        # DeleteBucket, which this fake does not model. Refused rather than answered `NoSuchKey` for
        # the empty key, which would read as an object-level result for a bucket-level operation.
        return _unsupported("deleting a bucket")

    existing = STORE.objects.get((bucket, key))
    rejection = _check_preconditions(existing, headers)
    if rejection is not None:
        return rejection
    if existing is None:
        return _no_such_key(key)

    del STORE.objects[(bucket, key)]
    # No versioning, so no delete marker is ever created and no noncurrent generation is retained.
    return Reply(204, b"", {"x-goog-generation": existing["generation"]})


def handle_batch_delete(bucket, body):
    keys = re.findall(r"<Key>(.*?)</Key>", body.decode("utf-8", "replace"), re.S)
    parts = []
    for raw in keys:
        key = urllib.parse.unquote(raw)
        STORE.objects.pop((bucket, key), None)
        parts.append("<Deleted><Key>{}</Key></Deleted>".format(_xml_escape(key)))
    payload = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<DeleteResult xmlns="{}">{}</DeleteResult>'.format(_XMLNS, "".join(parts))
    ).encode()
    return Reply(200, payload, {"Content-Type": "application/xml"})


def handle_list(bucket, query):
    prefix = query.get("prefix", [""])[0]
    delimiter = query.get("delimiter", [""])[0]
    max_keys = int(query.get("max-keys", ["1000"])[0])
    after = query.get("continuation-token", query.get("marker", [""]))[0]
    if "start-after" in query and not after:
        after = query["start-after"][0]

    candidates = sorted(k for (b, k) in STORE.objects if b == bucket and k.startswith(prefix))
    if after:
        candidates = [k for k in candidates if k > after]

    contents = []
    common = []
    seen_prefixes = set()
    truncated = False
    last = ""
    for key in candidates:
        if delimiter:
            rest = key[len(prefix) :]
            if delimiter in rest:
                folder = prefix + rest.split(delimiter, 1)[0] + delimiter
                if folder in seen_prefixes:
                    continue
                if len(contents) + len(common) >= max_keys:
                    truncated = True
                    break
                seen_prefixes.add(folder)
                common.append(folder)
                last = key
                continue
        if len(contents) + len(common) >= max_keys:
            truncated = True
            break
        contents.append(key)
        last = key

    items = []
    for key in contents:
        entry = STORE.objects[(bucket, key)]
        items.append(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag>"
            "<Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>".format(
                _xml_escape(key),
                _iso_date(entry["mtime"]),
                _xml_escape(entry["etag"]),
                len(entry["body"]),
            )
        )
    for folder in common:
        items.append("<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>".format(_xml_escape(folder)))

    payload = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="{ns}"><Name>{bucket}</Name><Prefix>{prefix}</Prefix>'
        "<Delimiter>{delimiter}</Delimiter><MaxKeys>{max_keys}</MaxKeys>"
        "<KeyCount>{count}</KeyCount><IsTruncated>{truncated}</IsTruncated>"
        "{next_token}{items}</ListBucketResult>".format(
            ns=_XMLNS,
            bucket=_xml_escape(bucket),
            prefix=_xml_escape(prefix),
            delimiter=_xml_escape(delimiter),
            max_keys=max_keys,
            count=len(contents) + len(common),
            truncated="true" if truncated else "false",
            next_token=(
                "<NextContinuationToken>{k}</NextContinuationToken><NextMarker>{k}</NextMarker>".format(
                    k=_xml_escape(last)
                )
                if truncated
                else ""
            ),
            items="".join(items),
        )
    ).encode()
    return Reply(200, payload, {"Content-Type": "application/xml"})


def handle_get_or_head(bucket, key, query, headers):
    unmodelled = _unmodelled_params(query)
    if unmodelled:
        return _unsupported(
            "{} with the subresource/parameter {}".format(
                "GET/HEAD", ", ".join(unmodelled)
            )
        )
    if "versioning" in query and not key:
        # No versioning: an absent Status is what the AWS SDK reads as "not enabled".
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<VersioningConfiguration xmlns="{}"/>'.format(_XMLNS)
        ).encode()
        return Reply(200, payload, {"Content-Type": "application/xml"})
    if "uploads" in query or "uploadId" in query:
        return _unsupported("listing or completing a multipart upload")
    if "location" in query and not key:
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<LocationConstraint xmlns="{}">us-east-1</LocationConstraint>'.format(_XMLNS)
        ).encode()
        return Reply(200, payload, {"Content-Type": "application/xml"})
    if "tagging" in query and key:
        entry = STORE.objects.get((bucket, key))
        if entry is None:
            return _no_such_key(key)
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Tagging xmlns="{}"><TagSet/></Tagging>'.format(_XMLNS)
        ).encode()
        return Reply(200, payload, {"Content-Type": "application/xml"})
    # Before the bare-listing shortcut, not after it: `versioning` and `location` are bucket-level and
    # `tagging` is object-level, so a subresource that reached here named a level nothing serves. The
    # listing shortcut would answer `GET /bucket?tagging` with a full object listing.
    unclaimed = _unclaimed_subresources(query)
    if unclaimed:
        return _unsupported(
            "GET/HEAD of {} with the subresource {}".format(
                "an object" if key else "a bucket", ", ".join(unclaimed)
            )
        )
    if not key:
        return handle_list(bucket, query)

    entry = STORE.objects.get((bucket, key))
    rejection = _check_preconditions(entry, headers)
    if rejection is not None:
        return rejection
    if entry is None:
        return _no_such_key(key)

    body = entry["body"]
    out_headers = _object_headers(entry)
    status = 200
    byte_range = headers.get("range")
    if byte_range:
        match = re.fullmatch(r"bytes=(\d*)-(\d*)", byte_range.strip())
        if not match:
            return _bad_request("unsupported Range header: {!r}".format(byte_range))
        start = int(match.group(1)) if match.group(1) else 0
        end = int(match.group(2)) if match.group(2) else len(body) - 1
        if start >= len(body) and len(body):
            return Reply(
                416,
                _error_xml("InvalidRange", "range {} is past the object end".format(byte_range)),
                {"Content-Type": "application/xml"},
            )
        end = min(end, len(body) - 1)
        out_headers["Content-Range"] = "bytes {}-{}/{}".format(start, end, len(body))
        body = body[start : end + 1]
        status = 206

    # The body is always returned so that `Content-Length` is right; `_send` withholds the bytes
    # themselves for a HEAD.
    return Reply(status, body, out_headers)


def handle_control(path, method, query):
    if path == "/_control/counters":
        return Reply(
            200, json.dumps(STORE.counters).encode(), {"Content-Type": "application/json"}
        )
    if path == "/_control/mode" and method == "POST":
        if "if_match" in query:
            value = query["if_match"][0]
            if value not in ("reject", "ignore"):
                return _bad_request("unknown if_match mode " + value)
            STORE.if_match_mode = value
        if "omit_generation" in query:
            STORE.omit_generation = query["omit_generation"][0] == "1"
        return Reply(
            200,
            json.dumps(
                {"if_match": STORE.if_match_mode, "omit_generation": STORE.omit_generation}
            ).encode(),
            {"Content-Type": "application/json"},
        )
    if path == "/_control/requests":
        return Reply(
            200,
            json.dumps(STORE.requests).encode(),
            {"Content-Type": "application/json"},
        )
    if path == "/_control/minted":
        return Reply(
            200,
            json.dumps(
                {
                    "generations": STORE.minted_generations,
                    "etags": STORE.minted_etags,
                }
            ).encode(),
            {"Content-Type": "application/json"},
        )
    if path == "/_control/reset" and method == "POST":
        STORE.requests = []
        STORE.counters = {}
        return Reply(200, b"OK")
    return Reply(404, _error_xml("NoSuchControl", "unknown control path " + path))


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _read_body(self):
        # Always drain the body, even for a request we are about to reject: the client keeps the
        # connection alive and a leftover body would corrupt the next request on it.
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size_line = self.rfile.readline().strip()
                size = int(size_line.split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.readline()
            return b"".join(chunks)
        length = int(self.headers.get("Content-Length", 0) or 0)
        return self.rfile.read(length) if length else b""

    def _dispatch(self, method, want_body=True):
        body = self._read_body()
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        headers = {name.lower(): value for name, value in self.headers.items()}

        if path == "/":
            self._send(Reply(200, b"OK", {"Content-Type": "text/plain"}), want_body=True)
            return

        if path.startswith("/_control/"):
            with _LOCK:
                reply = handle_control(path, method, query)
            self._send(reply, want_body=True)
            return

        stripped = path.lstrip("/")
        bucket, _, key = stripped.partition("/")

        with _LOCK:
            STORE.count("method_" + method)
            if method == "PUT":
                reply = handle_put(bucket, key, query, headers, body)
            elif method == "DELETE":
                reply = handle_delete(bucket, key, query, headers)
            elif method == "POST":
                if "delete" in query:
                    STORE.count("DeleteObjects")
                    reply = handle_batch_delete(bucket, body)
                else:
                    # Counted even though refused: a test asserting that a token-producing write
                    # issued no CreateMultipartUpload needs the count to be recorded whatever the
                    # answer is, or the refusal itself would make the count zero and the assertion
                    # would pass for the wrong reason.
                    if "uploads" in query:
                        STORE.count("CreateMultipartUpload")
                    if "uploadId" in query:
                        STORE.count("CompleteMultipartUpload")
                    reply = _unsupported("POST " + parsed.query)
            elif method in ("GET", "HEAD"):
                reply = handle_get_or_head(bucket, key, query, headers)
            else:
                reply = _unsupported(method)

            STORE.requests.append(
                {
                    "seq": len(STORE.requests),
                    "method": method,
                    "bucket": bucket,
                    "key": key,
                    "query": parsed.query,
                    "headers": headers,
                    "status": reply.status,
                    "response_generation": reply.headers.get("x-goog-generation"),
                    "response_etag": reply.headers.get("ETag"),
                }
            )

        self._send(reply, want_body)

    def _send(self, reply, want_body):
        payload = reply.body if want_body else b""
        self.send_response(reply.status)
        for name, value in reply.headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(reply.body)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def do_GET(self):
        self._dispatch("GET", want_body=True)

    def do_HEAD(self):
        self._dispatch("HEAD", want_body=False)

    def do_PUT(self):
        self._dispatch("PUT", want_body=True)

    def do_POST(self):
        self._dispatch("POST", want_body=True)

    def do_DELETE(self):
        self._dispatch("DELETE", want_body=True)


def main():
    port = int(sys.argv[1])
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
