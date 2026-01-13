#!/usr/bin/env python3
import http.server
import json
import os
import re
import sys
from urllib.parse import quote
import requests


WAREHOUSE_BUCKET = "warehouse"
WAREHOUSE_PREFIX = "".strip("/")
GCS_PROJECT ="test-project"
STORAGE_EMULATOR_HOST = "http://gcs:4443".rstrip("/")

REQUESTS_VERIFY = False

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

REGISTERED_TABLES = {}


def make_prefix(*parts: str) -> str:
    cleaned = [p.strip("/").strip() for p in parts if p and p.strip("/").strip()]
    if WAREHOUSE_PREFIX:
        cleaned = [WAREHOUSE_PREFIX] + cleaned
    prefix = "/".join(cleaned)
    return (prefix + "/") if prefix else ""


def _json_api_list(bucket: str, prefix: str, delimiter: str = "/") -> dict:
    url = f"{STORAGE_EMULATOR_HOST}/storage/v1/b/{bucket}/o"
    params = {"prefix": prefix, "delimiter": delimiter}
    r = SESSION.get(url, params=params, timeout=10, verify=REQUESTS_VERIFY)
    r.raise_for_status()
    return r.json()


def _json_api_object_exists(bucket: str, object_name: str) -> bool:
    url = f"{STORAGE_EMULATOR_HOST}/storage/v1/b/{bucket}/o/{quote(object_name, safe='')}"
    r = SESSION.get(url, timeout=10, verify=REQUESTS_VERIFY)
    if r.status_code == 404:
        return False
    r.raise_for_status()
    return True


def _json_api_download_text(bucket: str, object_name: str) -> str:
    url = f"{STORAGE_EMULATOR_HOST}/download/storage/v1/b/{bucket}/o/{quote(object_name, safe='')}"
    r = SESSION.get(url, params={"alt": "media"}, timeout=20, verify=REQUESTS_VERIFY)
    if r.status_code == 404:
        raise FileNotFoundError(object_name)
    r.raise_for_status()
    return r.text


def list_child_prefixes(prefix: str) -> list[str]:
    data = _json_api_list(WAREHOUSE_BUCKET, prefix=prefix, delimiter="/")

    res = []
    for p in data.get("prefixes", []) or []:
        child = p[len(prefix):].strip("/").split("/", 1)[0]
        if child:
            res.append(child)

    return sorted(set(res))


def read_metadata(ns: str, table: str) -> dict:
    obj_path = make_prefix(ns, table).rstrip("/") + "/metadata.json"
    if not _json_api_object_exists(WAREHOUSE_BUCKET, obj_path):
        raise FileNotFoundError(obj_path)

    txt = _json_api_download_text(WAREHOUSE_BUCKET, obj_path)
    return json.loads(txt)


def metadata_location(ns: str, table: str) -> str:
    path = make_prefix(ns, table).rstrip("/") + "/metadata.json"
    path = path.lstrip("/")
    return f"gs://{WAREHOUSE_BUCKET}/{path}"


def json_response(obj) -> tuple[str, int, str]:
    return json.dumps(obj, ensure_ascii=False), 200, "application/json; charset=utf-8"


def error_response(code: int, message: str) -> tuple[str, int, str]:
    return json.dumps({"error": {"message": message}}, ensure_ascii=False), code, "application/json; charset=utf-8"


class RequestHandler(http.server.BaseHTTPRequestHandler):
    server_version = "MockIcebergRest/0.2"

    def read_post_body(self) -> dict:
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            return {}
        body = self.rfile.read(content_length).decode("utf-8")
        try:
            return json.loads(body) if body else {}
        except json.JSONDecodeError:
            return {}

    def get_response(self, method: str = "GET", post_body: dict = None) -> tuple[str, int, str]:
        path_with_query = self.path.split("?")
        path = path_with_query[0]
        query_string = path_with_query[1] if len(path_with_query) > 1 else ""
        query_params = {}
        if query_string:
            for param in query_string.split("&"):
                if "=" in param:
                    key, value = param.split("=", 1)
                    query_params[key] = value

        if path == "/":
            return "OK", 200, "text/plain"

        if path == "/v1/config":
            return json_response({"defaults": {}, "overrides": {}})

        if path == "/v1/namespaces" and method == "GET":
            from urllib.parse import unquote
            parent_ns = None
            if "parent" in query_params:
                parent_ns_str = unquote(query_params["parent"])
                try:
                    parent_ns = json.loads(parent_ns_str)
                    if isinstance(parent_ns, list):
                        parent_ns = ".".join(parent_ns) if parent_ns else ""
                    else:
                        parent_ns = str(parent_ns)
                except (json.JSONDecodeError, TypeError):
                    parent_ns = parent_ns_str
            
            if parent_ns:
                prefix = make_prefix(parent_ns)
            else:
                prefix = make_prefix()
            nss_from_gcs = list_child_prefixes(prefix)
            
            nss_from_registered = set()
            for (reg_ns, _) in REGISTERED_TABLES.keys():
                if isinstance(reg_ns, (list, tuple)):
                    ns_str = ".".join(reg_ns) if reg_ns else ""
                else:
                    ns_str = str(reg_ns)
                if not ns_str:
                    continue
                
                if parent_ns:
                    if ns_str == parent_ns:
                        continue
                    elif ns_str.startswith(parent_ns + "."):
                        remaining = ns_str[len(parent_ns) + 1:]
                        direct_child_name = remaining.split(".", 1)[0]
                        if parent_ns:
                            direct_child = parent_ns + "." + direct_child_name
                        else:
                            direct_child = direct_child_name
                        nss_from_registered.add(direct_child)
                else:
                    if "." not in ns_str:
                        nss_from_registered.add(ns_str)
                    else:
                        root_ns = ns_str.split(".", 1)[0]
                        nss_from_registered.add(root_ns)
            
            all_nss = sorted(set(nss_from_gcs) | nss_from_registered)
            return json_response({"namespaces": [[ns] for ns in all_nss]})

        if path == "/v1/namespaces" and method == "POST":
            if post_body is None:
                post_body = {}
            namespace = post_body.get("namespace", [])
            if not namespace:
                return error_response(400, "namespace is required")
            body = json.dumps({"namespace": namespace}, ensure_ascii=False)
            return body, 201, "application/json; charset=utf-8"

        m = re.fullmatch(r"/v1/namespaces/([^/]+)/tables", path)
        if m and method == "GET":
            from urllib.parse import unquote
            ns = unquote(m.group(1))
            tables_from_gcs = list_child_prefixes(make_prefix(ns))
            registered_tables = []
            for (reg_ns, table_name) in REGISTERED_TABLES.keys():
                if str(reg_ns) == ns:
                    registered_tables.append(table_name)
            all_tables = sorted(set(tables_from_gcs + registered_tables))
            if not registered_tables and REGISTERED_TABLES:
                debug_info = {str(k): v for k, v in REGISTERED_TABLES.items()}
                return json_response({
                    "identifiers": [{"namespace": [ns], "name": t} for t in all_tables],
                    "_debug": {"requested_ns": ns, "registered": debug_info}
                })
            return json_response(
                {"identifiers": [{"namespace": [ns], "name": t} for t in all_tables]}
            )

        m = re.fullmatch(r"/v1/namespaces/([^/]+)/register", path)
        if m and method == "POST":
            from urllib.parse import unquote
            ns = unquote(m.group(1))
            if post_body is None:
                post_body = {}
            metadata_location = post_body.get("metadata-location")
            if not metadata_location:
                return error_response(400, "metadata-location is required")
            table_name = query_params.get("name")
            if not table_name:
                table_name = post_body.get("name")
            if not table_name:
                table_name = "t"
            if metadata_location.startswith("gs://"):
                path_part = metadata_location[5:]
                if "/" in path_part:
                    bucket_name, object_path = path_part.split("/", 1)
                    try:
                        metadata_text = _json_api_download_text(bucket_name, object_path)
                        metadata_dict = json.loads(metadata_text)
                        REGISTERED_TABLES[(ns, table_name)] = metadata_location
                        return json_response({
                            "metadata-location": metadata_location,
                            "metadata": metadata_dict,
                            "config": {}
                        })
                    except FileNotFoundError:
                        return error_response(404, f"metadata file not found: {object_path}")
                    except json.JSONDecodeError as e:
                        return error_response(500, f"invalid metadata.json: {e}")
                    except requests.RequestException as e:
                        return error_response(502, f"storage backend error: {e}")
            return error_response(400, "invalid metadata-location format")

        m = re.fullmatch(r"/v1/namespaces/([^/]+)/tables", path)
        if m and method == "POST":
            ns = m.group(1)
            if post_body is None:
                post_body = {}
            table_name = post_body.get("name")
            metadata_location = post_body.get("metadata-location")
            if not table_name or not metadata_location:
                return error_response(400, "name and metadata-location are required")
            REGISTERED_TABLES[(ns, table_name)] = metadata_location
            return json_response({
                "metadata-location": metadata_location,
                "config": {}
            })

        m = re.fullmatch(r"/v1/namespaces/([^/]+)/tables/([^/]+)", path)
        if m:
            from urllib.parse import unquote
            ns = unquote(m.group(1))
            table = unquote(m.group(2))
            if (ns, table) in REGISTERED_TABLES:
                metadata_location = REGISTERED_TABLES[(ns, table)]
                if metadata_location.startswith("gs://"):
                    path_part = metadata_location[5:]
                    if "/" in path_part:
                        bucket_name, object_path = path_part.split("/", 1)
                        try:
                            metadata_text = _json_api_download_text(bucket_name, object_path)
                            metadata_dict = json.loads(metadata_text)
                            return json_response({
                                "metadata-location": metadata_location,
                                "metadata": metadata_dict
                            })
                        except (FileNotFoundError, json.JSONDecodeError, requests.RequestException) as e:
                            return error_response(404, f"failed to load metadata: {e}")
            try:
                md = read_metadata(ns, table)
            except FileNotFoundError as e:
                return error_response(404, f"metadata not found: {e.args[0]}")
            except json.JSONDecodeError as e:
                return error_response(500, f"invalid metadata.json: {e}")
            except requests.RequestException as e:
                return error_response(502, f"storage backend error: {e}")
            return json_response(
                {"metadata-location": metadata_location(ns, table), "metadata": md}
            )

        m = re.fullmatch(r"/v1/tables/([^/]+)", path)
        if m:
            ident = m.group(1)
            if "." not in ident:
                return error_response(400, "expected /v1/tables/{namespace}.{table}")
            ns, table = ident.split(".", 1)
            try:
                md = read_metadata(ns, table)
            except FileNotFoundError as e:
                return error_response(404, f"metadata not found: {e.args[0]}")
            except json.JSONDecodeError as e:
                return error_response(500, f"invalid metadata.json: {e}")
            except requests.RequestException as e:
                return error_response(502, f"storage backend error: {e}")
            return json_response(
                {"metadata-location": metadata_location(ns, table), "metadata": md}
            )

        return error_response(404, f"not found: {path}")

    def do_HEAD(self):
        body, code, ctype = self.get_response()

        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", len(body.encode("utf-8")))
        self.end_headers()
        return body, code

    def do_GET(self):
        body, code, ctype = self.get_response("GET")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", len(body.encode("utf-8")))
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def do_POST(self):
        post_body = self.read_post_body()
        body, code, ctype = self.get_response("POST", post_body)
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", len(body.encode("utf-8")))
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


port = int(sys.argv[1]) if len(sys.argv) > 1 else 8182
httpd = http.server.HTTPServer(("0.0.0.0", port), RequestHandler)
httpd.serve_forever()
