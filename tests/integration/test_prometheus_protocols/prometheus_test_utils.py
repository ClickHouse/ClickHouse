import http
import json
import math
import os
import requests
import snappy
import sys
import urllib
import zipfile


PRESETS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "presets")

sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), "pb2"))
import prompb.remote_pb2 as remote_pb2
import prompb.types_pb2 as types_pb2


# Converts time series data
# [ ({'label_name1': 'label_value1], ...}, {timestamp1: value1, ...} ), ... ]
# to a protobuf message of type remote_pb2.WriteRequest.
def convert_time_series_to_protobuf(time_series):
    write_request = remote_pb2.WriteRequest()
    for src_timeseries in time_series:
        (src_labels, src_samples) = src_timeseries
        dest_timeseries = types_pb2.TimeSeries()
        for label_name, label_value in src_labels.items():
            dest_timeseries.labels.append(
                types_pb2.Label(name=label_name, value=label_value)
            )
        for timestamp, value in src_samples.items():
            timestamp_ms = int(round(timestamp * 1000))
            dest_timeseries.samples.append(
                types_pb2.Sample(timestamp=timestamp_ms, value=value)
            )
        write_request.timeseries.append(dest_timeseries)
    return write_request


# Loads a preset from folder "presets". The function returns a protobuf message of type remote_pb2.WriteRequest.
def load_preset(preset_name):
    preset_fullname = os.path.join(PRESETS_DIR, preset_name)
    if preset_fullname.endswith(".zip"):
        with zipfile.ZipFile(preset_fullname, mode="r") as zip:
            with zip.open(
                os.path.splitext(os.path.basename(preset_fullname))[0] + ".json",
                mode="r",
            ) as file:
                return load_preset_from_file(file)
    else:
        with open(os.path.join(PRESETS_DIR, preset_name), "r") as file:
            return load_preset_from_file(file)


def load_preset_from_file(preset_file):
    write_request = remote_pb2.WriteRequest()
    data = json.load(preset_file)
    is_matrix = data["resultType"] == "matrix"
    for time_series in data["result"]:
        dest_timeseries = types_pb2.TimeSeries()
        for label_name, label_value in time_series["metric"].items():
            dest_timeseries.labels.append(
                types_pb2.Label(name=label_name, value=label_value)
            )
        if is_matrix:
            for timestamp, value in time_series["values"]:
                timestamp_ms = int(round(float(timestamp) * 1000))
                fvalue = float(value)
                dest_timeseries.samples.append(
                    types_pb2.Sample(timestamp=timestamp_ms, value=fvalue)
                )
        else:
            (timestamp, value) = time_series["value"]
            timestamp_ms = int(round(float(timestamp) * 1000))
            fvalue = float(value)
            dest_timeseries.samples.append(
                types_pb2.Sample(timestamp=timestamp_ms, value=fvalue)
            )
        write_request.timeseries.append(dest_timeseries)
    return write_request


# Sends a protobuf message of type remote_pb2.WriteRequest to specified host and port via the RemoteWrite protocol.
def send_protobuf_to_remote_write(host, port, path, write_request_proto):
    response = get_response_to_remote_write(host, port, path, write_request_proto)
    check_remote_write_response(response)


def get_response_to_remote_write(host, port, path, write_request_proto):
    url = f"http://{host}:{port}/{path.strip('/')}"
    print(f"Posting {url}")
    response = requests.post(
        url,
        data=snappy.compress(data=write_request_proto.SerializeToString()),
        headers={
            "Content-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "User-Agent": requests.utils.default_user_agent(),
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        },
    )
    print(
        f"Status code: {response.status_code} {http.HTTPStatus(response.status_code).phrase}"
    )
    return response


def check_remote_write_response(response):
    # The status code SHOULD be 204 HTTP No Content. (https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/#response)
    if response.status_code != requests.codes.no_content:
        print(f"Response: {response.text}")
        raise Exception(f"Got unexpected status code {response.status_code}")


# Prepares a protobuf of type remote_pb2.ReadRequest to read time series via the RemoteRead protocol.
def convert_read_request_to_protobuf(
    metric_name_regexp, start_timestamp, end_timestamp
):
    read_request = remote_pb2.ReadRequest()
    query = remote_pb2.Query()
    query.start_timestamp_ms = int(round(float(start_timestamp) * 1000))
    query.end_timestamp_ms = int(round(float(end_timestamp) * 1000))
    matcher = types_pb2.LabelMatcher()
    matcher.type = types_pb2.LabelMatcher.Type.RE
    matcher.name = "__name__"
    matcher.value = metric_name_regexp
    query.matchers.append(matcher)
    read_request.queries.append(query)
    return read_request


# Reads a protobuf message of type from specified host and port via the RemoteRead protocol.
def receive_protobuf_from_remote_read(host, port, path, read_request_proto):
    response = get_response_to_remote_read(host, port, path, read_request_proto)
    return extract_protobuf_from_remote_read_response(response)


def get_response_to_remote_read(host, port, path, read_request_proto):
    url = f"http://{host}:{port}/{path.strip('/')}"
    print(f"Posting {url}")
    response = requests.get(
        url,
        data=snappy.compress(data=read_request_proto.SerializeToString()),
        headers={
            "Content-Encoding": "snappy",
            "Accept-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "User-Agent": requests.utils.default_user_agent(),
            "X-Prometheus-Remote-Read-Version": "0.1.0",
        },
    )
    print(
        f"Status code: {response.status_code} {http.HTTPStatus(response.status_code).phrase}"
    )
    return response


def extract_protobuf_from_remote_read_response(response):
    if response.status_code != requests.codes.ok:
        print(f"Response: {response.text}")
        raise Exception(f"Got unexpected status code {response.status_code}")
    read_response = remote_pb2.ReadResponse()
    read_response.ParseFromString(snappy.decompress(response.content))
    return read_response


# Executes an instant query using Prometheus HTTP API.
def execute_query_via_http_api(
    host, port, path, query, timestamp=None, expect_error=False
):
    response = get_response_to_http_api_query(host, port, path, query, timestamp)
    if expect_error:
        return extract_error_from_http_api_response(response)
    return extract_data_from_http_api_response(response)


# Executes a range query using Prometheus HTTP API.
def execute_range_query_via_http_api(
    host, port, path, query, start_timestamp, end_timestamp, step, expect_error=False
):
    response = get_response_to_http_api_range_query(
        host, port, path, query, start_timestamp, end_timestamp, step
    )
    if expect_error:
        return extract_error_from_http_api_response(response)
    return extract_data_from_http_api_response(response)


def get_response_to_http_api_query(host, port, path, query, timestamp=None):
    escaped_query = urllib.parse.quote_plus(query, safe="")
    url = f"http://{host}:{port}/{path.strip('/')}?query={escaped_query}"
    if timestamp is not None:
        url += f"&time={timestamp}"
    return get_response_to_http_api(url)


def get_response_to_http_api_range_query(
    host, port, path, query, start_timestamp, end_timestamp, step
):
    escaped_query = urllib.parse.quote_plus(query, safe="")
    url = f"http://{host}:{port}/{path.strip('/')}?query={escaped_query}&start={start_timestamp}&end={end_timestamp}&step={step}"
    return get_response_to_http_api(url)


def get_response_to_http_api(url):
    print(f"Requesting {url}")
    response = requests.get(url)
    print(
        f"Status code: {response.status_code} {http.HTTPStatus(response.status_code).phrase}"
    )
    return response


def extract_data_from_http_api_response(response):
    if response.status_code != requests.codes.ok:
        print(f"Response: {response.text}")
        raise Exception(
            f"Got response with unexpected status code {response.status_code}: {response.text}"
        )
    response_json = response.json()
    status = response_json.get("status")
    if status != "success":
        print(f"Response: {response.text}")
        raise Exception(
            f"Got response with unexpected status {status}: {response.text}"
        )
    return json.dumps(response_json["data"])


def extract_error_from_http_api_response(response):
    if response.status_code == requests.codes.ok:
        print(f"Response: {response.text}")
        raise Exception(
            f"Expected an error but succeeded with status_code={response.status_code}: {response.text}"
        )
    response_json = response.json()
    status = response_json.get("status")
    if status != "error":
        raise Exception(f"Error response missing status=error: {response.text}")
    if "error" not in response_json:
        raise Exception(f"Error response missing 'error' field: {response.text}")
    return response_json["error"]


# Returns whether the differences between correspondent values of two HTTP API responses are not greater than `eps`.
# Also may return False if any response has unexpected structure.
def http_api_response_close_to(response1, response2, eps=0):
    if response1 == response2:
        return True

    if eps == 0:
        return False

    json1 = json.loads(response1)
    json2 = json.loads(response2)

    if (
        ("resultType" not in json1)
        or ("resultType" not in json2)
        or ("result" not in json1)
        or ("result" not in json2)
    ):
        return False

    result_type1 = json1["resultType"]
    result_type2 = json2["resultType"]
    result1 = json1["result"]
    result2 = json2["result"]

    if (
        (result_type1 != result_type2)
        or (not isinstance(result1, list))
        or (not isinstance(result2, list))
    ):
        return False

    if result_type1 == "scalar":
        return time_value_pair_close_to(result1, result2, eps=eps)

    if result_type1 == "vector":
        if len(result1) != len(result2):
            return False

        for i, ts1 in enumerate(result1):
            ts2 = result2[i]

            if (
                ("metric" not in ts1)
                or ("metric" not in ts2)
                or ("value" not in ts1)
                or ("value" not in ts2)
            ):
                return False

            labels1 = ts1["metric"]
            labels2 = ts2["metric"]
            time_value_pair1 = ts1["value"]
            time_value_pair2 = ts2["value"]

            if (labels1 != labels2) or (
                not time_value_pair_close_to(
                    time_value_pair1, time_value_pair2, eps=eps
                )
            ):
                return False

        return True

    if result_type1 == "matrix":
        if len(result1) != len(result2):
            return False

        for i, ts1 in enumerate(result1):
            ts2 = result2[i]

            if (
                ("metric" not in ts1)
                or ("metric" not in ts2)
                or ("values" not in ts1)
                or ("values" not in ts2)
            ):
                return False

            labels1 = ts1["metric"]
            labels2 = ts2["metric"]
            values1 = ts1["values"]
            values2 = ts2["values"]

            if (
                (labels1 != labels2)
                or (not isinstance(values1, list))
                or (not isinstance(values2, list))
                or (len(values1) != len(values2))
            ):
                return False

            for j, time_value_pair1 in enumerate(values1):
                time_value_pair2 = values2[j]
                if not time_value_pair_close_to(
                    time_value_pair1, time_value_pair2, eps=eps
                ):
                    return False

        return True

    # Unexpected result_type
    return False


def time_value_pair_close_to(time_value_pair1, time_value_pair2, eps):
    if (
        (not isinstance(time_value_pair1, list))
        or (not isinstance(time_value_pair2, list))
        or (len(time_value_pair1) != 2)
        or (len(time_value_pair2) != 2)
    ):
        # Response has unexpected structure
        return False

    timestamp1, value1 = time_value_pair1
    timestamp2, value2 = time_value_pair2

    if float(timestamp1) != float(timestamp2):
        return False
    if not value_close_to(float(value1), float(value2), eps):
        return False

    return True


def value_close_to(value1, value2, eps):
    if math.isfinite(value1):
        return math.isfinite(value2) and abs(float(value1) - float(value2)) <= eps
    elif math.isinf(value1):
        return math.isinf(value2) and (value1 > 0) == (value2 > 0)
    else:
        return math.isnan(value2)
