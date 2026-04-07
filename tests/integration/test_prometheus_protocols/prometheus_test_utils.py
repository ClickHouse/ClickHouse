import http
import json
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
def execute_query_via_http_api(host, port, path, query, timestamp=None, expect_error=False):
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
        raise Exception(f"Got unexpected status code {response.status_code}")
    response_json = response.json()
    status = response_json["status"] if "status" in response_json else ""
    if status != "success":
        print(f"Response: {response.text}")
        raise Exception(f"Got response with unexpected status: {status}")
    return json.dumps(response_json["data"])


def extract_error_from_http_api_response(response):
    if response.status_code == requests.codes.ok:
        print(f"Response: {response.text}")
        raise Exception(f"Expected an error but succeeded with response {response.text}")
    return response.text
