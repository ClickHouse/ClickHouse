import pytest

from helpers.cluster import ClickHouseCluster
from http import HTTPStatus
import requests
import snappy


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def execute_remote_write_impl(host, port, path, body):
    if not path.startswith("/"):
        path += "/"
    url = f"http://{host}:{port}/{path.strip('/')}"
    print(f"Posting {url}")
    r = requests.post(
        url,
        data=snappy.compress(data=body),
        headers={
            "Content-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "User-Agent": requests.utils.default_user_agent(),
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        },
    )
    print(f"Status code: {r.status_code} {HTTPStatus(r.status_code).phrase}")
    if r.status_code != requests.codes.no_content:
        print(f"Response: {r.text}")
        raise Exception(f"Got unexpected status code {r.status_code}")
    return r.text


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS prometheus SYNC")


def test_read_auth():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    def get(path):
        headers = {
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
        }
        return requests.request(
            url=f"http://{node.ip_address}:{cluster.prometheus_remote_read_handler_port}{path}",
            method="GET",
            headers=headers,
        )

    auth_ok = get("/read_auth_ok")
    # FIXME: prometheus read handler requires proper payload with snappy
    # compression, but it will first try to authenticate and only after try to
    # interpret the payload, so those two lines below is a workaround to ensure
    # that the authentication works
    assert auth_ok.status_code == 500
    assert "DB::Exception: snappy uncomress failed" in auth_ok.text

    assert get("/read_auth_fail").status_code == 403


def test_remote_write_v1_status_code():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    # send a remote write v1 request created from the following object:
    # request := prompb.WriteRequest{
    # 	Metadata: []prompb.MetricMetadata{
    # 		{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: "time", Unit: "ms", Help: "Help Message"},
    # 	},
    # 	Timeseries: []prompb.TimeSeries{
    # 		{Labels: []prompb.Label{
    # 			{Name: "__name__", Value: "time"},
    # 			{Name: "region", Value: "us-east-1"},
    # 		}, Samples: []prompb.Sample{
    # 			{Value: 33.0, Timestamp: time.Now().Unix()},
    # 		}},
    # 	},
    # }
    # buf, _ := request.Marshal()
    # hex.EncodeToString(buf)
    body = bytes.fromhex(
        "0a380a100a085f5f6e616d655f5f120474696d650a130a06726567696f6e120975732d656173742d31120f09000000000080404010afdcb5bc061a1a0802120474696d65220c48656c70204d6573736167652a026d73"
    )
    execute_remote_write_impl(
        node.ip_address,
        cluster.prometheus_remote_write_handler_port,
        cluster.prometheus_remote_write_handler_path,
        body,
    )
