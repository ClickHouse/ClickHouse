import contextlib
import os
import urllib.error
import urllib.parse
import urllib.request

from helpers.cluster import ClickHouseCluster


class SimpleCluster:
    def close(self):
        self.cluster.shutdown()

    def __init__(self, cluster, name, config_dir):
        self.cluster = cluster
        self.instance = self.add_instance(name, config_dir)
        cluster.start()

    def add_instance(self, name, config_dir):
        return self.cluster.add_instance(
            name,
            main_configs=[os.path.join(config_dir, "config.xml")],
            user_configs=["users.d/users.yaml"],
        )


def test_dynamic_query_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "dynamic_handler", "test_dynamic_handler"
        )
    ) as cluster:
        test_query = urllib.parse.quote_plus(
            "SELECT * FROM system.settings WHERE name = 'max_threads'"
        )

        assert (
            404
            == cluster.instance.http_request(
                "?max_threads=1", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_dynamic_handler_get?max_threads=1",
                method="POST",
                headers={"XXX": "xxx"},
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_dynamic_handler_get?max_threads=1",
                method="GET",
                headers={"XXX": "bad"},
            ).status_code
        )

        assert (
            400
            == cluster.instance.http_request(
                "test_dynamic_handler_get?max_threads=1",
                method="GET",
                headers={"XXX": "xxx"},
            ).status_code
        )

        res_default = cluster.instance.http_request(
            "test_dynamic_handler_get?max_threads=1&get_dynamic_handler_query="
            + test_query,
            method="GET",
            headers={"XXX": "xxx"},
        )
        assert 200 == res_default.status_code
        assert (
            "text/tab-separated-values; charset=UTF-8"
            == res_default.headers["content-type"]
        )

        res_custom_ct = cluster.instance.http_request(
            "test_dynamic_handler_get_custom_content_type?max_threads=1&get_dynamic_handler_query="
            + test_query,
            method="GET",
            headers={"XXX": "xxx"},
        )
        assert 200 == res_custom_ct.status_code
        assert (
            "application/whatever; charset=cp1337"
            == res_custom_ct.headers["content-type"]
        )
        assert "it works" == res_custom_ct.headers["X-Test-Http-Response-Headers-Works"]
        assert (
            "also works"
            == res_custom_ct.headers["X-Test-Http-Response-Headers-Even-Multiple"]
        )

        assert (
            cluster.instance.http_request(
                "test_dynamic_handler_auth_with_password?query=select+currentUser()"
            )
            .content.strip()
            .decode()
            == "with_password"
        )
        assert (
            cluster.instance.http_request(
                "test_dynamic_handler_auth_with_password_fail?query=select+currentUser()"
            ).status_code
            == 403
        )
        assert (
            cluster.instance.http_request(
                "test_dynamic_handler_auth_without_password?query=select+currentUser()"
            )
            .content.strip()
            .decode()
            == "without_password"
        )


def test_predefined_query_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "predefined_handler", "test_predefined_handler"
        )
    ) as cluster:
        assert (
            404
            == cluster.instance.http_request(
                "?max_threads=1", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_predefined_handler_get?max_threads=1",
                method="GET",
                headers={"XXX": "bad"},
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_predefined_handler_get?max_threads=1",
                method="POST",
                headers={"XXX": "xxx"},
            ).status_code
        )

        assert (
            500
            == cluster.instance.http_request(
                "test_predefined_handler_get?max_threads=1",
                method="GET",
                headers={"XXX": "xxx"},
            ).status_code
        )

        res1 = cluster.instance.http_request(
            "test_predefined_handler_get?max_threads=1&setting_name=max_threads",
            method="GET",
            headers={"XXX": "xxx"},
        )
        assert b"max_threads\t1\n" == res1.content
        assert (
            "text/tab-separated-values; charset=UTF-8" == res1.headers["content-type"]
        )

        res2 = cluster.instance.http_request(
            "query_param_with_url/max_threads?max_threads=1&max_final_threads=1",
            headers={"XXX": "max_final_threads"},
        )
        assert b"max_final_threads\t1\nmax_threads\t1\n" == res2.content
        assert "application/generic+one" == res2.headers["content-type"]
        assert "it works" == res2.headers["X-Test-Http-Response-Headers-Works"]
        assert (
            "also works" == res2.headers["X-Test-Http-Response-Headers-Even-Multiple"]
        )

        cluster.instance.query(
            "CREATE TABLE test_table (id UInt32, data String) Engine=TinyLog"
        )
        res3 = cluster.instance.http_request(
            "test_predefined_handler_post_body?id=100",
            method="POST",
            data="TEST".encode("utf8"),
        )
        assert res3.status_code == 200
        assert cluster.instance.query("SELECT * FROM test_table") == "100\tTEST\n"
        cluster.instance.query("DROP TABLE test_table")

        cluster.instance.http_request(
            "test_predefined_handler_get?max_threads=1&param_setting_name=max_threads",
            method="GET",
            headers={"XXX": "xxx"},
        )
        assert b"max_threads\t1\n" == res1.content

        assert (
            cluster.instance.http_request("test_predefined_handler_auth_with_password")
            .content.strip()
            .decode()
            == "with_password"
        )
        assert (
            cluster.instance.http_request(
                "test_predefined_handler_auth_with_password_fail"
            ).status_code
            == 403
        )
        assert (
            cluster.instance.http_request(
                "test_predefined_handler_auth_without_password"
            )
            .content.strip()
            .decode()
            == "without_password"
        )


def test_predefined_handler_absent_header():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__),
            "predefined_handler_absent_header",
            "test_predefined_handler_absent_header",
        )
    ) as cluster:

        def get(headers):
            return cluster.instance.http_request(
                "test_predefined_handler_absent_header", method="GET", headers=headers
            )

        # The header regex (?P<value_from_header>.*) matches any value, including the empty string, so
        # the rule matches even when header XXX is absent. The captured value is passed to the query as
        # the parameter value_from_header. A missing header must be treated as an empty string instead
        # of raising an exception after the rule has already matched.
        response = get({"XXX": "hello"})
        assert response.status_code == 200, response.content
        assert response.content == b"hello\n"

        # Header absent: the rule still matches and the captured parameter is empty.
        response = get({})
        assert response.status_code == 200, response.content
        assert response.content == b"\n"

        # Header present but empty: same as absent.
        response = get({"XXX": ""})
        assert response.status_code == 200, response.content
        assert response.content == b"\n"


def test_fixed_static_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "static_handler", "test_static_handler"
        )
    ) as cluster:
        assert (
            404
            == cluster.instance.http_request(
                "", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="GET", headers={"XXX": "bad"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="POST", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            402
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )
        assert (
            "text/html; charset=UTF-8"
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="GET", headers={"XXX": "xxx"}
            ).headers["Content-Type"]
        )
        assert (
            b"Test get static handler and fix content"
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="GET", headers={"XXX": "xxx"}
            ).content
        )
        assert (
            "it works"
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="GET", headers={"XXX": "xxx"}
            ).headers["X-Test-Http-Response-Headers-Works"]
        )
        assert (
            "also works"
            == cluster.instance.http_request(
                "test_get_fixed_static_handler", method="GET", headers={"XXX": "xxx"}
            ).headers["X-Test-Http-Response-Headers-Even-Multiple"]
        )


def test_config_static_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "static_handler", "test_static_handler"
        )
    ) as cluster:
        assert (
            404
            == cluster.instance.http_request(
                "", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_config_static_handler", method="GET", headers={"XXX": "bad"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_config_static_handler", method="POST", headers={"XXX": "xxx"}
            ).status_code
        )

        # check default status code
        assert (
            200
            == cluster.instance.http_request(
                "test_get_config_static_handler", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )
        assert (
            "text/plain; charset=UTF-8"
            == cluster.instance.http_request(
                "test_get_config_static_handler", method="GET", headers={"XXX": "xxx"}
            ).headers["Content-Type"]
        )
        assert (
            b"Test get static handler and config content"
            == cluster.instance.http_request(
                "test_get_config_static_handler", method="GET", headers={"XXX": "xxx"}
            ).content
        )


def test_absolute_path_static_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "static_handler", "test_static_handler"
        )
    ) as cluster:
        cluster.instance.exec_in_container(
            [
                "bash",
                "-c",
                'echo "<html><body>Absolute Path File</body></html>" > /var/lib/clickhouse/user_files/absolute_path_file.html',
            ],
            privileged=True,
            user="root",
        )

        assert (
            404
            == cluster.instance.http_request(
                "", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_absolute_path_static_handler",
                method="GET",
                headers={"XXX": "bad"},
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_absolute_path_static_handler",
                method="POST",
                headers={"XXX": "xxx"},
            ).status_code
        )

        # check default status code
        assert (
            200
            == cluster.instance.http_request(
                "test_get_absolute_path_static_handler",
                method="GET",
                headers={"XXX": "xxx"},
            ).status_code
        )
        assert (
            "text/html; charset=UTF-8"
            == cluster.instance.http_request(
                "test_get_absolute_path_static_handler",
                method="GET",
                headers={"XXX": "xxx"},
            ).headers["Content-Type"]
        )
        assert (
            b"<html><body>Absolute Path File</body></html>\n"
            == cluster.instance.http_request(
                "test_get_absolute_path_static_handler",
                method="GET",
                headers={"XXX": "xxx"},
            ).content
        )


def test_relative_path_static_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "static_handler", "test_static_handler"
        )
    ) as cluster:
        cluster.instance.exec_in_container(
            [
                "bash",
                "-c",
                'echo "<html><body>Relative Path File</body></html>" > /var/lib/clickhouse/user_files/relative_path_file.html',
            ],
            privileged=True,
            user="root",
        )

        assert (
            404
            == cluster.instance.http_request(
                "", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_relative_path_static_handler",
                method="GET",
                headers={"XXX": "bad"},
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_get_relative_path_static_handler",
                method="POST",
                headers={"XXX": "xxx"},
            ).status_code
        )

        # check default status code
        assert (
            200
            == cluster.instance.http_request(
                "test_get_relative_path_static_handler",
                method="GET",
                headers={"XXX": "xxx"},
            ).status_code
        )
        assert (
            "text/html; charset=UTF-8"
            == cluster.instance.http_request(
                "test_get_relative_path_static_handler",
                method="GET",
                headers={"XXX": "xxx"},
            ).headers["Content-Type"]
        )
        assert (
            b"<html><body>Relative Path File</body></html>\n"
            == cluster.instance.http_request(
                "test_get_relative_path_static_handler",
                method="GET",
                headers={"XXX": "xxx"},
            ).content
        )


def test_defaults_http_handlers():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "defaults_handlers", "test_defaults_handlers"
        )
    ) as cluster:
        assert 200 == cluster.instance.http_request("", method="GET").status_code
        assert (
            b"Default server response"
            == cluster.instance.http_request("", method="GET").content
        )

        assert 200 == cluster.instance.http_request("ping", method="GET").status_code
        assert b"Ok.\n" == cluster.instance.http_request("ping", method="GET").content

        assert (
            200
            == cluster.instance.http_request(
                "replicas_status", method="get"
            ).status_code
        )
        assert (
            b"Ok.\n"
            == cluster.instance.http_request("replicas_status", method="get").content
        )

        assert (
            200
            == cluster.instance.http_request(
                "replicas_status?verbose=1", method="get"
            ).status_code
        )
        assert (
            b""
            == cluster.instance.http_request(
                "replicas_status?verbose=1", method="get"
            ).content
        )

        assert (
            200
            == cluster.instance.http_request(
                "?query=SELECT+1", method="GET"
            ).status_code
        )
        assert (
            b"1\n"
            == cluster.instance.http_request("?query=SELECT+1", method="GET").content
        )

        assert (
            404
            == cluster.instance.http_request(
                "/nonexistent?query=SELECT+1", method="GET"
            ).status_code
        )


def test_defaults_http_handlers_config_order():
    def check_predefined_query_handler():
        assert (
            200
            == cluster.instance.http_request(
                "?query=SELECT+1", method="GET"
            ).status_code
        )
        assert (
            b"1\n"
            == cluster.instance.http_request("?query=SELECT+1", method="GET").content
        )
        response = cluster.instance.http_request(
            "test_predefined_handler_get?max_threads=1&setting_name=max_threads",
            method="GET",
            headers={"XXX": "xxx"},
        )
        assert b"max_threads\t1\n" == response.content
        assert (
            "text/tab-separated-values; charset=UTF-8"
            == response.headers["content-type"]
        )

    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__),
            "defaults_handlers_config_order_first",
            "test_defaults_handlers_config_order/defaults_first",
        )
    ) as cluster:
        check_predefined_query_handler()

    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__),
            "defaults_handlers_config_order_first",
            "test_defaults_handlers_config_order/defaults_last",
        )
    ) as cluster:
        check_predefined_query_handler()


def test_prometheus_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "prometheus_handler", "test_prometheus_handler"
        )
    ) as cluster:
        assert (
            404
            == cluster.instance.http_request(
                "", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_prometheus", method="GET", headers={"XXX": "bad"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_prometheus", method="POST", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            200
            == cluster.instance.http_request(
                "test_prometheus", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )
        assert (
            b"ClickHouseProfileEvents_Query"
            in cluster.instance.http_request(
                "test_prometheus", method="GET", headers={"XXX": "xxx"}
            ).content
        )


def test_replicas_status_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__),
            "replicas_status_handler",
            "test_replicas_status_handler",
        )
    ) as cluster:
        assert (
            404
            == cluster.instance.http_request(
                "", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_replicas_status", method="GET", headers={"XXX": "bad"}
            ).status_code
        )

        assert (
            404
            == cluster.instance.http_request(
                "test_replicas_status", method="POST", headers={"XXX": "xxx"}
            ).status_code
        )

        assert (
            200
            == cluster.instance.http_request(
                "test_replicas_status", method="GET", headers={"XXX": "xxx"}
            ).status_code
        )
        assert (
            b"Ok.\n"
            == cluster.instance.http_request(
                "test_replicas_status", method="GET", headers={"XXX": "xxx"}
            ).content
        )


def test_headers_in_response():
    with contextlib.closing(
            SimpleCluster(
                ClickHouseCluster(__file__), "headers_in_response", "test_headers_in_response"
            )
    ) as cluster:
        for endpoint in ("static", "ping", "replicas_status", "play", "dashboard", "binary", "merges", "metrics",
                         "js/lz-string.js", "js/uplot.js", "?query=SELECT%201"):
            response = cluster.instance.http_request(endpoint, method="GET")

            assert "X-My-Answer" in response.headers
            assert "X-My-Common-Header" in response.headers

            assert response.headers["X-My-Common-Header"] == "Common header present"

            if endpoint == "?query=SELECT%201":
                assert response.headers["X-My-Answer"] == "Iam dynamic"
            else:
                assert response.headers["X-My-Answer"] == f"Iam {endpoint}"


        # Handle predefined_query_handler separately because we need to pass headers there
        response_predefined = cluster.instance.http_request(
            "query_param_with_url", method="GET", headers={"PARAMS_XXX": "test_param"})
        assert response_predefined.headers["X-My-Answer"] == "Iam predefined"
        assert response_predefined.headers["X-My-Common-Header"] == "Common header present"


def test_common_headers_without_per_handler():
    """Test that common_http_response_headers are present in responses from
    dynamic_query_handler and predefined_query_handler even when those handlers
    have no per-handler http_response_headers configured."""
    with contextlib.closing(
            SimpleCluster(
                ClickHouseCluster(__file__), "common_headers_no_per_handler",
                "test_common_headers_without_per_handler"
            )
    ) as cluster:
        # dynamic_query_handler without per-handler headers
        response = cluster.instance.http_request("?query=SELECT%201", method="GET")
        assert response.status_code == 200
        assert "X-My-Common-Header" in response.headers, \
            "common_http_response_headers missing from dynamic_query_handler without per-handler headers"
        assert response.headers["X-My-Common-Header"] == "Common header present"

        # predefined_query_handler without per-handler headers
        response_predefined = cluster.instance.http_request(
            "query_param_with_url", method="GET", headers={"PARAMS_XXX": "test_param"})
        assert response_predefined.status_code == 200
        assert "X-My-Common-Header" in response_predefined.headers, \
            "common_http_response_headers missing from predefined_query_handler without per-handler headers"
        assert response_predefined.headers["X-My-Common-Header"] == "Common header present"


def test_redirect_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "redirect_handler", "test_redirect_handler"
        )
    ) as cluster:
        def get(uri, *args, **kwargs):
            return cluster.instance.http_request(uri, method="GET", allow_redirects=False, *args, **kwargs)

        req = get("")
        assert req.status_code == 302
        assert req.headers["Location"] == "/play"

        req = get("/pla")
        assert req.status_code == 302
        assert req.headers["Location"] == "/play"

        req = get("/foo/pla")
        assert req.status_code == 404

        # Host does not match - no redirect, and we do not add defaults so, it will be 404
        req = get("/play")
        assert req.status_code == 404

        req = get("/dashboard")
        assert req.status_code == 302
        assert req.headers["Location"] == "/dashboard?from=http://:8123/dashboard"

        # Query string is not empty - no redirect, and we do not add defaults so, it will be 404
        req = get("/dashboard?foo=bar")
        assert req.status_code == 404


def test_predefined_handler_whitespace():
    """Test that predefined query handlers correctly trim whitespace from queries.

    This is a regression test for a bug where whitespace from XML indentation
    in the config file would be interpreted as binary data, causing parsing errors.
    """
    import struct

    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__),
            "predefined_handler_whitespace",
            "test_predefined_handler_whitespace",
        )
    ) as cluster:
        # Create test table
        cluster.instance.query(
            "CREATE TABLE test_table (id UInt64, value String) ENGINE = Memory"
        )

        # Prepare RowBinary data: a row with id=1 and value='test'
        # RowBinary format: UInt64 (8 bytes little-endian) + String (varint length + bytes)
        row_data = struct.pack("<Q", 1) + b"\x04test"  # 1 as UInt64 + "test" with length prefix

        # POST RowBinary data to the predefined handler
        # The handler has a query with leading/trailing whitespace in the config XML.
        # Without the fix, this whitespace would be interpreted as binary data.
        res = cluster.instance.http_request(
            "insert_rowbinary",
            method="POST",
            data=row_data,
        )
        assert res.status_code == 200, f"Insert failed: {res.content}"

        # Verify the data was inserted correctly
        result = cluster.instance.query("SELECT * FROM test_table")
        assert result.strip() == "1\ttest"

        cluster.instance.query("DROP TABLE test_table")


def test_url_prefix_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "url_prefix_handler", "test_url_prefix_handler"
        )
    ) as cluster:
        def get(path):
            return cluster.instance.http_request(path, method="GET")

        # The rule is <url_prefix>/test_prefix</url_prefix>: it must match the base path itself and
        # anything below it on a path-segment boundary, regardless of the query string.
        for matching in [
            "test_prefix",  # the base path itself
            "test_prefix/",  # the base path with a trailing slash
            "test_prefix/write",  # a sub-path
            "test_prefix/a/b/c",  # a deeper sub-path
            "test_prefix?param=value",  # query string is ignored
            "test_prefix/write?param=value",
        ]:
            response = get(matching)
            assert response.status_code == 200, f"{matching} -> {response.status_code}"
            assert response.content == b"prefix handler matched", matching

        # These must NOT match: a textual prefix that is not a path-segment boundary,
        # or a path that is not under the base at all.
        for not_matching in [
            "test_prefixbeta",  # not a segment boundary
            "test_prefixbeta/write",  # not a segment boundary
            "test_pre",  # not even a full prefix
            "other",  # unrelated path
            "",  # root
        ]:
            assert 404 == get(not_matching).status_code, not_matching


def test_url_regexp_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "url_regexp_handler", "test_url_regexp_handler"
        )
    ) as cluster:
        def get(path):
            return cluster.instance.http_request(path, method="GET")

        # The rule is <url_regexp>/test_regexp/[0-9]+</url_regexp>: the whole path must match the regular
        # expression, regardless of the query string.
        for matching in [
            "test_regexp/0",  # a single digit
            "test_regexp/123",  # several digits
            "test_regexp/123?param=value",  # query string is ignored
        ]:
            response = get(matching)
            assert response.status_code == 200, f"{matching} -> {response.status_code}"
            assert response.content == b"regex handler matched", matching

        # These must NOT match: the regex must match the whole path.
        for not_matching in [
            "test_regexp/abc",  # not digits
            "test_regexp/",  # no digits
            "test_regexp/123/extra",  # trailing segment is not part of the match
            "test_regexp",  # missing the digits segment
            "other",  # unrelated path
        ]:
            assert 404 == get(not_matching).status_code, not_matching


def test_headers_regexp_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__),
            "headers_regexp_handler",
            "test_headers_regexp_handler",
        )
    ) as cluster:
        def get(headers):
            return cluster.instance.http_request(
                "test_headers_regex", method="GET", headers=headers
            )

        # The rule is <headers_regexp><XXX>[0-9]+</XXX></headers_regexp>: the value of header XXX must
        # match the regular expression as a whole.
        response = get({"XXX": "123"})
        assert response.status_code == 200
        assert response.content == b"headers regex handler matched"

        # These must NOT match: the header is absent, empty, or does not match the regex as a whole.
        for not_matching in [
            {},  # header absent
            {"XXX": ""},  # empty value
            {"XXX": "abc"},  # not digits
            {"XXX": "12a"},  # not digits as a whole
        ]:
            assert 404 == get(not_matching).status_code, not_matching


def test_catch_all_handler():
    with contextlib.closing(
        SimpleCluster(
            ClickHouseCluster(__file__), "catch_all_handler", "test_catch_all_handler"
        )
    ) as cluster:
        # The single rule has only <handler> and no match conditions, so it must match every request
        # (any path, with or without a query string) instead of throwing an exception.
        for path in [
            "",  # root
            "anything",
            "a/b/c",  # a nested path
            "anything?param=value",  # query string present
        ]:
            response = cluster.instance.http_request(path, method="GET")
            assert response.status_code == 200, f"{path} -> {response.status_code}"
            assert response.content == b"catch-all matched", path
