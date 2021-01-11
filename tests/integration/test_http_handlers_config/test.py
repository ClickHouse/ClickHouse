import contextlib
import os
import urllib.request, urllib.parse, urllib.error

from helpers.cluster import ClickHouseCluster


class SimpleCluster:
    def close(self):
        self.cluster.shutdown()

    def __init__(self, cluster, name, config_dir):
        self.cluster = cluster
        self.instance = self.add_instance(name, config_dir)
        cluster.start()

    def add_instance(self, name, config_dir):
        script_path = os.path.dirname(os.path.realpath(__file__))
        return self.cluster.add_instance(name, main_configs=[os.path.join(script_path, config_dir, 'config.xml')])


def test_dynamic_query_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "dynamic_handler", "test_dynamic_handler")) as cluster:
        test_query = urllib.parse.quote_plus('SELECT * FROM system.settings WHERE name = \'max_threads\'')

        assert 404 == cluster.instance.http_request('?max_threads=1', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 400 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code

        assert 200 == cluster.instance.http_request(
            'test_dynamic_handler_get?max_threads=1&get_dynamic_handler_query=' + test_query,
            method='GET', headers={'XXX': 'xxx'}).status_code


def test_predefined_query_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "predefined_handler", "test_predefined_handler")) as cluster:
        assert 404 == cluster.instance.http_request('?max_threads=1', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_predefined_handler_get?max_threads=1', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_predefined_handler_get?max_threads=1', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        assert 500 == cluster.instance.http_request('test_predefined_handler_get?max_threads=1', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code

        assert b'max_threads\t1\n' == cluster.instance.http_request(
            'test_predefined_handler_get?max_threads=1&setting_name=max_threads', method='GET',
            headers={'XXX': 'xxx'}).content

        assert b'max_threads\t1\nmax_alter_threads\t1\n' == cluster.instance.http_request(
            'query_param_with_url/max_threads?max_threads=1&max_alter_threads=1',
            headers={'XXX': 'max_alter_threads'}).content


def test_fixed_static_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "static_handler", "test_static_handler")) as cluster:
        assert 404 == cluster.instance.http_request('', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_get_fixed_static_handler', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_get_fixed_static_handler', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        assert 402 == cluster.instance.http_request('test_get_fixed_static_handler', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code
        assert 'text/html; charset=UTF-8' == \
               cluster.instance.http_request('test_get_fixed_static_handler', method='GET',
                                             headers={'XXX': 'xxx'}).headers['Content-Type']
        assert b'Test get static handler and fix content' == cluster.instance.http_request(
            'test_get_fixed_static_handler', method='GET', headers={'XXX': 'xxx'}).content


def test_config_static_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "static_handler", "test_static_handler")) as cluster:
        assert 404 == cluster.instance.http_request('', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_get_config_static_handler', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_get_config_static_handler', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        # check default status code
        assert 200 == cluster.instance.http_request('test_get_config_static_handler', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code
        assert 'text/plain; charset=UTF-8' == \
               cluster.instance.http_request('test_get_config_static_handler', method='GET',
                                             headers={'XXX': 'xxx'}).headers['Content-Type']
        assert b'Test get static handler and config content' == cluster.instance.http_request(
            'test_get_config_static_handler', method='GET', headers={'XXX': 'xxx'}).content


def test_absolute_path_static_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "static_handler", "test_static_handler")) as cluster:
        cluster.instance.exec_in_container(
            ['bash', '-c',
             'echo "<html><body>Absolute Path File</body></html>" > /var/lib/clickhouse/user_files/absolute_path_file.html'],
            privileged=True, user='root')

        assert 404 == cluster.instance.http_request('', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_get_absolute_path_static_handler', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_get_absolute_path_static_handler', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        # check default status code
        assert 200 == cluster.instance.http_request('test_get_absolute_path_static_handler', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code
        assert 'text/html; charset=UTF-8' == \
               cluster.instance.http_request('test_get_absolute_path_static_handler', method='GET',
                                             headers={'XXX': 'xxx'}).headers['Content-Type']
        assert b'<html><body>Absolute Path File</body></html>\n' == cluster.instance.http_request(
            'test_get_absolute_path_static_handler', method='GET', headers={'XXX': 'xxx'}).content


def test_relative_path_static_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "static_handler", "test_static_handler")) as cluster:
        cluster.instance.exec_in_container(
            ['bash', '-c',
             'echo "<html><body>Relative Path File</body></html>" > /var/lib/clickhouse/user_files/relative_path_file.html'],
            privileged=True, user='root')

        assert 404 == cluster.instance.http_request('', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_get_relative_path_static_handler', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_get_relative_path_static_handler', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        # check default status code
        assert 200 == cluster.instance.http_request('test_get_relative_path_static_handler', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code
        assert 'text/html; charset=UTF-8' == \
               cluster.instance.http_request('test_get_relative_path_static_handler', method='GET',
                                             headers={'XXX': 'xxx'}).headers['Content-Type']
        assert b'<html><body>Relative Path File</body></html>\n' == cluster.instance.http_request(
            'test_get_relative_path_static_handler', method='GET', headers={'XXX': 'xxx'}).content


def test_defaults_http_handlers():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "defaults_handlers", "test_defaults_handlers")) as cluster:
        assert 200 == cluster.instance.http_request('', method='GET').status_code
        assert b'Default server response' == cluster.instance.http_request('', method='GET').content

        assert 200 == cluster.instance.http_request('ping', method='GET').status_code
        assert b'Ok.\n' == cluster.instance.http_request('ping', method='GET').content

        assert 200 == cluster.instance.http_request('replicas_status', method='get').status_code
        assert b'Ok.\n' == cluster.instance.http_request('replicas_status', method='get').content

        assert 200 == cluster.instance.http_request('replicas_status?verbose=1', method='get').status_code
        assert b'' == cluster.instance.http_request('replicas_status?verbose=1', method='get').content

        assert 200 == cluster.instance.http_request('?query=SELECT+1', method='GET').status_code
        assert b'1\n' == cluster.instance.http_request('?query=SELECT+1', method='GET').content


def test_prometheus_handler():
    with contextlib.closing(
            SimpleCluster(ClickHouseCluster(__file__), "prometheus_handler", "test_prometheus_handler")) as cluster:
        assert 404 == cluster.instance.http_request('', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_prometheus', method='GET', headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_prometheus', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        assert 200 == cluster.instance.http_request('test_prometheus', method='GET', headers={'XXX': 'xxx'}).status_code
        assert b'ClickHouseProfileEvents_Query' in cluster.instance.http_request('test_prometheus', method='GET',
                                                                                headers={'XXX': 'xxx'}).content


def test_replicas_status_handler():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "replicas_status_handler",
                                          "test_replicas_status_handler")) as cluster:
        assert 404 == cluster.instance.http_request('', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_replicas_status', method='GET',
                                                    headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_replicas_status', method='POST',
                                                    headers={'XXX': 'xxx'}).status_code

        assert 200 == cluster.instance.http_request('test_replicas_status', method='GET',
                                                    headers={'XXX': 'xxx'}).status_code
        assert b'Ok.\n' == cluster.instance.http_request('test_replicas_status', method='GET',
                                                        headers={'XXX': 'xxx'}).content
