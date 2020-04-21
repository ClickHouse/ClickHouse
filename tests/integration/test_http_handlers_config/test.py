import os
import urllib
import contextlib

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
        return self.cluster.add_instance(name, config_dir=os.path.join(script_path, config_dir),
                                         main_configs=[os.path.join(script_path, 'common_configs', 'common_config.xml')],
                                         user_configs=[os.path.join(script_path, 'common_configs', 'common_users.xml')])


def test_dynamic_query_handler():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "dynamic_handler", "test_dynamic_handler")) as cluster:
        test_query = urllib.quote_plus('SELECT * FROM system.settings WHERE name = \'max_threads\'')

        assert 404 == cluster.instance.http_request('?max_threads=1', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1', method='POST', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1', method='GET', headers={'XXX': 'bad'}).status_code

        assert 400 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 200 == cluster.instance.http_request('test_dynamic_handler_get?max_threads=1&get_dynamic_handler_query=' + test_query,
                method='GET', headers={'XXX': 'xxx'}).status_code


def test_predefine_query_handler():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "predefined_handler", "test_predefined_handler")) as cluster:
        assert 404 == cluster.instance.http_request('?max_threads=1', method='GET', headers={'XXX': 'xxx'}).status_code

        assert 404 == cluster.instance.http_request('test_predefine_handler_get?max_threads=1', method='GET', headers={'XXX': 'bad'}).status_code

        assert 404 == cluster.instance.http_request('test_predefine_handler_get?max_threads=1', method='POST', headers={'XXX': 'xxx'}).status_code

        assert 400 == cluster.instance.http_request('test_predefine_handler_get?max_threads=1', method='GET', headers={'XXX': 'xxx'}).status_code

        assert '1\n' == cluster.instance.http_request('test_predefine_handler_get?max_threads=1&setting_name=max_threads', method='GET', headers={'XXX': 'xxx'}).content