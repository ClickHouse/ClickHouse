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


def test_dynamic_query_handler_with_insert_and_select():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "dynamic_insert_and_select", "test_insert_and_select_dynamic")) as cluster:
        insert_data_query = urllib.quote_plus('INSERT INTO test.test VALUES')
        select_data_query = urllib.quote_plus('SELECT * FROM test.test ORDER BY id')
        create_database_query = urllib.quote_plus('CREATE DATABASE test')
        create_test_table_query = 'CREATE TABLE test.test (id UInt8) Engine = Memory'
        assert cluster.instance.http_request('create_test_table?max_threads=1&test_create_query_param=' + create_database_query, method='PUT') == ''
        assert cluster.instance.http_request('create_test_table?max_threads=1', method='PUT', data=create_test_table_query) == ''
        assert cluster.instance.http_request('insert_data_to_test?max_threads=1&test_insert_query_param=' + insert_data_query + '(1)', method='POST') == ''
        assert cluster.instance.http_request('insert_data_to_test?max_threads=1&test_insert_query_param=' + insert_data_query, method='POST', data='(2)') == ''
        assert cluster.instance.http_request('insert_data_to_test?max_threads=1', method='POST', data='INSERT INTO test.test VALUES(3)(4)') == ''
        assert cluster.instance.http_request('query_data_from_test?max_threads=1&test_select_query_param=' + select_data_query, method='GET') == '1\n2\n3\n4\n'


def test_predefine_query_handler_with_insert_and_select():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "predefine_insert_and_select", "test_insert_and_select_predefine")) as cluster:
        assert cluster.instance.http_request('create_test_table?max_threads=1', method='PUT') == ''
        assert cluster.instance.http_request('insert_data_to_test?max_threads=1', method='POST', data='(1)(2)(3)(4)') == ''
        assert cluster.instance.http_request('query_data_from_test?max_threads=1', method='GET') == '1\n2\n3\n4\n'


def test_dynamic_query_handler_with_params_and_settings():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "dynamic_params_and_settings", "test_param_and_settings_dynamic")) as cluster:
        settings = 'max_threads=1&max_alter_threads=2'
        query_param = 'param_name_1=max_threads&param_name_2=max_alter_threads'
        test_query = 'SELECT value FROM system.settings where name = {name_1:String} OR name = {name_2:String}'
        quoted_test_query = urllib.quote_plus(test_query)
        assert cluster.instance.http_request('post_query_params_and_settings?post_query_param=' + quoted_test_query + '&' + query_param + '&' + settings, method='POST') == '1\n2\n'
        assert cluster.instance.http_request('post_query_params_and_settings?' + query_param + '&' + settings, method='POST', data=test_query) == '1\n2\n'
        assert 'Syntax error' in cluster.instance.http_request('post_query_params_and_settings?post_query_param=' + test_query + '&' + settings, method='POST', data=query_param)
        assert 'Syntax error' in cluster.instance.http_request('post_query_params_and_settings?post_query_param=' + test_query + '&' + query_param, method='POST', data=settings)

        assert cluster.instance.http_request('get_query_params_and_settings?get_query_param=' + quoted_test_query + '&' + query_param + '&' + settings) == '1\n2\n'
        assert cluster.instance.http_request('query_param_with_url/123/max_threads/max_alter_threads?query_param=' + quoted_test_query + '&' + settings) == '1\n2\n'
        assert 'Duplicate name' in cluster.instance.http_request('query_param_with_url/123/max_threads_dump/max_alter_threads_dump?query_param=' + quoted_test_query + '&' + query_param + '&' + settings)

        assert cluster.instance.http_request('test_match_headers?query_param=' + quoted_test_query + '&' + settings, headers={'XXX': 'TEST_HEADER_VALUE', 'PARAMS_XXX': 'max_threads/max_alter_threads'}) == '1\n2\n'


def test_predefine_query_handler_with_params_and_settings():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "predefine_params_and_settings", "test_param_and_settings_predefine")) as cluster:
        settings = 'max_threads=1&max_alter_threads=2'
        query_param = 'name_1=max_threads&name_2=max_alter_threads'
        assert cluster.instance.http_request('get_query_params_and_settings?' + query_param + '&' + settings, method='GET') == '1\nmax_alter_threads\t2\n'
        assert cluster.instance.http_request('query_param_with_url/123/max_threads/max_alter_threads?' + settings) == '1\nmax_alter_threads\t2\n'
        assert 'Duplicate name' in cluster.instance.http_request('query_param_with_url/123/max_threads_dump/max_alter_threads_dump?' + query_param + '&' + settings)

        assert cluster.instance.http_request('post_query_params_and_settings?' + query_param, method='POST', data=settings) == '1\nmax_alter_threads\t2\n'
        assert cluster.instance.http_request('post_query_params_and_settings?' + settings, method='POST', data=query_param) == '1\nmax_alter_threads\t2\n'
        assert cluster.instance.http_request('post_query_params_and_settings?' + query_param + '&' + settings, method='POST') == '1\nmax_alter_threads\t2\n'
        assert cluster.instance.http_request('post_query_params_and_settings', method='POST', data=query_param + '&' + settings) == '1\nmax_alter_threads\t2\n'
        assert cluster.instance.http_request('test_match_headers?' + settings, headers={'XXX': 'TEST_HEADER_VALUE', 'PARAMS_XXX': 'max_threads/max_alter_threads'}) == '1\nmax_alter_threads\t2\n'


def test_other_configs():
    with contextlib.closing(SimpleCluster(ClickHouseCluster(__file__), "test_other_configs", "other_tests_configs")) as cluster:
        assert cluster.instance.http_request('', method='GET') == 'Ok.\n'
        assert cluster.instance.http_request('ping_test', method='GET') == 'Ok.\n'
        assert cluster.instance.http_request('404/NOT_FOUND', method='GET') == 'There is no handle /404/NOT_FOUND\n\ntest not found\n'

        cluster.instance.query('CREATE DATABASE test')
        cluster.instance.query('CREATE TABLE test.test (id UInt8) Engine = Memory')
        assert cluster.instance.http_request('test_one_handler_with_insert_and_select?id=1', method='POST', data='(1)(2)') == '2\n'
        assert 'Cannot parse input' in cluster.instance.http_request('test_one_handler_with_insert_and_select', method='POST', data='id=1')
