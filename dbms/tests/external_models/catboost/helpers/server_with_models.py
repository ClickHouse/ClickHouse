from server import ClickHouseServer
from client import ClickHouseClient
from table import ClickHouseTable
import os
import errno
from shutil import rmtree

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CATBOOST_ROOT = os.path.dirname(SCRIPT_DIR)

CLICKHOUSE_CONFIG = \
'''
<yandex>
    <timezone>Europe/Moscow</timezone>
    <listen_host>::</listen_host>
    <path>{path}</path>
    <tmp_path>{tmp_path}</tmp_path>
    <models_config>{models_config}</models_config>
    <mark_cache_size>5368709120</mark_cache_size>
    <users_config>users.xml</users_config>
    <tcp_port>{tcp_port}</tcp_port>
    <catboost_dynamic_library_path>{catboost_dynamic_library_path}</catboost_dynamic_library_path>
</yandex>
'''

CLICKHOUSE_USERS = \
'''
<yandex>
    <profiles>
        <default>
        </default>
	<readonly>
            <readonly>1</readonly>
        </readonly>
    </profiles>

    <users>
        <readonly>
            <password></password>
            <profile>readonly</profile>
            <quota>default</quota>
        </readonly>

        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <networks incl="networks" replace="replace">
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>

        </default>
    </users>

    <quotas>
        <default>
        </default>
    </quotas>
</yandex>
'''

CATBOOST_MODEL_CONFIG = \
'''
<models>
    <model>
        <type>catboost</type>
        <name>{name}</name>
        <path>{path}</path>
        <lifetime>0</lifetime>
    </model>
</models>
'''


class ClickHouseServerWithCatboostModels:
    def __init__(self, name, binary_path, port, shutdown_timeout=10, clean_folder=False):
        self.models = {}
        self.name = name
        self.binary_path = binary_path
        self.port = port
        self.shutdown_timeout = shutdown_timeout
        self.clean_folder = clean_folder
        self.root = os.path.join(CATBOOST_ROOT, 'data', 'servers')
        self.config_path = os.path.join(self.root, 'config.xml')
        self.users_path = os.path.join(self.root, 'users.xml')
        self.models_dir = os.path.join(self.root, 'models')
        self.server = None

    def _get_server(self):
        stdout_file = os.path.join(self.root, 'server_stdout.txt')
        stderr_file = os.path.join(self.root, 'server_stderr.txt')
        return ClickHouseServer(self.binary_path, self.config_path, stdout_file, stderr_file, self.shutdown_timeout)

    def add_model(self, model_name, model):
        self.models[model_name] = model

    def apply_model(self, name, df, cat_feature_names):
        names = list(df)
        float_feature_names = tuple(name for name in names if name not in cat_feature_names)
        with ClickHouseTable(self.server, self.port, name, df) as table:
            return table.apply_model(name, cat_feature_names, float_feature_names)

    def _create_root(self):
        try:
            os.makedirs(self.root)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(self.root):
                pass
            else:
                raise

    def _clean_root(self):
        rmtree(self.root)

    def _save_config(self):
        params = {
            'tcp_port': self.port,
            'path': os.path.join(self.root, 'clickhouse'),
            'tmp_path': os.path.join(self.root, 'clickhouse', 'tmp'),
            'models_config': os.path.join(self.models_dir, '*_model.xml'),
            'catboost_dynamic_library_path': os.path.join(CATBOOST_ROOT, 'data', 'libcatboostmodel.so')
        }
        config = CLICKHOUSE_CONFIG.format(**params)

        with open(self.config_path, 'w') as f:
            f.write(config)

        with open(self.users_path, 'w') as f:
            f.write(CLICKHOUSE_USERS)

    def _save_models(self):
        if not os.path.exists(self.models_dir):
            os.makedirs(self.models_dir)

        for name, model in self.models.items():
            model_path = os.path.join(self.models_dir, name + '.cbm')
            config_path = os.path.join(self.models_dir, name + '_model.xml')
            params = {
                'name': name,
                'path': model_path
            }
            config = CATBOOST_MODEL_CONFIG.format(**params)
            with open(config_path, 'w') as f:
                f.write(config)

            model.save_model(model_path)

    def __enter__(self):
        self._create_root()
        self._save_config()
        self._save_models()
        self.server = self._get_server().__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        res = self.server.__exit__(exc_type, exc_val, exc_tb)
        if self.clean_folder:
            self._clean_root()
        return res

