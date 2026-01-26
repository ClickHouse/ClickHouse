import time


class YTsaurusCLI:
    def __init__(self, cluster, instance, proxy, port, ytcluster_name=None):
        self.instance = instance
        self.cluster = cluster
        self.proxy = proxy
        self.port = port
        self.yt_cluster_name = ytcluster_name

    def exec(self, command, retry_count=100, time_to_sleep=0.5):
        print(command)
        for retry in range(retry_count + 1):
            try:
                res = self.cluster.exec_in_container(
                    self.cluster.get_container_id(self.proxy),
                    [
                        "bash",
                        "-c",
                        command,
                    ],
                )
                return res
            except Exception as e:
                ## For some reasons ytstaurs can receive queries with not fully loaded resouces.
                ## And we can have errors like:
                ## ` Account <acc> is over tablet count limit `
                ## Haven't found better solution than simple retries.
                print(f"Exception : {retry}/{retry_count} Command {command}")
                if retry == retry_count:
                    raise e
                time.sleep(time_to_sleep)

    def _generate_replica_name(self, table_table):
        ### Generaly there might be several replicas, but for simplicity using only one.
        return table_table + "_r1"

    def _generate_table_attributes_str(
        self, dynamic, schema=None, sorted_columns=None, upstream_replica=None
    ):
        if not schema:
            return ""
        return (
            "--attributes '{"
            + ("dynamic=%true;" if dynamic else "")
            + (f'upstream_replica_id="{upstream_replica}";' if upstream_replica else "")
            + "schema= ["
            + ";".join(
                f"{{required = true; name = {name}; type = {type}; {'sort_order=ascending' if name in sorted_columns else ''}}}"
                for name, type in schema.items()
            )
            + "]}'"
        )

    def create_replciated_table(
        self,
        table_path,
        ytcluster_name,
        data,
        schema,
        sorted_columns=set(),
        retry_count=100,
        time_to_sleep=0.5,
    ):
        replica_path = self._generate_replica_name(table_path)

        create_replicated_table_cmd = (
            f"yt create replicated_table {table_path} "
            + self._generate_table_attributes_str(True, schema, sorted_columns)
        )
        self.exec(create_replicated_table_cmd, retry_count, time_to_sleep)

        create_replica_cmd = f'yt create table_replica --attr \'{{table_path="{table_path}"; cluster_name="{ytcluster_name}";replica_path="{replica_path}"}}\''
        table_replica_id = self.exec(
            create_replica_cmd, retry_count, time_to_sleep
        ).strip()
        self.create_table(
            replica_path,
            dynamic=True,
            schema=schema,
            sorted_columns=sorted_columns,
            upstream_replica=table_replica_id,
            retry_count=retry_count,
            time_to_sleep=time_to_sleep,
        )

        self.exec(f"yt mount-table {replica_path}", 0, 0)
        self.exec(f"yt mount-table {table_path}", 0, 0)

        enable_replica_cmd = (
            f"yt alter-table-replica {table_replica_id} --enable --mode sync"
        )
        self.exec(enable_replica_cmd)
        self.write_table(table_path, data, True)

    def remove_replicated_table(self, table_path):
        self.remove_table(table_path)
        self.remove_table(self._generate_replica_name(table_path))

    def create_table(
        self,
        table_path,
        data=None,
        dynamic=False,
        schema=None,
        sorted_columns=set(),
        retry_count=100,
        time_to_sleep=0.5,
        upstream_replica=None,
    ):
        schema_arribute = self._generate_table_attributes_str(
            dynamic, schema, sorted_columns, upstream_replica
        )

        create_table_cmd = "yt create table {} {}".format(table_path, schema_arribute)
        print(create_table_cmd)
        self.exec(create_table_cmd, retry_count, time_to_sleep)

        if dynamic:
            mount_table_cmd = f"yt mount-table {table_path}"
            self.exec(mount_table_cmd, 0, time_to_sleep)

        if data:
            self.write_table(table_path, data, dynamic)

    def write_table(self, table_path, data, dynamic):
        insert_data_cmd = "echo '{}' | yt {} '{}' --format '<encode_utf8=false;uuid_mode=text_yql>json'".format(
            data, "insert-rows" if dynamic else "write-table", table_path
        )

        self.exec(insert_data_cmd, 0, 0)

    def remove_table(self, table_path):
        self.exec("yt remove {}".format(table_path), 0, 0)


class YtsaurusURIHelper:
    def __init__(
        self, port, host="ytsaurus_backend1", token="password", ytcluster_name="primary"
    ):
        self.host = host
        self.port = port
        self.token = token
        self.ytcluster_name = ytcluster_name
        self.uri = f"http://{self.host}:{self.port}"
