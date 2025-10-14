import time


class YTsaurusCLI:
    def __init__(self, cluster, instance, proxy, port):
        self.instance = instance
        self.cluster = cluster
        self.proxy = proxy
        self.port = port

    def exec(self, command, retry_count=100, time_to_sleep=0.5):

        for retry in range(retry_count):
            try:
                self.cluster.exec_in_container(
                    self.cluster.get_container_id(self.proxy),
                    [
                        "bash",
                        "-c",
                        command,
                    ],
                )
                break
            except Exception as e:
                ## For some reasons ytstaurs can receive queries with not fully loaded resouces.
                ## And we can have errors like:
                ## ` Account <acc> is over tablet count limit `
                ## Haven't found better solution than simple retries.
                print(f"Exception : {retry}/{retry_count}")
                if retry == retry_count - 1:
                    raise e
                time.sleep(time_to_sleep)

    def create_table(
        self,
        table_path,
        data,
        dynamic=False,
        schema=None,
        sorted_columns=set(),
        retry_count=100,
        time_to_sleep=0.5,
    ):

        schema_arribute = ""

        if schema:
            schema_arribute = (
                '--attributes "{'
                + ("dynamic=%true;" if dynamic else "")
                + "schema= ["
                + ";".join(
                    f"{{required = true; name = {name}; type = {type}; {'sort_order=ascending' if name in sorted_columns else ''}}}"
                    for name, type in schema.items()
                )
                + ']}"'
            )
        print("yt create table {} {}".format(table_path, schema_arribute))
        for retry in range(retry_count):
            try:
                self.cluster.exec_in_container(
                    self.cluster.get_container_id(self.proxy),
                    [
                        "bash",
                        "-c",
                        "yt create table {} {}".format(table_path, schema_arribute),
                    ],
                )
                break
            except Exception as e:
                ## For some reasons ytstaurs can receive queries with not fully loaded resouces.
                ## And we can have errors like:
                ## ` Account <acc> is over tablet count limit `
                ## Haven't found better solution than simple retries.
                print(f"Exception : {retry}/{retry_count}")
                if retry == retry_count - 1:
                    raise e
                time.sleep(time_to_sleep)

        if dynamic:
            self.cluster.exec_in_container(
                self.cluster.get_container_id(self.proxy),
                [
                    "bash",
                    "-c",
                    "yt mount-table {}".format(table_path),
                ],
            )
        self.cluster.exec_in_container(
            self.cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "echo '{}' | yt {} '{}' --format '<encode_utf8=false;uuid_mode=text_yql>json'".format(
                    data, "insert-rows" if dynamic else "write-table", table_path
                ),
            ],
        )

    def write_table(self, table_path, data):
        self.cluster.exec_in_container(
            self.cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "echo '{}' | yt {} '{}' --format '<encode_utf8=false;uuid_mode=text_yql>json'".format(
                    data, "write-table", table_path
                ),
            ],
        )

    def exists(self, table_path):
        self.cluster.exec_in_container(
            self.cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "yt remove {}".format(self.proxy),
            ],
        )

    def remove_table(self, table_path):
        self.cluster.exec_in_container(
            self.cluster.get_container_id(self.proxy),
            [
                "bash",
                "-c",
                "yt remove {}".format(table_path),
            ],
        )


class YtsaurusURIHelper:
    def __init__(self, port, host="ytsaurus_backend1", token="password"):
        self.host = host
        self.port = port
        self.token = token
        self.uri = f"http://{self.host}:{self.port}"
