import time

import pymysql


class MySQLNodeInstance:
    def __init__(self, user='root', password='clickhouse', hostname='127.0.0.1', port=3308):
        self.user = user
        self.port = port
        self.hostname = hostname
        self.password = password
        self.mysql_connection = None  # lazy init

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()

    def alloc_connection(self):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.hostname, port=self.port, autocommit=True)
        return self.mysql_connection

    def query(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            def execute(query):
                res = cursor.execute(query)
                if query.lstrip().lower().startswith(('select', 'show')):
                    # Mimic output of the ClickHouseInstance, which is:
                    # tab-sparated values and newline (\n)-separated rows.
                    rows = []
                    for row in cursor.fetchall():
                        rows.append("\t".join(str(item) for item in row))
                    res = "\n".join(rows)
                return res

            if isinstance(execution_query, (str, bytes)):
                return execute(execution_query)
            else:
                return [execute(q) for q in execution_query]

    def wait_mysql_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.alloc_connection()
                print("Mysql Started")
                return
            except Exception as ex:
                print("Can't connect to MySQL " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait MySQL container")
