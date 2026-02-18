def start_unity_catalog(node):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""cp -r /unitycatalog /var/lib/clickhouse/user_files/ && cd /var/lib/clickhouse/user_files/unitycatalog && nohup bin/start-uc-server > uc.log 2>&1 &""",
        ]
    )
