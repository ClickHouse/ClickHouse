def corrupt_part_data_on_disk(node, table, part_name, file_ext=".bin", database=None):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}' {}".format(
            table,
            part_name,
            f"AND database = '{database}'" if database is not None else "",
        )
    ).strip()

    corrupt_part_data_by_path(node, part_path, file_ext)


def corrupt_part_data_by_path(node, part_path, file_ext=".bin"):
    print("Corrupting part", part_path, "at", node.name)
    print(
        "Will corrupt: ",
        node.exec_in_container(
            ["bash", "-c", f"cd {part_path} && ls *{file_ext} | head -n 1"]
        ),
    )

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cd {part_path} && ls *{file_ext} | head -n 1 | xargs -I{{}} sh -c 'truncate -s -1 $1' -- {{}}",
        ],
        privileged=True,
    )


def break_part(node, table, part_name):
    path = f"/var/lib/clickhouse/data/default/{table}/{part_name}/columns.txt"
    print(f"Corrupting part {part_name}, removing file: {path}")
    node.exec_in_container(["bash", "-c", f"rm {path}"])
