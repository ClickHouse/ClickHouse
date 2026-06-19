from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")
TMP_DIR = Path(__file__).resolve().parents[3] / "tmp"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def external_users_file(started_cluster):
    TMP_DIR.mkdir(exist_ok=True)
    local_path = TMP_DIR / "external_tables_interactive_mode_users.csv"
    local_path.write_text("1,Alice\n2,Bob\n3,Charlie\n", encoding="utf-8")

    try:
        yield local_path
    finally:
        local_path.unlink(missing_ok=True)


def test_external_tables_interactive_mode(started_cluster, external_users_file):
    command = (
        f"{started_cluster.get_client_cmd()} --host {node.ip_address} --port 9000 "
        f"--external --file={external_users_file} --name=users "
        "--structure='id UInt32, name String' --format=CSV "
        "--disable_suggestion --highlight=0"
    )

    with client(command=command) as interactive_client:
        interactive_client.expect(prompt)

        interactive_client.send("SET max_threads = 4; SELECT * FROM users ORDER BY id;")
        interactive_client.expect("Alice")
        interactive_client.expect("Bob")
        interactive_client.expect("Charlie")
        interactive_client.expect(prompt)

        interactive_client.send("SET max_block_size = 1000;")
        interactive_client.expect(prompt)

        interactive_client.send("SELECT count() FROM users;")
        interactive_client.expect("3")
        interactive_client.expect(prompt)

        interactive_client.send("DESCRIBE TABLE users SETTINGS describe_compact_output = 1;")
        interactive_client.expect("id")
        interactive_client.expect("UInt32")
        interactive_client.expect("name")
        interactive_client.expect("String")
        interactive_client.expect(prompt)

        interactive_client.send(
            "EXPLAIN SELECT * FROM numbers() WHERE number IN (SELECT id FROM users) LIMIT 1;"
        )
        interactive_client.expect("ReadFromMemoryStorage")
        interactive_client.expect(prompt)
