import os

import tempfile
import xml.etree.ElementTree as ET

def get_database_disk_name(node):
    if not node.with_remote_database_disk:
        return "default"
    tree = ET.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), "remote_database_disk.xml"))
    root = tree.getroot()

    disk_element = root.find(".//database_disk/disk")
    return disk_element.text if disk_element is not None else "default"


def replace_text_in_metadata(node, metadata_path: str, old_value: str, new_value: str):
    db_disk_name = get_database_disk_name(node)
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml --disk {db_disk_name} --save-logs --query "

    old_metadata = node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'read --path-from {metadata_path}'"]
    )

    new_metadata = old_metadata.replace(old_value, new_value)
    write_to_file(node, db_disk_name, metadata_path, new_metadata)


def write_metadata(node, metadata_path: str, content: str):
    db_disk_name = get_database_disk_name(node)
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml --disk {db_disk_name} --save-logs --query "

    old_metadata = node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'read --path-from {metadata_path}'"]
    )
    write_to_file(node, db_disk_name, metadata_path, content)


def write_to_file(node, db_disk_name: str, file_path: str, content: str):
    # Escape backticks to avoid command substitution
    escaped_content = content.replace('"', r"\"").replace("`", r"\`")
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml --save-logs --disk {db_disk_name} --query "
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""printf "%s" "{escaped_content}" | {disk_cmd_prefix} 'w --path-to {file_path}'""",
        ]
    )
