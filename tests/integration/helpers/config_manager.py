import os
from typing import Any

from .cluster import ClickHouseInstance


class ConfigManager:
    """Allows to temporarily add configuration files to the "config.d" or "users.d" directories.

    Can act as a context manager:

    with ConfigManager() as cm:
        cm.add_main_config("configs/test_specific_config.xml") # copy "configs/test_specific_config.xml" to "/etc/clickhouse-server/config.d"
        ...
        # "/etc/clickhouse-server/config.d/test_specific_config.xml" is removed automatically

    """

    def __init__(self) -> None:
        self.__added_configs: list[tuple[ClickHouseInstance, str]] = []

    def add_main_config(self, node_or_nodes: ClickHouseInstance | list[ClickHouseInstance], local_path: str, reload_config: bool = True) -> None:
        """Temporarily adds a configuration file to the "config.d" directory."""
        self.__add_config(
            node_or_nodes, local_path, dest_dir="config.d", reload_config=reload_config
        )

    def add_user_config(self, node_or_nodes: ClickHouseInstance | list[ClickHouseInstance], local_path: str, reload_config: bool = True) -> None:
        """Temporarily adds a configuration file to the "users.d" directory."""
        self.__add_config(
            node_or_nodes, local_path, dest_dir="users.d", reload_config=reload_config
        )

    def reset(self, reload_config: bool = True) -> None:
        """Removes all configuration files added by this ConfigManager."""
        if not self.__added_configs:
            return
        for node, dest_path in self.__added_configs:
            node.remove_file_from_container(dest_path)
        if reload_config:
            for node, _ in self.__added_configs:
                node.query("SYSTEM RELOAD CONFIG")
        self.__added_configs = []

    def __add_config(self, node_or_nodes: ClickHouseInstance | list[ClickHouseInstance], local_path: str, dest_dir: str, reload_config: bool) -> None:
        nodes_to_add_config = (
            node_or_nodes if (type(node_or_nodes) is list) else [node_or_nodes]
        )
        for node in nodes_to_add_config:
            src_path = os.path.join(node.cluster.base_dir, local_path)
            dest_path = os.path.join(
                "/etc/clickhouse-server", dest_dir, os.path.basename(local_path)
            )
            # Function copy_file_to_container() is not atomic, so to add a configuration file atomically
            # first we create a temporary file and then we rename it.
            temp_dest_path = dest_path + ".temp"
            node.copy_file_to_container(src_path, temp_dest_path)
            node.move_file_in_container(temp_dest_path, dest_path)
        if reload_config:
            for node in nodes_to_add_config:
                node.query("SYSTEM RELOAD CONFIG")
        for node in nodes_to_add_config:
            dest_path = os.path.join(
                "/etc/clickhouse-server", dest_dir, os.path.basename(local_path)
            )
            self.__added_configs.append((node, dest_path))

    def __enter__(self) -> "ConfigManager":
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:  # pyright: ignore[reportAny, reportExplicitAny]
        self.reset()
