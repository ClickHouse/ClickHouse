def get_remote_paths(node, table_name, only_remote_path=True):
    """
    Returns remote paths of blobs related to the table.
    """
    uuid = node.query(
        f"""
        SELECT uuid
        FROM system.tables
        WHERE name = '{table_name}'
        """
    ).strip()
    assert uuid
    return (
        node.query(
            f"""
        SELECT {"remote_path" if only_remote_path else "*"}
        FROM system.remote_data_paths
        WHERE
            local_path LIKE '%{uuid}%'
            AND local_path NOT LIKE '%format_version.txt%'
        ORDER BY remote_path;
        """
        )
        .strip()
        .splitlines()
    )
