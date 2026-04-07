import time


def check_on_cluster(
    nodes,
    expected,
    *,
    what,
    cluster_name="test_auto_cluster",
    msg=None,
    retries=5,
    query_params={},
):
    """
    Select data from `system.clusters` on specified nodes and check the result
    """
    assert 1 <= retries <= 6

    node_results = {}
    for retry in range(1, retries + 1):
        for node in nodes:
            if node_results.get(node.name) == expected:
                # do not retry node after success
                continue
            query_text = (
                f"SELECT {what} FROM system.clusters WHERE cluster = '{cluster_name}'"
            )
            node_results[node.name] = int(node.query(query_text, **query_params))

        if all(actual == expected for actual in node_results.values()):
            break

        print(f"Retry {retry}/{retries} unsuccessful, result: {node_results}")

        if retry != retries:
            time.sleep(2**retry)
    else:
        msg = msg or f"Wrong '{what}' result"
        raise Exception(
            f"{msg}: {node_results}, expected: {expected} (after {retries} retries)"
        )
