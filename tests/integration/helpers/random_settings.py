import random


def randomize_settings():
    yield "max_joined_block_size_rows", random.randint(8000, 100000)
    if random.random() < 0.5:
        yield "max_block_size", random.randint(8000, 100000)
    if random.random() < 0.5:
        yield "query_plan_join_inner_table_selection", random.choice(["auto", "left"])


def write_random_settings_config(destination):
    with open(destination, "w") as f:
        f.write(
            """
<clickhouse>
    <profiles>
        <default>
"""
        )
        for setting, value in randomize_settings():
            f.write(f"<{setting}>{value}</{setting}>\n")

        f.write(
            """
        </default>
    </profiles>
</clickhouse>
"""
        )
