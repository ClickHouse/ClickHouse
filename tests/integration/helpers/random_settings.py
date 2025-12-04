import random
from collections.abc import Iterator


def randomize_settings() -> Iterator[tuple[str, int | str]]:
    yield "max_joined_block_size_rows", random.randint(8000, 100000)
    if random.random() < 0.5:
        yield "max_block_size", random.randint(8000, 100000)
    if random.random() < 0.5:
        yield "query_plan_join_swap_table", random.choice(["auto", "true", "false"])


def write_random_settings_config(destination: str) -> None:
    with open(destination, "w") as f:
        _ = f.write(
            """
<clickhouse>
    <profiles>
        <default>
"""
        )
        for setting, value in randomize_settings():
            f.write(f"<{setting}>{value}</{setting}>\n")

        _ = f.write(
            """
        </default>
    </profiles>
</clickhouse>
"""
        )
