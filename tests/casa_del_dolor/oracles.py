import random
from integration.helpers.client import Client
from integration.helpers.cluster import ClickHouseInstance


final_supported_engines = [
    'ReplacingMergeTree',
    'CoalescingMergeTree',
    'SummingMergeTree',
    'AggregatingMergeTree',
    'CollapsingMergeTree',
    'VersionedCollapsingMergeTree',
    'GraphiteMergeTree'
]


def dump_table(instances: list[ClickHouseInstance], logger):
    next_node = random.choice(instances)

    client = Client(host=next_node.ip_address)

    tables_str = client.query("""
        SELECT database, name, engine
        FROM system.tables
        WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
    """)

    if tables_str == ""
        return
    tables = [tuple(line.split('\t')) for line in tables_str.split('\n') if line]
    random_table = random.choice(tables)[0]

    logger.info(f"Dumping table: {random_table[0]}.{random_table[1]} before a restart")

    query = f"SELECT * FROM `{random_table[0]}`.`{random_table[1]}`{" FINAL" if random_table[2] in final_supported_engines else ""} ORDER BY ALL INTO OUTFILE '';"

# 5. Execute and dump to CSV
data = client.execute(query, with_column_types=True)
columns = [col[0] for col in data[1]]  # Extract column names
df = pd.DataFrame(data[0], columns=columns)

output_file = f"{random_table}_dump.csv"
df.to_csv(output_file, index=False)

print(f"Data dumped to: {output_file}")
print(f"Used FINAL: {use_final} | Engine: {table_engine}")
