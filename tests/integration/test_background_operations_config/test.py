import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# This test validates that background operations can use a dedicated 'background' user profile when it's configured.
# The feature and behavior is described at #93905
# We leverage 'sleepEachRow' for mutate queries to trigger operation timeout:
# - in cases where 'default' is used - mutations are expected to fail
# - in cases where the new 'background' profile is used - mutations will succeed, as the timeout will be increased

# We spin up a node with unique configuration for each scnenario to be checked
# 1. [EXPECTED FAILURE] Default configuration only, operation timeout is 3s and implicitly applied via 'default' profile
node1 = cluster.add_instance("node1")
# 2. [EXPECTED SUCCESS] 'background' profile sets timeout to 5s and mutations are using it
node2 = cluster.add_instance("node2", user_configs=['configs/background_profile.xml'])
# 3. [EXPECTED FAILURE] Default configuration only, operation timeout is set to 0.3s and we check that mutation are using it via 'default' profile
node3 = cluster.add_instance("node3", user_configs=['configs/tightened_default_profile.xml'])
# 4. [EXPECTED SUCCESS] 'background' profile sets timeout to 0.5s and mutations are using it
node4 = cluster.add_instance("node4", user_configs=["configs/tightened_background_profile.xml"])
# 5. [EXPECTED SUCCESS] 'custom' profile is set to be used by background operations, it sets timeout to 0.5s and mutations are using it
node5 = cluster.add_instance(
    "node5",
    main_configs=["configs/custom_server_config.xml"],
    user_configs=["configs/tightened_custom_profile.xml"]
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
  try:
      cluster.start()
      yield cluster
  finally:
      cluster.shutdown()

def get_table_name(instance):
    table_name = "test_table"
    return f"{table_name}_{instance.name}"


def prepare_data(instance):
  table_name = get_table_name(instance)

  instance.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

  instance.query(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      v1 UInt16
    ) ENGINE = MergeTree()
  """)

  instance.query(f"INSERT INTO {table_name} VALUES (1), (2), (3), (4)")

def assert_mutate_blocking(instance, sleep_sec_per_row, expected_failure=False):
  query = f"""
    ALTER TABLE {get_table_name(instance)} UPDATE v1 = (v1 + 1)
    WHERE sleepEachRow({sleep_sec_per_row})==0
    SETTINGS mutations_sync=1
  """

  if expected_failure:
    instance.query_and_get_error(query)
  else:
    instance.query(query)


def test_excpected_failure_with_default_profile(started_cluster):
  # this case is kinda redundant, it's destined to fire if the default for <function_sleep_max_microseconds_per_block> ever changes
  # and we'll have to adjust timeout expectations for this test
  prepare_data(instance=node1)
  assert_mutate_blocking(instance=node1, sleep_sec_per_row=1, expected_failure=True)
  print(f"Check 1: background operation failed as expected with all defaults")

def test_background_profile_configured_with_all_defaults(started_cluster):
  prepare_data(instance=node2)
  assert_mutate_blocking(instance=node2, sleep_sec_per_row=1)
  print(f"Check 2: background operation succeeds with 'background' profile minimally configured")

def test_excpected_failure_with_tightened_default_profile(started_cluster):
  prepare_data(instance=node3)
  assert_mutate_blocking(instance=node3, sleep_sec_per_row=0.1, expected_failure=True)
  print(f"Check 3: background operation failed as expected with a tighter query timeout")

def test_background_profile_configured(started_cluster):
  prepare_data(instance=node4)
  assert_mutate_blocking(instance=node4, sleep_sec_per_row=0.1)
  print(f"Check 4: background operation succeeds with 'background' profile configured")

def test_custom_background_profile_configured(started_cluster):
  prepare_data(instance=node5)
  assert_mutate_blocking(instance=node5, sleep_sec_per_row=0.1)
  print(f"Check 5: background operation succeeds with a custom background profile configured")
