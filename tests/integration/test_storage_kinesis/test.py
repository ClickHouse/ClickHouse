#!/usr/bin/env python3
import os
import sys
import pytest
import logging
import time
import json
import random
import base64

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KINESIS_LOCALSTACK_ENDPOINT_URL = "http://localstack:4566" 

cluster_fixture = ClickHouseCluster(__file__)
instance = cluster_fixture.add_instance('instance',
    main_configs=['configs/kinesis.xml'],
    with_localstack=True,
    stay_alive=True
)

def _run_aws_kinesis_command(cluster_obj: ClickHouseCluster, command_args: list, nothrow=False):
    localstack_container_id = cluster_obj.get_container_id('localstack')
    if not localstack_container_id:
        logger.error("Failed to get LocalStack container ID.")
        if nothrow:
            return "ERROR: LocalStack container ID not found" 
        raise Exception("Could not find LocalStack container.")

    base_command = f"aws --endpoint-url={KINESIS_LOCALSTACK_ENDPOINT_URL} kinesis"
    full_command = f"{base_command} {' '.join(command_args)}"
    logger.info(f"Executing AWS Kinesis command in LocalStack container ({localstack_container_id}): {full_command}")
    
    try:
        return cluster_obj.exec_in_container(localstack_container_id, ["bash", "-c", full_command], nothrow=nothrow)
    except Exception as e:
        logger.error(f"Command '{full_command}' failed in LocalStack container {localstack_container_id}: {e}")
        if nothrow:
            return f"ERROR: Command execution failed - {str(e)}"
        raise

def create_kinesis_stream(cluster_obj: ClickHouseCluster, stream_name: str, shard_count: int = 1) -> bool:
    logger.info(f"Attempting to create Kinesis stream: {stream_name} with {shard_count} shard(s)...")
    try:
        _run_aws_kinesis_command(
            cluster_obj,
            ["create-stream", "--stream-name", stream_name, "--shard-count", str(shard_count)]
        )
        logger.info(f"Create-stream command for {stream_name} successful. Waiting for stream to become active...")
        return wait_for_stream_active(cluster_obj, stream_name)

    except Exception as e:
        exception_str = str(e)
        if "ResourceInUseException" in exception_str:
            logger.warning(f"Stream {stream_name} already exists (ResourceInUseException caught). Verifying if it's active and has correct shard count...")
            
            try:
                response_str = _run_aws_kinesis_command(cluster_obj, ["describe-stream", "--stream-name", stream_name])
                response_json = json.loads(response_str)
                stream_description = response_json['StreamDescription']
                stream_status = stream_description['StreamStatus']
                shards = stream_description['Shards']
                
                active_shards_count = sum(1 for shard in shards if shard.get('SequenceNumberRange', {}).get('EndingSequenceNumber') is None)

                logger.info(f"Existing stream {stream_name} status: {stream_status}, active shards: {active_shards_count}, expected shards: {shard_count}")
                
                if stream_status == 'ACTIVE' and active_shards_count == shard_count:
                    logger.info(f"Existing stream {stream_name} is ACTIVE and has the correct shard count ({shard_count}).")
                    return True
                elif stream_status == 'CREATING' or stream_status == 'UPDATING':
                     logger.info(f"Existing stream {stream_name} is {stream_status}. Waiting for it to become active with correct shard count...")
                     return wait_for_stream_active(cluster_obj, stream_name) # wait_for_stream_active должен сам проверить статус
                else:
                    logger.error(f"Existing stream {stream_name} is in status {stream_status} or has {active_shards_count} active shards (expected {shard_count}). Cannot use.")
                    return False
            except Exception as desc_e:
                logger.error(f"Could not describe existing stream {stream_name} after ResourceInUseException: {desc_e}")
                return False
        else:
            logger.error(f"Failed to create Kinesis stream {stream_name} due to a non-ResourceInUseException: {exception_str}")
            return False

def wait_for_stream_active(cluster_obj: ClickHouseCluster, stream_name: str, timeout_seconds: int = 90) -> bool:
    logger.info(f"Waiting for Kinesis stream {stream_name} to become active...")
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            response_str = _run_aws_kinesis_command(cluster_obj, ["describe-stream", "--stream-name", stream_name])
            logger.debug(f"Describe stream '{stream_name}' response: {response_str.strip()}")
            response_json = json.loads(response_str) # AWS CLI обычно возвращает JSON
            stream_status = response_json['StreamDescription']['StreamStatus']
            logger.info(f"Stream {stream_name} status: {stream_status}")
            if stream_status == 'ACTIVE':
                logger.info(f"Stream {stream_name} is ACTIVE.")
                return True
            elif stream_status == 'DELETING':
                logger.error(f"Stream {stream_name} is DELETING. Cannot proceed.")
                return False
        except json.JSONDecodeError as je:
            logger.warning(f"Failed to parse JSON from describe-stream for {stream_name}: {response_str}. Error: {je}. Retrying...")
        except KeyError as ke:
            logger.warning(f"Unexpected JSON structure from describe-stream for {stream_name}: {response_str}. Missing key: {ke}. Retrying...")
        except Exception as e:
            logger.warning(f"Error describing Kinesis stream {stream_name}: {e}. Retrying...")
        time.sleep(5)
    logger.error(f"Timeout waiting for stream {stream_name} to become active.")
    return False

def delete_kinesis_stream(cluster_obj: ClickHouseCluster, stream_name: str) -> bool:
    logger.info(f"Attempting to delete Kinesis stream: {stream_name}...")
    response_str = ""
    try:
        response_str = _run_aws_kinesis_command(
            cluster_obj, 
            ["delete-stream", "--stream-name", stream_name, "--enforce-consumer-deletion"],
            nothrow=True
        )
        logger.info(f"Delete stream '{stream_name}' response: {response_str.strip()}")
        if "ResourceNotFoundException" in response_str:
            logger.warning(f"Stream {stream_name} not found during deletion, possibly already deleted.")
            return True
        if response_str.startswith("ERROR:"):
             logger.error(f"Failed to delete Kinesis stream {stream_name} due to: {response_str}")
             return False
        return True
    except Exception as e:
        logger.error(f"Exception during Kinesis stream deletion {stream_name}: {e}")
        return False

@pytest.fixture(scope="module")
def started_cluster():
    try:
        logger.info("Starting ClickHouse cluster for Kinesis tests...")
        cluster_fixture.start()
        ch_instance = cluster_fixture.instances['instance']
        ch_instance.query("CREATE DATABASE IF NOT EXISTS test")
        logger.info("ClickHouse cluster started and database 'test' created.")
        yield cluster_fixture, ch_instance 
    finally:
        logger.info("Shutting down ClickHouse cluster for Kinesis tests...")
        cluster_fixture.shutdown()
        logger.info("ClickHouse cluster shut down.")

@pytest.fixture(scope="function")
def kinesis_stream_manager(started_cluster):
    cluster_obj, _ = started_cluster
    
    stream_name = f"ch_test_kinesis_stream_{random.randint(10000, 99999)}_{int(time.time())}"
    
    logger.info(f"Kinesis stream fixture: creating stream {stream_name}")
    if not create_kinesis_stream(cluster_obj, stream_name):
        pytest.fail(f"Kinesis stream fixture: Failed to create Kinesis stream {stream_name}.")
    
    if not wait_for_stream_active(cluster_obj, stream_name):
        delete_kinesis_stream(cluster_obj, stream_name)
        pytest.fail(f"Kinesis stream fixture: Stream {stream_name} did not become active.")
        
    yield stream_name
    
    logger.info(f"Kinesis stream fixture: deleting stream {stream_name}")
    delete_kinesis_stream(cluster_obj, stream_name)


def check_result(result, reference):
    result_lines = sorted([line.strip() for line in result.strip().split('\n') if line.strip()])
    reference_lines = sorted([line.strip() for line in reference.strip().split('\n') if line.strip()])
    logger.info(f"Result lines: {result_lines}")
    logger.info(f"Reference lines: {reference_lines}")
    return result_lines == reference_lines

def test_kinesis_basics(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    stream_name = kinesis_stream_manager
    
    logger.info(f"Starting test_kinesis_basics for stream: {stream_name}...")

    kinesis_table_name = f'test.kinesis_table_{stream_name}'
    receiver_table_name = f'test.receiver_table_{stream_name}'

    try:
        logger.info(f"Creating Kinesis table: {kinesis_table_name} for stream {stream_name}")
        ch_instance.query(f"""
            CREATE TABLE {kinesis_table_name} (
                key UInt64,
                value String
            ) ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_max_records_per_request = 10,
                kinesis_starting_position = 'TRIM_HORIZON'
            """)

        logger.info(f"Inserting data into {kinesis_table_name}...")
        insert_query = f"INSERT INTO {kinesis_table_name} VALUES (1, 'kinesis_data_1'), (2, 'kinesis_data_2'), (3, 'kinesis_data_3')"
        ch_instance.query(insert_query)

        time.sleep(2) 

        ch_instance.query(f"""
            CREATE TABLE {receiver_table_name} (
                key UInt64,
                value String
            ) ENGINE = Memory
        """)
        ch_instance.query(f"""
            INSERT INTO {receiver_table_name}
            SELECT * FROM {kinesis_table_name}
        """)

        time.sleep(1)

        expected_data = "1\tkinesis_data_1\n2\tkinesis_data_2\n3\tkinesis_data_3"
        
        result = ch_instance.query(f'SELECT * FROM {receiver_table_name} ORDER BY key')
        assert check_result(result, expected_data)

    finally:
        logger.info(f"Cleaning up tables for stream {stream_name}...")
        ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_table_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {receiver_table_name}")
        
        logger.info(f"test_kinesis_basics for stream {stream_name} finished.")

def test_kinesis_materialized_view_streaming(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    stream_name = kinesis_stream_manager

    logger.info(f"Starting test_kinesis_materialized_view_streaming for stream: {stream_name}...")

    kinesis_source_table_name = f'test.kinesis_source_mv_table_{stream_name}'
    target_table_name = f'test.target_mv_table_{stream_name}'
    kinesis_mv_name = f'test.kinesis_mv_{stream_name}'

    try:
        logger.info(f"Creating Kinesis source table: {kinesis_source_table_name} for stream {stream_name}")
        ch_instance.query(f"""
            CREATE TABLE {kinesis_source_table_name} (
                key UInt64,
                value String
            ) ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_max_records_per_request = 10,
                kinesis_starting_position = 'TRIM_HORIZON' 
        """)

        logger.info(f"Creating target Memory table: {target_table_name}")
        ch_instance.query(f"""
            CREATE TABLE {target_table_name} (
                key UInt64,
                value String
            ) ENGINE = Memory
        """)

        logger.info(f"Creating Materialized View: {kinesis_mv_name} to {target_table_name} from {kinesis_source_table_name}")
        ch_instance.query(f"""
            CREATE MATERIALIZED VIEW {kinesis_mv_name} TO {target_table_name} AS
            SELECT key, value FROM {kinesis_source_table_name}
        """)

        logger.info(f"Inserting data into {kinesis_source_table_name}...")
        insert_data = [
            (1, 'mv_data_1'),
            (2, 'mv_data_2'),
            (3, 'mv_data_3')
        ]
        values_str = ", ".join([f"({row[0]}, '{row[1]}')" for row in insert_data])
        insert_query = f"INSERT INTO {kinesis_source_table_name} VALUES {values_str}"
        ch_instance.query(insert_query)
        logger.info("Data inserted into Kinesis source table.")

        time.sleep(2)
        expected_data = "\n".join([f"{row[0]}\t{row[1]}" for row in insert_data])
        

        result = ch_instance.query(f'SELECT * FROM {target_table_name} ORDER BY key')

        num_expected_rows = len(insert_data)
        num_actual_rows = len(result.strip().split('\n')) if result.strip() else 0

        assert num_actual_rows == num_expected_rows and check_result(result, expected_data)
        
    finally:
        logger.info(f"Cleaning up resources for stream {stream_name} in test_kinesis_materialized_view_streaming...")
        ch_instance.query(f"DROP VIEW IF EXISTS {kinesis_mv_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {target_table_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_source_table_name}")
        logger.info("Cleanup finished for test_kinesis_materialized_view_streaming.")

def test_kinesis_split_shard(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    stream_name = kinesis_stream_manager # Эта фикстура уже создает стрим с 1 шардом

    logger.info(f"Starting test_kinesis_split_shard for stream: {stream_name}...")

    kinesis_table_name = f'test.kinesis_split_table_{stream_name}'
    target_table_name = f'test.target_split_table_{stream_name}'
    kinesis_mv_name = f'test.kinesis_split_mv_{stream_name}'

    initial_data = [(1, 'split_data_A1'), (2, 'split_data_A2'), (3, 'split_data_A3')]
    data_after_split = [(101, 'split_data_B1'), (102, 'split_data_B2'), (201, 'split_data_C1')] 
    all_expected_data_tuples = sorted(initial_data + data_after_split)
    all_expected_data_str = "\n".join([f"{row[0]}\t{row[1]}" for row in all_expected_data_tuples])

    try:
        describe_response_str = _run_aws_kinesis_command(cluster_obj, ["describe-stream", "--stream-name", stream_name])
        describe_response_json = json.loads(describe_response_str)
        initial_shards = describe_response_json['StreamDescription']['Shards']
        assert len(initial_shards) == 1, f"Expected 1 initial shard, got {len(initial_shards)}"
        shard_to_split_id = initial_shards[0]['ShardId']
        new_starting_hash_key_for_split = "170141183460469231731687303715884105728" 
        logger.info(f"Initial shard to split: {shard_to_split_id}. New starting hash key for split: {new_starting_hash_key_for_split}")

        logger.info(f"Creating Kinesis table: {kinesis_table_name} for stream {stream_name}")
        ch_instance.query(f"""
            CREATE TABLE {kinesis_table_name} (
                key UInt64,
                value String
                {{additional_table_options}}
            ) ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_max_records_per_request = 10, 
                kinesis_starting_position = 'TRIM_HORIZON'
        """.format(additional_table_options=""))

        logger.info(f"Creating target Memory table: {target_table_name}")
        ch_instance.query(f"""
            CREATE TABLE {target_table_name} (
                key UInt64,
                value String
            ) ENGINE = Memory
        """)

        logger.info(f"Creating Materialized View: {kinesis_mv_name} to {target_table_name} from {kinesis_table_name}")
        ch_instance.query(f"""
            CREATE MATERIALIZED VIEW {kinesis_mv_name} TO {target_table_name} AS
            SELECT key, value FROM {kinesis_table_name}
        """)

        logger.info(f"Inserting initial data into {kinesis_table_name} (before split)...")
        values_str_initial = ", ".join([f"({row[0]}, '{row[1]}')" for row in initial_data])
        ch_instance.query(f"INSERT INTO {kinesis_table_name} VALUES {values_str_initial}")
        logger.info("Initial data inserted.")

        time.sleep(5)

        logger.info(f"Splitting shard {shard_to_split_id} for stream {stream_name}...")
        _run_aws_kinesis_command(cluster_obj, [
            "split-shard",
            "--stream-name", stream_name,
            "--shard-to-split", shard_to_split_id,
            "--new-starting-hash-key", new_starting_hash_key_for_split
        ])
        logger.info("Split-shard command issued. Waiting for stream to become active again...")
        
        if not wait_for_stream_active(cluster_obj, stream_name, timeout_seconds=120):
            pytest.fail(f"Stream {stream_name} did not become active after split-shard.")
        
        time.sleep(10)
        describe_response_str_after_split = _run_aws_kinesis_command(cluster_obj, ["describe-stream", "--stream-name", stream_name])
        describe_response_json_after_split = json.loads(describe_response_str_after_split)
        shards_after_split = describe_response_json_after_split['StreamDescription']['Shards']
        active_shards_after_split = [s for s in shards_after_split if s.get('SequenceNumberRange', {}).get('EndingSequenceNumber') is None]
        
        logger.info(f"Shards description after split: {json.dumps(describe_response_json_after_split, indent=2)}")
        assert len(active_shards_after_split) == 2, f"Expected 2 active shards after split, got {len(active_shards_after_split)}. Total shards reported: {len(shards_after_split)}"
        logger.info(f"Stream has {len(active_shards_after_split)} active shards after split. Parent shard was {shard_to_split_id}.")

        logger.info(f"Inserting data into {kinesis_table_name} (after split)...")
        values_str_after_split = ", ".join([f"({row[0]}, '{row[1]}')" for row in data_after_split])
        ch_instance.query(f"INSERT INTO {kinesis_table_name} VALUES {values_str_after_split}")
        logger.info("Data after split inserted.")

        logger.info(f"Verifying all data in {target_table_name}...")
        query_attempts = 10
        wait_interval = 5
        data_verified = False
        last_result_text = ""

        for attempt in range(query_attempts):
            logger.info(f"Verification attempt {attempt + 1}/{query_attempts} for all data in {target_table_name}...")
            time.sleep(wait_interval)
            
            current_result_text = ch_instance.query(f'SELECT * FROM {target_table_name} ORDER BY key, value').strip()
            last_result_text = current_result_text
            
            num_expected_rows = len(all_expected_data_tuples)
            current_rows = [line.strip() for line in current_result_text.split('\n') if line.strip()]
            num_actual_rows = len(current_rows)

            logger.info(f"Expected {num_expected_rows} rows. Got {num_actual_rows} rows. Current data:\n{current_result_text}")

            if num_actual_rows == num_expected_rows:
                if check_result(current_result_text, all_expected_data_str):
                    data_verified = True
                    logger.info(f"All data successfully verified in {target_table_name}.")
                    break
                else:
                    logger.warning(f"Row count matches ({num_actual_rows}), but content differs.")
            elif num_actual_rows < num_expected_rows:
                 logger.warning(f"Not all data arrived yet. Expected {num_expected_rows} rows, got {num_actual_rows}.")
            else:
                logger.error(f"Too many rows. Expected {num_expected_rows}, got {num_actual_rows}.")
        
        assert data_verified, f"Data verification failed after {query_attempts} attempts. Expected:\n{all_expected_data_str}\nGot:\n{last_result_text}"
        logger.info("test_kinesis_split_shard successfully verified all data.")

    finally:
        logger.info(f"Cleaning up resources for stream {stream_name} in test_kinesis_split_shard...")
        ch_instance.query(f"DROP VIEW IF EXISTS {kinesis_mv_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {target_table_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_table_name}")
        logger.info("Cleanup finished for test_kinesis_split_shard.")

def test_kinesis_merge_shards(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    base_stream_name = kinesis_stream_manager 
    
    logger.info(f"test_kinesis_merge_shards: Original stream {base_stream_name} (1 shard) will be replaced.")
    delete_kinesis_stream(cluster_obj, base_stream_name)
    logger.info(f"Waiting 5 seconds after deleting stream {base_stream_name} before recreating...")
    time.sleep(5)
    
    stream_name = base_stream_name
    initial_shard_count = 2
    if not create_kinesis_stream(cluster_obj, stream_name, shard_count=initial_shard_count):
        pytest.fail(f"Failed to create Kinesis stream {stream_name} with {initial_shard_count} shards.")
    
    logger.info(f"Successfully created or confirmed stream {stream_name} with {initial_shard_count} shards for merge test.")

    kinesis_table_name = f'test.kinesis_merge_table_{stream_name}'
    target_table_name = f'test.target_merge_table_{stream_name}'
    kinesis_mv_name = f'test.kinesis_merge_mv_{stream_name}'

    data_before_merge = [(1, 'merge_data_A1'), (200, 'merge_data_A2'), (3, 'merge_data_A3'), (300, 'merge_data_A4')]
    data_after_merge = [(501, 'merge_data_B1'), (502, 'merge_data_B2')]
    all_expected_data_tuples = sorted(data_before_merge + data_after_merge)
    all_expected_data_str = "\n".join([f"{row[0]}\t{row[1]}" for row in all_expected_data_tuples])

    try:
        describe_response_str = _run_aws_kinesis_command(cluster_obj, ["describe-stream", "--stream-name", stream_name])
        describe_response_json = json.loads(describe_response_str)
        initial_shards_info = describe_response_json['StreamDescription']['Shards']
        logger.info(f"Initial shards info: {json.dumps(initial_shards_info, indent=2)}")
        
        active_initial_shards = [s for s in initial_shards_info if s.get('SequenceNumberRange', {}).get('EndingSequenceNumber') is None]
        assert len(active_initial_shards) == initial_shard_count, f"Expected {initial_shard_count} active initial shards, got {len(active_initial_shards)}"

        shard_to_merge_1_id = active_initial_shards[0]['ShardId']
        shard_to_merge_2_id = active_initial_shards[1]['ShardId']
        logger.info(f"Shards to merge: {shard_to_merge_1_id} and {shard_to_merge_2_id}")

        logger.info(f"Creating Kinesis table: {kinesis_table_name} for stream {stream_name}")
        ch_instance.query(f"""
            CREATE TABLE {kinesis_table_name} (
                key UInt64,
                value String
            ) ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_max_records_per_request = 10,
                kinesis_starting_position = 'TRIM_HORIZON'
        """)

        logger.info(f"Creating target Memory table: {target_table_name}")
        ch_instance.query(f"""
            CREATE TABLE {target_table_name} (
                key UInt64,
                value String
            ) ENGINE = Memory
        """)

        logger.info(f"Creating Materialized View: {kinesis_mv_name}")
        ch_instance.query(f"""
            CREATE MATERIALIZED VIEW {kinesis_mv_name} TO {target_table_name} AS
            SELECT key, value FROM {kinesis_table_name}
        """)

        logger.info(f"Inserting data into {kinesis_table_name} (before merge)...")
        values_str_before = ", ".join([f"({row[0]}, '{row[1]}')" for row in data_before_merge])
        ch_instance.query(f"INSERT INTO {kinesis_table_name} VALUES {values_str_before}")
        logger.info("Data before merge inserted.")
        time.sleep(5)

        logger.info(f"Merging shards {shard_to_merge_1_id} and {shard_to_merge_2_id} for stream {stream_name}...")
        _run_aws_kinesis_command(cluster_obj, [
            "merge-shards",
            "--stream-name", stream_name,
            "--shard-to-merge", shard_to_merge_1_id,
            "--adjacent-shard-to-merge", shard_to_merge_2_id
        ])
        logger.info("Merge-shards command issued. Waiting for stream to become active again...")
        
        if not wait_for_stream_active(cluster_obj, stream_name, timeout_seconds=120):
            pytest.fail(f"Stream {stream_name} did not become active after merge-shards.")

        time.sleep(10)
        describe_response_str_after_merge = _run_aws_kinesis_command(cluster_obj, ["describe-stream", "--stream-name", stream_name])
        describe_response_json_after_merge = json.loads(describe_response_str_after_merge)
        shards_after_merge = describe_response_json_after_merge['StreamDescription']['Shards']
        active_shards_after_merge = [s for s in shards_after_merge if s.get('SequenceNumberRange', {}).get('EndingSequenceNumber') is None]
        
        logger.info(f"Shards description after merge: {json.dumps(describe_response_json_after_merge, indent=2)}")
        assert len(active_shards_after_merge) == 1, f"Expected 1 active shard after merge, got {len(active_shards_after_merge)}. Total shards reported: {len(shards_after_merge)}"
        logger.info(f"Stream has {len(active_shards_after_merge)} active shard after merge.")

        logger.info(f"Inserting data into {kinesis_table_name} (after merge)...")
        values_str_after = ", ".join([f"({row[0]}, '{row[1]}')" for row in data_after_merge])
        ch_instance.query(f"INSERT INTO {kinesis_table_name} VALUES {values_str_after}")
        logger.info("Data after merge inserted.")

        logger.info(f"Verifying all data in {target_table_name}...")
        query_attempts = 12 
        wait_interval = 5 
        data_verified = False
        last_result_text = ""

        for attempt in range(query_attempts):
            logger.info(f"Verification attempt {attempt + 1}/{query_attempts} for all data in {target_table_name}...")
            time.sleep(wait_interval)
            current_result_text = ch_instance.query(f'SELECT * FROM {target_table_name} ORDER BY key, value').strip()
            last_result_text = current_result_text
            num_expected_rows = len(all_expected_data_tuples)
            current_rows = [line.strip() for line in current_result_text.split('\n') if line.strip()]
            num_actual_rows = len(current_rows)
            logger.info(f"Expected {num_expected_rows} rows. Got {num_actual_rows} rows. Current data:\n{current_result_text}")
            if num_actual_rows == num_expected_rows and check_result(current_result_text, all_expected_data_str):
                data_verified = True
                logger.info(f"All data successfully verified in {target_table_name}.")
                break
            elif num_actual_rows < num_expected_rows:
                 logger.warning(f"Not all data arrived yet. Expected {num_expected_rows} rows, got {num_actual_rows}.")
            else:
                logger.error(f"Too many or mismatched rows. Expected {num_expected_rows}, got {num_actual_rows}.")
        
        assert data_verified, f"Data verification failed. Expected:\n{all_expected_data_str}\nGot:\n{last_result_text}"
        logger.info("test_kinesis_merge_shards successfully verified all data.")

    finally:
        logger.info(f"Cleaning up resources for stream {stream_name} in test_kinesis_merge_shards...")
        ch_instance.query(f"DROP VIEW IF EXISTS {kinesis_mv_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {target_table_name}")
        ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_table_name}")
        logger.info("Cleanup finished for test_kinesis_merge_shards.")

def test_kinesis_select_inserts(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    stream_name = kinesis_stream_manager
    
    logger.info(f"Starting test_kinesis_select_inserts for stream: {stream_name}...")
    
    kinesis_source_table_name = f'test.kinesis_select_inserts_source_{stream_name}'

    data_batch_1 = [(1, 'trim_horizon_A1'), (2, 'trim_horizon_A2')]

    logger.info(f"Creating Kinesis source table: {kinesis_source_table_name} with TRIM_HORIZON")
    ch_instance.query(f"""
        CREATE TABLE {kinesis_source_table_name} (
            key UInt64,
            value String
        ) ENGINE = Kinesis
        SETTINGS
            kinesis_stream_name = '{stream_name}',
            kinesis_format = 'JSONEachRow',
            kinesis_num_consumers = 1,
            kinesis_save_checkpoints = true
    """)
    
    logger.info(f"Inserting data_batch_1 into {kinesis_source_table_name}...")
    values_str_1 = ", ".join([f"({row[0]}, '{row[1]}')" for row in data_batch_1])
    ch_instance.query(f"INSERT INTO {kinesis_source_table_name} VALUES {values_str_1}")
    logger.info("data_batch_1 inserted.")   
    
    time.sleep(10)
    
    select_res = ch_instance.query(f"SELECT * FROM {kinesis_source_table_name} ORDER BY key, value")
    logger.info(f"SELECT result: {select_res}")
    assert select_res == "1\ttrim_horizon_A1\n2\ttrim_horizon_A2\n", "SELECT result is not as expected"
    
    ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_source_table_name}")

def test_kinesis_restore_disabled(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    stream_name = kinesis_stream_manager
    
    logger.info(f"Starting test_kinesis_restore_disabled for stream: {stream_name}...")
    
    kinesis_source_table_name = f'test.kinesis_restore_disabled_source_{stream_name}'

    data_batch_1 = [(1, 'trim_horizon_A1'), (2, 'trim_horizon_A2')]

    logger.info(f"Creating Kinesis source table: {kinesis_source_table_name} with TRIM_HORIZON")
    ch_instance.query(f"""
        CREATE TABLE {kinesis_source_table_name} (
            key UInt64,
            value String
        ) ENGINE = Kinesis
        SETTINGS
            kinesis_stream_name = '{stream_name}',
            kinesis_format = 'JSONEachRow',
            kinesis_num_consumers = 1,
            kinesis_save_checkpoints = false
    """)
    
    logger.info(f"Inserting data_batch_1 into {kinesis_source_table_name}...")
    values_str_1 = ", ".join([f"({row[0]}, '{row[1]}')" for row in data_batch_1])
    ch_instance.query(f"INSERT INTO {kinesis_source_table_name} VALUES {values_str_1}")
    logger.info("data_batch_1 inserted.")   
    
    time.sleep(10)
    
    select_res = ch_instance.query(f"SELECT * FROM {kinesis_source_table_name} ORDER BY key, value")
    logger.info(f"SELECT result: {select_res}")
    assert select_res == "1\ttrim_horizon_A1\n2\ttrim_horizon_A2\n", "SELECT result is not as expected"
    
    logger.info(f"Restarting ClickHouse server instance...")
    ch_instance.restart_clickhouse()
    time.sleep(2)
    assert ch_instance.query("SELECT 1").strip() == "1", "ClickHouse server did not restart properly."
    logger.info("ClickHouse server restarted successfully.")
    
    select_2_res = ch_instance.query(f"SELECT * FROM {kinesis_source_table_name} ORDER BY key, value")
    logger.info(f"SELECT result: {select_2_res}")
    
    assert select_res == select_2_res, "SELECT results are not the same after restart"
    
    ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_source_table_name}")

def test_kinesis_restore_enabled(started_cluster, kinesis_stream_manager):
    cluster_obj, ch_instance = started_cluster
    stream_name = kinesis_stream_manager
    
    logger.info(f"Starting test_kinesis_restore_enabled for stream: {stream_name}...")
    
    kinesis_source_table_name = f'test.kinesis_restore_enabled_source_{stream_name}'

    data_batch_1 = [(1, 'trim_horizon_A1'), (2, 'trim_horizon_A2')]

    logger.info(f"Creating Kinesis source table: {kinesis_source_table_name} with TRIM_HORIZON")
    ch_instance.query(f"""
        CREATE TABLE {kinesis_source_table_name} (
            key UInt64,
            value String
        ) ENGINE = Kinesis
        SETTINGS
            kinesis_stream_name = '{stream_name}',
            kinesis_format = 'JSONEachRow',
            kinesis_num_consumers = 1,
            kinesis_save_checkpoints = true
    """)
    
    logger.info(f"Inserting data_batch_1 into {kinesis_source_table_name}...")
    values_str_1 = ", ".join([f"({row[0]}, '{row[1]}')" for row in data_batch_1])
    ch_instance.query(f"INSERT INTO {kinesis_source_table_name} VALUES {values_str_1}")
    logger.info("data_batch_1 inserted.")   
    
    time.sleep(2)
    
    select_res = ch_instance.query(f"SELECT * FROM {kinesis_source_table_name} ORDER BY key, value")
    logger.info(f"SELECT result: {select_res}")
    assert select_res == "1\ttrim_horizon_A1\n2\ttrim_horizon_A2\n", "SELECT result is not as expected"
    
    logger.info(f"Restarting ClickHouse server instance...")
    ch_instance.restart_clickhouse()
    time.sleep(2)
    assert ch_instance.query("SELECT 1").strip() == "1", "ClickHouse server did not restart properly."
    logger.info("ClickHouse server restarted successfully.")
    
    select_2_res = ch_instance.query(f"SELECT * FROM {kinesis_source_table_name} ORDER BY key, value")
    logger.info(f"SELECT result: {select_2_res}")
    
    # second select should return empty result
    assert select_2_res == "", "SELECT results are not empty after restart"
    
    ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_source_table_name}")


# Disabled because of EFO needs HTTP2 Client Factory, but table engine uses HTTP1.1 now

# def test_kinesis_enhanced_consumer(started_cluster, kinesis_stream_manager):
#     cluster_obj, ch_instance = started_cluster
#     stream_name = kinesis_stream_manager

#     logger.info(f"Starting test_kinesis_enhanced_consumer for stream: {stream_name}...")

#     kinesis_source_table_name = f'test.kinesis_enhanced_consumer_source_{stream_name}'
#     target_table_name = f'test.target_enhanced_consumer_table_{stream_name}'
#     kinesis_mv_name = f'test.kinesis_enhanced_consumer_mv_{stream_name}'

#     try:
#         logger.info(f"Creating Kinesis source table: {kinesis_source_table_name} for stream {stream_name}")
#         ch_instance.query(f"""
#             CREATE TABLE {kinesis_source_table_name} (
#                 key UInt64,
#                 value String
#             ) ENGINE = Kinesis
#             SETTINGS
#                 kinesis_stream_name = '{stream_name}',
#                 kinesis_format = 'JSONEachRow',
#                 kinesis_starting_position = 'TRIM_HORIZON',
#                 kinesis_enhanced_fan_out = true,
#                 kinesis_consumer_name = 'test_consumer',
#                 kinesis_max_execution_time_ms = 10000
#         """)

#         logger.info(f"Creating target Memory table: {target_table_name}")
#         ch_instance.query(f"""
#             CREATE TABLE {target_table_name} (
#                 key UInt64,
#                 value String
#             ) ENGINE = Memory
#         """)

#         logger.info(f"Creating Materialized View: {kinesis_mv_name} to {target_table_name} from {kinesis_source_table_name}")
#         ch_instance.query(f"""
#             CREATE MATERIALIZED VIEW {kinesis_mv_name} TO {target_table_name} AS
#             SELECT key, value FROM {kinesis_source_table_name}
#         """)

#         logger.info(f"Inserting data into {kinesis_source_table_name}...")
#         insert_data = [
#             (1, 'mv_data_1'),
#             (2, 'mv_data_2'),
#             (3, 'mv_data_3')
#         ]
#         values_str = ", ".join([f"({row[0]}, '{row[1]}')" for row in insert_data])
#         insert_query = f"INSERT INTO {kinesis_source_table_name} VALUES {values_str}"
#         ch_instance.query(insert_query)
#         logger.info("Data inserted into Kinesis source table.")

#         time.sleep(2)
#         expected_data = "\n".join([f"{row[0]}\t{row[1]}" for row in insert_data])
        

#         result = ch_instance.query(f'SELECT * FROM {target_table_name} ORDER BY key')

#         num_expected_rows = len(insert_data)
#         num_actual_rows = len(result.strip().split('\n')) if result.strip() else 0

#         assert num_actual_rows == num_expected_rows and check_result(result, expected_data)
        
#     finally:
#         logger.info(f"Cleaning up resources for stream {stream_name} in test_kinesis_enhanced_consumer...")
#         ch_instance.query(f"DROP VIEW IF EXISTS {kinesis_mv_name}")
#         ch_instance.query(f"DROP TABLE IF EXISTS {target_table_name}")
#         ch_instance.query(f"DROP TABLE IF EXISTS {kinesis_source_table_name}")
#         logger.info("Cleanup finished for test_kinesis_enhanced_consumer.")
    

if __name__ == '__main__':
    logger.info("Starting Kinesis test script for manual execution (not recommended for AWS CLI interactions without proper setup)...")
    
    print("Manual execution of this script is complex due to LocalStack and AWS CLI setup.")
    print("Please use 'pytest tests/integration/test_storage_kinesis/test.py' instead.")
