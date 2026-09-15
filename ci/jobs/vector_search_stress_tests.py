# vector_search_stress_tests.py : Stress testing of ClickHouse Vector Search
# Documentation : https://clickhouse.com/docs/engines/table-engines/mergetree-family/annindexes

import os
import random
import sys
import threading
import time
import traceback

import clickhouse_connect
import numpy as np

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"

TABLE = "table"
S3_URLS = "s3urls"
SCHEMA = "schema"
ID_COLUMN = "id_column"
VECTOR_COLUMN = "vector_column"
DISTANCE_METRIC = "distance_metric"
DIMENSION = "dimension"
SOURCE_SELECT_LIST = "source_select_list"
FETCH_COLUMNS_LIST = "fetch_columns_list"
MERGE_TREE_SETTINGS = "merge_tree_settings"
OTHER_SETTINGS = "other_settings"

LIMIT_N = "limit"
TRUTH_SET_FILES = "truth_set_files"
QUANTIZATION = "quantization"
HNSW_M = "hnsw_M"
HNSW_EF_CONSTRUCTION = "hnsw_ef_construction"
HNSW_EF_SEARCH = "hnsw_ef_search"
VECTOR_SEARCH_INDEX_FETCH_MULTIPLIER = "vector_search_index_fetch_multiplier"
GENERATE_TRUTH_SET = "generate_truth_set"
TRUTH_SET_COUNT = "truth_set_count"
RECALL_K = "recall_k"
NEW_TRUTH_SET_FILE = "new_truth_set_file"
CONCURRENCY_TEST = "concurrency_test"

dataset_hackernews_openai = {
    TABLE: "hackernews_openai",
    S3_URLS: [
        "https://clickhouse-datasets.s3.amazonaws.com/hackernews-openai/hackernews_openai_part_1_of_1.parquet",
    ],
    SCHEMA: """
        id            UInt32,
        type          Enum8('story' = 1, 'comment' = 2, 'poll' = 3, 'pollopt' = 4, 'job' = 5),
        time          DateTime,
        update_time   DateTime,
        url           String,
        title         String,
        text          String,
        vector        Array(Float32)
     """,
    ID_COLUMN: "id",
    VECTOR_COLUMN: "vector",
    DISTANCE_METRIC: "cosineDistance",
    DIMENSION: 1536,
    SOURCE_SELECT_LIST: None,
}

# The full 100M vectors - will take hours to run
dataset_laion_5b_100m = {
    TABLE: "laion5b_100m",
    # individual files, so that load progress can be seen
    S3_URLS: [
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_1_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_2_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_3_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_4_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_5_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_6_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_7_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_8_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_9_of_10.parquet",
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_10_of_10.parquet",
    ],
    SCHEMA: """
     """,
    ID_COLUMN: "id",
    VECTOR_COLUMN: "vector",
    DISTANCE_METRIC: "cosineDistance",
    DIMENSION: 768,
    FETCH_COLUMNS_LIST: "url, width, height",  # fetch additional columns in ANN
}

# Dataset with <= 10 million subset of LAION
dataset_laion_5b_mini_for_quick_test = {
    TABLE: "laion_test",
    S3_URLS: [
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion5b_100m_part_1_of_10.parquet"
    ],
    SCHEMA: """
        id Int32,
        vector Array(Float32)
     """,
    ID_COLUMN: "id",
    VECTOR_COLUMN: "vector",
    SOURCE_SELECT_LIST: "id, vector",  # Columns to select from the source Parquet file
    DISTANCE_METRIC: "cosineDistance",
    DIMENSION: 768,
}

test_params_laion_5b_full_run = {
    LIMIT_N: None,
    # Pass a filename to reuse a pre-generated truth set, else test will generate truth set (default)
    # Running 10000 brute force KNN queries over a 100 million dataset could take time.
    TRUTH_SET_FILES: [
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/truth_set_10k.tar"
    ],
    QUANTIZATION: "bf16",  # 'b1' for binary quantization
    HNSW_M: 64,
    HNSW_EF_CONSTRUCTION: 512,
    HNSW_EF_SEARCH: None,  # Default in CH is 256, use higher value for maybe 'b1' indexes
    VECTOR_SEARCH_INDEX_FETCH_MULTIPLIER: None,  # Set a value for 'b1' indexes
    GENERATE_TRUTH_SET: False,
    MERGE_TREE_SETTINGS: None,
    OTHER_SETTINGS: None,
    CONCURRENCY_TEST: True,
}

test_params_laion_5b_quick_test = {
    LIMIT_N: 100000,  # Adds a LIMIT clause to load exact number of rows
    TRUTH_SET_FILES: [
        "https://clickhouse-datasets.s3.amazonaws.com/laion-5b/laion_100k_1k.tar"
    ],
    QUANTIZATION: "bf16",
    HNSW_M: 16,
    HNSW_EF_CONSTRUCTION: 256,
    HNSW_EF_SEARCH: None,
    VECTOR_SEARCH_INDEX_FETCH_MULTIPLIER: None,
    GENERATE_TRUTH_SET: False,
    TRUTH_SET_COUNT: 1000,  # Quick test! 10000 or 1000 is a good value
    RECALL_K: 100,
    NEW_TRUTH_SET_FILE: "laion_100k_1k_new",
    MERGE_TREE_SETTINGS: None,
    OTHER_SETTINGS: None,
    CONCURRENCY_TEST: True,
}

test_params_laion_5b_1m = {
    LIMIT_N: 1000000,  # Adds a LIMIT clause to load exact number of rows
    TRUTH_SET_FILES: None,
    QUANTIZATION: "bf16",
    HNSW_M: 64,
    HNSW_EF_CONSTRUCTION: 256,
    HNSW_EF_SEARCH: None,
    VECTOR_SEARCH_INDEX_FETCH_MULTIPLIER: None,
    GENERATE_TRUTH_SET: True,  # Will take some time!
    TRUTH_SET_COUNT: 10000,  # Quick test! 10000 or 1000 is a good value
    RECALL_K: 100,
    NEW_TRUTH_SET_FILE: "laion_1m_10k",
    MERGE_TREE_SETTINGS: None,
    OTHER_SETTINGS: None,
    CONCURRENCY_TEST: True,
}

test_params_hackernews_10m = {
    LIMIT_N: None,
    TRUTH_SET_FILES: [
        "https://clickhouse-datasets.s3.amazonaws.com/hackernews-openai/hackernews_openai_10m_1k.tar"
    ],
    QUANTIZATION: "bf16",
    HNSW_M: 64,
    HNSW_EF_CONSTRUCTION: 256,
    HNSW_EF_SEARCH: None,
    VECTOR_SEARCH_INDEX_FETCH_MULTIPLIER: None,
    GENERATE_TRUTH_SET: False,
    NEW_TRUTH_SET_FILE: None,
    TRUTH_SET_COUNT: 1000,
    RECALL_K: 100,
    MERGE_TREE_SETTINGS: None,
    OTHER_SETTINGS: None,
    CONCURRENCY_TEST: True,
}


def get_new_connection():
    chclient = clickhouse_connect.get_client(send_receive_timeout=1800)
    return chclient


def current_time_ms():
    return round(time.time() * 1000)


def current_time():
    return time.ctime(time.time())


def logger(s):
    print(current_time(), " : ", s)


class RunTest:

    def __init__(self, chclient, dataset, test_params):
        self._chclient = chclient
        self._dataset = dataset
        self._test_params = test_params

        self._table = dataset[TABLE]
        self._id_column = dataset[ID_COLUMN]
        self._vector_column = dataset[VECTOR_COLUMN]
        self._distance_metric = dataset[DISTANCE_METRIC]
        self._dimension = dataset[DIMENSION]

        self._query_count = int(test_params[TRUTH_SET_COUNT])
        self._k = int(test_params[RECALL_K])

        self._truth_set = {}
        self._result_set = {}

    def load_data(self):
        logger(f"Begin loading data into {self._table}")

        create_table = f"CREATE TABLE {self._table} ( {self._dataset[SCHEMA]} ) ENGINE = MergeTree ORDER BY {self._id_column}"
        self._chclient.query(create_table)

        for url in self._dataset[S3_URLS]:
            logger(f"Loading rows from location : {url}")
            select_list = "*"

            # Exact columns to read from the source files?
            if self._dataset[SOURCE_SELECT_LIST] is not None:
                select_list = self._dataset[SOURCE_SELECT_LIST]

            insert = f"INSERT INTO {self._table} SELECT {select_list} FROM s3('{url}')"

            if self._test_params[LIMIT_N] is not None:
                insert = (
                    insert
                    + f" ORDER BY {self._id_column} LIMIT {self._test_params[LIMIT_N]}"
                )
            self._chclient.query(insert)

            result = self._chclient.query(f"SELECT count() FROM {self._table}")
            rows = result.result_rows[0][0]
            self._rows_inserted = rows
            logger(f"Loaded total {rows} rows")

    def optimize_table(self):
        logger("Optimizing table...")
        # Wait for existing merges to complete
        while True:
            result = self._chclient.query(
                f"SELECT COUNT(*) FROM system.merges WHERE table = '{self._table}'"
            )
            if result.result_rows[0][0] == 0:
                break
            logger("Waiting for existing merges to complete...")
            time.sleep(5)

        try:
            self._chclient.query(
                f"OPTIMIZE TABLE {self._table} FINAL SETTINGS mutations_sync=0"
            )
        except Exception as e:
            logger(f"optimize table error {e}")

        # Wait for OPTIMIZE to complete
        while True:
            result = self._chclient.query(
                f"SELECT COUNT(*) FROM system.merges WHERE table = '{self._table}'"
            )
            if result.result_rows[0][0] == 0:
                break
            logger("Waiting for optimize table to complete...")
            time.sleep(5)

        result = self._chclient.query(
            f"SELECT name, formatReadableSize(bytes) FROM system.parts WHERE table = '{self._table}' AND active=1"
        )
        logger(f"Current active parts found for {self._table} -> ")
        for row in result.result_rows:
            logger(f"{row[0]}\t\t{row[1]} bytes")

    # Runs ALTER TABLE ... ADD INDEX and then MATERIALIZE INDEX
    def build_index(self):
        logger("Adding vector similarity index")
        quantization = self._test_params[QUANTIZATION]
        hnsw_M = self._test_params[HNSW_M]
        hnsw_ef_C = self._test_params[HNSW_EF_CONSTRUCTION]

        add_index = f"ALTER TABLE {self._table} ADD INDEX vector_index {self._vector_column} TYPE vector_similarity('hnsw','{self._distance_metric}', {self._dimension}, {quantization}, {hnsw_M}, {hnsw_ef_C})"
        self._chclient.query(add_index)

        logger("Materialzing the index")
        materialize_index = f"ALTER TABLE {self._table} MATERIALIZE INDEX vector_index SETTINGS mutations_sync = 0"
        self._chclient.query(materialize_index)

        # wait for the materialize to complete
        while True:
            result = self._chclient.query(
                f"SELECT COUNT(*) FROM system.mutations WHERE table = '{self._table}' AND is_done = 0"
            )
            if result.result_rows[0][0] == 0:
                break
            logger("Waiting for materialize index to complete...")
            time.sleep(30)

        logger("Done building the vector index")
        result = self._chclient.query(
            f"SELECT name, type, formatReadableSize(data_compressed_bytes), formatReadableSize(data_uncompressed_bytes) FROM system.data_skipping_indices WHERE table = '{self._table}'"
        )
        logger(f"Index size for {self._table} -> ")
        for row in result.result_rows:
            logger(f"{row[0]}\t\t{row[1]}\t\t{row[2]}\t\t{row[3]}")

    # Run brute force KNN using randomly picked vectors from the dataset to generate the truth set
    def generate_truth_set(self):
        i = 0
        runtime = 0
        truth_set = {}

        # Get the MAX id value
        result = self._chclient.query(
            f"SELECT max({self._id_column}) FROM {self._table}"
        )
        max_id = result.result_rows[0][0]

        while i < self._query_count:
            query_vector_id = random.randint(1, max_id)
            if query_vector_id in truth_set:  # already used
                continue
            subquery = f"(SELECT {self._vector_column} FROM {self._table} WHERE {self._id_column} = {query_vector_id})"

            q_start = current_time_ms()
            knn_search_query = f"SELECT {self._id_column}, distance FROM {self._table} ORDER BY {self._distance_metric}( {self._vector_column}, {subquery} ) AS distance LIMIT {self._k} SETTINGS use_skip_indexes = 0"
            result = self._chclient.query(knn_search_query)
            q_end = current_time_ms()
            runtime = runtime + (q_end - q_start)

            neighbours = []
            distances = []
            for row in result.result_rows:
                neighbour_id = row[0]
                distance = row[1]
                neighbours.append(neighbour_id)
                distances.append(distance)

            # If too may zeroes in the distances, then drop this query vector
            if distances[int(self._k / 2)] == 0.0:
                continue

            truth_set[query_vector_id] = (neighbours, distances, (q_end - q_start))
            i = i + 1

        self._truth_set = truth_set
        logger(f"Runtime for KNN : {runtime / 1000} seconds")

    # Load the truth set from a file (instead of generating at runtime)
    def load_truth_set(self, path):
        logger(f"Loading truth set from file {path} ...")
        tarfile = path
        if "https" in path:  # Files are in S3 or local(for quick test)
            results = []
            commands = [f"wget -nv {path}"]
            results.append(
                Result.from_commands_run(
                    name=f"Download truthset {path}", command=commands
                )
            )
            tarfile = os.path.basename(path)

        results = []
        commands = [f"tar xvf {tarfile}"]
        results.append(
            Result.from_commands_run(
                name=f"Extract truthset {tarfile}", command=commands
            )
        )
        name, _ = os.path.splitext(tarfile)
        query_vectors = np.load(name + "_vectors.npy")
        neighbours = np.load(name + "_neighbours.npy")
        distances = np.load(name + "_distances.npy")

        truth_set = {}
        for i in range(len(query_vectors)):
            truth_set[query_vectors[i]] = (neighbours[i], distances[i], 0)

        logger(f"Loaded truth set containing {len(query_vectors)} vectors")
        return truth_set

    # Save the current truth set into files in npy format
    def save_truth_set(self, name):
        logger(f"Saving truth set locally {name} ...")
        if self._truth_set is None:
            return

        q_array = []
        n_array = []
        d_array = []

        for vector_id, result in self._truth_set.items():
            q_array.append(vector_id)
            n_array.append(result[0])
            d_array.append(result[1])

        query_vectors = np.array(q_array)
        neighbours = np.array(n_array)
        distances = np.array(d_array)

        np.save(name + "_vectors", query_vectors)
        np.save(name + "_neighbours", neighbours)
        np.save(name + "_distances", distances)

    # Run ANN on the query vectors in the truth set
    def run_search_for_truth_set(self, use_chclient=None):
        runtime = 0
        result_set = {}
        if use_chclient is not None:
            chclient = use_chclient
        else:
            chclient = self._chclient

        chclient.query("SET use_skip_indexes = 1, max_parallel_replicas = 1")

        # First execute a query to load the vector index, could take few minutes
        # We loop because API could timeout and raise exception.(even with higher receive_timeout)
        while True:
            try:
                subquery = f"(SELECT {self._vector_column} FROM {self._table} WHERE {self._id_column} = 100)"
                ann_search_query = f"SELECT {self._id_column}, distance FROM {self._table} ORDER BY {self._distance_metric}( {self._vector_column}, {subquery} ) AS distance LIMIT {self._k}"
                result = chclient.query(ann_search_query)
                logger(f"Vector indexes have loaded!")
                break
            except Exception as e:
                logger(f"Waiting for indexes to load...")
                time.sleep(30)

        for vector_id, result in self._truth_set.items():
            subquery = f"(SELECT {self._vector_column} FROM {self._table} WHERE {self._id_column} = {vector_id})"
            q_start = current_time_ms()
            ann_search_query = f"SELECT {self._id_column}, distance FROM {self._table} ORDER BY {self._distance_metric}( {self._vector_column}, {subquery} ) AS distance LIMIT {self._k} SETTINGS use_skip_indexes = 1, max_parallel_replicas = 1"
            result = chclient.query(ann_search_query)
            q_end = current_time_ms()
            runtime = runtime + (q_end - q_start)

            neighbours = []
            distances = []
            for row in result.result_rows:
                neighbour_id = row[0]
                distance = row[1]
                neighbours.append(neighbour_id)
                distances.append(distance)

            result_set[vector_id] = (neighbours, distances, (q_end - q_start))

        # self._result_set = result_set
        logger(f"Runtime for ANN : {runtime / 1000} seconds")
        return result_set

    def run_search_and_calculate_recall_mt(self, thread_number):
        chclient = get_new_connection()
        result_set = self.run_search_for_truth_set(chclient)
        self.calculate_recall(result_set)

    # Calculate the recall using the original truth set and passed in result_set
    def calculate_recall(self, result_set):
        logger("Calculating recall...")
        recall = 0.0
        for vector_id, result in self._truth_set.items():
            search_result = result_set[vector_id]

            # Recall = How many of the neighbours in the truth set(KNN) do we have in the result set(ANN)?
            intersection = list(set(result[0]) & set(search_result[0]))
            recall = recall + (len(intersection) / self._k)

        # Average
        recall = recall / self._query_count
        logger(f"Recall is {recall}")
        return recall

    # Run ANN search from multiple threads - test concurreny of CH + usearch
    def concurrency_test(self):
        nthreads = int(os.cpu_count() * 0.80)
        logger(f"Running concurrency test with {nthreads} threads...")
        threads = []
        for i in range(nthreads):
            t = threading.Thread(
                target=self.run_search_and_calculate_recall_mt, args=(i,)
            )
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()


def run_single_test(test_name, dataset, test_params):
    try:
        chclient = get_new_connection()
        test_runner = RunTest(chclient, dataset, test_params)

        test_runner.load_data()
        test_runner.optimize_table()

        # Run KNN queries first before building the index.
        if test_runner._test_params[GENERATE_TRUTH_SET]:
            test_runner.generate_truth_set()
            test_runner.save_truth_set(test_runner._test_params[NEW_TRUTH_SET_FILE])

        test_runner.build_index()

        if test_runner._test_params[GENERATE_TRUTH_SET]:
            result_set = test_runner.run_search_for_truth_set()
            test_runner.calculate_recall(result_set)

        # Run ANN search using pre-generated truth sets and calculate recall
        if test_runner._test_params[TRUTH_SET_FILES] and len(
            test_runner._test_params[TRUTH_SET_FILES]
        ):
            for tf in test_runner._test_params[TRUTH_SET_FILES]:
                generated_truth_set = test_runner.load_truth_set(tf)
                test_runner._truth_set = generated_truth_set
                result_set = test_runner.run_search_for_truth_set()
                test_runner.calculate_recall(result_set)

        # Run concurrency test on the current truth set
        if test_runner._test_params[CONCURRENCY_TEST]:
            test_runner.concurrency_test()
    except Exception as e:
        print(traceback.format_exc(), file=sys.stdout)
        return False

    return True


def install_and_start_clickhouse():
    res = True
    results = []
    ch = ClickHouseProc()
    info = Info()

    if Utils.is_arm():
        latest_ch_master_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
    elif Utils.is_amd():
        latest_ch_master_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
    else:
        assert False, f"Unknown processor architecture"

    if True:
        step_name = "Download ClickHouse"
        logger(step_name)
        commands = [
            f"wget -nv -P {temp_dir} {latest_ch_master_url}",
            f"chmod +x {temp_dir}/clickhouse",
            f"{temp_dir}/clickhouse --version",
        ]
        results.append(Result.from_commands_run(name=step_name, command=commands))
        res = results[-1].is_ok()

    if res:
        step_name = "Install ClickHouse"
        print(step_name)

        def install():
            # implement required ch configuration
            return (
                ch.install_clickbench_config() and ch.install_vector_search_config()
            )  # reuses config used for clickbench job, it's more or less default ch configuration

        results.append(Result.from_commands_run(name=step_name, command=[install]))
        res = results[-1].is_ok()

    if res:
        step_name = "Start ClickHouse"
        print(step_name)

        def start():
            return ch.start_light()

        results.append(
            Result.from_commands_run(
                name=step_name,
                command=[
                    start,  # command could be python callable or bash command as a string
                ],
            )
        )

    return results


# Array of (dataset, test_params)
TESTS_TO_RUN = [
    (
        "Test using the laion dataset",
        dataset_laion_5b_mini_for_quick_test,
        test_params_laion_5b_1m,
    ),
    (
        "Test using the hackernews dataset",
        dataset_hackernews_openai,
        test_params_hackernews_10m,
    ),
]


def main():
    test_results = []

    test_results = install_and_start_clickhouse()

    if test_results is None or not test_results[-1].is_ok():
        return

    for test in TESTS_TO_RUN:
        test_results.append(
            Result.from_commands_run(
                name=test[0], command=lambda: run_single_test(test[0], test[1], test[2])
            )
        )

    Result.create_from(
        results=test_results, files=[], info="Check index build time & recall"
    ).complete_job()


if __name__ == "__main__":
    main()
