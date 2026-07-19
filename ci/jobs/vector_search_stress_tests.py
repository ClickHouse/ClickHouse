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
SOURCE_FORMAT = "source_format"
SOURCE_STRUCTURE = "source_structure"
FETCH_COLUMNS_LIST = "fetch_columns_list"
MERGE_TREE_SETTINGS = "merge_tree_settings"
OTHER_SETTINGS = "other_settings"

LIMIT_N = "limit"
TRUTH_SET_FILES = "truth_set_files"
TRUTH_SET_QUERY_SOURCE = "truth_set_query_source"
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

TRUTH_SET_QUERY_SOURCE_ID = "id"
TRUTH_SET_QUERY_SOURCE_VECTOR = "vector"

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

dataset_cohere_wiki_20m = {
    TABLE: "cohere_wiki_20m",
    S3_URLS: [
        "https://clickhouse-datasets.s3.amazonaws.com/cohere-20M/cohere_wiki_20m.npy",
    ],
    SCHEMA: """
        id            UInt32,
        vector        Array(Float32)
     """,
    ID_COLUMN: "id",
    VECTOR_COLUMN: "vector",
    DISTANCE_METRIC: "cosineDistance",
    DIMENSION: 1024,
    SOURCE_SELECT_LIST: "toUInt32(rowNumberInAllBlocks()) AS id, vector",
    SOURCE_FORMAT: "Npy",
    SOURCE_STRUCTURE: "vector Array(Float32)",
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
    TRUTH_SET_QUERY_SOURCE: TRUTH_SET_QUERY_SOURCE_ID,
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
    TRUTH_SET_QUERY_SOURCE: TRUTH_SET_QUERY_SOURCE_ID,
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
    TRUTH_SET_QUERY_SOURCE: TRUTH_SET_QUERY_SOURCE_ID,
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
    TRUTH_SET_QUERY_SOURCE: TRUTH_SET_QUERY_SOURCE_ID,
    GENERATE_TRUTH_SET: False,
    NEW_TRUTH_SET_FILE: None,
    TRUTH_SET_COUNT: 1000,
    RECALL_K: 100,
    MERGE_TREE_SETTINGS: None,
    OTHER_SETTINGS: None,
    CONCURRENCY_TEST: True,
}

test_params_cohere_wiki_20m = {
    LIMIT_N: None,
    TRUTH_SET_FILES: [
        "https://clickhouse-datasets.s3.amazonaws.com/cohere-20M/cohere_wiki_20m_25k.tar"
    ],
    QUANTIZATION: "bf16",
    HNSW_M: 64,
    HNSW_EF_CONSTRUCTION: 256,
    HNSW_EF_SEARCH: None,
    VECTOR_SEARCH_INDEX_FETCH_MULTIPLIER: None,
    TRUTH_SET_QUERY_SOURCE: TRUTH_SET_QUERY_SOURCE_VECTOR,
    GENERATE_TRUTH_SET: False,
    NEW_TRUTH_SET_FILE: None,
    TRUTH_SET_COUNT: 25000,
    RECALL_K: 10,
    # Let's have more than 1 part for this dataset (7 - 9 parts)
    MERGE_TREE_SETTINGS: "max_bytes_to_merge_at_max_space_in_pool=11811160064",
    OTHER_SETTINGS: "min_insert_block_size_rows = 3000000, min_insert_block_size_bytes=11737418240",
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

        self._truth_set = []
        self._result_set = {}
        self._query_source_warning_logged = False
        self._vector_serialization_warning_logged = False

    def load_data(self):
        logger(f"Begin loading data into {self._table}")

        create_table = f"CREATE TABLE {self._table} ( {self._dataset[SCHEMA]} ) ENGINE = MergeTree ORDER BY {self._id_column}"
        if self._test_params[MERGE_TREE_SETTINGS] is not None:
            create_table += f" SETTINGS {self._test_params[MERGE_TREE_SETTINGS]}"
        self._chclient.query(create_table)

        for url in self._dataset[S3_URLS]:
            logger(f"Loading rows from location : {url}")
            select_list = "*"

            # Exact columns to read from the source file?
            configured_select_list = self._dataset.get(SOURCE_SELECT_LIST)
            if configured_select_list is not None:
                select_list = configured_select_list

            source = f"s3('{url}')"
            source_format = self._dataset.get(SOURCE_FORMAT)
            if source_format is not None:
                source = f"s3('{url}', '{source_format}'"
                source_structure = self._dataset.get(SOURCE_STRUCTURE)
                if source_structure is not None:
                    source = source + f", '{source_structure}'"
                source = source + ")"

            insert = f"INSERT INTO {self._table} SELECT {select_list} FROM {source}"

            if self._test_params[LIMIT_N] is not None:
                insert = (
                    insert
                    + f" ORDER BY {self._id_column} LIMIT {self._test_params[LIMIT_N]}"
                )

            if self._test_params[OTHER_SETTINGS] is not None:
                insert += f" SETTINGS {self._test_params[OTHER_SETTINGS]}"

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

        if self._test_params[MERGE_TREE_SETTINGS] is not None:
            if "max_bytes_to_merge_at_max_space_in_pool" in self._test_params[MERGE_TREE_SETTINGS]:
                logger("Skipping OPTIMIZE TABLE")
                return

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

    # Runs ALTER TABLE ... ADD INDEX and then MATERIALIZE INDEX
    def build_index(self):
        logger("Adding vector similarity index")

        result = self._chclient.query(
            f"SELECT name, formatReadableSize(bytes) FROM system.parts WHERE table = '{self._table}' AND active=1"
        )
        logger(f"Current active parts found for {self._table} -> ")
        for row in result.result_rows:
            logger(f"{row[0]}\t\t{row[1]} bytes")

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
        truth_set = []
        used_query_ids = set()

        # Get the MAX id value
        result = self._chclient.query(
            f"SELECT max({self._id_column}) FROM {self._table}"
        )
        max_id = result.result_rows[0][0]

        while i < self._query_count:
            query_vector_id = random.randint(1, max_id)
            if query_vector_id in used_query_ids:  # already used
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

            truth_set.append(
                self._make_truth_record(
                    query_id=query_vector_id,
                    query_vector=None,
                    neighbours=neighbours,
                    distances=distances,
                    runtime_ms=(q_end - q_start),
                )
            )
            used_query_ids.add(query_vector_id)
            i = i + 1

        self._truth_set = truth_set
        logger(f"Runtime for KNN : {runtime / 1000} seconds")

    def _make_truth_record(
        self, query_id, query_vector, neighbours, distances, runtime_ms
    ):
        normalized_query_vector = None
        if query_vector is not None:
            normalized_query_vector = np.asarray(query_vector, dtype=np.float32)

        return {
            "query_id": query_id,
            "query_vector": normalized_query_vector,
            "neighbours": np.asarray(neighbours),
            "distances": np.asarray(distances),
            "runtime_ms": runtime_ms,
        }

    def _vector_literal(self, query_vector):
        values = ",".join(repr(float(value)) for value in query_vector.tolist())
        return f"CAST([{values}], 'Array(Float32)')"

    def _get_truth_set_query_source(self):
        return self._test_params.get(
            TRUTH_SET_QUERY_SOURCE, TRUTH_SET_QUERY_SOURCE_ID
        )

    def _validate_truth_set_arrays(self, query_values, neighbours, distances):
        if len(query_values) != len(neighbours) or len(query_values) != len(distances):
            raise ValueError("Truth set arrays must have the same number of rows")

    def _load_truth_records_from_arrays(self, query_values, neighbours, distances):
        self._validate_truth_set_arrays(query_values, neighbours, distances)

        truth_set_query_source = self._get_truth_set_query_source()
        truth_set = []
        for i in range(len(query_values)):
            if truth_set_query_source == TRUTH_SET_QUERY_SOURCE_ID:
                truth_set.append(
                    self._make_truth_record(
                        query_id=int(query_values[i]),
                        query_vector=None,
                        neighbours=neighbours[i],
                        distances=distances[i],
                        runtime_ms=0,
                    )
                )
            elif truth_set_query_source == TRUTH_SET_QUERY_SOURCE_VECTOR:
                truth_set.append(
                    self._make_truth_record(
                        query_id=None,
                        query_vector=query_values[i],
                        neighbours=neighbours[i],
                        distances=distances[i],
                        runtime_ms=0,
                    )
                )
            else:
                raise ValueError(
                    f"Unknown truth set query source: {truth_set_query_source}"
                )

        return truth_set

    def _materialize_query_vector_if_needed(self, truth_record):
        if truth_record["query_vector"] is not None:
            return truth_record["query_vector"]

        if truth_record["query_id"] is None:
            raise ValueError("Truth record must contain either `query_id` or `query_vector`")

        result = self._chclient.query(
            f"SELECT {self._vector_column} FROM {self._table} WHERE {self._id_column} = {truth_record['query_id']}"
        )
        if not result.result_rows:
            raise ValueError(
                f"Failed to materialize query vector for id {truth_record['query_id']}"
            )

        truth_record["query_vector"] = np.asarray(
            result.result_rows[0][0], dtype=np.float32
        )
        return truth_record["query_vector"]

    def _render_query_source_sql(self, truth_record):
        if truth_record["query_vector"] is not None:
            if (
                truth_record["query_id"] is not None
                and not self._query_source_warning_logged
            ):
                logger(
                    "Warning: truth record contains both `query_id` and `query_vector`, using `query_vector`"
                )
                self._query_source_warning_logged = True
            return self._vector_literal(truth_record["query_vector"])

        if truth_record["query_id"] is None:
            raise ValueError("Truth record must contain either `query_id` or `query_vector`")

        return f"(SELECT {self._vector_column} FROM {self._table} WHERE {self._id_column} = {int(truth_record['query_id'])})"

    def _save_truth_records(self, name):
        if self._truth_set is None:
            return

        q_array = []
        n_array = []
        d_array = []

        serialize_query_vectors = any(
            truth_record["query_vector"] is not None for truth_record in self._truth_set
        )
        if serialize_query_vectors and any(
            truth_record["query_vector"] is None for truth_record in self._truth_set
        ):
            if not self._vector_serialization_warning_logged:
                logger(
                    "Warning: truth set contains mixed query sources, serializing `_vectors.npy` as raw query vectors"
                )
                self._vector_serialization_warning_logged = True

        for truth_record in self._truth_set:
            if serialize_query_vectors:
                q_array.append(self._materialize_query_vector_if_needed(truth_record))
            else:
                if truth_record["query_id"] is None:
                    raise ValueError(
                        "Cannot serialize truth set as query ids without `query_id`"
                    )
                q_array.append(truth_record["query_id"])

            n_array.append(truth_record["neighbours"])
            d_array.append(truth_record["distances"])

        query_values = np.array(q_array)
        neighbours = np.array(n_array)
        distances = np.array(d_array)

        np.save(name + "_vectors", query_values)
        np.save(name + "_neighbours", neighbours)
        np.save(name + "_distances", distances)

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

        truth_set_query_source = self._get_truth_set_query_source()
        if truth_set_query_source == TRUTH_SET_QUERY_SOURCE_ID:
            if query_vectors.ndim != 1:
                raise ValueError(
                    "Truth set configured for query ids but `_vectors.npy` is not 1-D"
                )
        elif truth_set_query_source == TRUTH_SET_QUERY_SOURCE_VECTOR:
            if query_vectors.ndim != 2:
                raise ValueError(
                    "Truth set configured for query vectors but `_vectors.npy` is not 2-D"
                )
        else:
            raise ValueError(
                f"Unknown truth set query source: {truth_set_query_source}"
            )

        truth_set = self._load_truth_records_from_arrays(
            query_vectors, neighbours, distances
        )
        logger(f"Loaded truth set containing {len(query_vectors)} vectors")
        return truth_set

    # Save the current truth set into files in npy format
    def save_truth_set(self, name):
        logger(f"Saving truth set locally {name} ...")
        self._save_truth_records(name)

    # Run ANN on the query vectors in the truth set
    def run_search_for_truth_set(self, use_chclient=None):
        runtime = 0
        result_set = []
        if use_chclient is not None:
            chclient = use_chclient
        else:
            chclient = self._chclient

        chclient.query("SET use_skip_indexes = 1, max_parallel_replicas = 1")

        # First execute a query to load the vector index, could take few minutes
        # We loop because API could timeout and raise exception.(even with higher receive_timeout)
        warmup_query_source = f"(SELECT {self._vector_column} FROM {self._table} ORDER BY {self._id_column} LIMIT 1)"

        while True:
            try:
                ann_search_query = f"SELECT {self._id_column}, distance FROM {self._table} ORDER BY {self._distance_metric}( {self._vector_column}, {warmup_query_source} ) AS distance LIMIT {self._k}"
                result = chclient.query(ann_search_query)
                logger(f"Vector indexes have loaded!")
                break
            except Exception as e:
                logger(f"Waiting for indexes to load...")
                time.sleep(30)

        for truth_record in self._truth_set:
            query_source = self._render_query_source_sql(truth_record)
            q_start = current_time_ms()
            ann_search_query = f"SELECT {self._id_column}, distance FROM {self._table} ORDER BY {self._distance_metric}( {self._vector_column}, {query_source} ) AS distance LIMIT {self._k} SETTINGS use_skip_indexes = 1, max_parallel_replicas = 1"
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

            result_set.append(
                self._make_truth_record(
                    query_id=None,
                    query_vector=None,
                    neighbours=neighbours,
                    distances=distances,
                    runtime_ms=(q_end - q_start),
                )
            )

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
        if len(self._truth_set) != len(result_set):
            raise ValueError("Truth set and ANN result set must have the same length")

        recall = 0.0
        for truth_record, search_record in zip(self._truth_set, result_set):
            truth_neighbours = set(truth_record["neighbours"].tolist())
            search_neighbours = set(search_record["neighbours"].tolist())

            # Recall = How many of the neighbours in the truth set(KNN) do we have in the result set(ANN)?
            intersection = list(truth_neighbours & search_neighbours)
            recall = recall + (len(intersection) / self._k)

        # Average
        if self._truth_set:
            recall = recall / len(self._truth_set)
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

    def drop_table(self):
        logger(f"Dropping table {self._table} ...")
        self._chclient.query(
            f"DROP TABLE IF EXISTS {self._table} SYNC SETTINGS max_table_size_to_drop = 0"
        )


def run_single_test(test_name, dataset, test_params):
    chclient = None
    test_runner = None
    result = True
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
        result = False
    finally:
        if test_runner is not None:
            try:
                test_runner.drop_table()
            except Exception:
                print(traceback.format_exc(), file=sys.stdout)

    return result


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
    (
        "Test using the cohere wiki dataset",
        dataset_cohere_wiki_20m,
        test_params_cohere_wiki_20m,
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
