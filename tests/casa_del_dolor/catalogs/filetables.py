import bz2
import copy
import gzip
import io
import json
import logging
import lzma
import os
import random
import re
import threading
import zlib
from datetime import timezone

import pyarrow as pa
import pyarrow.orc as pa_orc
import pyarrow.parquet as pa_pq

import snappy  # python-snappy: hadoop framing, like ClickHouse's `snappy` file wrapper
import zstandard

import avro.schema
from avro.codecs import KNOWN_CODECS
from avro.datafile import DataFileReader, DataFileWriter
from avro.errors import AvroException
from avro.io import DatumReader, DatumWriter

from integration.helpers.client import Client
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    CharType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VarcharType,
)

try:
    from pyspark.sql.types import TimestampNTZType
except ImportError:
    TimestampNTZType = None

from .clickhousetospark import ClickHouseMapping, ClickHouseTypeMapper
from .laketables import LakeCatalogs, LakeFormat, SparkColumn, TableStorage

_TS_TYPES = (
    (TimestampType,) if TimestampNTZType is None else (TimestampType, TimestampNTZType)
)

# Avro names must be valid identifiers, unlike ClickHouse's
_AVRO_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# snappy / zstandard registration depends on their optional packages being installed
_AVRO_CODECS = [
    c for c in ("null", "deflate", "snappy", "zstandard") if c in KNOWN_CODECS
]

logger = logging.getLogger(__name__)

# ClickHouse's user_files inside the container
_SERVER_USER_FILES = "/var/lib/clickhouse/user_files"

# ClickHouse compression method aliases, normalized
_COMPRESSION_ALIASES = {"auto": "none", "gz": "gzip", "lzma": "xz", "zst": "zstd"}

# Errors a corrupt or ClickHouse-overwritten data file can raise on read
_DATA_FILE_ERRORS = (
    pa.ArrowException,
    AvroException,
    OSError,
    EOFError,
    zlib.error,
    lzma.LZMAError,
    zstandard.ZstdError,
    snappy.UncompressError,
)


def _compress_bytes(data: bytes, compression: str) -> bytes:
    """Apply the whole-file compression wrapper the `File` engine declares as third argument."""
    if compression == "none":
        return data
    if compression == "gzip":
        return gzip.compress(data)
    if compression == "deflate":
        return zlib.compress(data)
    if compression == "xz":
        return lzma.compress(data)
    if compression == "zstd":
        return zstandard.ZstdCompressor().compress(data)
    if compression == "bz2":
        return bz2.compress(data)
    if compression == "snappy":
        return snappy.HadoopStreamCompressor().add_chunk(data)
    raise ValueError(f"Unsupported file compression {compression}")


def _decompress_bytes(data: bytes, compression: str) -> bytes:
    if compression == "none":
        return data
    if compression == "gzip":
        return gzip.decompress(data)
    if compression == "deflate":
        return zlib.decompress(data)
    if compression == "xz":
        return lzma.decompress(data)
    if compression == "zstd":
        return zstandard.ZstdDecompressor().decompressobj().decompress(data)
    if compression == "bz2":
        return bz2.decompress(data)
    if compression == "snappy":
        return snappy.HadoopStreamDecompressor().decompress(data)
    raise ValueError(f"Unsupported file compression {compression}")


def host_data_path(cluster, server_path: str) -> str:
    """The host location of a file ClickHouse sees under its `user_files`. docker-compose
    bind-mounts `<instances_dir>/node0/database` onto the container `/var/lib/clickhouse`,
    so the host writes here what ClickHouse reads at `server_path`."""
    host_root = os.path.join(cluster.instances_dir, "node0", "database", "user_files")
    return server_path.replace(_SERVER_USER_FILES, host_root, 1)


class FileTable:
    """A data file written by Spark + pyarrow / Avro, read by a ClickHouse `File` engine table."""

    # No lake behind this table: SparkAndClickHouseCheck then skips the engine-match
    # and snapshot / time-travel branches, which key on the lake format.
    lake_format = None

    def __init__(
        self,
        database_name: str,
        table_name: str,
        columns: dict[str, SparkColumn],
        ch_column_names: set[str],
        deterministic: bool,
        file_format: str,
        path: str,
        compression: str,
    ):
        self.database_name = database_name
        self.table_name = table_name
        # Columns the data file contains; consumed by LakeDataGenerator._create_random_df,
        # like SparkTable.columns. May diverge from the ClickHouse-declared schema.
        self.columns = columns
        # Columns the ClickHouse table declares (extra file columns are ignored by name,
        # declared-but-missing file columns are filled with defaults on read)
        self.ch_column_names = ch_column_names
        self.deterministic = deterministic
        # "Arrow" (file format, footer at the end), "ArrowStream", "Parquet", "ORC" or "Avro"
        self.file_format = file_format
        self.path = path
        # Whole-file wrapper declared in the `File` engine, normalized ("gzip", "zstd", ...)
        self.compression = compression

    def get_clickhouse_path(self) -> str:
        return f"{self.database_name}.{self.table_name}"

    def get_table_full_path(self) -> str:
        """Spark-side reads go through a session-scoped temp view over the data file."""
        return "dolor_file_view"

    def for_check(self):
        """A copy restricted to the columns present on BOTH sides: comparing an extra file
        column (unknown to ClickHouse) or a skipped one (defaults on the ClickHouse side
        only) would always mismatch."""
        res = copy.copy(self)
        res.columns = {
            k: v for k, v in self.columns.items() if k in self.ch_column_names
        }
        return res


def avro_compatible(dtype, name) -> bool:
    """Avro cannot represent every Spark type or name: names must be identifiers and
    map keys must be strings."""
    if name is not None and not _AVRO_NAME_RE.match(name):
        return False
    if isinstance(
        dtype,
        (
            BooleanType,
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            StringType,
            CharType,
            VarcharType,
            BinaryType,
            DecimalType,
            DateType,
        )
        + _TS_TYPES,
    ):
        return True
    if isinstance(dtype, ArrayType):
        return avro_compatible(dtype.elementType, None)
    if isinstance(dtype, MapType):
        return isinstance(
            dtype.keyType, (StringType, CharType, VarcharType)
        ) and avro_compatible(dtype.valueType, None)
    if isinstance(dtype, StructType):
        return all(avro_compatible(f.dataType, f.name) for f in dtype.fields)
    return False


def _avro_type(dtype, nullable: bool, counter: list):
    """The Avro schema for a Spark type; nullable columns become a union with null."""
    if isinstance(dtype, BooleanType):
        base = "boolean"
    elif isinstance(dtype, (ByteType, ShortType, IntegerType)):
        base = "int"
    elif isinstance(dtype, LongType):
        base = "long"
    elif isinstance(dtype, FloatType):
        base = "float"
    elif isinstance(dtype, DoubleType):
        base = "double"
    elif isinstance(dtype, (StringType, CharType, VarcharType)):
        base = "string"
    elif isinstance(dtype, BinaryType):
        base = "bytes"
    elif isinstance(dtype, DecimalType):
        base = {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": dtype.precision,
            "scale": dtype.scale,
        }
    elif isinstance(dtype, DateType):
        base = {"type": "int", "logicalType": "date"}
    elif isinstance(dtype, _TS_TYPES):
        base = {"type": "long", "logicalType": "timestamp-micros"}
    elif isinstance(dtype, ArrayType):
        base = {
            "type": "array",
            "items": _avro_type(dtype.elementType, dtype.containsNull, counter),
        }
    elif isinstance(dtype, MapType):
        base = {
            "type": "map",
            "values": _avro_type(dtype.valueType, dtype.valueContainsNull, counter),
        }
    elif isinstance(dtype, StructType):
        counter[0] += 1
        base = {
            "type": "record",
            "name": f"r{counter[0]}",
            "fields": [
                {"name": f.name, "type": _avro_type(f.dataType, f.nullable, counter)}
                for f in dtype.fields
            ],
        }
    else:
        raise ValueError(f"Spark type {dtype} is not representable in Avro")
    return ["null", base] if nullable else base


def _avro_value(value, dtype):
    """pyarrow's to_pylist output -> what Avro's DatumWriter expects."""
    if value is None:
        return None
    if isinstance(dtype, MapType):
        return {k: _avro_value(v, dtype.valueType) for k, v in value}
    if isinstance(dtype, ArrayType):
        return [_avro_value(v, dtype.elementType) for v in value]
    if isinstance(dtype, StructType):
        return {
            f.name: _avro_value(value.get(f.name), f.dataType) for f in dtype.fields
        }
    if isinstance(dtype, _TS_TYPES) and value.tzinfo is None:
        # timestamp-micros requires an aware datetime
        return value.replace(tzinfo=timezone.utc)
    return value


def _from_avro_value(value, dtype):
    """Avro reader output -> what Spark's createDataFrame accepts."""
    if value is None:
        return None
    if isinstance(dtype, MapType):
        return {k: _from_avro_value(v, dtype.valueType) for k, v in value.items()}
    if isinstance(dtype, ArrayType):
        return [_from_avro_value(v, dtype.elementType) for v in value]
    if isinstance(dtype, StructType):
        return {
            f.name: _from_avro_value(value.get(f.name), f.dataType)
            for f in dtype.fields
        }
    if (
        TimestampNTZType is not None
        and isinstance(dtype, TimestampNTZType)
        and value.tzinfo is not None
    ):
        # NTZ columns want naive datetimes back
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def write_data_file(table: FileTable, arrow_table) -> None:
    """Write the data file atomically, so a concurrent ClickHouse read never sees a
    partial file. Format-level compression and batching options are randomized to also
    exercise ClickHouse's compressed and multi-batch reading paths, and the whole file
    is wrapped with the compression the `File` engine declares."""
    fmt = table.file_format.lower()
    tmp_path = table.path + ".tmp"
    os.makedirs(os.path.dirname(table.path), exist_ok=True)
    if fmt in ("arrow", "arrowstream"):
        options = pa.ipc.IpcWriteOptions(
            compression=random.choice([None, None, None, "lz4", "zstd"])
        )
        sink = pa.BufferOutputStream()
        if fmt == "arrow":
            writer = pa.ipc.new_file(sink, arrow_table.schema, options=options)
        else:
            writer = pa.ipc.new_stream(sink, arrow_table.schema, options=options)
        try:
            max_chunksize = (
                random.randint(1, 100)
                if arrow_table.num_rows > 0 and random.randint(1, 2) == 1
                else None
            )
            writer.write_table(arrow_table, max_chunksize=max_chunksize)
        finally:
            writer.close()
        payload = sink.getvalue().to_pybytes()
    elif fmt == "parquet":
        sink = pa.BufferOutputStream()
        pa_pq.write_table(
            arrow_table,
            sink,
            compression=random.choice(
                ["none", "snappy", "gzip", "lz4", "zstd", "brotli"]
            ),
            use_dictionary=random.randint(1, 2) == 1,
            row_group_size=(
                random.randint(1, 100)
                if arrow_table.num_rows > 0 and random.randint(1, 2) == 1
                else None
            ),
        )
        payload = sink.getvalue().to_pybytes()
    elif fmt == "orc":
        sink = pa.BufferOutputStream()
        pa_orc.write_table(
            arrow_table,
            sink,
            compression=random.choice(
                ["uncompressed", "snappy", "zlib", "lz4", "zstd"]
            ),
        )
        payload = sink.getvalue().to_pybytes()
    elif fmt == "avro":
        counter = [0]
        fields = [
            {
                "name": c.column_name,
                "type": _avro_type(c.spark_type, c.nullable, counter),
            }
            for c in table.columns.values()
        ]
        schema = avro.schema.parse(
            json.dumps({"type": "record", "name": "row", "fields": fields})
        )
        codec = random.choice(_AVRO_CODECS)
        sink = io.BytesIO()
        with DataFileWriter(sink, DatumWriter(), schema, codec=codec) as writer:
            for rec in arrow_table.to_pylist():
                writer.append(
                    {
                        c.column_name: _avro_value(rec.get(c.column_name), c.spark_type)
                        for c in table.columns.values()
                    }
                )
            writer.flush()
            payload = sink.getvalue()
    else:
        raise ValueError(f"Unsupported file format {table.file_format}")
    with open(tmp_path, "wb") as out:
        out.write(_compress_bytes(payload, table.compression))
    os.replace(tmp_path, table.path)
    logger.info(
        f"Wrote {arrow_table.num_rows} rows to {table.get_clickhouse_path()} data file"
        f" ({table.file_format}, {table.compression} compression)"
    )


def read_data_file(table: FileTable):
    """Read the data file back with a reader independent from ClickHouse: pyarrow for the
    arrow-family formats, the Apache Avro library for Avro. Returns a pyarrow table, or
    a list of records for Avro."""
    fmt = table.file_format.lower()
    with open(table.path, "rb") as source:
        payload = _decompress_bytes(source.read(), table.compression)
    if fmt == "arrow":
        return pa.ipc.open_file(pa.BufferReader(payload)).read_all()
    if fmt == "arrowstream":
        return pa.ipc.open_stream(pa.BufferReader(payload)).read_all()
    if fmt == "parquet":
        return pa_pq.read_table(pa.BufferReader(payload))
    if fmt == "orc":
        return pa_orc.read_table(pa.BufferReader(payload))
    if fmt == "avro":
        with DataFileReader(io.BytesIO(payload), DatumReader()) as reader:
            return list(reader)
    raise ValueError(f"Unsupported file format {table.file_format}")


# Spark's Arrow bridge has no unsigned integer types, so a ClickHouse-written file
# (e.g. after a truncating INSERT) with UInt* columns fails `createDataFrame`. Widen
# each unsigned type to the smallest signed type that holds all its values.
_UNSIGNED_TO_SIGNED = {
    pa.uint8(): pa.int16(),
    pa.uint16(): pa.int32(),
    pa.uint32(): pa.int64(),
    pa.uint64(): pa.decimal128(20, 0),
}


def _spark_compatible_arrow_type(t):
    for unsigned, signed in _UNSIGNED_TO_SIGNED.items():
        if t.equals(unsigned):
            return signed
    if pa.types.is_large_list(t):
        return pa.large_list(_spark_compatible_field(t.value_field))
    if pa.types.is_list(t):
        return pa.list_(_spark_compatible_field(t.value_field))
    if pa.types.is_fixed_size_list(t):
        return pa.list_(_spark_compatible_field(t.value_field), t.list_size)
    if pa.types.is_map(t):
        return pa.map_(
            _spark_compatible_arrow_type(t.key_type),
            _spark_compatible_field(t.item_field),
        )
    if pa.types.is_struct(t):
        return pa.struct([_spark_compatible_field(t.field(i)) for i in range(t.num_fields)])
    return t


def _spark_compatible_field(f):
    return f.with_type(_spark_compatible_arrow_type(f.type))


def _to_spark_compatible_arrow(arrow_table):
    """Cast any unsigned integer types (nested included) to signed so Spark can ingest them."""
    new_schema = pa.schema([_spark_compatible_field(f) for f in arrow_table.schema])
    if new_schema.equals(arrow_table.schema):
        return arrow_table
    return arrow_table.cast(new_schema)


class FileHandler:
    """Manages ClickHouse `File` engine tables whose data file is written by Spark + pyarrow."""

    def __init__(self, spark_handler):
        self.logger = logging.getLogger(__name__)
        # Back-reference for Spark sessions and the data generator
        self.spark = spark_handler
        self.file_tables: dict[tuple[str, str], FileTable] = {}
        self.file_lock = threading.Lock()

    def _random_file_df(self, next_session, table: FileTable):
        """Build a random DataFrame for the table, collected to a driver-side pyarrow table."""
        df = self.spark.data_generator._create_random_df(
            next_session, table, random.randint(0, 100)
        )
        return df.toArrow()

    def create_table(self, cluster, data) -> bool:
        mapper = ClickHouseTypeMapper()
        columns = {}
        mapper.reset()
        deterministic = data["deterministic"] > 0
        is_avro = data["format"].lower() == "avro"
        ch_column_names = {val["name"] for val in data["columns"]}
        file_columns = list(data["columns"])
        # Like laketables: add extra columns beyond the ClickHouse-declared schema,
        # which ClickHouse ignores by name on read
        if not deterministic and random.randint(1, 11) == 1:
            for i in range(0, random.randint(1, 3)):
                # On a name collision the extra would shadow a declared column with a new
                # type, making the comparison mismatch on ClickHouse's declared type
                if f"cx{i}" in ch_column_names:
                    continue
                file_columns.append(
                    {
                        "name": f"cx{i}",
                        "type": mapper.generate_random_clickhouse_type(
                            True, True, random.randint(1, 4), 0
                        ),
                    }
                )
        first = True
        for val in file_columns:
            mapper.increment()
            _, nullable, spark_type = mapper.clickhouse_to_spark(
                val["type"], False, ClickHouseMapping.Spark
            )
            if is_avro and not avro_compatible(spark_type, val["name"]):
                continue
            # Sometimes skip columns: ClickHouse fills them with defaults on read. Not
            # for Avro, where `input_format_avro_allow_missing_fields` is 0 by default,
            # so a missing field would fail every default ClickHouse read.
            if (
                not first
                and not deterministic
                and not is_avro
                and random.randint(1, 11) == 1
            ):
                continue
            columns[val["name"]] = SparkColumn(
                val["name"], spark_type, nullable, False, val["type"]
            )
            first = False
        if not columns:
            raise ValueError(
                f"No Avro-compatible columns for {data['database_name']}.{data['table_name']}"
            )
        compression = data.get("compression", "none").lower()
        next_table = FileTable(
            data["database_name"],
            data["table_name"],
            columns,
            ch_column_names,
            deterministic,
            data["format"],
            host_data_path(cluster, data["path"]),
            _COMPRESSION_ALIASES.get(compression, compression),
        )
        with self.spark.spark_lock:
            next_session = self.spark.get_next_session(
                cluster,
                "file_tables",
                TableStorage.storage_from_str("local"),
                LakeFormat.lakeformat_from_str("iceberg"),
                LakeCatalogs.NoCatalog,
            )
            try:
                write_data_file(
                    next_table, self._random_file_df(next_session, next_table)
                )
            finally:
                next_session.stop()
        with self.file_lock:
            self.file_tables[(next_table.database_name, next_table.table_name)] = (
                next_table
            )
        return True

    @staticmethod
    def _full_decode_settings(table: FileTable) -> str:
        """Settings for the full-decode query: pin the missing-columns behavior so skipped
        columns survive settings randomization, and rotate between ClickHouse's readers.
        """
        fmt = table.file_format.lower()
        # ClickHouse may have truncated the data file to 0 bytes (e.g. TRUNCATE) between the
        # independent read above and this query; skip empty files to read 0 rows rather than error
        settings = ["engine_file_skip_empty_files = 1"]
        if fmt in ("arrow", "arrowstream"):
            settings.append("input_format_arrow_allow_missing_columns = 1")
            # Prefer the new native Arrow reader, keeping some runs on the library one
            use_native_reader = 1 if random.randint(1, 4) != 4 else 0
            settings.append(
                f"input_format_arrow_use_native_reader = {use_native_reader}"
            )
        elif fmt == "parquet":
            settings.append("input_format_parquet_allow_missing_columns = 1")
        elif fmt == "orc":
            settings.append("input_format_orc_allow_missing_columns = 1")
            settings.append(
                f"input_format_orc_use_fast_decoder = {1 if random.randint(1, 4) != 4 else 0}"
            )
        elif fmt == "avro":
            settings.append("input_format_avro_allow_missing_fields = 1")
        return ", ".join(settings)

    def check_table(self, cluster, next_session, table: FileTable) -> bool:
        """Both sides read the same bytes, so this compares ClickHouse's format reader
        against an independent one. The count and value comparison is delegated to
        SparkAndClickHouseCheck, with the file loaded into a temp view standing in for
        the Spark-side table."""
        # Read independently first: if ClickHouse mutated the file (INSERT appends a
        # second file, TRUNCATE empties it), this raises and the caller regenerates it
        data_read = read_data_file(table)
        client = Client(
            host=(
                cluster.instances["node0"].ip_address
                if hasattr(cluster, "instances")
                else "localhost"
            ),
            port=9000,
            command=cluster.client_bin_path,
        )
        # Force a full decode of every column: tablecheck swallows exceptions as a pass,
        # while an error thrown here propagates and fails the external command
        client.query(
            f"SELECT * FROM {table.get_clickhouse_path()} FORMAT Null"
            f" SETTINGS {self._full_decode_settings(table)};"
        )
        if isinstance(data_read, list):
            # Avro records: rebuild rows in the declared column order
            struct = StructType(
                [
                    StructField(c.column_name, c.spark_type, c.nullable)
                    for c in table.columns.values()
                ]
            )
            rows = [
                tuple(
                    _from_avro_value(rec.get(f.name), f.dataType) for f in struct.fields
                )
                for rec in data_read
            ]
            df = next_session.createDataFrame(rows, schema=struct)
        else:
            df = next_session.createDataFrame(_to_spark_compatible_arrow(data_read))
        df.createOrReplaceTempView(table.get_table_full_path())
        # Compare only the columns both sides share
        return self.spark.table_check.check_table(
            cluster, next_session, table.for_check()
        )

    def update_or_check_table(self, cluster, next_session, data) -> bool:
        # Hold the lock for the whole operation: concurrent async rewrites of the same
        # file during a check would show up as spurious count/hash mismatches
        with self.file_lock:
            next_table = self.file_tables[(data["catalog_name"], data["table_name"])]
            try:
                if random.randint(1, 10) < 8:
                    # Rewrite the file with fresh random data (atomic replace)
                    write_data_file(
                        next_table, self._random_file_df(next_session, next_table)
                    )
                    return True
                return self.check_table(cluster, next_session, next_table)
            except _DATA_FILE_ERRORS as e:
                # ClickHouse is allowed to write into the table, and appending to or
                # truncating the data file makes it unreadable: regenerate it instead
                # of reporting a false mismatch
                self.logger.warning(
                    f"Data file of {next_table.get_clickhouse_path()} is not readable"
                    f" anymore ({e}), regenerating it"
                )
                write_data_file(
                    next_table, self._random_file_df(next_session, next_table)
                )
                return True
