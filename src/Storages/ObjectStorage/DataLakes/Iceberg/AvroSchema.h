#pragma once

#include <string>

namespace DB
{

static constexpr const char * manifest_list_v1_schema = R"(
{
	"type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
        {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
        {
            "name": "added_snapshot_id",
            "type": ["null", "long"],
            "doc": "Snapshot ID that added the manifest",
            "field-id": 503
        },
        {
            "name": "added_data_files_count",
            "type": ["null", "int"],
            "doc": "Added entry count",
            "field-id": 504
        },
        {
            "name": "existing_data_files_count",
            "type": ["null", "int"],
            "doc": "Existing entry count",
            "field-id": 505
        },
        {
            "name": "deleted_data_files_count",
            "type": ["null", "int"],
            "doc": "Deleted entry count",
            "field-id": 506
        },
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "r508",
                        "fields": [
                            {
                                "name": "contains_null",
                                "type": "boolean",
                                "doc": "True if any file has a null partition value",
                                "field-id": 509
                            },
                            {
                                "name": "contains_nan",
                                "type": ["null", "boolean"],
                                "doc": "True if any file has a nan partition value",
                                "field-id": 518
                            },
                            {
                                "name": "lower_bound",
                                "type": ["null", "bytes"],
                                "doc": "Partition lower bound for all files",
                                "field-id": 510
                            },
                            {
                                "name": "upper_bound",
                                "type": ["null", "bytes"],
                                "doc": "Partition upper bound for all files",
                                "field-id": 511
                            }
                        ]
                    },
                    "element-id": 508
                }
            ],
            "doc": "Summary for each partition",
            "field-id": 507
        },
        {"name": "added_rows_count", "type": ["null", "long"], "doc": "Added rows count", "field-id": 512},
        {
            "name": "existing_rows_count",
            "type": ["null", "long"],
            "doc": "Existing rows count",
            "field-id": 513
        },
        {
            "name": "deleted_rows_count",
            "type": ["null", "long"],
            "doc": "Deleted rows count",
            "field-id": 514
        }
    ]
}
)";

static constexpr const char * manifest_list_v2_schema = R"(
{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
        {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
        {"name": "content", "type": "int", "doc": "Contents of the manifest: 0=data, 1=deletes", "field-id": 517},
        {
            "name": "sequence_number",
            "type": "long",
            "doc": "Sequence number when the manifest was added",
            "field-id": 515
        },
        {
            "name": "min_sequence_number",
            "type": "long",
            "doc": "Lowest sequence number in the manifest",
            "field-id": 516
        },
        {"name": "added_snapshot_id", "type": "long", "doc": "Snapshot ID that added the manifest", "field-id": 503},
        {"name": "added_files_count", "type": "int", "doc": "Added entry count", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "doc": "Existing entry count", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "doc": "Deleted entry count", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "doc": "Added rows count", "field-id": 512},
        {"name": "existing_rows_count", "type": "long", "doc": "Existing rows count", "field-id": 513},
        {"name": "deleted_rows_count", "type": "long", "doc": "Deleted rows count", "field-id": 514},
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items":
                    {
                        "type": "record",
                        "name": "r508",
                        "fields": [
                            {
                                "name": "contains_null",
                                "type": "boolean",
                                "doc": "True if any file has a null partition value",
                                "field-id": 509
                            },
                            {
                                "name": "contains_nan",
                                "type": ["null", "boolean"],
                                "doc": "True if any file has a nan partition value",
                                "field-id": 518
                            },
                            {
                                "name": "lower_bound",
                                "type": ["null", "bytes"],
                                "doc": "Partition lower bound for all files",
                                "field-id": 510
                            },
                            {
                                "name": "upper_bound",
                                "type": ["null", "bytes"],
                                "doc": "Partition upper bound for all files",
                                "field-id": 511
                            }
                        ]
                    },
                    "element-id": 508
                }
            ],
            "doc": "Summary for each partition",
            "field-id": 507
        }
    ]
}
)";

/// NOTE: This string is just a template for the actual schema. To use it, you must first replace "#" with the correct value.
static constexpr const char * manifest_entry_v1_schema = R"(
{
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": "long", "field-id": 1},
        {
            "name": "data_file",
            "type":
            {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
                    {
                        "name": "file_format",
                        "type": "string",
                        "doc": "File format name: avro, orc, or parquet",
                        "field-id": 101
                    },
                    {
                        "name": "partition",
                        "type":
                        {
                            "type": "record",
                            "name": "r102",
                            "fields": #
                        },
                        "field-id": 102
                    },
                    {"name": "record_count", "type": "long", "doc": "Number of records in the file", "field-id": 103},
                    {"name": "file_size_in_bytes", "type": "long", "doc": "Total file size in bytes", "field-id": 104},
                    {"name": "block_size_in_bytes", "type": "long", "field-id": 105},
                    {
                        "name": "column_sizes",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k117_v118",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 117},
                                        {"name": "value", "type": "long", "field-id": 118}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to total size on disk",
                        "field-id": 108
                    },
                    {
                        "name": "value_counts",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k119_v120",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 119},
                                        {"name": "value", "type": "long", "field-id": 120}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to total count, including null and NaN",
                        "field-id": 109
                    },
                    {
                        "name": "null_value_counts",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k121_v122",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 121},
                                        {"name": "value", "type": "long", "field-id": 122}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to null value count",
                        "field-id": 110
                    },
                    {
                        "name": "nan_value_counts",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k138_v139",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 138},
                                        {"name": "value", "type": "long", "field-id": 139}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to number of NaN values in the column",
                        "field-id": 137
                    },
                    {
                        "name": "lower_bounds",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k126_v127",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 126},
                                        {"name": "value", "type": "bytes", "field-id": 127}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to lower bound",
                        "field-id": 125
                    },
                    {
                        "name": "upper_bounds",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k129_v130",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 129},
                                        {"name": "value", "type": "bytes", "field-id": 130}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to upper bound",
                        "field-id": 128
                    },
                    {
                        "name": "key_metadata",
                        "type": ["null", "bytes"],
                        "doc": "Encryption key metadata blob",
                        "field-id": 131
                    },
                    {
                        "name": "split_offsets",
                        "type": ["null", {"type": "array", "items": "long", "element-id": 133}],
                        "doc": "Splittable offsets",
                        "field-id": 132
                    },
                    {
                        "name": "sort_order_id",
                        "type": ["null", "int"],
                        "doc": "Sort order ID",
                        "field-id": 140
                    }
                ]
            },
            "field-id": 2
        }
    ]
}
)";

/// NOTE: This string is just a template for the actual schema. To use it, you must first replace "#" with the correct value.
static constexpr const char * manifest_entry_v2_schema = R"(
{
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1},
        {"name": "sequence_number", "type": ["null", "long"], "field-id": 3},
        {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4},
        {
            "name": "data_file",
            "type":
            {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "content", "type": "int", "doc": "Type of content stored by the data file", "field-id": 134},
                    {"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
                    {
                        "name": "file_format",
                        "type": "string",
                        "doc": "File format name: avro, orc, or parquet",
                        "field-id": 101
                    },
                    {
                        "name": "partition",
                        "type":
                        {
                            "type": "record",
                            "name": "r102",
                            "fields": #
                        },
                        "field-id": 102
                    },
                    {"name": "record_count", "type": "long", "doc": "Number of records in the file", "field-id": 103},
                    {"name": "file_size_in_bytes", "type": "long", "doc": "Total file size in bytes", "field-id": 104},
                    {
                        "name": "column_sizes",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k117_v118",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 117},
                                        {"name": "value", "type": "long", "field-id": 118}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to total size on disk",
                        "field-id": 108
                    },
                    {
                        "name": "value_counts",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k119_v120",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 119},
                                        {"name": "value", "type": "long", "field-id": 120}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to total count, including null and NaN",
                        "field-id": 109
                    },
                    {
                        "name": "null_value_counts",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k121_v122",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 121},
                                        {"name": "value", "type": "long", "field-id": 122}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to null value count",
                        "field-id": 110
                    },
                    {
                        "name": "nan_value_counts",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k138_v139",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 138},
                                        {"name": "value", "type": "long", "field-id": 139}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to number of NaN values in the column",
                        "field-id": 137
                    },
                    {
                        "name": "lower_bounds",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k126_v127",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 126},
                                        {"name": "value", "type": "bytes", "field-id": 127}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to lower bound",
                        "field-id": 125
                    },
                    {
                        "name": "upper_bounds",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items":
                                {
                                    "type": "record",
                                    "name": "k129_v130",
                                    "fields": [
                                        {"name": "key", "type": "int", "field-id": 129},
                                        {"name": "value", "type": "bytes", "field-id": 130}
                                    ]
                                },
                                "logicalType": "map"
                            }
                        ],
                        "doc": "Map of column id to upper bound",
                        "field-id": 128
                    },
                    {
                        "name": "key_metadata",
                        "type": ["null", "bytes"],
                        "doc": "Encryption key metadata blob",
                        "field-id": 131
                    },
                    {
                        "name": "split_offsets",
                        "type": ["null", {"type": "array", "items": "long", "element-id": 133}],
                        "doc": "Splittable offsets",
                        "field-id": 132
                    },
                    {
                        "name": "equality_ids",
                        "type": ["null", {"type": "array", "items": "int", "element-id": 136}],
                        "doc": "Field ids used to determine row equality for delete files",
                        "field-id": 135
                    },
                    {
                        "name": "sort_order_id",
                        "type": ["null", "int"],
                        "doc": "Sort order ID",
                        "field-id": 140
                    }
                ]
            },
            "field-id": 2
        }
    ]
}
)";

}
