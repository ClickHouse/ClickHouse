#pragma once
#include <string>
#include <Core/Types.h>

#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnString.h>
#include <Processors/Port.h>
#include <Decoder.hh>
#include <Encoder.hh>
#include <Specific.hh>
#include <Core/Field.h>

namespace Iceberg
{

struct PartitionRecord
{
    std::string name;
    DB::AvroSerializer::SerializeFn serializer;
    DB::ColumnPtr column;
    size_t row_num;
};

struct PartitionInManifest
{
    std::vector<PartitionRecord> partition_values;
};

using Binary = std::vector<uint8_t>;

struct DataFile
{
    int32_t content;
    std::string file_path;
    std::string file_format;
    PartitionInManifest partition;
    int64_t record_count;
    int64_t file_size_in_bytes;
    std::vector<std::pair<int, int64_t>> column_sizes;
    std::vector<std::pair<int, int64_t>> value_counts;
    std::vector<std::pair<int, int64_t>> null_value_counts;
    std::vector<std::pair<int, int64_t>> nan_value_counts;
    std::vector<std::pair<int, Binary>> lower_bounds;
    std::vector<std::pair<int, Binary>> upper_bounds;
    std::optional<Binary> key_metadata;

    std::vector<int64_t> split_offsets;
    std::vector<int32_t> equality_ids;

    std::optional<int32_t> sort_order_id;
    std::optional<int32_t> first_row_id;
    std::optional<std::string> referenced_data_file;
    std::optional<int64_t> content_offset;
    std::optional<int64_t> content_size_in_bytes;
};

struct Manifest
{
    int32_t status;
    std::optional<int64_t> snapshot_id;
    std::optional<int64_t> sequence_number;
    std::optional<int64_t> file_sequence_number;
    DataFile data_file;

    static inline const std::string SCHEMA = R"({
  "type" : "record",
  "name" : "manifest_entry",
  "fields" : [ {
    "name" : "status",
    "type" : "int",
    "field-id" : 0
  }, {
    "name" : "snapshot_id",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 1
  }, {
    "name" : "sequence_number",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 3
  }, {
    "name" : "file_sequence_number",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 4
  }, {
    "name" : "data_file",
    "type" : {
      "type" : "record",
      "name" : "r2",
      "fields" : [ {
        "name" : "content",
        "type" : "int",
        "doc" : "Contents of the file: 0=data, 1=position deletes, 2=equality deletes",
        "field-id" : 134
      }, {
        "name" : "file_path",
        "type" : "string",
        "doc" : "Location URI with FS scheme",
        "field-id" : 100
      }, {
        "name" : "file_format",
        "type" : "string",
        "doc" : "File format name: avro, orc, or parquet",
        "field-id" : 101
      }, {
        "name" : "partition",
        "type" : {
          "type" : "record",
          "name" : "r102",
          "fields" : [ {
            "name" : "name_trunc",
            "type" : [ "null", "string" ],
            "default" : null,
            "field-id" : 1000
          } ]
        },
        "doc" : "Partition data tuple, schema based on the partition spec",
        "field-id" : 102
      }, {
        "name" : "record_count",
        "type" : "long",
        "doc" : "Number of records in the file",
        "field-id" : 103
      }, {
        "name" : "file_size_in_bytes",
        "type" : "long",
        "doc" : "Total file size in bytes",
        "field-id" : 104
      }, {
        "name" : "column_sizes",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k117_v118",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 117
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 118
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to total size on disk",
        "default" : null,
        "field-id" : 108
      }, {
        "name" : "value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k119_v120",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 119
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 120
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to total count, including null and NaN",
        "default" : null,
        "field-id" : 109
      }, {
        "name" : "null_value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k121_v122",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 121
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 122
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to null value count",
        "default" : null,
        "field-id" : 110
      }, {
        "name" : "nan_value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k138_v139",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 138
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 139
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to number of NaN values in the column",
        "default" : null,
        "field-id" : 137
      }, {
        "name" : "lower_bounds",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k126_v127",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 126
            }, {
              "name" : "value",
              "type" : "bytes",
              "field-id" : 127
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to lower bound",
        "default" : null,
        "field-id" : 125
      }, {
        "name" : "upper_bounds",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k129_v130",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 129
            }, {
              "name" : "value",
              "type" : "bytes",
              "field-id" : 130
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to upper bound",
        "default" : null,
        "field-id" : 128
      }, {
        "name" : "key_metadata",
        "type" : [ "null", "bytes" ],
        "doc" : "Encryption key metadata blob",
        "default" : null,
        "field-id" : 131
      }, {
        "name" : "split_offsets",
        "type" : [ "null", {
          "type" : "array",
          "items" : "long",
          "element-id" : 133
        } ],
        "doc" : "Splittable offsets",
        "default" : null,
        "field-id" : 132
      }, {
        "name" : "equality_ids",
        "type" : [ "null", {
          "type" : "array",
          "items" : "int",
          "element-id" : 136
        } ],
        "doc" : "Equality comparison field IDs",
        "default" : null,
        "field-id" : 135
      }, {
        "name" : "sort_order_id",
        "type" : [ "null", "int" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 140
      }, {
        "name" : "first_row_id",
        "type" : [ "null", "int" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 142
       }, {
        "name" : "referenced_data_file",
        "type" : [ "null", "string" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 143
       }, {
        "name" : "content_offset",
        "type" : [ "null", "long" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 144
       }, {
        "name" : "content_size_in_bytes",
        "type" : [ "null", "long" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 145
       }
]
    },
    "field-id" : 2
  } ]
}
    )";

    static inline const std::string SCHEMA_NO_PARTITION = R"({
  "type" : "record",
  "name" : "manifest_entry",
  "fields" : [ {
    "name" : "status",
    "type" : "int",
    "field-id" : 0
  }, {
    "name" : "snapshot_id",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 1
  }, {
    "name" : "sequence_number",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 3
  }, {
    "name" : "file_sequence_number",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 4
  }, {
    "name" : "data_file",
    "type" : {
      "type" : "record",
      "name" : "r2",
      "fields" : [ {
        "name" : "content",
        "type" : "int",
        "doc" : "Contents of the file: 0=data, 1=position deletes, 2=equality deletes",
        "field-id" : 134
      }, {
        "name" : "file_path",
        "type" : "string",
        "doc" : "Location URI with FS scheme",
        "field-id" : 100
      }, {
        "name" : "file_format",
        "type" : "string",
        "doc" : "File format name: avro, orc, or parquet",
        "field-id" : 101
      }, {
        "name" : "record_count",
        "type" : "long",
        "doc" : "Number of records in the file",
        "field-id" : 103
      }, {
        "name" : "file_size_in_bytes",
        "type" : "long",
        "doc" : "Total file size in bytes",
        "field-id" : 104
      }, {
        "name" : "column_sizes",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k117_v118",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 117
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 118
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to total size on disk",
        "default" : null,
        "field-id" : 108
      }, {
        "name" : "value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k119_v120",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 119
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 120
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to total count, including null and NaN",
        "default" : null,
        "field-id" : 109
      }, {
        "name" : "null_value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k121_v122",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 121
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 122
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to null value count",
        "default" : null,
        "field-id" : 110
      }, {
        "name" : "nan_value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k138_v139",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 138
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 139
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to number of NaN values in the column",
        "default" : null,
        "field-id" : 137
      }, {
        "name" : "lower_bounds",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k126_v127",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 126
            }, {
              "name" : "value",
              "type" : "bytes",
              "field-id" : 127
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to lower bound",
        "default" : null,
        "field-id" : 125
      }, {
        "name" : "upper_bounds",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k129_v130",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 129
            }, {
              "name" : "value",
              "type" : "bytes",
              "field-id" : 130
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to upper bound",
        "default" : null,
        "field-id" : 128
      }, {
        "name" : "key_metadata",
        "type" : [ "null", "bytes" ],
        "doc" : "Encryption key metadata blob",
        "default" : null,
        "field-id" : 131
      }, {
        "name" : "split_offsets",
        "type" : [ "null", {
          "type" : "array",
          "items" : "long",
          "element-id" : 133
        } ],
        "doc" : "Splittable offsets",
        "default" : null,
        "field-id" : 132
      }, {
        "name" : "equality_ids",
        "type" : [ "null", {
          "type" : "array",
          "items" : "int",
          "element-id" : 136
        } ],
        "doc" : "Equality comparison field IDs",
        "default" : null,
        "field-id" : 135
      }, {
        "name" : "sort_order_id",
        "type" : [ "null", "int" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 140
      }, {
        "name" : "first_row_id",
        "type" : [ "null", "int" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 142
       }, {
        "name" : "referenced_data_file",
        "type" : [ "null", "string" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 143
       }, {
        "name" : "content_offset",
        "type" : [ "null", "long" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 144
       }, {
        "name" : "content_size_in_bytes",
        "type" : [ "null", "long" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 145
       }
]
    },
    "field-id" : 2
  } ]
}
    )";

};

}

namespace avro
{

template <typename T>
struct codec_traits<std::optional<T>>
{
    static void encode(Encoder & e, const std::optional<T> & v)
    {
        if (v.has_value())
        {
            e.encodeUnionIndex(1);
            avro::encode(e, *v);
        }
        else
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }
    }

    static void decode(Decoder & d, std::optional<T> & v)
    {
        size_t n = d.decodeUnionIndex();

        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        if (n == 0)
        {
            d.decodeNull();
        }
        else
        {
            avro::decode(d, *v);
        }
    }
};


template <typename K, typename V>
struct codec_traits<std::vector<std::pair<K, V>>>
{
    static void encode(Encoder & e, const std::vector<std::pair<K, V>> & v)
    {
        std::cerr << "ENCODING VECTOR OF SIZE: " << v.size() << std::endl;
        if (!v.empty())
        {
            e.encodeUnionIndex(1);
            e.arrayStart();
            e.setItemCount(v.size());
            for (auto it = v.begin(); it != v.end(); ++it)
            {
                e.startItem();
                avro::encode(e, it->first);
                avro::encode(e, it->second);
            }
            e.arrayEnd();
        }
        else
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }
    }

    static void decode(Decoder & d, std::vector<std::pair<K, V>> & v)
    {
        v.clear();
        size_t n = d.decodeUnionIndex();

        std::cerr << "DECODING VECTOR OF INDEX: " << n << std::endl;
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        if (n == 0)
        {
            d.decodeNull();
        }
        else
        {
            for (size_t j = d.arrayStart(); j != 0; j = d.arrayNext())
            {
                for (size_t i = 0; i < j; ++i)
                {
                    K k;
                    avro::decode(d, k);
                    V t;
                    avro::decode(d, t);
                    v.emplace_back(k, t);
                }
            }
        }
    }
};


template <>
struct codec_traits<Iceberg::PartitionInManifest>
{
    static void encode(Encoder & e, const Iceberg::PartitionInManifest & v)
    {
        for (const auto & value : v.partition_values)
        {
            e.encodeUnionIndex(1);
            value.serializer(*value.column, value.row_num, e);
            //std::cerr << "ENCODING: " << value.value.safeGet<std::string>() << std::endl;
            //e.encodeString(value.value.safeGet<std::string>());
        }
    }
    static void decode(Decoder & d, Iceberg::PartitionInManifest & v)
    {
        v.partition_values.clear();
        std::cerr << "Parsing partition\n";
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                std::cerr << "INDEX:" << d.decodeUnionIndex() << std::endl;
                auto column = DB::ColumnString::create();
                column->insert(d.decodeString());
                v.partition_values.emplace_back(Iceberg::PartitionRecord{"AAAA", nullptr, std::move(column), 0});
                switch (it)
                {
                    default:
                        break;
                }
            }
        }
        else
        {
            std::cerr << "INDEX:" << d.decodeUnionIndex() << std::endl;
            auto column = DB::ColumnString::create();
            column->insert(d.decodeString());
            v.partition_values.emplace_back(Iceberg::PartitionRecord{"AAAA", nullptr, std::move(column), 0});
        }
    }
};

template <>
struct codec_traits<Iceberg::DataFile>
{
    static void encode(Encoder & e, const Iceberg::DataFile & v)
    {
        avro::encode(e, v.content);
        avro::encode(e, v.file_path);
        avro::encode(e, v.file_format);
        avro::encode(e, v.partition);
        avro::encode(e, v.record_count);
        avro::encode(e, v.file_size_in_bytes);
        std::cerr << "ENCODING COLUMn SIZES\n";
        avro::encode(e, v.column_sizes);
        std::cerr << "DONE\n";
        avro::encode(e, v.value_counts);
        avro::encode(e, v.null_value_counts);
        avro::encode(e, v.nan_value_counts);
        avro::encode(e, v.lower_bounds);
        avro::encode(e, v.upper_bounds);
        avro::encode(e, v.key_metadata);
        if (v.split_offsets.empty())
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }
        else
        {
            e.encodeUnionIndex(1);
            avro::encode(e, v.split_offsets);
        }
        if (v.equality_ids.empty())
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }
        else
        {
            e.encodeUnionIndex(1);
            avro::encode(e, v.equality_ids);
        }
        avro::encode(e, v.sort_order_id);
        avro::encode(e, v.first_row_id);
        avro::encode(e, v.referenced_data_file);
        avro::encode(e, v.content_offset);
        avro::encode(e, v.content_size_in_bytes);
    }
    static void decode(Decoder & d, Iceberg::DataFile & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                std::cerr << "IT: " <<it <<std::endl;
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.content);
                        break;
                    case 1:
                        avro::decode(d, v.file_path);
                        break;
                    case 2:
                        avro::decode(d, v.file_format);
                        break;
                    case 3:
                        avro::decode(d, v.partition);
                        break;
                    case 4:
                        avro::decode(d, v.record_count);
                        break;
                    case 5:
                        avro::decode(d, v.file_size_in_bytes);
                        break;
                    case 6:
                        avro::decode(d, v.column_sizes);
                        break;
                    case 7:
                        avro::decode(d, v.value_counts);
                        break;
                    case 8:
                        avro::decode(d, v.null_value_counts);
                        break;
                    case 9:
                        avro::decode(d, v.nan_value_counts);
                        break;
                    case 10:
                        avro::decode(d, v.lower_bounds);
                        break;
                    case 11:
                        avro::decode(d, v.upper_bounds);
                        break;
                    case 12:
                        avro::decode(d, v.key_metadata);
                        break;
                    case 13:
                    {
                        std::cerr << "AAA\n";

                        size_t n = d.decodeUnionIndex();
                        if (n >= 2)
                            throw avro::Exception("Union index too big");
                        if (n == 0)
                            d.decodeNull();
                        else
                        {
                             for (size_t j = d.arrayStart(); n != 0; n = d.arrayNext())
                             {
                                 for (size_t i = 0; i < j; ++i)
                                 {
                                     int64_t t;
                                     avro::decode(d, t);
                                     v.split_offsets.push_back(t);
                                 }
                             }
                           
                        }
                        std::cerr << "AAA\n";
                        break;
                    }
                    case 14:
                    {
                        std::cerr << "AAA\n";
                        size_t n = d.decodeUnionIndex();
                        if (n >= 2)
                            throw avro::Exception("Union index too big");
                        if (n == 0)
                            d.decodeNull();
                        else
                        {
                             for (size_t j = d.arrayStart(); n != 0; n = d.arrayNext())
                             {
                                 for (size_t i = 0; i < j; ++i)
                                 {
                                     int32_t t;
                                     avro::decode(d, t);
                                     v.equality_ids.push_back(t);
                                 }
                             }
                        }
                        std::cerr << "BBB\n";
                        break;
                    }
                    case 15:
                        avro::decode(d, v.sort_order_id);
                        break;
                    case 16:
                        avro::decode(d, v.first_row_id);
                        break;
                    case 17:
                        avro::decode(d, v.referenced_data_file);
                        break;
                    case 18:
                        avro::decode(d, v.content_offset);
                        break;
                    case 19:
                        avro::decode(d, v.content_size_in_bytes);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            std::cerr << "CONTENT\n";
            avro::decode(d, v.content);
            std::cerr << "CONTENT DONE\n";
            avro::decode(d, v.file_path);
            std::cerr << "PATH\n";
            avro::decode(d, v.file_format);
            std::cerr << "FORMAT\n";
            avro::decode(d, v.partition);
            std::cerr << "Parsing further\n";
            avro::decode(d, v.record_count);
            std::cerr << "Done record count\n";
            avro::decode(d, v.file_size_in_bytes);
            std::cerr << "Done record count1\n";
            avro::decode(d, v.column_sizes);
            std::cerr << "Done record count2\n";
            avro::decode(d, v.value_counts);
            std::cerr << "Done record count3\n";
            avro::decode(d, v.null_value_counts);
            std::cerr << "Done record count4\n";
            avro::decode(d, v.nan_value_counts);
            std::cerr << "Done record count5\n";
            avro::decode(d, v.lower_bounds);
            std::cerr << "Done record count6\n";
            avro::decode(d, v.upper_bounds);
            std::cerr << "Done record count7\n";
            avro::decode(d, v.key_metadata);
            std::cerr << "Done record count8\n";
            {
                size_t n = d.decodeUnionIndex();
                if (n >= 2)
                    throw avro::Exception("Union index too big");
                if (n == 0)
                    d.decodeNull();
                else
                {
                     for (size_t j = d.arrayStart(); n != 0; n = d.arrayNext())
                     {
                         for (size_t i = 0; i < j; ++i)
                         {
                             int64_t t;
                             avro::decode(d, t);
                             v.split_offsets.push_back(t);
                         }
                     }

                }
            }
            std::cerr << "Done record count9\n";
            {
                size_t n = d.decodeUnionIndex();
                if (n >= 2)
                    throw avro::Exception("Union index too big");
                if (n == 0)
                    d.decodeNull();
                else
                {
                     for (size_t j = d.arrayStart(); n != 0; n = d.arrayNext())
                     {
                         for (size_t i = 0; i < j; ++i)
                         {
                             int32_t t;
                             avro::decode(d, t);
                             v.equality_ids.push_back(t);
                         }
                     }
                }
            }
            std::cerr << "Done record count10\n";
            avro::decode(d, v.sort_order_id);
            std::cerr << "Done record count11\n";
            avro::decode(d, v.first_row_id);
            std::cerr << "Done record count12\n";
            avro::decode(d, v.referenced_data_file);
            std::cerr << "Done record count13\n";
            avro::decode(d, v.content_offset);
            std::cerr << "Done record count14\n";
            avro::decode(d, v.content_size_in_bytes);
            std::cerr << "Done record count15\n";
        }
    }
};

template <>
struct codec_traits<Iceberg::Manifest>
{
    static void encode(Encoder & e, const Iceberg::Manifest & v)
    {
        avro::encode(e, v.status);
        avro::encode(e, v.snapshot_id);
        avro::encode(e, v.sequence_number);
        avro::encode(e, v.file_sequence_number);
        avro::encode(e, v.data_file);
    }
    static void decode(Decoder & d, Iceberg::Manifest & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.status);
                        break;
                    case 1:
                        avro::decode(d, v.snapshot_id);
                        break;
                    case 2:
                        avro::decode(d, v.sequence_number);
                        break;
                    case 3:
                        avro::decode(d, v.file_sequence_number);
                        break;
                    case 4:
                        avro::decode(d, v.data_file);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.status);
            avro::decode(d, v.snapshot_id);
            avro::decode(d, v.sequence_number);
            avro::decode(d, v.file_sequence_number);
            avro::decode(d, v.data_file);
        }
    }
};

}
