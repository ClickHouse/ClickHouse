#pragma once

#include <string>
#include <Core/Types.h>

#include <Common/logger_useful.h>
#include <Decoder.hh>
#include <Encoder.hh>
#include <Specific.hh>

namespace Iceberg
{

struct Partition
{
    std::optional<bool> contains_nan;
    bool contains_null;
    std::vector<uint8_t> lower_bound;
    std::vector<uint8_t> upper_bound;
};

using Partitions = std::vector<Partition>;

struct ManifestList
{
    std::string manifest_path;
    int64_t manifest_length;
    int32_t partition_spec_id;
    int32_t content;
    int64_t sequence_number;
    int64_t min_sequence_number;
    int64_t added_snapshot_id;
    int32_t added_files_count;
    int32_t existing_files_count;
    int32_t deleted_files_count;
    int64_t added_rows_count;
    int64_t existing_rows_count;
    int64_t deleted_rows_count;

    Partitions partitions;
    std::vector<uint8_t> key_metadata;

    static inline const std::string SCHEMA = R"({
  "type" : "record",
  "name" : "manifest_file",
  "fields" : [ {
    "name" : "manifest_path",
    "type" : "string",
    "doc" : "Location URI with FS scheme",
    "field-id" : 500
  }, {
    "name" : "manifest_length",
    "type" : "long",
    "doc" : "Total file size in bytes",
    "field-id" : 501
  }, {
    "name" : "partition_spec_id",
    "type" : "int",
    "doc" : "Spec ID used to write",
    "field-id" : 502
  }, {
    "name" : "content",
    "type" : "int",
    "doc" : "Contents of the manifest: 0=data, 1=deletes",
    "field-id" : 517
  }, {
    "name" : "sequence_number",
    "type" : "long",
    "doc" : "Sequence number when the manifest was added",
    "field-id" : 515
  }, {
    "name" : "min_sequence_number",
    "type" : "long",
    "doc" : "Lowest sequence number in the manifest",
    "field-id" : 516
  }, {
    "name" : "added_snapshot_id",
    "type" : "long",
    "doc" : "Snapshot ID that added the manifest",
    "field-id" : 503
  }, {
    "name" : "added_data_files_count",
    "type" : "int",
    "doc" : "Added entry count",
    "field-id" : 504
  }, {
    "name" : "existing_data_files_count",
    "type" : "int",
    "doc" : "Existing entry count",
    "field-id" : 505
  }, {
    "name" : "deleted_data_files_count",
    "type" : "int",
    "doc" : "Deleted entry count",
    "field-id" : 506
  }, {
    "name" : "added_rows_count",
    "type" : "long",
    "doc" : "Added rows count",
    "field-id" : 512
  }, {
    "name" : "existing_rows_count",
    "type" : "long",
    "doc" : "Existing rows count",
    "field-id" : 513
  }, {
    "name" : "deleted_rows_count",
    "type" : "long",
    "doc" : "Deleted rows count",
    "field-id" : 514
  }, {
    "name" : "partitions",
    "type" : [ "null", {
      "type" : "array",
      "items" : {
        "type" : "record",
        "name" : "r508",
        "fields" : [ {
          "name" : "contains_null",
          "type" : "boolean",
          "doc" : "True if any file has a null partition value",
          "field-id" : 509
        }, {
          "name" : "contains_nan",
          "type" : [ "null", "boolean" ],
          "doc" : "True if any file has a nan partition value",
          "default" : null,
          "field-id" : 518
        }, {
          "name" : "lower_bound",
          "type" : [ "null", "bytes" ],
          "doc" : "Partition lower bound for all files",
          "default" : null,
          "field-id" : 510
        }, {
          "name" : "upper_bound",
          "type" : [ "null", "bytes" ],
          "doc" : "Partition upper bound for all files",
          "default" : null,
          "field-id" : 511
        } ]
      },
      "element-id" : 508
    } ],
    "doc" : "Summary for each partition",
    "default" : null,
    "field-id" : 507
  }, {
    "name" : "key_metadata",
    "type" : [ "null", "bytes" ],
    "doc" : "Implementation-specific key metadata for encryption",
    "default" : null,
    "field-id" : 519
  }
 ]})";

};

}

namespace avro
{

template <>
struct codec_traits<Iceberg::Partition>
{
    static void encode(Encoder & e, Iceberg::Partition v)
    {
        e.encodeBool(v.contains_null);

        if (v.contains_nan.has_value())
        {
            e.encodeUnionIndex(1);
            e.encodeBool(*v.contains_nan);
        }
        else
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }

        for (const auto & bound : {v.lower_bound, v.upper_bound})
        {
            if (bound.empty())
            {
                e.encodeUnionIndex(0);
                e.encodeNull();
            }
            else
            {
                e.encodeUnionIndex(1);
                e.encodeBytes(bound);

            }
        }
    }

    static void decode(Decoder & d, Iceberg::Partition & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                    {
                        v.contains_null = d.decodeBool();
                        break;
                    }
                    case 1:
                    {
                        size_t n = d.decodeUnionIndex();
                        if (n == 0)
                            d.decodeNull();
                        else if (n == 1)
                            v.contains_nan = d.decodeBool();
                        break;
                    }
                    case 2:
                    {
                        size_t n = d.decodeUnionIndex();
                        if (n == 0)
                            d.decodeNull();
                        else if (n == 1)
                            d.decodeBytes(v.lower_bound);
                        break;
                    }
                    case 3:
                    {
                        size_t n = d.decodeUnionIndex();
                        if (n == 0)
                            d.decodeNull();
                        else if (n == 1)
                            d.decodeBytes(v.upper_bound);
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        else
        {
            v.contains_null = d.decodeBool();

            size_t n = d.decodeUnionIndex();
            if (n == 0)
                d.decodeBool();
            else if (n == 1)
                v.contains_nan = d.decodeBool();

            n = d.decodeUnionIndex();
            if (n == 0)
                d.decodeNull();
            else if (n == 1)
                d.decodeBytes(v.lower_bound);

            n = d.decodeUnionIndex();
            if (n == 0)
                d.decodeNull();
            else if (n == 1)
                d.decodeBytes(v.upper_bound);
        }
    }
};

template <>
struct codec_traits<Iceberg::Partitions>
{
    static void encode(Encoder & e, Iceberg::Partitions v)
    {
        std::cerr << "ENCODING PARTITIONS " << v.size() << std::endl;
        if (v.empty())
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }
        else
        {
            e.encodeUnionIndex(1);
            e.arrayStart();
            e.setItemCount(v.size());
            for (const auto & partition : v)
            {
                std::cerr << "WRITING PARTITION" << std::endl;
                e.startItem();
                avro::encode(e, partition);
                std::cerr << "WRITING PARTITION FINISHED" << std::endl;
            }
            e.arrayEnd();
        }
    }
    static void decode(Decoder & d, Iceberg::Partitions & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
            throw avro::Exception("Union index too big");
        std::cerr << "DECODING PARTITIONS " << n << std::endl;

        if (n == 0)
            d.decodeNull();
        else if (n == 1)
        {
            for (size_t j = d.arrayStart(); n != 0; n = d.arrayNext())
            {
                for (size_t i = 0; i < j; ++i)
                {
                    Iceberg::Partition t;
                    avro::decode(d, t);
                    v.push_back(t);
                }
            }
        }

        std::cerr << "DECODING SIZE " << v.size() << std::endl;
    }
};

template <>
struct codec_traits<Iceberg::ManifestList>
{
    static void encode(Encoder & e, const Iceberg::ManifestList & v)
    {
        avro::encode(e, v.manifest_path);
        avro::encode(e, v.manifest_length);
        avro::encode(e, v.partition_spec_id);
        avro::encode(e, v.content);
        avro::encode(e, v.sequence_number);
        avro::encode(e, v.min_sequence_number);
        avro::encode(e, v.added_snapshot_id);
        avro::encode(e, v.added_files_count);
        avro::encode(e, v.existing_files_count);
        avro::encode(e, v.deleted_files_count);
        avro::encode(e, v.added_rows_count);
        avro::encode(e, v.existing_rows_count);
        avro::encode(e, v.deleted_rows_count);
        avro::encode(e, v.partitions);
        if (v.key_metadata.empty())
        {
            e.encodeUnionIndex(0);
            e.encodeNull();
        }
        else
        {
            e.encodeUnionIndex(1);
            avro::encode(e, v.key_metadata);
        }
    }

    static void decode(Decoder & d, Iceberg::ManifestList & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            std::cerr << "RESOLVING DECODER\n";
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.manifest_path);
                        break;
                    case 1:
                        avro::decode(d, v.manifest_length);
                        break;
                    case 2:
                        avro::decode(d, v.partition_spec_id);
                        break;
                    case 3:
                        avro::decode(d, v.content);
                        break;
                    case 4:
                        avro::decode(d, v.sequence_number);
                        break;
                    case 5:
                        avro::decode(d, v.min_sequence_number);
                        break;
                    case 6:
                        avro::decode(d, v.added_snapshot_id);
                        break;
                    case 7:
                        avro::decode(d, v.added_files_count);
                        break;
                    case 8:
                        avro::decode(d, v.existing_files_count);
                        break;
                    case 9:
                        avro::decode(d, v.deleted_files_count);
                        break;
                    case 10:
                        avro::decode(d, v.added_rows_count);
                        break;
                    case 11:
                        avro::decode(d, v.existing_rows_count);
                        break;
                    case 12:
                        avro::decode(d, v.deleted_rows_count);
                        break;
                    case 13:
                        avro::decode(d, v.partitions);
                        break;
                    case 14:
                    {
                        size_t n = d.decodeUnionIndex();
                        if (n == 1)
                        {
                            avro::decode(d, v.key_metadata);
                        }
                        else
                        {
                            d.decodeNull();
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        else
        {
            std::cerr << "DEFAULT DECODER\n";
            avro::decode(d, v.manifest_path);
            avro::decode(d, v.manifest_length);
            avro::decode(d, v.partition_spec_id);
            avro::decode(d, v.content);
            avro::decode(d, v.sequence_number);
            avro::decode(d, v.min_sequence_number);
            avro::decode(d, v.added_snapshot_id);
            avro::decode(d, v.added_files_count);
            avro::decode(d, v.existing_files_count);
            avro::decode(d, v.deleted_files_count);
            avro::decode(d, v.added_rows_count);
            avro::decode(d, v.existing_rows_count);
            avro::decode(d, v.deleted_rows_count);
            avro::decode(d, v.partitions);
            size_t n = d.decodeUnionIndex();
            if (n == 1)
                avro::decode(d, v.key_metadata);
            else
                d.decodeNull();
        }
    }
};

}
