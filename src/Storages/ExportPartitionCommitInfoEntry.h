#pragma once

#include <sstream>
#include <base/types.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

/// All Iceberg fields are empty for non-Iceberg destinations. They may also be
/// empty for an Iceberg destination if the committing node crashed between
/// writing the object-storage files and recording this entry; in that case the
/// task still reaches COMPLETED through the recovery path but the commit info
/// remains absent. This is best-effort observability and acceptable.
struct ExportPartitionCommitInfoEntry
{
    /// Iceberg: path (in destination object storage) of the new vN.metadata.json
    /// written by the commit.
    String iceberg_metadata_file;

    /// Iceberg: path of the snap-<id>-<format_version>-<uuid>.avro manifest list
    /// referenced by the new snapshot.
    String iceberg_manifest_list;

    /// Iceberg: path of the manifest entry file (*.avro) referenced by the
    /// manifest list.
    String iceberg_manifest_file;

    /// Plain object storage: path of the commit marker file written by
    /// StorageObjectStorage::commitExportPartitionTransaction. Empty for Iceberg.
    String commit_marker_file;

    Poco::JSON::Object::Ptr toJsonObject() const
    {
        Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
        json->set("iceberg_metadata_file", iceberg_metadata_file);
        json->set("iceberg_manifest_list", iceberg_manifest_list);
        json->set("iceberg_manifest_file", iceberg_manifest_file);
        json->set("commit_marker_file", commit_marker_file);
        return json;
    }

    static ExportPartitionCommitInfoEntry fromJsonObject(const Poco::JSON::Object::Ptr & json)
    {
        ExportPartitionCommitInfoEntry entry;

        if (json->has("iceberg_metadata_file"))
            entry.iceberg_metadata_file = json->getValue<String>("iceberg_metadata_file");
        if (json->has("iceberg_manifest_list"))
            entry.iceberg_manifest_list = json->getValue<String>("iceberg_manifest_list");

        if (json->has("iceberg_manifest_file"))
            entry.iceberg_manifest_file = json->getValue<String>("iceberg_manifest_file");

        if (json->has("commit_marker_file"))
            entry.commit_marker_file = json->getValue<String>("commit_marker_file");

        return entry;
    }

    std::string toJsonString() const
    {
        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        toJsonObject()->stringify(oss);
        return oss.str();
    }

    static ExportPartitionCommitInfoEntry fromJsonString(const std::string & json_string)
    {
        if (json_string.empty())
            return {};

        Poco::JSON::Parser parser;
        return fromJsonObject(parser.parse(json_string).extract<Poco::JSON::Object::Ptr>());
    }
};

}
