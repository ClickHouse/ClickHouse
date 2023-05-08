#include <config.h>

#if USE_AWS_S3

#    include <IO/Operators.h>
#    include <IO/ReadBufferFromString.h>
#    include <Storages/S3Queue/S3QueueSettings.h>
#    include <Storages/S3Queue/S3QueueTableMetadata.h>
#    include <Storages/StorageS3.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_MISMATCH;
}

S3QueueTableMetadata::S3QueueTableMetadata(const StorageS3::Configuration & configuration, const S3QueueSettings & engine_settings)
{
    format_name = configuration.format;
    after_processing = engine_settings.after_processing.toString();
    mode = engine_settings.mode.toString();
    s3queue_max_set_size = engine_settings.s3queue_max_set_size;
    s3queue_max_set_age_s = engine_settings.s3queue_max_set_age_s;
}

void S3QueueTableMetadata::write(WriteBuffer & out) const
{
    out << "metadata format version: 1\n"
        << "after processing: " << after_processing << "\n"
        << "mode: " << mode << "\n"
        << "s3queue_max_set_size: " << s3queue_max_set_size << "\n"
        << "s3queue_max_set_age_s: " << s3queue_max_set_age_s << "\n"
        << "format name: " << format_name << "\n";
}

String S3QueueTableMetadata::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

void S3QueueTableMetadata::read(ReadBuffer & in)
{
    in >> "metadata format version: 1\n";
    in >> "after processing: " >> after_processing >> "\n";
    in >> "mode: " >> mode >> "\n";
    in >> "s3queue_max_set_size: " >> s3queue_max_set_size >> "\n";
    in >> "s3queue_max_set_age_s: " >> s3queue_max_set_age_s >> "\n";
    in >> "format name: " >> format_name >> "\n";
}

S3QueueTableMetadata S3QueueTableMetadata::parse(const String & s)
{
    S3QueueTableMetadata metadata;
    ReadBufferFromString buf(s);
    metadata.read(buf);
    return metadata;
}


void S3QueueTableMetadata::checkImmutableFieldsEquals(const S3QueueTableMetadata & from_zk) const
{
    if (after_processing != from_zk.after_processing)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs "
            "in action after processing. Stored in ZooKeeper: {}, local: {}",
            DB::toString(from_zk.after_processing),
            DB::toString(after_processing));

    if (mode != from_zk.mode)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in engine mode. "
            "Stored in ZooKeeper: {}, local: {}",
            DB::toString(from_zk.after_processing),
            DB::toString(after_processing));

    if (s3queue_max_set_size != from_zk.s3queue_max_set_size)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set size. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.s3queue_max_set_size,
            s3queue_max_set_size);

    if (s3queue_max_set_age_s != from_zk.s3queue_max_set_age_s)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in max set age. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.s3queue_max_set_age_s,
            s3queue_max_set_age_s);

    if (format_name != from_zk.format_name)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Existing table metadata in ZooKeeper differs in format name. "
            "Stored in ZooKeeper: {}, local: {}",
            from_zk.format_name,
            format_name);
}

void S3QueueTableMetadata::checkEquals(const S3QueueTableMetadata & from_zk) const
{
    checkImmutableFieldsEquals(from_zk);
}

}

#endif
