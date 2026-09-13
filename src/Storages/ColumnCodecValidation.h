#pragma once

#include <Storages/ColumnCodecDescription.h>

#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

struct CodecValidationSettings;
struct Settings;

struct ApplicableCodecStream
{
    CodecPath logical_path;
    /// Null for structural streams such as Array offsets and Nullable null maps.
    DataTypePtr leaf_type;
    bool structural = false;
};

/// Return the logical Tuple-element path recorded in a serialization stream path.
CodecPath getCodecPath(const ISerialization::SubstreamPath & path);

/// Classify one physical serialization stream. Runtime resolution uses the same function.
ApplicableCodecStream classifyCodecStream(const ISerialization::SubstreamPath & path);

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

/// Validate the complete policy. Apply session settings only to codecs declared by this ALTER.
/// Validate retained codecs as trusted metadata, including their paths and data types.
ColumnCodecDescription validateColumnCodecDescriptionForAlter(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings);

/// Checks that the session settings allow new declarations of codecs of Tuple elements. They are allowed by
/// `enable_tuple_element_codecs`, and also by `enable_time_series_table` because the `TimeSeries` table engine
/// declares such codecs in the samples tables it generates.
void checkTupleElementCodecsAreEnabled(const Settings & settings);

}
