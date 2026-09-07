#pragma once

#include <Storages/ColumnCodecDescription.h>

#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <vector>

namespace DB
{

struct CodecValidationSettings;

struct ApplicableCodecStream
{
    CodecPath logical_path;
    /// Null for structural streams such as Array offsets and Nullable null maps.
    DataTypePtr leaf_type;
    bool structural = false;
};

struct EffectiveCodecStream
{
    ApplicableCodecStream stream;
    CodecPath declaration_path;
    ASTPtr normalized_codec;
    bool codec_is_part_default = false;
};

struct ColumnCodecValidationResult
{
    ColumnCodecDescription codec;
    std::vector<EffectiveCodecStream> effective_streams;
};

/// Return the logical Tuple-element path recorded in a serialization stream path.
CodecPath getCodecPath(const ISerialization::SubstreamPath & path);

/// Classify one physical serialization stream. Runtime resolution uses the same function.
ApplicableCodecStream classifyCodecStream(const ISerialization::SubstreamPath & path);

ColumnCodecValidationResult validateColumnCodecDescriptionAndGetStreams(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

ColumnCodecValidationResult validateColumnCodecDescriptionForAlterAndGetStreams(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings);

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

/// Validate the complete policy. Apply session settings only to codecs changed by this ALTER.
/// Validate retained codecs as trusted metadata, including their paths and data types.
ColumnCodecDescription validateColumnCodecDescriptionForAlter(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings);

}
