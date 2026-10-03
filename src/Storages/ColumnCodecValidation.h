#pragma once

#include <Storages/ColumnCodecPath.h>
#include <Storages/ColumnCodecPolicy.h>

#include <DataTypes/Serializations/ISerialization.h>

#include <optional>

namespace DB
{

struct CodecValidationSettings;
struct NameAndTypePair;

struct ApplicableCodecStream
{
    TupleCodecPath logical_path;
    DataTypePtr leaf_type;
    bool structural = false;
    /// The physical route to this stream enters a Map. Lossy codecs are unsafe
    /// there because they can alter serialized keys.
    bool enters_map = false;
};

/// Shared logical classification for DDL validation, runtime writing, key checks,
/// and subcolumn introspection.
class CodecStreamClassifier
{
public:
    explicit CodecStreamClassifier(DataTypePtr owner_type_);
    CodecStreamClassifier(DataTypePtr owner_type_, const NameAndTypePair & written_column);

    /// Empty means that the complete path identifies an ephemeral stream.
    std::optional<ApplicableCodecStream> classify(const ISerialization::SubstreamPath & path) const;

private:
    DataTypePtr owner_type;
    /// Exact serialization route from the owner to a partially written subcolumn.
    /// It preserves structural steps such as array sizes and nullable null maps.
    ISerialization::SubstreamPath written_column_substream_prefix;
};

struct NormalizedColumnCodecPolicy
{
    ASTPtr root;
    TupleElementCodecs elements;
};

NormalizedColumnCodecPolicy validateColumnCodecPolicy(
    const ColumnCodecPolicyView & policy,
    const DataTypePtr & owner_type,
    const CodecValidationSettings & settings,
    const CodecDeclarationPaths * declarations_requiring_full_validation = nullptr);

}
