#pragma once

#include <Storages/ColumnCodecDescription.h>
#include <Storages/ColumnCodecValidation.h>

#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

struct NameAndTypePair;

struct ResolvedCodecDeclaration
{
    ASTPtr codec;
    CodecPath declaration_path;
    bool codec_is_part_default = true;
    ApplicableCodecStream stream;
};

/// Resolves one column policy against the physical streams of a full or partial column write.
class ColumnCodecResolver
{
public:
    ColumnCodecResolver(
        const ColumnCodecDescription & policy_,
        DataTypePtr owning_type_,
        const NameAndTypePair & written_column,
        ASTPtr part_default_,
        bool apply_adaptive_default_ = false);

    ResolvedCodecDeclaration resolve(const ISerialization::SubstreamPath & stream_path) const;
    CompressionCodecPtr getCodec(
        const ISerialization::SubstreamPath & stream_path,
        const CompressionCodecPtr & part_default_codec) const;

private:
    CodecPath getLogicalPath(const ISerialization::SubstreamPath & stream_path) const;
    ResolvedCodecDeclaration resolveRaw(const ISerialization::SubstreamPath & stream_path) const;

    const ColumnCodecDescription & policy;
    DataTypePtr owning_type;
    CodecPath written_column_prefix;
    ASTPtr part_default;
    bool apply_adaptive_default = false;
};

}
