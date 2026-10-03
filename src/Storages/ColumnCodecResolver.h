#pragma once

#include <Storages/ColumnCodecPolicy.h>
#include <Storages/ColumnCodecValidation.h>

#include <Compression/ICompressionCodec.h>

namespace DB
{

struct NameAndTypePair;

struct ResolvedCodecDeclaration
{
    ASTPtr codec;
    TupleCodecPath declaration_path;
    bool codec_is_part_default = true;
    ApplicableCodecStream stream;
};

/// Resolves one borrowed policy against the real streams of a full or partial write.
class ColumnCodecResolver
{
public:
    ColumnCodecResolver(
        ColumnCodecPolicyView policy_,
        DataTypePtr owner_type_,
        const NameAndTypePair & written_column,
        ASTPtr part_default_,
        bool apply_adaptive_default_ = false);

    ResolvedCodecDeclaration resolve(const ISerialization::SubstreamPath & stream_path) const;

    CompressionCodecPtr getCodec(
        const ISerialization::SubstreamPath & stream_path,
        const CompressionCodecPtr & part_default_codec) const;

private:
    std::optional<ResolvedCodecDeclaration> tryResolveRaw(
        const ISerialization::SubstreamPath & stream_path) const;

    ColumnCodecPolicyView policy;
    CodecStreamClassifier classifier;
    ASTPtr part_default;
    bool apply_adaptive_default = false;
};

}
