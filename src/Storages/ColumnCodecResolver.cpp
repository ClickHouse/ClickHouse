#include <Storages/ColumnCodecResolver.h>

#include <Compression/CompressionCodecAdaptive.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
ASTPtr getGenericCodecDescription(const ASTPtr & codec)
{
    const auto & function = codec->as<ASTFunction &>();
    ASTs stages;
    for (const auto & stage : function.arguments->children)
    {
        const auto * identifier = stage->as<ASTIdentifier>();
        if (identifier && identifier->name() == DEFAULT_CODEC_NAME)
        {
            stages.push_back(stage->clone());
            continue;
        }
        auto single = makeASTFunction("CODEC", stage->clone());
        if (CompressionCodecFactory::instance()
                .get(single, static_cast<const IDataType *>(nullptr), nullptr, /* only_generic = */ true)
                ->isGenericCompression())
            stages.push_back(stage->clone());
    }
    if (stages.empty())
        stages.push_back(make_intrusive<ASTIdentifier>("NONE"));
    auto result = makeASTFunction("CODEC");
    result->setKind(ASTFunction::Kind::CODEC);
    result->arguments->children = std::move(stages);
    return result;
}
}

ColumnCodecResolver::ColumnCodecResolver(
    ColumnCodecPolicyView policy_,
    DataTypePtr owner_type_,
    const NameAndTypePair & written_column,
    ASTPtr part_default_,
    bool apply_adaptive_default_)
    : policy(std::move(policy_))
    , classifier(std::move(owner_type_), written_column)
    , part_default(std::move(part_default_))
    , apply_adaptive_default(apply_adaptive_default_)
{
}

std::optional<ResolvedCodecDeclaration> ColumnCodecResolver::tryResolveRaw(
    const ISerialization::SubstreamPath & stream_path) const
{
    auto stream = classifier.classify(stream_path);
    if (!stream)
        return std::nullopt;

    const auto declaration = policy.find(stream->logical_path);
    const bool use_part_default = !declaration || CompressionCodecFactory::isDefaultCodec(declaration->codec);
    ASTPtr codec = use_part_default && part_default ? part_default : (declaration ? declaration->codec : ASTPtr{});
    TupleCodecPath declaration_path = declaration ? declaration->declaration_path : TupleCodecPath{};

    return ResolvedCodecDeclaration{
        .codec = std::move(codec),
        .declaration_path = std::move(declaration_path),
        .codec_is_part_default = use_part_default,
        .stream = std::move(*stream),
    };
}

ResolvedCodecDeclaration ColumnCodecResolver::resolve(const ISerialization::SubstreamPath & stream_path) const
{
    auto resolved = tryResolveRaw(stream_path);
    if (!resolved)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot resolve a codec for an ephemeral stream");

    if (resolved->stream.structural && resolved->codec
        && !(resolved->codec_is_part_default && !part_default))
        resolved->codec = getGenericCodecDescription(resolved->codec);
    return std::move(*resolved);
}

CompressionCodecPtr ColumnCodecResolver::getCodec(
    const ISerialization::SubstreamPath & stream_path,
    const CompressionCodecPtr & part_default_codec) const
{
    auto resolved = tryResolveRaw(stream_path);
    if (!resolved)
        return nullptr;
    auto codec = CompressionCodecFactory::instance().get(
        resolved->codec,
        resolved->stream.structural ? nullptr : resolved->stream.leaf_type.get(),
        part_default_codec,
        resolved->stream.structural);
    if (apply_adaptive_default && resolved->codec_is_part_default && !codec->isEncryption())
        return std::make_shared<CompressionCodecAdaptive>(resolved->stream.leaf_type, codec);
    return codec;
}

}
