#include <Storages/ColumnCodecResolver.h>

#include <Compression/CompressionCodecAdaptive.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>

#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Keep only generic compression stages without replacing symbolic Default.
ASTPtr getGenericCodecDescription(const ASTPtr & codec)
{
    const auto & codec_function = codec->as<ASTFunction &>();
    ASTs generic_stages;
    for (const auto & stage : codec_function.arguments->children)
    {
        const auto * identifier = stage->as<ASTIdentifier>();
        if (identifier && identifier->name() == DEFAULT_CODEC_NAME)
        {
            generic_stages.push_back(stage->clone());
            continue;
        }

        auto single_stage = makeASTFunction("CODEC", stage->clone());
        if (CompressionCodecFactory::instance()
                .get(single_stage, static_cast<const IDataType *>(nullptr), nullptr, /* only_generic = */ true)
                ->isGenericCompression())
            generic_stages.push_back(stage->clone());
    }

    if (generic_stages.empty())
        generic_stages.push_back(make_intrusive<ASTIdentifier>("NONE"));

    auto result = makeASTFunction("CODEC");
    result->setKind(ASTFunction::Kind::CODEC);
    result->arguments->children = std::move(generic_stages);
    return result;
}

/// Find the Tuple path represented by a partial written column once per resolver.
CodecPath getWrittenColumnPrefix(const NameAndTypePair & written_column, const DataTypePtr & owning_type)
{
    if (!written_column.isSubcolumn())
        return {};

    CodecPath prefix;
    bool found = false;
    IDataType::forEachSubcolumn(
        [&](const auto & path, const auto & name, const auto &)
        {
            if (name == written_column.getSubcolumnName())
            {
                prefix = getCodecPath(path);
                found = true;
            }
        },
        ISerialization::SubstreamData(owning_type->getDefaultSerialization()).withType(owning_type));
    if (!found)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot resolve subcolumn {} in type {}",
            written_column.getSubcolumnName(),
            owning_type->getName());
    return prefix;
}

}

ColumnCodecResolver::ColumnCodecResolver(
    const ColumnCodecDescription & policy_,
    DataTypePtr owning_type_,
    const NameAndTypePair & written_column,
    ASTPtr part_default_,
    bool apply_adaptive_default_)
    : policy(policy_)
    , owning_type(std::move(owning_type_))
    , written_column_prefix(getWrittenColumnPrefix(written_column, owning_type))
    , part_default(std::move(part_default_))
    , apply_adaptive_default(apply_adaptive_default_)
{
}

CodecPath ColumnCodecResolver::getLogicalPath(const ISerialization::SubstreamPath & stream_path) const
{
    CodecPath suffix = getCodecPath(stream_path);
    if (suffix.size() >= written_column_prefix.size()
        && std::equal(written_column_prefix.begin(), written_column_prefix.end(), suffix.begin()))
        return suffix;

    CodecPath result = written_column_prefix;
    result.insert(result.end(), suffix.begin(), suffix.end());
    return result;
}

ResolvedCodecDeclaration ColumnCodecResolver::resolveRaw(const ISerialization::SubstreamPath & stream_path) const
{
    auto stream = classifyCodecStream(stream_path);
    stream.logical_path = getLogicalPath(stream_path);

    auto declaration = policy.find(stream.logical_path);
    const bool use_part_default = !declaration.codec || CompressionCodecFactory::isDefaultCodec(declaration.codec);
    ASTPtr codec = use_part_default && part_default ? part_default : declaration.codec;

    return {
        .codec = std::move(codec),
        .declaration_path = std::move(declaration.declaration_path),
        .codec_is_part_default = use_part_default,
        .stream = std::move(stream),
    };
}

ResolvedCodecDeclaration ColumnCodecResolver::resolve(const ISerialization::SubstreamPath & stream_path) const
{
    auto resolved = resolveRaw(stream_path);
    /// Preserve symbolic Default when no concrete part default is available for introspection.
    if (resolved.stream.structural && resolved.codec && !(resolved.codec_is_part_default && !part_default))
        resolved.codec = getGenericCodecDescription(resolved.codec);
    return resolved;
}

CompressionCodecPtr ColumnCodecResolver::getCodec(
    const ISerialization::SubstreamPath & stream_path,
    const CompressionCodecPtr & part_default_codec) const
{
    auto resolved = resolveRaw(stream_path);
    auto codec = CompressionCodecFactory::instance().get(
        resolved.codec,
        resolved.stream.structural ? nullptr : resolved.stream.leaf_type.get(),
        part_default_codec,
        resolved.stream.structural);

    if (apply_adaptive_default && resolved.codec_is_part_default && resolved.stream.leaf_type && !codec->isEncryption())
        return std::make_shared<CompressionCodecAdaptive>(resolved.stream.leaf_type, codec);
    return codec;
}

}
