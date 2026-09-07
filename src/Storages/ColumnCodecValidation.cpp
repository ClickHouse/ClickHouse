#include <Storages/ColumnCodecValidation.h>

#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnCodecAST.h>

#include <Common/Exception.h>

#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CodecPath getCodecPath(const ISerialization::SubstreamPath & path)
{
    CodecPath result;
    for (const auto & entry : path)
        if (entry.type == ISerialization::Substream::TupleElement)
            result.push_back(entry.name_of_substream);
    return result;
}

ApplicableCodecStream classifyCodecStream(const ISerialization::SubstreamPath & path)
{
    if (path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot classify an empty serialization stream path");

    const bool structural = !ISerialization::isSpecialCompressionAllowed(path);
    return {
        .logical_path = getCodecPath(path),
        .leaf_type = structural ? nullptr : path.back().data.type,
        .structural = structural,
    };
}

namespace
{

using StreamsByDeclaration = std::map<CodecPath, std::vector<ApplicableCodecStream>>;

/// Enumerate the physical streams that can receive a column codec.
std::vector<ApplicableCodecStream> enumerateApplicableStreams(const DataTypePtr & logical_type)
{
    std::vector<ApplicableCodecStream> result;
    auto serialization = logical_type->getDefaultSerialization();
    serialization->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            if (!path.empty() && !ISerialization::isEphemeralSubcolumn(path, path.size()))
                result.push_back(classifyCodecStream(path));
        },
        logical_type);
    return result;
}

/// Canonicalize, normalize, and instantiate every effective declaration.
ColumnCodecValidationResult validatePolicy(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings,
    const ColumnCodecDescription::CodecsByPath * declarations_to_admit)
{
    ColumnCodecDescription canonical_policy;
    for (const auto & [declaration_path, codec] : policy.getCodecs())
    {
        auto canonical_path = declaration_path.empty() ? CodecPath{} : canonicalizeCodecPath(logical_type, declaration_path);
        if (canonical_policy.getCodecs().contains(canonical_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for Tuple subcolumn");
        canonical_policy.set(std::move(canonical_path), codec);
    }

    const auto streams = enumerateApplicableStreams(logical_type);
    StreamsByDeclaration streams_by_declaration;
    std::vector<ApplicableCodecStream> part_default_streams;
    for (const auto & stream : streams)
    {
        auto declaration = canonical_policy.find(stream.logical_path);
        if (declaration.codec)
            streams_by_declaration[declaration.declaration_path].push_back(stream);
        else
            part_default_streams.push_back(stream);
    }

    ColumnCodecValidationResult result;
    for (auto & stream : part_default_streams)
        result.effective_streams.push_back({std::move(stream), {}, nullptr, true});
    const auto trusted_settings = CodecValidationSettings::trusted();
    auto & factory = CompressionCodecFactory::instance();

    for (const auto & [declaration_path, ast] : canonical_policy.getCodecs())
    {
        const bool use_session_settings = !declarations_to_admit || declarations_to_admit->contains(declaration_path);
        const auto & declaration_settings = use_session_settings ? settings : trusted_settings;
        const auto stream_group_it = streams_by_declaration.find(declaration_path);

        ASTPtr common_normalized;
        bool has_value_stream = false;
        bool all_normalized_equal = true;
        if (stream_group_it != streams_by_declaration.end())
        {
            for (const auto & stream : stream_group_it->second)
            {
                if (stream.structural)
                    continue;
                has_value_stream = true;
                auto candidate = factory.validateCodecAndGetPreprocessedAST(ast, stream.leaf_type, declaration_settings);
                if (!common_normalized)
                    common_normalized = candidate;
                else if (common_normalized->formatWithSecretsOneLine() != candidate->formatWithSecretsOneLine())
                    all_normalized_equal = false;
            }
        }

        if (!common_normalized)
            factory.validateCodecAndGetPreprocessedAST(ast, DataTypePtr{}, declaration_settings);

        /// With no value type, keep implicit type parameters unresolved. A dormant parent such as
        /// CODEC(Delta) must not turn into CODEC(Delta(1)) before a child override is removed.
        const ASTPtr stored = has_value_stream && all_normalized_equal ? common_normalized : ast;
        result.codec.set(declaration_path, stored);
        const bool declaration_is_part_default = CompressionCodecFactory::isDefaultCodec(ast);

        if (stream_group_it == streams_by_declaration.end())
        {
            /// A shadowed declaration still needs complete argument and chain validation.
            factory.get(stored, static_cast<const IDataType *>(nullptr));
            continue;
        }

        for (const auto & stream : stream_group_it->second)
        {
            ASTPtr effective_codec;
            if (declaration_is_part_default)
            {
                /// The part default is selected later and can change for a recompression TTL.
                effective_codec = stored;
            }
            else if (stream.structural)
            {
                auto instantiated = factory.get(stored, static_cast<const IDataType *>(nullptr), nullptr, /* only_generic = */ true);
                effective_codec = instantiated->getFullCodecDesc();
            }
            else
            {
                auto normalized = factory.validateCodecAndGetPreprocessedAST(ast, stream.leaf_type, declaration_settings);
                factory.get(normalized, stream.leaf_type);
                effective_codec = std::move(normalized);
            }

            result.effective_streams.push_back({
                .stream = stream,
                .declaration_path = declaration_path,
                .normalized_codec = std::move(effective_codec),
                .codec_is_part_default = declaration_is_part_default,
            });
        }
    }

    return result;
}

}

ColumnCodecValidationResult validateColumnCodecDescriptionAndGetStreams(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    return validatePolicy(policy, logical_type, settings, nullptr);
}

ColumnCodecValidationResult validateColumnCodecDescriptionForAlterAndGetStreams(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings)
{
    return validatePolicy(policy, logical_type, settings, &declarations_to_admit);
}

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    return validateColumnCodecDescriptionAndGetStreams(policy, logical_type, settings).codec;
}

ColumnCodecDescription validateColumnCodecDescriptionForAlter(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings)
{
    return validateColumnCodecDescriptionForAlterAndGetStreams(
        policy, logical_type, declarations_to_admit, settings).codec;
}

}
