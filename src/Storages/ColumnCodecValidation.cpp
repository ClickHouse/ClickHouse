#include <Storages/ColumnCodecValidation.h>

#include <Compression/CompressionFactory.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnCodecAST.h>

#include <Common/Exception.h>

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

/// Canonicalize every declaration and normalize it against the type it covers.
ColumnCodecDescription validatePolicy(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings,
    const ColumnCodecDescription::CodecsByPath * declarations_to_admit)
{
    if (policy.empty())
        return {};

    ColumnCodecDescription canonical_policy;
    for (const auto & [declaration_path, codec] : policy.getCodecs())
    {
        auto canonical_path = declaration_path.empty() ? CodecPath{} : canonicalizeCodecPath(logical_type, declaration_path);
        if (canonical_policy.getCodecs().contains(canonical_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for Tuple subcolumn");
        canonical_policy.set(std::move(canonical_path), codec);
    }

    ColumnCodecDescription result;
    const auto trusted_settings = CodecValidationSettings::trusted();
    auto & factory = CompressionCodecFactory::instance();

    for (const auto & [declaration_path, ast] : canonical_policy.getCodecs())
    {
        const bool use_session_settings = !declarations_to_admit || declarations_to_admit->contains(declaration_path);
        const auto & declaration_settings = use_session_settings ? settings : trusted_settings;

        /// A declaration is validated against the type it covers, the same way a whole-column codec
        /// is validated against the column type. Type-derived codec parameters are substituted only
        /// when every value stream of that type resolves them identically.
        const auto declaration_type = getCodecPathType(logical_type, declaration_path);
        result.set(declaration_path, factory.validateCodecAndGetPreprocessedAST(ast, declaration_type, declaration_settings));
    }

    return result;
}

}

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    return validatePolicy(policy, logical_type, settings, nullptr);
}

ColumnCodecDescription validateColumnCodecDescriptionForAlter(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings)
{
    return validatePolicy(policy, logical_type, settings, &declarations_to_admit);
}

}
