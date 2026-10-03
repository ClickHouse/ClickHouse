#include <Storages/ColumnCodecValidation.h>

#include <Compression/CompressionCodecQuantized.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

CodecStreamClassifier::CodecStreamClassifier(DataTypePtr owner_type_)
    : owner_type(std::move(owner_type_))
{
}

CodecStreamClassifier::CodecStreamClassifier(DataTypePtr owner_type_, const NameAndTypePair & written_column)
    : owner_type(std::move(owner_type_))
{
    if (!written_column.isSubcolumn())
        return;
    const auto info = owner_type->tryGetSubcolumnInfo(written_column.getSubcolumnName());
    if (!info)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot resolve subcolumn {} in type {}", written_column.getSubcolumnName(), owner_type->getName());
    written_column_substream_prefix = info->substreams_path;
}

std::optional<ApplicableCodecStream> CodecStreamClassifier::classify(const ISerialization::SubstreamPath & path) const
{
    /// A subcolumn serialization enumerates paths relative to that subcolumn. Always prepend
    /// the exact owner-to-subcolumn route; guessing from equal path entries is ambiguous for
    /// types such as Tuple(a Tuple(a UInt64)).
    ISerialization::SubstreamPath complete = written_column_substream_prefix;
    complete.insert(complete.end(), path.begin(), path.end());
    if (complete.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot classify an empty serialization stream path");
    /// A partial write can append the subcolumn type's own stream after the resolved
    /// owner-to-subcolumn prefix. In that form the ephemeral marker is not necessarily
    /// the final entry, so check every prefix rather than only the complete path.
    for (size_t prefix_size = 1; prefix_size <= complete.size(); ++prefix_size)
        if (ISerialization::isEphemeralSubcolumn(complete, prefix_size))
            return std::nullopt;

    TupleCodecPath logical_path;
    DataTypePtr logical_type = owner_type;
    DataTypePtr physical_route_type = owner_type;
    bool crossed_unsupported_branch = false;
    bool enters_map = false;
    for (const auto & entry : complete)
    {
        if (typeid_cast<const DataTypeMap *>(physical_route_type.get()))
            enters_map = true;

        /// Follow the actual serialization route separately from the public Tuple path.
        /// In particular, Nested is not a declaration-path wrapper, but we still need to
        /// notice a Map reached below it for lossy-codec validation.
        if (entry.type == ISerialization::Substream::ArrayElements)
        {
            if (const auto * array = typeid_cast<const DataTypeArray *>(physical_route_type.get()))
                physical_route_type = array->getNestedType();
        }
        else if (entry.type == ISerialization::Substream::NullableElements)
        {
            if (const auto * nullable = typeid_cast<const DataTypeNullable *>(physical_route_type.get()))
                physical_route_type = nullable->getNestedType();
        }
        else if (entry.type == ISerialization::Substream::TupleElement)
        {
            if (const auto * tuple = typeid_cast<const DataTypeTuple *>(physical_route_type.get()))
                if (const auto position = tuple->tryGetPositionByName(entry.name_of_substream))
                    physical_route_type = tuple->getElements()[*position];
        }

        if (entry.type == ISerialization::Substream::ArrayElements)
        {
            if (const auto * array = typeid_cast<const DataTypeArray *>(logical_type.get()); array && !isNested(logical_type))
                logical_type = array->getNestedType();
            else
                crossed_unsupported_branch = true;
        }
        else if (entry.type == ISerialization::Substream::NullableElements)
        {
            if (const auto * nullable = typeid_cast<const DataTypeNullable *>(logical_type.get()))
                logical_type = nullable->getNestedType();
        }
        else if (entry.type == ISerialization::Substream::TupleElement && !crossed_unsupported_branch)
        {
            const auto * tuple = typeid_cast<const DataTypeTuple *>(logical_type.get());
            if (!tuple)
            {
                /// Map is physically Array(Tuple(keys, values)). Those TupleElement markers are
                /// serialization details and must not become user-visible codec path components.
                crossed_unsupported_branch = true;
                continue;
            }

            const auto position = tuple->tryGetPositionByName(entry.name_of_substream);
            if (!position)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Serialization stream refers to unknown Tuple element {} in type {}",
                    entry.name_of_substream, tuple->getName());
            logical_path.push_back(tuple->getNameByPosition(*position + 1));
            logical_type = tuple->getElements()[*position];
        }
    }
    if (typeid_cast<const DataTypeMap *>(physical_route_type.get()))
        enters_map = true;

    return ApplicableCodecStream{
        .logical_path = std::move(logical_path),
        .leaf_type = complete.back().data.type,
        .structural = !ISerialization::isSpecialCompressionAllowed(complete),
        .enters_map = enters_map,
    };
}

namespace
{
using StreamsByDeclaration = std::map<TupleCodecPath, std::vector<ApplicableCodecStream>>;
}

NormalizedColumnCodecPolicy validateColumnCodecPolicy(
    const ColumnCodecPolicyView & policy,
    const DataTypePtr & owner_type,
    const CodecValidationSettings & settings,
    const CodecDeclarationPaths * declarations_requiring_full_validation)
{
    ASTPtr canonical_root = policy.getRoot();
    TupleElementCodecs canonical_elements;
    if (const auto * elements = policy.getElements())
    {
        for (const auto & [path, codec] : *elements)
        {
            if (path.empty() || !codec)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Tuple-element codec declarations require a non-empty path and codec");
            auto canonical_path = canonicalizeTupleCodecPath(owner_type, path);
            if (!canonical_elements.emplace(canonical_path, codec).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for Tuple element");
            if (tryExtractQuantizedCodecParams(codec))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Quantized codec on Tuple elements is not supported because its serialization is not path-aware");
        }
    }

    ColumnCodecPolicyView canonical_policy(canonical_root, &canonical_elements);
    StreamsByDeclaration streams_by_declaration;
    CodecStreamClassifier classifier(owner_type);
    owner_type->getDefaultSerialization()->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            const auto stream = classifier.classify(path);
            if (!stream)
                return;
            const auto declaration = canonical_policy.find(stream->logical_path);
            if (declaration)
                streams_by_declaration[declaration->declaration_path].push_back(*stream);
        }, owner_type);

    NormalizedColumnCodecPolicy result;
    auto & factory = CompressionCodecFactory::instance();
    const auto trusted = CodecValidationSettings::trusted();
    const auto validate_declaration = [&](const TupleCodecPath & declaration_path, const ASTPtr & ast) -> ASTPtr
    {
        const bool session_validation = !declarations_requiring_full_validation
            || declarations_requiring_full_validation->contains(declaration_path);
        const auto & declaration_settings = session_validation ? settings : trusted;
        const auto group_it = streams_by_declaration.find(declaration_path);
        ASTPtr common_normalized;
        std::optional<UInt64> common_hash;
        bool hashes_equal = true;
        bool has_value_stream = false;

        if (group_it != streams_by_declaration.end())
        {
            for (const auto & stream : group_it->second)
            {
                if (stream.structural)
                    continue;
                has_value_stream = true;
                auto normalized = factory.validateCodecAndGetPreprocessedAST(ast, stream.leaf_type, declaration_settings);
                auto codec = factory.get(normalized, stream.leaf_type);
                if (codec->isLossyCompression() && stream.enters_map)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lossy codec cannot be applied to a Map stream");
                const auto hash = codec->getHash();
                if (!common_normalized)
                {
                    common_normalized = normalized;
                    common_hash = hash;
                }
                else if (*common_hash != hash)
                    hashes_equal = false;
            }
        }

        if (!common_normalized)
        {
            /// Validate the complete chain and current-session sanity gates, but keep the
            /// original AST so a dormant type-dependent declaration remains symbolic.
            factory.validateCodecAndGetPreprocessedAST(ast, DataTypePtr{}, declaration_settings);
        }
        ASTPtr stored = has_value_stream && hashes_equal ? common_normalized : ast;

        if (group_it == streams_by_declaration.end())
        {
            factory.get(stored, static_cast<const IDataType *>(nullptr));
            return stored;
        }
        if (!CompressionCodecFactory::isDefaultCodec(ast))
        {
            for (const auto & stream : group_it->second)
            {
                if (stream.structural)
                    factory.get(stored, static_cast<const IDataType *>(nullptr), nullptr, /* only_generic = */ true);
                else
                {
                    auto normalized = factory.validateCodecAndGetPreprocessedAST(ast, stream.leaf_type, declaration_settings);
                    factory.get(normalized, stream.leaf_type);
                }
            }
        }
        return stored;
    };

    if (canonical_root)
        result.root = validate_declaration({}, canonical_root);
    for (const auto & [path, codec] : canonical_elements)
        result.elements.emplace(path, validate_declaration(path, codec));
    return result;
}

}
