#include <Compression/CompressionPipeline.h>

namespace DB
{

PipePtr CompressionCodecFactory::get_pipe(ASTPtr& ast_codec) const
{
    Codecs codecs;
    for (auto & codec : typeid_cast<IAST &>(*ast_codec).children) {
        codecs.emplace_back(std::move(get(codec)));
    }
    return std::make_shared<CompressionPipeline>(codecs);
}

PipePtr CompressionCodecFactory::get_pipe(String & full_declaration) const
{
    ParserCodecDeclarationList parser;
    ASTPtr ast = parseQuery(parser, full_declaration.data(), full_declaration.data() + full_declaration.size(), "codecs", 0);
    // construct pipeline out of codecs
    Codecs codecs;
    for (auto & codec : typeid_cast<IAST &>(*ast).children) {
        codecs.emplace_back(std::move(get(codec)));
    }
    return std::make_shared<CompressionPipeline>(codecs);
}

PipePtr CompressionCodecFactory::get_pipe(ReadBuffer*& header) const
{
    return std::make_shared<CompressionPipeline>(header);
}

};