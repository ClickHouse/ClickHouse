#include <Compression/CompressionPipeline.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

CompressionPipeline::CompressionPipeline()
{
    throw Exception("Pipeline could not be created without arguments", 0);
}

CompressionPipeline::CompressionPipeline(ReadBuffer *& header)
{
    const CompressionCodecFactory & codec_factory = CompressionCodecFactory::instance();
    char last_codec_bytecode;
    PODArray<char> _header;
    _header.resize(1);
    /// Read codecs, while continuation bit is set
    do {
        header->readStrict(&_header[0], 1);
        header_size += 1;

        last_codec_bytecode = (_header[0]) & ~static_cast<uint8_t>(CompressionMethodByte::CONT_BIT);
        auto _codec = codec_factory.get(last_codec_bytecode);

        _header.resize(_codec->getHeaderSize());
        header->readStrict(&_header[0], _codec->getHeaderSize());
        header_size += _codec->parseHeader(&_header[0]);
        codecs.push_back(_codec);
    }
    while (last_codec_bytecode & static_cast<uint8_t>(CompressionMethodByte::CONT_BIT));
    /// Load and reverse sizes part of a header, listed from later codecs to the original size, - see `compress`.
    auto codecs_amount = codecs.size();
    data_sizes.resize(codecs_amount + 1);

    _header.resize(sizeof(UInt32) * (codecs_amount + 1));
    header->readStrict(&_header[0], sizeof(UInt32) * (codecs_amount + 1));

    for (size_t i = 0; i <= codecs_amount; ++i)
    {
        data_sizes[codecs_amount - i] = unalignedLoad<UInt32>(&_header[sizeof(UInt32) * i]);
    }
}

PipePtr CompressionPipeline::get_pipe(ReadBuffer *& header)
{
    return std::make_shared<CompressionPipeline>(header);
}

PipePtr CompressionPipeline::get_pipe(ASTPtr& ast_codec)
{
    Codecs codecs;
    for (auto & codec : typeid_cast<IAST &>(*ast_codec).children) {
        codecs.emplace_back(std::move(CompressionCodecFactory::instance().get(codec)));
    }
    return std::make_shared<CompressionPipeline>(codecs);
}

PipePtr CompressionPipeline::get_pipe(String & full_declaration)
{
    ParserCodecDeclarationList parser;
    ASTPtr ast = parseQuery(parser, full_declaration.data(), full_declaration.data() + full_declaration.size(), "codecs", 0);
    // construct pipeline out of codecs
    Codecs codecs;
    for (auto & codec : typeid_cast<IAST &>(*ast).children) {
        codecs.emplace_back(std::move(CompressionCodecFactory::instance().get(codec)));
    }
    return std::make_shared<CompressionPipeline>(codecs);
}

};