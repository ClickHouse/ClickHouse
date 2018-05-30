#include <IO/ReadBuffer.h>
#include <IO/CompressedStream.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionPipeline.h>
#include <Compression/CompressionCodecFactory.h>

#include <DataTypes/IDataType.h>
#include <iostream>


namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

CompressionPipeline::CompressionPipeline(ReadBuffer* header)
{
    const CompressionCodecFactory & codec_factory = CompressionCodecFactory::instance();
    char last_codec_bytecode, last_bytecode;
    PODArray<char> _header;
    /// Read codecs, while continuation bit is set
    do {
        _header.resize(1);
        header->readStrict(&_header[0], 1);
        header_size += 1;

        last_bytecode = _header[0];
        last_codec_bytecode = last_bytecode & ~static_cast<uint8_t>(CompressionMethodByte::CONTINUATION_BIT);
        auto _codec = codec_factory.get(last_codec_bytecode);

        if (_codec->getHeaderSize())
        {
            _header.resize(_codec->getHeaderSize());
            header->readStrict(&_header[0], _codec->getHeaderSize());
            header_size += _codec->parseHeader(&_header[0]);
        }
        codecs.push_back(_codec);
    }
    while (last_bytecode & static_cast<uint8_t>(CompressionMethodByte::CONTINUATION_BIT));
    /// Load and reverse sizes part of a header, listed from later codecs to the original size, - see `compress`.
    auto codecs_amount = codecs.size();
    data_sizes.resize(codecs_amount + 1);

    header_size += sizeof(UInt32) * (codecs_amount + 1);
    _header.resize(sizeof(UInt32) * (codecs_amount + 1));
    header->readStrict(&_header[0], sizeof(UInt32) * (codecs_amount + 1));

    for (size_t i = 0; i <= codecs_amount; ++i)
        data_sizes[codecs_amount - i] = unalignedLoad<UInt32>(&_header[sizeof(UInt32) * i]);
    data_sizes[codecs_amount] -= getHeaderSize(); /// remove header size from last data size

    if (header_size != getHeaderSize())
        throw("Incorrect header read size: " + std::to_string(header_size) + ", expected " +
              std::to_string(getHeaderSize()), ErrorCodes::LOGICAL_ERROR);
}

CompressionPipePtr CompressionPipeline::get_pipe(ReadBuffer* header)
{
    return std::make_shared<CompressionPipeline>(header);
}

CompressionPipePtr CompressionPipeline::get_pipe(ASTPtr& ast_codec)
{
    Codecs codecs;
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_codec).children;

    if (args_func.size() != 1)
        throw Exception("Codecs pipeline definition must have parameters.", ErrorCodes::LOGICAL_ERROR);

    ASTs & ast_codecs = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;
    for (auto & codec : ast_codecs) {
        codecs.emplace_back(std::move(CompressionCodecFactory::instance().get(codec)));
    }
    auto tmp = std::make_shared<CompressionPipeline>(codecs);
    tmp->codec_ptr = ast_codec;
    return tmp;
}

CompressionPipePtr CompressionPipeline::get_pipe(const String & full_declaration)
{
    Codecs codecs;
    ParserCodecDeclarationList codecs_parser;
    ASTPtr ast = parseQuery(codecs_parser, full_declaration.data(),
                            full_declaration.data() + full_declaration.size(),
                            "codecs", 0);
    ASTs & ast_codecs = typeid_cast<ASTExpressionList &>(*ast).children;
    for (auto & codec : ast_codecs) {
        codecs.emplace_back(std::move(CompressionCodecFactory::instance().get(codec)));
    }
    return std::make_shared<CompressionPipeline>(codecs);
}

String CompressionPipeline::getName() const
{
    String name("CODEC(");
    bool first = true;
    for (auto & codec: codecs)
        name += first ? codec->getName() : ", " + codec->getName();
    name += ")";
    return name;
}

const char * CompressionPipeline::getFamilyName() const
{
    return "CODEC";
}

size_t CompressionPipeline::getCompressedSize() const
{
    return data_sizes.back();
}

size_t CompressionPipeline::getDecompressedSize() const
{
    return data_sizes.front();
}

size_t CompressionPipeline::writeHeader(char* out)
{
    size_t wrote_size = 0;
    for (size_t i = 0; i < codecs.size(); ++i)
    {
        auto wrote = codecs[i]->writeHeader(out);
        if (i != codecs.size() - 1)
            *out |= static_cast<uint8_t>(CompressionMethodByte::CONTINUATION_BIT);
        out += wrote;
        wrote_size += wrote;
    }
    for (int32_t i = data_sizes.size() - 1; i >= 0; --i)
    {
        if (i == (int32_t)(codecs.size()))
        {
            uint32_t compressed_size = data_sizes[i] + getHeaderSize();
            unalignedStore(&out[sizeof(uint32_t) * (codecs.size() - i)], const_cast<uint32_t&>(compressed_size));
        }
        else
            unalignedStore(&out[sizeof(uint32_t) * (codecs.size() - i)], data_sizes[i]);
    }
    wrote_size += sizeof(uint32_t) * data_sizes.size();
    return wrote_size;
}

size_t CompressionPipeline::getHeaderSize() const
{
    /// Header size as sum of codecs headers' sizes
    size_t _hs = sizeof(UInt32); /// decompressed size
    for (auto &codec : codecs)
        ///    bytecode  + arguments part + data size part
        _hs += 1 + codec->getHeaderSize() + sizeof(UInt32);
    return _hs;
}

size_t CompressionPipeline::getMaxCompressedSize(size_t uncompressed_size) const
{
    return codecs[0]->getMaxCompressedSize(uncompressed_size);
}

size_t CompressionPipeline::compress(char *source, PODArray<char> &dest, size_t inputSize, size_t maxOutputSize)
{
    data_sizes.clear();
    data_sizes.resize(codecs.size() + 1);
    data_sizes[0] = inputSize;
    char *_source = source;
    auto *_dest = &buffer1;

    for (size_t i = 0; i < codecs.size(); ++i)
    {
        (*_dest).resize(maxOutputSize);
        inputSize = codecs[i]->compress(_source, &(*_dest)[0], inputSize, maxOutputSize);

        _source = &(*_dest)[0];
        _dest = _dest == &dest ? &buffer1: &dest;
        maxOutputSize = i + 1 < codecs.size() ? codecs[i + 1]->getMaxCompressedSize(inputSize) : inputSize;
        data_sizes[i + 1] = maxOutputSize;
    }

    if (_dest != &dest)
    {
        buffer1.resize(inputSize);
        memcpy(&buffer1[0], &dest[0], inputSize);
    }
    /// Write header data
    dest.resize(getHeaderSize() + inputSize);
    size_t header_wrote_size = writeHeader(&dest[0]);

    if (getHeaderSize() != header_wrote_size)
        throw Exception("Bad header formatting", ErrorCodes::LOGICAL_ERROR);

    memcpy(&dest[header_wrote_size], &buffer1[0], maxOutputSize);

    return maxOutputSize + header_wrote_size;
}

size_t CompressionPipeline::decompress(char *source, char *dest, size_t inputSize, size_t outputSize)
{
    char *_source = source;
    auto *_dest = &buffer1;
    size_t midOutputSize = 0;
    for (int i = codecs.size() - 1; i >= 0; --i) {
        midOutputSize = data_sizes[i];
        if (!i) /// output would be dest
        {
            inputSize = codecs[i]->decompress(_source, dest, inputSize, midOutputSize);
        }
        else
        {
            (*_dest).resize(midOutputSize);
            inputSize = codecs[i]->decompress(_source, &(*_dest)[0], inputSize, midOutputSize);
            _source = &(*_dest)[0];
            _dest = _dest == &buffer1 ? &buffer2 : &buffer1;
        }
    }

    if (midOutputSize != outputSize)
        throw Exception("Decoding problem", ErrorCodes::LOGICAL_ERROR);

    return inputSize;
}

void CompressionPipeline::setDataType(DataTypePtr _data_type)
{
    data_type = _data_type;
    for (auto & codec: codecs)
    {
        codec->setDataType(data_type);
    }
}

std::vector<UInt32> CompressionPipeline::getDataSizes() const
{
    auto ds(data_sizes);
    return ds;
}

};