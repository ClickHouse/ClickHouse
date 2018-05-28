#include <IO/ReadBuffer.h>
#include <IO/CompressedStream.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionPipeline.h>
#include <Compression/CompressionCodecFactory.h>

#include <DataTypes/IDataType.h>


namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

CompressionPipeline::CompressionPipeline(ReadBuffer* header)
{
    const CompressionCodecFactory & codec_factory = CompressionCodecFactory::instance();
    char last_codec_bytecode;
    PODArray<char> _header;
    /// Read codecs, while continuation bit is set
    do {
        _header.resize(1);
        header->readStrict(&_header[0], 1);
        header_size += 1;

        last_codec_bytecode = _header[0] & ~static_cast<uint8_t>(CompressionMethodByte::CONTINUATION_BIT);
        auto _codec = codec_factory.get(last_codec_bytecode);

        if (_codec->getHeaderSize())
        {
            _header.resize(_codec->getHeaderSize());
            header->readStrict(&_header[0], _codec->getHeaderSize());
            header_size += _codec->parseHeader(&_header[0]);
        }
        codecs.push_back(_codec);
    }
    while (last_codec_bytecode & static_cast<uint8_t>(CompressionMethodByte::CONTINUATION_BIT));
    /// Load and reverse sizes part of a header, listed from later codecs to the original size, - see `compress`.
    auto codecs_amount = codecs.size();
    data_sizes.resize(codecs_amount + 1);

    header_size += sizeof(UInt32) * (codecs_amount + 1);
    _header.resize(sizeof(UInt32) * (codecs_amount + 1));
    header->readStrict(&_header[0], sizeof(UInt32) * (codecs_amount + 1));

    for (size_t i = 0; i <= codecs_amount; ++i)
        data_sizes[codecs_amount - i] = unalignedLoad<UInt32>(&_header[sizeof(UInt32) * i]);
}

PipePtr CompressionPipeline::get_pipe(ReadBuffer* header)
{
    return std::make_shared<CompressionPipeline>(header);
}

PipePtr CompressionPipeline::get_pipe(ASTPtr& ast_codec)
{
    Codecs codecs;
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_codec).children;

    if (args_func.size() != 1)
        throw Exception("Codecs pipeline definition must have parameters.", ErrorCodes::LOGICAL_ERROR);

    ASTs & ast_codecs = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;
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
    for (int i = codecs.size() - 1; i >= 0; --i)
    {
        auto wrote = codecs[i]->writeHeader(out);
        *out |= i ? static_cast<char>(CompressionMethodByte::CONTINUATION_BIT) : 0;
        out += wrote;
        wrote_size += wrote;
    }
    for (int32_t i = data_sizes.size() - 1; i >= 0; --i)
    {
        if (i == (int32_t)(data_sizes.size() - 1))
            unalignedStore(&out[sizeof(uint32_t) * i], data_sizes[i] + getHeaderSize());
        else
            unalignedStore(&out[sizeof(uint32_t) * i], data_sizes[i]);
        wrote_size += sizeof(uint32_t) * i;
    }
    return wrote_size;
}

size_t CompressionPipeline::getHeaderSize() const
{
    /// Header size as sum of codecs headers' sizes
    if (!header_size)
    {
        size_t _hs = 0;
        for (auto &codec : codecs)
            _hs += codec->getHeaderSize();
        return _hs;
    }

    return header_size;
}


size_t CompressionPipeline::getMaxCompressedSize(size_t uncompressed_size) const
{
    return codecs[0]->getMaxCompressedSize(uncompressed_size);
}

size_t CompressionPipeline::compress(char* source, PODArray<char>& dest, int inputSize, int maxOutputSize)
{
    PODArray<char> buffer;
    char *_source = source;
    auto *_dest = &dest;

    for (size_t i = 0; i < codecs.size(); ++i)
    {
        (*_dest).resize(maxOutputSize);
        inputSize = codecs[i]->compress(_source, &(*_dest)[0], inputSize, maxOutputSize);

        _source = &(*_dest)[0];
        _dest = _dest == &dest ? &buffer: &dest;
        data_sizes.push_back(inputSize);
        maxOutputSize = i + 1 < codecs.size() ? codecs[i + 1]->getMaxCompressedSize(inputSize) : inputSize;
    }

    if (_dest != &dest)
    {
        dest.resize(maxOutputSize);
        memcpy(&dest[0], &buffer[0], maxOutputSize);
    }
    /// Write header data
    buffer.resize(header_size);
    writeHeader(&buffer[0]);
    dest.resize(maxOutputSize + header_size);
    dest.insert(dest.begin(), buffer.begin(), buffer.end());

    return maxOutputSize + header_size;
}

size_t CompressionPipeline::decompress(char* source, char* dest, int inputSize, int)
{
    PODArray<char> buffer1, buffer2;
    char *_source = source;
    size_t midOutputSize = data_sizes[codecs.size() - 1];
    auto *_dest = &buffer1;
    for (int i = codecs.size() - 1; i >= 0; --i) {
        if (!i) /// output would be dest
        {
            inputSize = codecs[i]->decompress(_source, dest, inputSize, getDecompressedSize());
        } else {
            (*_dest).resize(midOutputSize);
            inputSize = codecs[i]->decompress(_source, &(*_dest)[0], inputSize, midOutputSize);
            _source = &(*_dest)[0];
            _dest = _dest == &buffer1 ? &buffer2 : &buffer1;
            midOutputSize = data_sizes[i - 1];
        }
    }
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

};