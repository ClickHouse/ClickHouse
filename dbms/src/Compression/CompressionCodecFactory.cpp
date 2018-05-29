#include <Compression/CompressionCodecFactory.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>

#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_CODEC;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int CODEC_CANNOT_HAVE_ARGUMENTS;
}

CodecPtr CompressionCodecFactory::get(char& bytecode) const
{

    {
        auto it = bytecodes_codecs.find(bytecode);
        if (bytecodes_codecs.end() != it)
            return it->second();
    }

    throw Exception("Unknown codec bytecode: " + std::to_string(bytecode), ErrorCodes::UNKNOWN_CODEC);
}

CodecPtr CompressionCodecFactory::get(const String & full_name) const
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "codec", 0);
    return get(ast);
}

CodecPtr CompressionCodecFactory::get(const ASTPtr & ast) const
{
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(ast.get()))
    {
        if (func->parameters)
            throw Exception("Codec cannot have multiple parenthesed parameters.", ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
        return get(func->name, func->arguments);
    }

    if (const ASTIdentifier * ident = typeid_cast<const ASTIdentifier *>(ast.get()))
    {
        return get(ident->name, {});
    }

    if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(ast.get()))
    {
        if (lit->value.isNull())
            return get("None", {});
    }

    throw Exception("Unexpected AST element for compression codec.", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

CodecPtr CompressionCodecFactory::get(const String & family_name, const ASTPtr & parameters) const
{

    {
        String family_name_lowercase = Poco::toLower(family_name);
        CodecsDictionary::const_iterator it = codecs.find(family_name_lowercase);
        if (codecs.end() != it)
            return it->second(parameters);
    }

    throw Exception("Unknown codec family: " + family_name, ErrorCodes::UNKNOWN_CODEC);
}

void CompressionCodecFactory::registerCodec(const String & family_name, Creator creator)
{
    if (creator == nullptr)
        throw Exception("CompressionCodecFactory: the codec family " + family_name + " has been provided "
                " a null constructor", ErrorCodes::LOGICAL_ERROR);

    String family_name_lowercase = Poco::toLower(family_name);

    if (!codecs.emplace(family_name_lowercase, creator).second)
        throw Exception("CompressionCodecFactory: the codec family name '" + family_name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}


void CompressionCodecFactory::registerSimpleCodec(const String & name, SimpleCreator creator)
{
    if (creator == nullptr)
        throw Exception("CompressionCodecFactory: the codec " + name + " has been provided "
                " a null constructor", ErrorCodes::LOGICAL_ERROR);

    registerCodec(name, [name, creator](const ASTPtr & ast)
    {
        if (ast)
            throw Exception("Codec type " + name + " cannot have arguments", ErrorCodes::CODEC_CANNOT_HAVE_ARGUMENTS);
        return creator();
    });
}

void CompressionCodecFactory::registerCodecBytecode(const char& bytecode, SimpleCreator creator)
{
    if (creator == nullptr)
        throw Exception("CompressionCodecFactory: the codec has been provided a null constructor",
                        ErrorCodes::LOGICAL_ERROR); // TODO: add bytecode to exception

    if (!bytecodes_codecs.emplace(bytecode, creator).second)
        throw Exception("CompressionCodecFactory: the codec bytecode is not unique",
                        ErrorCodes::LOGICAL_ERROR); // TODO: add bytecode to exception
}

void registerCodecNone(CompressionCodecFactory & factory);
void registerCodecLZ4(CompressionCodecFactory & factory);
void registerCodecLZ4HC(CompressionCodecFactory & factory);
void registerCodecZSTD(CompressionCodecFactory & factory);
void registerCodecDelta(CompressionCodecFactory & factory);

CompressionCodecFactory::CompressionCodecFactory()
{
    registerCodecNone(*this);
    registerCodecLZ4(*this);
    registerCodecLZ4HC(*this);
    registerCodecZSTD(*this);
    registerCodecDelta(*this);
}

}
