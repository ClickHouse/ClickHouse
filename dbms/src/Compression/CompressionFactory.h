#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <ext/singleton.h>
#include <Common/IFactoryWithAliases.h>
#include <Compression/ICompressionCodec.h>
#include <IO/CompressedStream.h>
#include <IO/CompressionSettings.h>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

class IAST;

using ASTPtr = std::shared_ptr<IAST>;

/** Creates a codec object by name of compression algorithm family and parameters.
 */
class CompressionCodecFactory final : public ext::singleton<CompressionCodecFactory>
{
protected:
    using Creator = std::function<CompressionCodecPtr(const ASTPtr & parameters)>;
    using SimpleCreator = std::function<CompressionCodecPtr()>;
    using CompressionCodecsDictionary = std::unordered_map<String, Creator>;
    using CompressionCodecsCodeDictionary = std::unordered_map<UInt8, Creator>;
public:

    CompressionCodecPtr getDefaultCodec() const;

    CompressionCodecPtr get(const ASTPtr & ast) const;

    CompressionCodecPtr get(const UInt8 byte_code) const;

    CompressionCodecPtr get(const CompressionSettings & settings) const;

    void registerCompressionCodec(const String & family_name, UInt8 byte_code, Creator creator);

    void registerSimpleCompressionCodec(const String & family_name, UInt8 byte_code, SimpleCreator creator);

protected:
    CompressionCodecPtr getImpl(const String & family_name, const ASTPtr & arguments) const;

private:
    CompressionCodecsDictionary family_name_with_codec;
    CompressionCodecsCodeDictionary family_code_with_codec;
    CompressionCodecPtr default_codec;

    ASTPtr convertSettingsToAst(const CompressionSettings & settings) const;

    CompressionCodecFactory();

    friend class ext::singleton<CompressionCodecFactory>;
};

}
