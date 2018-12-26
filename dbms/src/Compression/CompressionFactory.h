#pragma once

#include <memory>
#include <functional>
#include <optional>
#include <unordered_map>
#include <ext/singleton.h>
#include <Common/IFactoryWithAliases.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

using CodecNameWithLevel = std::pair<String, std::optional<int>>;

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

    /// Return default codec (currently LZ4)
    CompressionCodecPtr getDefaultCodec() const;

    /// Get codec by AST
    CompressionCodecPtr get(const ASTPtr & ast) const;

    /// Get codec by method byte (no params available)
    CompressionCodecPtr get(const UInt8 byte_code) const;

    /// For backward compatibility with config settings
    CompressionCodecPtr get(const String & family_name, std::optional<int> level) const;

    CompressionCodecPtr get(const std::vector<CodecNameWithLevel> & codecs) const;
    /// Register codec with parameters
    void registerCompressionCodec(const String & family_name, std::optional<UInt8> byte_code, Creator creator);

    /// Register codec without parameters
    void registerSimpleCompressionCodec(const String & family_name, std::optional<UInt8> byte_code, SimpleCreator creator);

protected:
    CompressionCodecPtr getImpl(const String & family_name, const ASTPtr & arguments) const;

private:
    CompressionCodecsDictionary family_name_with_codec;
    CompressionCodecsCodeDictionary family_code_with_codec;
    CompressionCodecPtr default_codec;

    CompressionCodecFactory();

    friend class ext::singleton<CompressionCodecFactory>;
};

}
