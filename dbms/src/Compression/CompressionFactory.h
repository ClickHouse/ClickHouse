#pragma once

#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>

#include <ext/singleton.h>

#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

using CodecNameWithLevel = std::pair<String, std::optional<int>>;

/** Creates a codec object by name of compression algorithm family and parameters.
 */
class CompressionCodecFactory final : public ext::singleton<CompressionCodecFactory>
{
protected:
    using Creator = std::function<CompressionCodecPtr(const ASTPtr & parameters)>;
    using CreatorWithType = std::function<CompressionCodecPtr(const ASTPtr & parameters, DataTypePtr column_type)>;
    using SimpleCreator = std::function<CompressionCodecPtr()>;
    using CompressionCodecsDictionary = std::unordered_map<String, CreatorWithType>;
    using CompressionCodecsCodeDictionary = std::unordered_map<UInt8, CreatorWithType>;
public:

    /// Return default codec (currently LZ4)
    CompressionCodecPtr getDefaultCodec() const;

    /// Get codec by AST and possible column_type
    /// some codecs can use information about type to improve inner settings
    /// but every codec should be able to work without information about type
    CompressionCodecPtr get(const ASTPtr & ast, DataTypePtr column_type = nullptr) const;

    /// Get codec by method byte (no params available)
    CompressionCodecPtr get(const UInt8 byte_code) const;

    /// For backward compatibility with config settings
    CompressionCodecPtr get(const String & family_name, std::optional<int> level) const;

    /// Register codec with parameters and column type
    void registerCompressionCodecWithType(const String & family_name, std::optional<UInt8> byte_code, CreatorWithType creator);
    /// Register codec with parameters
    void registerCompressionCodec(const String & family_name, std::optional<UInt8> byte_code, Creator creator);

    /// Register codec without parameters
    void registerSimpleCompressionCodec(const String & family_name, std::optional<UInt8> byte_code, SimpleCreator creator);

protected:
    CompressionCodecPtr getImpl(const String & family_name, const ASTPtr & arguments, DataTypePtr column_type) const;

private:
    CompressionCodecsDictionary family_name_with_codec;
    CompressionCodecsCodeDictionary family_code_with_codec;
    CompressionCodecPtr default_codec;

    CompressionCodecFactory();

    friend class ext::singleton<CompressionCodecFactory>;
};

}
