#pragma once

#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>

#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

namespace DB
{

static constexpr auto DEFAULT_CODEC_NAME = "Default";

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

using CodecNameWithLevel = std::pair<String, std::optional<int>>;

/** Creates a codec object by name of compression algorithm family and parameters.
 */
class CompressionCodecFactory final : private boost::noncopyable
{
protected:
    using Creator = std::function<CompressionCodecPtr(const ASTPtr & parameters)>;
    using CreatorWithType = std::function<CompressionCodecPtr(const ASTPtr & parameters, const IDataType * column_type)>;
    using SimpleCreator = std::function<CompressionCodecPtr()>;
    using CompressionCodecsDictionary = std::unordered_map<String, CreatorWithType>;
    using CompressionCodecsCodeDictionary = std::unordered_map<uint8_t, CreatorWithType>;
public:

    static CompressionCodecFactory & instance();

    /// Return default codec (currently LZ4)
    CompressionCodecPtr getDefaultCodec() const;

    /// Validate codecs AST specified by user and parses codecs description (substitute default parameters)
    ASTPtr validateCodecAndGetPreprocessedAST(const ASTPtr & ast, const DataTypePtr & column_type, bool sanity_check, bool allow_experimental_codecs, bool enable_zstd_qat_codec) const;

    /// Validate codecs AST specified by user
    void validateCodec(const String & family_name, std::optional<int> level, bool sanity_check, bool allow_experimental_codecs, bool enable_zstd_qat_codec) const;

    /// Get codec by AST and possible column_type. Some codecs can use
    /// information about type to improve inner settings, but every codec should
    /// be able to work without information about type. Also AST can contain
    /// codec, which can be alias to current default codec, which can be changed
    /// in runtime. If only_generic is true than method will filter all
    /// isGenericCompression() == false codecs from result. If nothing found
    /// will return codec NONE. It's useful for auxiliary parts of complex columns
    /// like Nullable, Array and so on. If all codecs are non generic and
    /// only_generic = true, than codec NONE will be returned.
    CompressionCodecPtr get(const ASTPtr & ast, const IDataType * column_type, CompressionCodecPtr current_default = nullptr, bool only_generic = false) const;

    /// Just wrapper for previous method.
    CompressionCodecPtr get(const ASTPtr & ast, const DataTypePtr & column_type, CompressionCodecPtr current_default = nullptr, bool only_generic = false) const
    {
        return get(ast, column_type.get(), current_default, only_generic);
    }

    /// Get codec by method byte (no params available)
    CompressionCodecPtr get(uint8_t byte_code) const;

    /// For backward compatibility with config settings
    CompressionCodecPtr get(const String & family_name, std::optional<int> level) const;

    /// Get codec by name with optional params. Example: LZ4, ZSTD(3)
    CompressionCodecPtr get(const String & compression_codec) const;

    /// Register codec with parameters and column type
    void registerCompressionCodecWithType(const String & family_name, std::optional<uint8_t> byte_code, CreatorWithType creator);
    /// Register codec with parameters
    void registerCompressionCodec(const String & family_name, std::optional<uint8_t> byte_code, Creator creator);

    /// Register codec without parameters
    void registerSimpleCompressionCodec(const String & family_name, std::optional<uint8_t> byte_code, SimpleCreator creator);

protected:
    CompressionCodecPtr getImpl(const String & family_name, const ASTPtr & arguments, const IDataType * column_type) const;

private:
    CompressionCodecsDictionary family_name_with_codec;
    CompressionCodecsCodeDictionary family_code_with_codec;
    CompressionCodecPtr default_codec;

    CompressionCodecFactory();
};

}
